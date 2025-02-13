use anyhow::bail;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::get,
    Router,
};
use axum_extra::headers;
use axum_extra::headers::HeaderMapExt;
use caches::{DBCache, DeltaCache};
use std::{io::SeekFrom, panic, path::PathBuf, str::FromStr, sync::Arc, time::Duration};
use tokio::{fs::File, io::AsyncSeekExt, sync::Mutex, time::Instant};
use tokio_util::io::ReaderStream;
use tracing::{debug, error, info, trace, Instrument};

use async_file_cache::FileCache;

use parsing::{Delta, Package};

use std::sync::OnceLock;

static MIRROR: OnceLock<Str> = OnceLock::new();
static FALLBACK_MIRROR: OnceLock<Str> = OnceLock::new();
static CACHE_DIR: OnceLock<Str> = OnceLock::new();

type Str = Box<str>;

use reqwest::Client;

mod caches;

pub fn get_pkg_path() -> PathBuf {
    let mut basepath = PathBuf::from(CACHE_DIR.get().expect("initialized").as_ref());
    basepath.push("pkg");
    basepath
}

pub fn get_delta_path() -> PathBuf {
    let mut basepath = PathBuf::from(CACHE_DIR.get().expect("initialized").as_ref());
    basepath.push("delta");
    basepath
}

pub fn get_db_path() -> PathBuf {
    let mut basepath = PathBuf::from(CACHE_DIR.get().expect("initialized").as_ref());
    basepath.push("db");
    basepath
}
fn main() {
    tracing_subscriber::fmt::init();
    fn get_env_or_fallback<S: Into<Str>>(key: &str, fallback: S) -> Str {
        std::env::var(key).map(String::into_boxed_str).unwrap_or_else(|_e| {
            let fallback: Str = fallback.into();
            info!(fallback = fallback, "no {key} set, using fallback");
            fallback
        })
    }

    MIRROR
        .set(get_env_or_fallback("MIRROR", "http://mirror.moson.org/arch/"))
        .expect("init only once");
    FALLBACK_MIRROR
        .set(get_env_or_fallback(
            "FALLBACK_MIRROR",
            "https://europe.archive.pkgbuild.com/packages/.all/",
        ))
        .expect("init only once");
    CACHE_DIR
        .set(get_env_or_fallback("CACHE_DIR", "./deltaserver"))
        .expect("init only once");

    std::fs::create_dir_all(get_pkg_path()).unwrap();
    std::fs::create_dir_all(get_delta_path()).unwrap();
    std::fs::create_dir_all(get_db_path()).unwrap();

    let client = Client::new();

    // max 3 parallel downloads from mirrors
    let package_cache = FileCache::new(caches::PackageCache(client.clone()), 3.into());

    let parallel = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    trace!(parallel = parallel, "cpu parallelity");

    let delta_cache = FileCache::new(caches::DeltaCache(package_cache), parallel.into());
    let delta_cache = delta_cache;

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 3000));
            let last_activity;
            let listener = if let Some(listener) = listenfd::ListenFd::from_env().take_tcp_listener(0).ok().flatten() {
                info!("listening on {:?}", listener);
                last_activity = Some(Arc::new(Mutex::new(Instant::now())));
                tokio::net::TcpListener::from_std(listener).unwrap()
            } else {
                info!("binding to {:?}", addr);
                last_activity = None;
                tokio::net::TcpListener::bind(addr).await.unwrap()
            };

            let state = AppState {
                pkg: delta_cache.into(),
                core: DBCache::new("core".into(), client.clone()).unwrap().into(),
                extra: DBCache::new("extra".into(), client.clone()).unwrap().into(),
                multilib: DBCache::new("multilib".into(), client.clone()).unwrap().into(),
                last_activity: last_activity.clone(),
            };
            let app = Router::new()
                .fallback(fallback)
                .route("/", get(root))
                .route("/arch/{from}/{to}", get(gen_delta))
                .route("/archdb/{name}", get(db))
                .route("/archdb/{name}/{old}", get(dbdelta))
                .with_state(state);

            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown(last_activity))
                .await
                .unwrap();
        })
}

async fn shutdown(active: Option<Arc<Mutex<Instant>>>) {
    use tokio::signal::unix;
    let terminate = async {
        unix::signal(unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };
    if let Some(active) = active {
        let timeout = async {
            loop {
                let deadline = active.lock().await.checked_add(Duration::from_secs(11 * 60)).unwrap();
                tokio::time::sleep_until(deadline).await;
                if Instant::now().duration_since(*active.lock().await) > Duration::from_secs(10 * 60) {
                    info!("no activity since {active:?}, shutting down");
                    break;
                }
            }
        };
        tokio::select! {
            _ = terminate => (),
            _ = timeout => ()
        };
    } else {
        terminate.await;
    };
}

#[derive(Clone)]
struct AppState {
    pkg: Arc<FileCache<DeltaCache>>,
    core: Arc<DBCache>,
    extra: Arc<DBCache>,
    multilib: Arc<DBCache>,
    //TODO: replace by atomic
    last_activity: Option<Arc<Mutex<Instant>>>,
}

type RangeFile = axum_range::RangedStream<axum_range::KnownSize<File>>;

#[tracing::instrument(level = "debug", skip(s), "DBDL")]
async fn db(
    State(s): State<AppState>,
    Path(name): Path<Str>,
) -> Result<(StatusCode, HeaderMap, Body), (StatusCode, String)> {
    debug!("entering db fn with {}", name);

    let db = match name.as_ref() {
        "core" => s.core,
        "extra" => s.extra,
        "multilib" => s.multilib,
        _ => {
            return Err((
                StatusCode::FORBIDDEN,
                "currently only core, extra and multilib are supported.".into(),
            ))
        }
    };
    if let Some(activity) = s.last_activity {
        let mut activity = activity.lock().await;
        *activity = Instant::now();
    };

    info!(name = name, "getting");

    let mut h = HeaderMap::new();
    h.typed_insert(headers::ContentType::from_str("application/octet-stream").unwrap());
    info!(name = name, "fulldb requested for",);
    let (stamp, mut file) = db.get_newest_db().await.expect("db download failed");
    h.insert(
        axum::http::header::CONTENT_DISPOSITION,
        axum::http::HeaderValue::from_str(format!("attachment; filename=\"{}-{}\"", name, stamp).as_str()).unwrap(),
    );
    let len = file.seek(SeekFrom::End(0)).await.unwrap();
    file.rewind().await.unwrap();
    h.typed_insert(headers::ContentLength(len));
    let body = Body::from_stream(ReaderStream::new(file));
    Ok((StatusCode::OK, h, body))
}

#[tracing::instrument(level = "debug", skip(s), "DBDelta")]
async fn dbdelta(
    State(s): State<AppState>,
    Path((name, old)): Path<(Str, u64)>,
) -> Result<(StatusCode, HeaderMap, Body), (StatusCode, String)> {
    //TODO: handle range request for 0 bytes with 0 byte response
    debug!("entering db fn with {} {:?}", name, old);

    let db = match name.as_ref() {
        "core" => s.core,
        "extra" => s.extra,
        "multilib" => s.multilib,
        _ => {
            return Err((
                StatusCode::FORBIDDEN,
                "currently only core, extra and multilib are supported.".into(),
            ))
        }
    };
    if let Some(activity) = s.last_activity {
        let mut activity = activity.lock().await;
        *activity = Instant::now();
    };

    info!(name = name, "getting");

    let mut h = HeaderMap::new();
    h.typed_insert(headers::ContentType::from_str("application/octet-stream").unwrap());
    info!(name = name, old = old, "dbdelta requested for",);
    match db.get_delta_to(old).await {
        Ok((stamp, patch)) => {
            h.insert(
                axum::http::header::CONTENT_DISPOSITION,
                axum::http::HeaderValue::from_str(
                    format!("attachment; filename=\"{}-{}-{}\"", name, old, stamp).as_str(),
                )
                .unwrap(),
            );

            let body = Body::from_stream(ReaderStream::new(patch));
            Ok((StatusCode::OK, h, body))
        }
        Err(caches::DeltaError::Identical) => Err((StatusCode::NOT_MODIFIED, "{name}-{ts} is up-to-date".into())),
        //TODO the old version the client has may have already been expunged, that is a somewhat common case so better handle it gracefully.
        Err(e) => panic!("db delta generation failed: {e}"),
    }
}

async fn gen_delta(
    State(s): State<AppState>,
    Path((from, to)): Path<(Str, Str)>,
    range: Option<axum_extra::TypedHeader<axum_extra::headers::Range>>,
) -> Result<(StatusCode, HeaderMap, RangeFile), (StatusCode, String)> {
    if let Some(activity) = s.last_activity {
        let mut activity = activity.lock().await;
        *activity = Instant::now();
    };
    let s = s.pkg.clone();
    let c_span = tracing::info_span!("delta request", from, to);
    let c = || async {
        let from = Package::try_from(&*from)?;
        let to = Package::try_from(&*to)?;
        if strsim::levenshtein(from.get_name(), to.get_name()) > 3 {
            bail!("packages are too different")
        }
        let delta: Delta = (from, to).try_into()?;

        let file = {
            let delta = delta.clone();
            // spawning is done here so the generation continues even if the original request times out or is cancled
            match tokio::spawn(async move { s.get_or_generate(delta).await })
                .await
                .map_err(|e| e.try_into_panic())
            {
                Err(Ok(p)) => panic::resume_unwind(p),
                Err(Err(_)) => unreachable!("this task does not get cancled"),
                Ok(r) => r??,
            }
        };

        let body = axum_range::KnownSize::file(file).await?;
        let range = range.map(|axum_extra::TypedHeader(range)| range);
        let is_range_req = range.is_some();
        debug!("range: {range:?} for {delta:?}");
        let resp = axum_range::Ranged::new(range, body).try_respond().unwrap();

        let mut h = HeaderMap::new();
        h.typed_insert(resp.content_length);
        if let Some(range) = resp.content_range {
            h.typed_insert(range)
        };
        h.typed_insert(headers::ContentType::from_str("application/octet-stream").unwrap());
        h.insert(
            axum::http::header::CONTENT_DISPOSITION,
            axum::http::HeaderValue::from_str(format!("attachment; filename=\"{}\"", delta).as_str()).unwrap(),
        );
        let status = if is_range_req {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        anyhow::Ok((status, h, resp.stream))
    };
    c().instrument(c_span).await.map_err(|e| {
        error!("{:?}", e);
        (
            axum::http::status::StatusCode::INTERNAL_SERVER_ERROR,
            format!("error producing delta: {}", e),
        )
    })
}

async fn root() -> (StatusCode, &'static str) {
    (StatusCode::OK, "welcome to the inofficial archlinux delta server")
}

async fn fallback() -> (StatusCode, &'static str) {
    (StatusCode::NOT_FOUND, "Page could not be found")
}
