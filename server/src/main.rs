use anyhow::bail;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::get,
    Router,
};
use axum_extra::headers::HeaderMapExt;
use caches::DeltaCache;
use std::{panic, path::PathBuf, str::FromStr, sync::Arc};
use tokio::fs::File;
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
        .set(get_env_or_fallback(
            "MIRROR",
            "http://mirror.moson.org/arch/pool/packages/",
        ))
        .expect("init only once");
    FALLBACK_MIRROR
        .set(get_env_or_fallback(
            "FALLBACK_MIRROR",
            "http://europe.archive.pkgbuild.com/packages/.all/",
        ))
        .expect("init only once");
    CACHE_DIR
        .set(get_env_or_fallback("CACHE_DIR", "./deltaserver"))
        .expect("init only once");

    std::fs::create_dir_all(get_pkg_path()).unwrap();
    std::fs::create_dir_all(get_delta_path()).unwrap();

    // max 3 parallel downloads from mirrors
    let package_cache = FileCache::new(caches::PackageCache(Client::new()), 3.into());

    let parallel = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    trace!(parallel = parallel, "cpu parallelity");

    let delta_cache = FileCache::new(caches::DeltaCache(package_cache), parallel.into());
    let delta_cache = Arc::new(delta_cache);

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let app = Router::new()
                .fallback(fallback)
                .route("/", get(root))
                .route("/arch/:from/:to", get(gen_delta))
                .with_state(delta_cache);

            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 3000));
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        })
}

async fn gen_delta(
    State(s): State<Arc<FileCache<DeltaCache>>>,
    Path((from, to)): Path<(Str, Str)>,
    range: Option<axum_extra::TypedHeader<axum_extra::headers::Range>>,
) -> Result<
    (
        StatusCode,
        HeaderMap,
        axum_range::RangedStream<axum_range::KnownSize<File>>,
    ),
    (StatusCode, String),
> {
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

        //let len = file.metadata().await?.len();
        let body = axum_range::KnownSize::file(file).await?;
        let range = range.map(|axum_extra::TypedHeader(range)| range);
        debug!("range: {range:?} for {delta:?}");
        let resp = axum_range::Ranged::new(range, body).try_respond().unwrap();

        let mut h = HeaderMap::new();
        h.typed_insert(resp.content_length);
        if let Some(range) = resp.content_range {
            h.typed_insert(range)
        };
        use axum_extra::headers;
        h.typed_insert(headers::ContentType::from_str("application/octet-stream").unwrap());
        h.insert(
            axum::http::header::CONTENT_DISPOSITION,
            axum::http::HeaderValue::from_str(format!("attachment; filename=\"{}\"", delta).as_str()).unwrap(),
        );
        anyhow::Ok((StatusCode::OK, h, resp.stream))
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
