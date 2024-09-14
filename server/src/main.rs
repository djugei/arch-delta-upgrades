#![allow(dead_code, unreachable_code, unused_variables)]
use anyhow::bail;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::get,
    Router,
};
use axum_extra::headers::HeaderMapExt;
use core::future::Future;
use std::{panic, path::PathBuf, str::FromStr, sync::Arc};
use thiserror::Error;
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

    fn get_pkg_path() -> PathBuf {
        let mut basepath = PathBuf::from(CACHE_DIR.get().expect("initialized").as_ref());
        basepath.push("pkg");
        basepath
    }

    fn get_delta_path() -> PathBuf {
        let mut basepath = PathBuf::from(CACHE_DIR.get().expect("initialized").as_ref());
        basepath.push("delta");
        basepath
    }

    std::fs::create_dir_all(get_pkg_path()).unwrap();
    std::fs::create_dir_all(get_delta_path()).unwrap();

    let package_cache = {
        let kf = |s: &_, p: &Package| {
            let mut path = get_pkg_path();
            path.push(p.to_string());
            path
        };
        #[tracing::instrument(level = "info", skip(client, file), "Downloading")]
        async fn inner_f(client: Client, key: Package, mut file: File) -> Result<File, DownloadError> {
            use tokio::io::AsyncWriteExt;

            let mirror = MIRROR.get().expect("initialized");
            let uri = format!("{mirror}{key}");
            debug!(key = key.to_string(), uri, "getting from primary");
            let mut response = client.get(uri).send().await?;

            if response.status() == reqwest::StatusCode::NOT_FOUND {
                // fall back to archive mirror
                let fallback_mirror = FALLBACK_MIRROR.get().expect("initialized");
                let uri = format!("{fallback_mirror}{key}");
                info!(key = key.to_string(), "using fallback mirror");
                response = client.get(uri).send().await?;
            }
            if !response.status().is_success() {
                return Err(DownloadError::Status {
                    status: response.status(),
                    url: response.url().clone(),
                });
            }

            while let Some(mut chunk) = response.chunk().await? {
                file.write_all_buf(&mut chunk).await?;
            }
            Ok(file)
        }
        let parallel = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        trace!(parallel = parallel, "cpu parallelity");
        FileCache::new(Client::new(), kf, inner_f, parallel.into())
    };

    let delta_cache = {
        let kf = |_: &_, d: &Delta| {
            let mut p = get_delta_path();
            p.push(d.to_string());
            p
        };

        async fn inner_f<S, KF, F, FF>(
            state: Arc<FileCache<Package, S, KF, F, FF, DownloadError>>,
            key: Delta,
            patch: File,
        ) -> Result<File, DeltaError>
        where
            S: Clone + Send,
            KF: Send + Fn(&S, &Package) -> PathBuf,
            F: Send + Fn(S, Package, File) -> FF,
            FF: Future<Output = Result<File, DownloadError>> + Send,
        {
            let keystring = key.to_string();
            let old = state.get_or_generate(key.clone().get_old());
            let new = state.get_or_generate(key.get_new());
            let (old, new) = tokio::join!(old, new);
            let (old, new) = (old??, new??);

            let patch = patch.into_std().await;
            let old = old.into_std().await;
            let mut old = zstd::Decoder::new(old)?;
            let new = new.into_std().await;
            let mut new = zstd::Decoder::new(new)?;
            let span = tracing::info_span!("delta request", key = keystring);

            let f: tokio::task::JoinHandle<Result<_, DeltaError>> = tokio::task::spawn_blocking(move || {
                let mut zpatch = zstd::Encoder::new(patch, 22)?;
                let e = zpatch.set_parameter(zstd::zstd_safe::CParameter::NbWorkers(4));
                if let Err(e) = e {
                    debug!("failed to make zstd multithread");
                }
                let mut last_report = 0;
                ddelta::generate_chunked(&mut old, &mut new, &mut zpatch, None, |s| match s {
                    ddelta::State::Reading => debug!(key = keystring, "reading"),
                    ddelta::State::Sorting => debug!(key = keystring, "sorting"),
                    ddelta::State::Working(p) => {
                        const MB: u64 = 1024 * 1024;
                        if p > last_report + (8 * MB) {
                            debug!(key = keystring, "working: {}MB done", p / MB);
                            last_report = p;
                        }
                    }
                })?;
                Ok(zpatch.finish()?)
            });
            let f = f.instrument(span).await.expect("threading error")?;

            let f = File::from_std(f);

            Ok(f)
        }
        FileCache::new(Arc::new(package_cache), kf, inner_f, 4.into())
    };
    let delta_cache = Arc::new(delta_cache);

    tokio::runtime::Builder::new_multi_thread()
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
#[derive(Error, Debug)]
enum DownloadError {
    #[error("could not write to file: {0}")]
    Io(#[from] std::io::Error),
    #[error("http request failed: {0}")]
    Connection(#[from] reqwest::Error),
    #[error("bad status code: {status} while fetching {url}")]
    Status {
        url: reqwest::Url,
        status: reqwest::StatusCode,
    },
}

#[derive(Error, Debug)]
enum DeltaError {
    #[error("could not download file: {0}")]
    Download(#[from] DownloadError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("generation error: {0}")]
    DeltaGen(#[from] ddelta::DiffError),
}
async fn gen_delta<S, KF, F, FF>(
    State(s): State<Arc<FileCache<Delta, S, KF, F, FF, DeltaError>>>,
    Path((from, to)): Path<(Str, Str)>,
    range: Option<axum_extra::TypedHeader<axum_extra::headers::Range>>,
) -> Result<
    (
        StatusCode,
        HeaderMap,
        axum_range::RangedStream<axum_range::KnownSize<File>>,
    ),
    (StatusCode, String),
>
where
    S: Send + Sync + 'static + Clone,
    KF: Send + Sync + 'static + Fn(&S, &Delta) -> PathBuf,
    F: Send + Sync + 'static + Fn(S, Delta, File) -> FF,
    FF: Send + 'static + Future<Output = Result<File, DeltaError>>,
{
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

        let len = file.metadata().await?.len();
        let body = axum_range::KnownSize::file(file).await?;
        let range = range.map(|axum_extra::TypedHeader(range)| range);
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
