#![allow(dead_code, unreachable_code, unused_variables)]
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Router,
};
use core::future::Future;
use log::{debug, error};
use std::{path::PathBuf, sync::Arc};
use thiserror::Error;
use tokio::fs::File;

use async_file_cache::FileCache;

use parsing::{Delta, Package};

const MIRROR: &str = "http://europe.archive.pkgbuild.com/packages/.all/";
const LOCAL: &str = "./deltaserver/";

type Str = Box<str>;

use reqwest::Client;

fn main() {
    env_logger::init();

    let mut path = PathBuf::from(LOCAL);
    path.push("pkg");
    std::fs::create_dir_all(path).unwrap();
    let mut path = PathBuf::from(LOCAL);
    path.push("delta");
    std::fs::create_dir_all(path).unwrap();

    let package_cache = {
        let kf = |s: &_, p: &Package| {
            let mut path = PathBuf::from(LOCAL);
            path.push("pkg");
            path.push(p.to_string());
            path
        };
        async fn inner_f(
            client: Client,
            key: Package,
            mut file: File,
        ) -> Result<File, DownloadError> {
            let mut uri = String::new();
            uri.push_str(MIRROR);
            uri.push_str(&key.to_string());
            match client.get(uri).send().await {
                Ok(mut response) => {
                    use tokio::io::AsyncWriteExt;
                    while let Some(mut chunk) = response.chunk().await? {
                        file.write_all_buf(&mut chunk).await?;
                    }
                    Ok(file)
                }
                //fixme: return Ok(None) on 404
                Err(e) => Err(e.into()),
            }
        }
        FileCache::new(Client::new(), kf, inner_f, 8.into())
    };

    let delta_cache = {
        let kf = |_: &_, d: &Delta| {
            let mut p = PathBuf::from(LOCAL);
            p.push("delta");
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
            let old = state.get_or_generate(key.clone().get_old());
            let new = state.get_or_generate(key.get_new());
            let (old, new) = tokio::join!(old, new);
            let (old, new) = (old??, new??);

            let patch = patch.into_std().await;
            let old = old.into_std().await;
            let mut old = zstd::Decoder::new(old)?;
            let new = new.into_std().await;
            let mut new = zstd::Decoder::new(new)?;

            let f: tokio::task::JoinHandle<Result<_, DeltaError>> =
                tokio::task::spawn_blocking(move || {
                    let mut zpatch = zstd::Encoder::new(patch, 22)?;
                    let e = zpatch.set_parameter(zstd::zstd_safe::CParameter::NbWorkers(4));
                    if let Err(e) = e {
                        debug!("failed to make zstd multithread");
                    }
                    let mut last_report = 0;
                    ddelta::generate_chunked(&mut old, &mut new, &mut zpatch, None, |s| match s {
                        ddelta::State::Reading => debug!("reading"),
                        ddelta::State::Sorting => debug!("sorting"),
                        ddelta::State::Working(p) => {
                            const MB: u64 = 1024 * 1024;
                            if p > last_report + (8 * MB) {
                                debug!("working: {}MB done", p / MB);
                                last_report = p;
                            }
                        }
                    })?;
                    Ok(zpatch.finish()?)
                });
            let f = f.await.expect("threading error")?;

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
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .unwrap();
        })
}
#[derive(Error, Debug)]
enum DownloadError {
    #[error("could not write to file")]
    Io(#[from] std::io::Error),
    #[error("http request failed")]
    Req(#[from] reqwest::Error),
}
#[derive(Error, Debug)]
enum DeltaError {
    #[error("could not download file")]
    Download(#[from] DownloadError),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("other")]
    Other(#[from] anyhow::Error),
}
use axum::body::StreamBody;
use hyper::header::HeaderName;
use tokio_util::io::ReaderStream;
async fn gen_delta<S, KF, F, FF>(
    State(s): State<Arc<FileCache<Delta, S, KF, F, FF, DeltaError>>>,
    Path((from, to)): Path<(Str, Str)>,
) -> Result<
    (
        StatusCode,
        [(HeaderName, String); 3],
        StreamBody<ReaderStream<File>>,
    ),
    (StatusCode, String),
>
where
    S: Send + Sync + 'static + Clone,
    KF: Send + Sync + 'static + Fn(&S, &Delta) -> PathBuf,
    F: Send + Sync + 'static + Fn(S, Delta, File) -> FF,
    FF: Send + 'static + Future<Output = Result<File, DeltaError>>,
{
    let c = || async {
        let from = Package::try_from(&*from)?;
        let to = Package::try_from(&*to)?;
        let delta: Delta = (from, to).try_into()?;

        let file = {
            let delta = delta.clone();
            let (snd, rcv) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let f = s.get_or_generate(delta).await;
                snd.send(f).expect("request was probably canceled");
            });
            rcv.await
                .expect("uncaught error in FileCache, thats a bug")??
        };

        let len = file.metadata().await?.len();
        let stream = tokio_util::io::ReaderStream::new(file);
        let body = axum::body::StreamBody::new(stream);
        use hyper::header;
        let headers = [
            (header::CONTENT_LENGTH, len.to_string()),
            (header::CONTENT_TYPE, "application/octet-stream".to_owned()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", delta),
            ),
        ];
        anyhow::Ok((StatusCode::OK, headers, body))
    };
    c().await.map_err(|e| {
        error!("{:?}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("error producing delta: {}", e),
        )
    })
}

async fn root() -> (StatusCode, &'static str) {
    (
        StatusCode::OK,
        "welcome to the inofficial archlinux delta server",
    )
}

async fn fallback() -> (StatusCode, &'static str) {
    (StatusCode::NOT_FOUND, "Page could not be found")
}
