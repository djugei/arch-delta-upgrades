#![allow(dead_code, unreachable_code, unused_variables)]
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Router,
};
use log::debug;
use std::{path::PathBuf, sync::Arc};
use tokio::fs::File;

use cache::FileCache;

use parsing::{Delta, Package};

mod cache;

const MIRROR: &str = "http://europe.archive.pkgbuild.com/packages/.all/";
const LOCAL: &str = "./deltaserver/";

type Str = Box<str>;

use reqwest::Client;

fn main() {
    pretty_env_logger::init();

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
        ) -> Result<File, std::io::Error> {
            let mut uri = String::new();
            uri.push_str(MIRROR);
            uri.push_str(&key.to_string());
            match client.get(uri).send().await {
                Ok(mut response) => {
                    use tokio::io::AsyncWriteExt;
                    while let Some(mut chunk) = response.chunk().await.unwrap() {
                        file.write_all_buf(&mut chunk).await?;
                    }
                    Ok(file)
                }
                //fixme: return Ok(None) on 404
                Err(e) => panic!("{}", e),
            }
        }
        let f = |s: Client, key: Package, file: File| Box::pin(inner_f(s, key, file)) as _;
        FileCache::new(Client::new(), kf, f, 8.into())
    };
    let delta_cache = {
        let kf = |_: &_, d: &Delta| {
            let mut p = PathBuf::from(LOCAL);
            p.push("delta");
            p.push(d.to_string());
            p
        };

        async fn inner_f<S, KF, F>(
            state: Arc<FileCache<Package, S, KF, F, std::io::Error>>,
            key: Delta,
            patch: File,
        ) -> Result<File, std::io::Error>
        where
            S: Clone + Send,
            KF: Send + Fn(&S, &Package) -> PathBuf,
            F: Send
                + Fn(
                    S,
                    Package,
                    File,
                ) -> std::pin::Pin<
                    Box<dyn core::future::Future<Output = Result<File, std::io::Error>> + Send>,
                >,
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

            let f = tokio::spawn(async move {
                let mut zpatch = zstd::Encoder::new(patch, 20)?;
                ddelta::generate_chunked(&mut old, &mut new, &mut zpatch, None, |s| match s {
                    ddelta::State::Reading => debug!("reading"),
                    ddelta::State::Sorting => debug!("sorting"),
                    ddelta::State::Working(p) => {
                        debug!("working: {}KB done", p / (1024))
                    }
                })
                .unwrap();
                zpatch.finish()
            })
            .await
            .unwrap()
            .unwrap();

            let f = File::from_std(f);

            Ok(f)
        }

        let f = |state, key: Delta, file: File| Box::pin(inner_f(state, key, file)) as _;

        FileCache::new(Arc::new(package_cache), kf, f, 4.into())
    };
    let delta_cache = Arc::new(delta_cache);

    let delta = |State(s): State<Arc<FileCache<_, _, _, _, _>>>,
                 Path((from, to)): Path<(Str, Str)>| async move {
        let from = Package::try_from(&*from).unwrap();
        let to = Package::try_from(&*to).unwrap();
        let delta: Delta = (from, to).try_into().unwrap();
        let future = s.get_or_generate(delta.clone());
        let file = future.await.unwrap().unwrap();
        let len = file.metadata().await.unwrap().len();
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
        (StatusCode::OK, headers, body)
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let app = Router::new()
                .fallback(fallback)
                .route("/", get(root))
                .route("/arch/:from/:to", get(delta))
                .with_state(delta_cache);

            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 3000));
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .unwrap();
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
