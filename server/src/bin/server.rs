#![allow(dead_code, unreachable_code, unused_variables)]
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Router,
};
use log::debug;
use std::{hash::Hash, path::PathBuf, sync::Arc};
use tokio::fs::File;

use deltaserver::FileCache;

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
        let s = tokio_util::io::ReaderStream::new(file);
        let body = axum::body::StreamBody::new(s);
        use hyper::header;
        let headers = [
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

            let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 3000));
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
struct Package {
    name: Str,
    version: Str,
    arch: Str,
    trailer: Str,
}

impl<'s> TryFrom<&'s str> for Package {
    type Error = ();

    fn try_from(value: &'s str) -> Result<Self, Self::Error> {
        if value.contains('/') {
            return Err(());
        }
        let (left, trailer) = value.rsplit_once('-').ok_or(())?;
        let mut idx = left.rmatch_indices('-');
        let _ = idx.next();
        let (idx, _) = idx.next().ok_or(())?;
        let (name, version) = left.split_at(idx);
        let version = &version[1..];
        let (arch, trailer) = trailer.split_once('.').ok_or(())?;

        Ok(Package {
            name: name.into(),
            version: version.into(),
            arch: arch.into(),
            trailer: trailer.into(),
        })
    }
}

impl std::fmt::Display for Package {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}.{}",
            self.name, self.version, self.arch, self.trailer
        )
    }
}

#[test]
fn package_parse() {
    let s = "6tunnel-0.13-1-x86_64.pkg.tar.xz";
    let p = Package::try_from(s).unwrap();

    assert_eq!(&*p.name, "6tunnel");
    assert_eq!(&*p.version, "0.13-1");
    assert_eq!(&*p.arch, "x86_64");
    assert_eq!(&*p.trailer, "pkg.tar.xz");

    assert_eq!(p.to_string(), s);
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
struct Delta {
    name: Str,
    old: Str,
    new: Str,
    arch: Str,
    trailer: Str,
}

impl Delta {
    fn get_old(self) -> Package {
        Package {
            name: self.name,
            arch: self.arch,
            trailer: self.trailer,
            version: self.old,
        }
    }
    fn get_new(self) -> Package {
        Package {
            name: self.name,
            arch: self.arch,
            trailer: self.trailer,
            version: self.new,
        }
    }
}

impl TryFrom<(Package, Package)> for Delta {
    type Error = ();

    fn try_from((p1, p2): (Package, Package)) -> Result<Self, Self::Error> {
        #[allow(clippy::if_same_then_else)]
        if (&p1.name, &p1.arch, &p1.trailer) != (&p2.name, &p2.arch, &p2.trailer) {
            Err(())
        } else if p1.version >= p2.version {
            Err(())
        } else {
            Ok(Delta {
                name: p1.name,
                arch: p1.arch,
                trailer: p1.trailer,
                old: p1.version,
                new: p2.version,
            })
        }
    }
}

impl std::fmt::Display for Delta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}_to_{}-{}.{}",
            self.name, self.old, self.new, self.arch, self.trailer
        )
    }
}

// should possibly take file descriptors instead of paths
// or take (path, url) touples
// to decouple http stuff from file management stuff
// there should be a PkgManager struct to do the pkg management
/*
async fn gen_or_get(&self, fm: &FileManager, old: Package, new: Package) -> String {
    assert_eq!(old.name, new.name);
    let oldpath = format!("{}/pkg/{}", LOCAL, old);
    let newpath = format!("{}/pkg/{}", LOCAL, old);
    let patchpath = format!(
        "{}/patch/{}:{}_to_{}",
        LOCAL, old.name, old.version, new.version
    );

    // use max_par_dl semaphore
    let oldfile = fm.dl_or_get(old);
    let newfile = fm.dl_or_get(new);

    let patchfile = if let Ok(f) = tokio::fs::OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(patchpath)
        .await
    {
        todo!()
    } else {
        // generate patch
        let permit = self.max_par_gen.acquire().await.unwrap();
        let joinhandle = tokio::spawn(async move {
            todo!("generate delta");
        });
        joinhandle.await.unwrap();
        patchpath
    }
    todo!()
}
*/
