use std::path::PathBuf;
use std::time::SystemTime;

use async_file_cache::{Cacheable, FileCache};
use parsing::{Delta, Package};
use reqwest::Client;
use thiserror::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{debug, info, Instrument};

use crate::Str;
use crate::{get_db_path, FALLBACK_MIRROR, MIRROR};

#[derive(Error, Debug)]
pub enum DownloadError {
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

pub struct PackageCache(pub Client);
impl Cacheable for PackageCache {
    type Key = Package;
    type Error = DownloadError;

    fn key_to_path(&self, p: &Self::Key) -> PathBuf {
        let mut path = crate::get_pkg_path();
        path.push(p.to_string());
        path
    }

    #[tracing::instrument(level = "info", skip(self, file), "Downloading")]
    async fn gen_value(&self, key: &Self::Key, mut file: File) -> Result<File, Self::Error> {
        let mirror = MIRROR.get().expect("initialized");
        let uri = format!("{mirror}pool/packages/{key}");
        debug!(key = key.to_string(), uri, "getting from primary");
        let mut response = self.0.get(uri).send().await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            // fall back to archive mirror
            let fallback_mirror = FALLBACK_MIRROR.get().expect("initialized");
            let uri = format!("{fallback_mirror}{key}");
            info!(key = key.to_string(), "using fallback mirror");
            response = self.0.get(uri).send().await?;
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
}

#[derive(Error, Debug)]
pub enum DeltaError {
    #[error("could not download file: {0}")]
    Download(#[from] DownloadError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("generation error: {0}")]
    DeltaGen(#[from] ddelta::DiffError),
}

pub struct DeltaCache(pub FileCache<PackageCache>);

impl Cacheable for DeltaCache {
    type Key = Delta;
    type Error = DeltaError;

    fn key_to_path(&self, d: &Self::Key) -> PathBuf {
        let mut p = crate::get_delta_path();
        p.push(d.to_string());
        p
    }

    async fn gen_value(&self, key: &Self::Key, patch: File) -> Result<File, Self::Error> {
        let keystring = key.to_string();
        let (old, new) = key.clone().get_both();
        let old = self.0.get_or_generate(old);
        let new = self.0.get_or_generate(new);
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
                debug!("failed to make zstd multithread: {e:?}");
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
}

trait SystemTimeExt {
    fn to_timestamp(&self) -> u64;
    fn from_timestamp(ts: u64) -> Self;
    fn to_path(&self, dbname: &str) -> PathBuf;
}

impl SystemTimeExt for SystemTime {
    fn to_timestamp(&self) -> u64 {
        self.duration_since(SystemTime::UNIX_EPOCH)
            .expect("time travel")
            .as_secs()
    }

    fn from_timestamp(ts: u64) -> Self {
        SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(ts)
    }

    fn to_path(&self, dbname: &str) -> PathBuf {
        let filename = format!("{}-{}", dbname, self.to_timestamp());
        get_db_path().join(filename)
    }
}

pub struct DBCache {
    cache: FileCache<DBCacheable>,
    sync: Mutex<DBCacheSync>,
}

struct DBCacheable {
    name: Str,
}

struct DBCacheSync {
    last_check: SystemTime,
    last_sync: SystemTime,
    client: Client,
}

impl DBCache {
    pub fn new(name: Str, client: Client) -> Result<Self, DownloadError> {
        let s = DBCache {
            cache: FileCache::new(DBCacheable { name }, None),
            sync: Mutex::new(DBCacheSync {
                last_check: SystemTime::UNIX_EPOCH,
                last_sync: SystemTime::UNIX_EPOCH,
                client,
            }),
        };
        //TODO: search for most recent db on disk.
        Ok(s)
    }

    /// Updates internally then returns the newest available db
    pub async fn get_newest_db(&self) -> Result<(u64, File), DownloadError> {
        let ts = self.update().await?;
        let path = ts.to_path(&self.cache.state().name);
        let file = File::open(path).await?;
        Ok((ts.to_timestamp(), file))
    }

    /// Updates internally then returns a delta from old to the newest available db
    pub async fn get_delta_to(&self, old: u64) -> Result<(u64, File), DeltaError> {
        let ts = self.update().await?.to_timestamp();
        let patch = self.cache.get_or_generate((old, ts)).await??;
        Ok((ts, patch))
    }

    /// Gets a new version of the DB if necessary,
    /// returns the timestamp of the most up-to-date version.
    async fn update(&self) -> Result<SystemTime, DownloadError> {
        debug!(name = self.cache.state().name, "updating");
        let mut sync = self.sync.lock().await;
        let now = SystemTime::now();

        // Only have one database version every 10 minutes
        let grouping_expired =
            now.duration_since(sync.last_sync).unwrap_or_default() > std::time::Duration::from_secs(10 * 60);
        // If 10 minutes have passed check at most every 30 seconds for new databases
        let pacing_expired =
            now.duration_since(sync.last_check).unwrap_or_default() > std::time::Duration::from_secs(60);

        debug!(grouping = grouping_expired, pacing = pacing_expired, "time check");
        if grouping_expired && pacing_expired {
            let mirror = MIRROR.get().expect("initialized");
            let uri = format!("{mirror}{0}/os/x86_64/{0}.db", self.cache.state().name);
            debug!(uri = uri, "getting db");
            let mut response = sync
                .client
                .get(&uri)
                // This is technically not HTTP-compliant, but idgaf
                .header(
                    reqwest::header::IF_MODIFIED_SINCE,
                    sync.last_sync.to_timestamp().to_string(),
                )
                .send()
                .await?;

            if response.status() == reqwest::StatusCode::NOT_MODIFIED {
                debug!(uri, "not modified");
                // No updates in the last interval
            } else if response.status() == reqwest::StatusCode::OK {
                // db has been updated, update our local copy
                let path = now.to_path(&self.cache.state().name);
                debug!(uri, "updating stored db {:?}", &path);
                let mut file = File::create(path).await?;
                while let Some(mut chunk) = response.chunk().await? {
                    file.write_all_buf(&mut chunk).await?;
                }
                sync.last_sync = now;
            } else {
                return Err(DownloadError::Status {
                    url: response.url().clone(),
                    status: response.status(),
                });
            }

            sync.last_check = now;
        }
        return Ok(sync.last_sync);
    }
}

impl Cacheable for DBCacheable {
    // old, local
    type Key = (u64, u64);
    type Error = DeltaError;

    fn key_to_path(&self, k: &Self::Key) -> PathBuf {
        let mut name = self.name.to_string();
        name.push_str(&k.0.to_string());
        name.push(':');
        name.push_str(&k.1.to_string());

        let mut p = crate::get_db_path();
        p.push(name);
        p
    }

    #[tracing::instrument(level = "info", skip(self, patch), "DBdelta")]
    async fn gen_value(&self, (old, new): &Self::Key, patch: File) -> Result<File, Self::Error> {
        let old = SystemTime::from_timestamp(*old);
        let new = SystemTime::from_timestamp(*new);

        let oldp = old.to_path(&self.name);
        let newp = new.to_path(&self.name);

        let oldf = File::open(oldp);
        let newf = File::open(newp);

        let (oldf, newf) = tokio::join!(oldf, newf);
        let oldf = oldf?.into_std().await;
        let newf = newf?.into_std().await;
        let patch = patch.into_std().await;

        use flate2::read::GzDecoder;
        let mut oldf = GzDecoder::new(oldf);
        let mut newf = GzDecoder::new(newf);

        let span = tracing::info_span!(
            "dbdelta request",
            name = self.name,
            old = old.to_timestamp(),
            new = new.to_timestamp()
        );
        let name = self.name.clone();
        let f: tokio::task::JoinHandle<Result<_, DeltaError>> = tokio::task::spawn_blocking(move || {
            let mut zpatch = zstd::Encoder::new(patch, 22)?;
            let e = zpatch.set_parameter(zstd::zstd_safe::CParameter::NbWorkers(4));
            if let Err(e) = e {
                debug!("failed to make zstd multithread: {e:?}");
            }
            let mut last_report = 0;
            ddelta::generate_chunked(&mut oldf, &mut newf, &mut zpatch, None, |s| match s {
                ddelta::State::Reading => debug!(name = name, "reading"),
                ddelta::State::Sorting => debug!(name = name, "sorting"),
                ddelta::State::Working(p) => {
                    const MB: u64 = 1024 * 1024;
                    if p > last_report + (8 * MB) {
                        debug!(name = name, "working: {}MB done", p / MB);
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
}
