use std::path::PathBuf;

use async_file_cache::{Cacheable, FileCache};
use parsing::{Delta, Package};
use reqwest::Client;
use thiserror::Error;
use tokio::fs::File;
use tracing::{debug, info, Instrument};

use crate::{FALLBACK_MIRROR, MIRROR};

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

    fn key_to_path(p: &Self::Key) -> PathBuf {
        let mut path = crate::get_pkg_path();
        path.push(p.to_string());
        path
    }

    #[tracing::instrument(level = "info", skip(self, file), "Downloading")]
    async fn gen_value(&self, key: &Self::Key, mut file: File) -> Result<File, Self::Error> {
        use tokio::io::AsyncWriteExt;

        let mirror = MIRROR.get().expect("initialized");
        let uri = format!("{mirror}{key}");
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

    fn key_to_path(d: &Self::Key) -> PathBuf {
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
