use std::{collections::HashMap, io::BufRead, path::PathBuf, process::Command};

use anyhow::bail;
use http::StatusCode;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, info, trace};
use memmap2::Mmap;
use parsing::Package;
use reqwest::Client;
use tokio::{
    io::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    pin,
    sync::Semaphore,
};

type Str = Box<str>;
type PackageVersions = HashMap<Str, Vec<(Str, Str, Str, PathBuf)>>;

static PACMAN_CACHE: &str = "/var/cache/pacman/pkg/";

pub(crate) async fn do_download<W: AsyncRead + AsyncWrite + AsyncSeek, G: AsRef<Semaphore>>(
    multi: MultiProgress,
    pkg: &Package,
    client: Client,
    request_guard: G,
    dl_guard: G,
    url: String,
    target: W,
) -> Result<ProgressBar, anyhow::Error> {
    let style =
        ProgressStyle::with_template("{prefix}{msg} [{wide_bar}] {bytes}/{total_bytes} {binary_bytes_per_sec} {eta}")
            .unwrap()
            .progress_chars("█▇▆▅▄▃▂▁  ");
    let pg = ProgressBar::hidden()
        .with_prefix(format!("deltadownload {}-{}", pkg.get_name(), pkg.get_version()))
        .with_style(style)
        .with_message(": waiting for server, this may take up to a few minutes");

    let pg = multi.add(pg);
    pg.tick();
    pin!(target);
    let mut tries = 3;
    'retry: loop {
        trace!("acquiring request guard");
        let guard = request_guard.as_ref().acquire().await?;
        trace!("acquired request guard");
        pg.set_position(0);
        let mut delta = {
            loop {
                // catch both client and server timeouts and simply retry
                match client.get(&url).send().await {
                    Ok(d) => match d.status() {
                        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => {
                            debug!("timeout; retrying {}", url)
                        }
                        status if !status.is_success() => bail!("request failed with {}", status),
                        _ => break d,
                    },
                    Err(e) => {
                        if !e.is_timeout() {
                            bail!("{}", e);
                        }
                    }
                }
            }
        };
        std::mem::drop(guard);
        trace!("dropped request guard");

        pg.reset_elapsed();
        pg.set_length(delta.content_length().unwrap_or(0));
        pg.set_message("");
        pg.tick();

        // acquire guard after sending request but before using the body
        // so the deltas can get generated on the server as parallel as possible
        // but the download does not get fragmented/overwhelmed
        let guard = dl_guard.as_ref().acquire().await?;
        loop {
            match delta.chunk().await {
                Ok(Some(chunk)) => {
                    let len = chunk.len();
                    target.write_all(&chunk).await?;
                    pg.inc(len.try_into().unwrap());
                }
                Ok(None) => {
                    drop(guard);
                    break 'retry;
                }
                Err(e) => {
                    if tries != 0 {
                        target.seek(std::io::SeekFrom::Start(0)).await?;
                        pg.set_position(0);
                        debug!("download failed, {tries} left");
                        tries -= 1;
                        drop(guard);
                        continue 'retry;
                    } else {
                        anyhow::bail!(e);
                    }
                }
            }
        }
    }
    Ok(pg)
}

pub(crate) fn find_deltaupgrade_candidates(
    blacklist: &[Str],
) -> Result<Vec<(String, Package, Package, Mmap, u64)>, anyhow::Error> {
    let upgrades = Command::new("pacman").args(["-Sup"]).output()?.stdout;
    let mut packageversions = build_package_versions().expect("io error on local disk");
    let mut lines: Vec<_> = upgrades
        .lines()
        .map(|l| l.expect("pacman abborted output???"))
        .filter(|l| !l.starts_with("file"))
        .filter_map(|line| {
            let (_, filename) = line.rsplit_once('/').unwrap();
            let pkg = Package::try_from(filename).unwrap();
            let name = pkg.get_name();
            if (&blacklist).into_iter().any(|e| **e == *name) {
                info!("{name} is blacklisted, skipping");
                return None;
            }
            if let Some((oldpkg, oldpath)) = newest_cached(&packageversions, &pkg.get_name()).or_else(|| {
                let (alternative, _) = packageversions
                    .keys()
                    .map(|name| (name, strsim::levenshtein(name, pkg.get_name())))
                    .filter(|(_name, sim)| sim <= &2)
                    .min()?;
                let prompt =
                    format!("could not find cached package for {name}, {alternative} is similar, use that instead?");
                if dialoguer::Confirm::new().with_prompt(prompt).interact().unwrap() {
                    newest_cached(&mut packageversions, &"lol")
                } else {
                    None
                }
            }) {
                // try to find the decompressed size for better progress monitoring
                let oldfile = std::fs::File::open(oldpath).expect("io error on local disk");
                // safety: i promise to not open the same file as writable at the same time
                let oldfile = unsafe { Mmap::map(&oldfile).expect("mmap failed") };
                // 16 megabytes seems like an ok average size
                // todo: find the actual average size of a decompressed package
                let default_size = 16 * 1024 * 1024;
                // due to pacman packages being compressed in streaming mode
                // zstd does not have an exact decompressed size and heavily overestimates
                let dec_size = zstd::bulk::Decompressor::upper_bound(&oldfile).unwrap_or_else(|| {
                    debug!("using default size for {name}");
                    default_size
                });
                Some((line, pkg, oldpkg, oldfile, (dec_size as u64)))
            } else {
                info!("no cached package found, leaving {} for pacman", filename);
                None
            }
        })
        .collect();
    lines.sort_unstable_by_key(|(_, _, _, _, size)| std::cmp::Reverse(*size));
    Ok(lines)
}

/// {package -> [(version, arch, trailer, path)]}
fn build_package_versions() -> std::io::Result<PackageVersions> {
    let mut package_versions: PackageVersions = HashMap::new();
    for line in std::fs::read_dir(PACMAN_CACHE)? {
        let line = line?;
        if !line.file_type()?.is_file() {
            continue;
        }
        let filename = line.file_name();
        let filename = filename.to_string_lossy();
        if !filename.ends_with(".zst") {
            continue;
        }
        let path = line.path().into();
        let package = Package::try_from(&*filename).expect("non-pkg zstd file in pacman cache dir?");
        let (name, version, arch, trailer) = package.destructure();

        match package_versions.entry(name) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                e.get_mut().push((version, arch, trailer, path));
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(vec![(version, arch, trailer, path)]);
            }
        }
    }
    package_versions.shrink_to_fit();

    for e in package_versions.values_mut() {
        e.sort()
    }

    Ok(package_versions)
}

/// returns the newest package in /var/cache/pacman/pkg that
/// fits the passed package
fn newest_cached(pv: &PackageVersions, package_name: &str) -> Option<(Package, PathBuf)> {
    pv.get(package_name)
        .map(|e| e.last().expect("each entry has at least one version"))
        .map(|(version, arch, trailer, path)| {
            (
                Package::from_parts((package_name.into(), version.clone(), arch.clone(), trailer.clone())),
                path.clone(),
            )
        })
}
