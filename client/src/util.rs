use std::{
    collections::HashMap,
    io::{BufRead, Read},
    path::PathBuf,
    process::Command,
};

use anyhow::{Context, bail};
use bytesize::ByteSize;
use common::Package;
use http::StatusCode;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use itertools::Itertools;
use log::{debug, info, trace};
use memmap2::Mmap;
use reqwest::{Client, Url};
use ruma_headers::ContentDisposition;
use tokio::{
    io::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    pin,
};

type Str = Box<str>;
type PackageVersions = HashMap<Str, Vec<(Str, Str, Str, PathBuf)>>;

static PACMAN_CACHE: &str = "/var/cache/pacman/pkg/";

pub(crate) fn progress_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix} {msg} [{wide_bar}] {bytes}/{total_bytes} {binary_bytes_per_sec} {eta}")
        .unwrap()
        .progress_chars("█▇▆▅▄▃▂▁  ")
}

pub(crate) async fn do_delta_download<W: AsyncRead + AsyncWrite + AsyncSeek>(
    global: crate::GlobalState,
    pkg: &Package,
    url: Url,
    target: W,
) -> Result<ProgressBar, common::DLError> {
    let limit = common::Limits {
        maxpar_dl: global.maxpar_dl,
        maxpar_req: global.maxpar_req,
    };
    let pg = ProgressBar::hidden()
        .with_message(format!("{}-{}", pkg.get_name(), pkg.get_version()))
        .with_style(progress_style());
    let pg = global.multi.add(pg);

    pin!(target);
    common::dl_body(limit, global.client, pg.clone(), url, target)
        .await
        .map(|()| pg)
}

pub(crate) async fn do_boring_download(
    global: crate::GlobalState,
    url: Url,
    mut target_dir: PathBuf,
) -> Result<u64, common::DLError> {
    let limit = common::Limits {
        maxpar_dl: global.maxpar_dl,
        maxpar_req: global.maxpar_req,
    };
    let name = url.path_segments().and_then(Iterator::last).expect("malformed url");
    let pkg = Package::try_from(name).expect("invalid file name");
    let pg = ProgressBar::hidden()
        .with_message(format!("{}-{}", pkg.get_name(), pkg.get_version()))
        .with_style(progress_style());
    let pg = global.multi.add(pg);

    target_dir.push(name);
    let target = tokio::fs::File::create(target_dir).await?;

    pin!(target);
    common::dl_body(limit, global.client, pg.clone(), url, &mut target).await?;
    let size = target.seek(std::io::SeekFrom::End(0)).await?;
    Ok(size)
}

pub(crate) fn find_deltaupgrade_candidates(
    blacklist: &[Str],
    fuz: bool,
) -> Result<(Vec<(String, Package, Package, Mmap, u64)>, Vec<Url>), anyhow::Error> {
    let upgrades = Command::new("pacman").args(["-Sup"]).output()?.stdout;
    let packageversions = build_package_versions().expect("io error on local disk");
    let (mut delta_upgrades, downloads): (Vec<_>, Vec<_>) = upgrades
        .lines()
        .map(|l| l.expect("pacman abborted output???"))
        .filter(|l| !l.starts_with("file"))
        .map(|line| {
            let (_, filename) = line.rsplit_once('/').unwrap();
            let pkg = Package::try_from(filename).unwrap();
            let name = pkg.get_name();
            if (&blacklist).into_iter().any(|e| **e == *name) {
                info!("{name} is blacklisted, skipping");
                return Err(line);
            }
            if let Some((oldpkg, oldpath)) = newest_cached(&packageversions, &pkg.get_name()).or_else(|| {
                if fuz {
                    let (alternative, _) = packageversions
                        .keys()
                        .map(|name| (name, strsim::levenshtein(name, pkg.get_name())))
                        .filter(|(_name, sim)| sim <= &2)
                        .min()?;
                    let prompt = format!(
                        "could not find cached package for {name}, {alternative} has a similar name, use that instead?"
                    );
                    if dialoguer::Confirm::new().with_prompt(prompt).interact().unwrap() {
                        newest_cached(&packageversions, alternative)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }) {
                // Try to find the decompressed size for better progress monitoring
                let oldfile = std::fs::File::open(oldpath).expect("io error on local disk");
                // Safety: I promise to not open the same file as writable at the same time
                let oldfile = unsafe { Mmap::map(&oldfile).expect("mmap failed") };
                // Testing reveals the average size to be 17.2 MB
                let default_size = 17 * 1024 * 1024;
                // Due to pacman packages being compressed in streaming mode
                // zstd does not have an exact decompressed size and heavily overestimates
                let dec_size = zstd::bulk::Decompressor::upper_bound(&oldfile)
                    // The real ratio is 18/47.4, 4/11 is close enough
                    .map(|s| (s as u64) * 4 / 11)
                    .unwrap_or_else(|| {
                        debug!("using default size for {name}");
                        default_size
                    });
                Ok((line, pkg, oldpkg, oldfile, (dec_size as u64)))
            } else {
                info!("no cached package found, leaving {} for pacman", filename);
                return Err(line);
            }
        })
        .partition_result();
    delta_upgrades.sort_unstable_by_key(|(_, _, _, _, size)| std::cmp::Reverse(*size));
    let downloads: Result<Vec<Url>, _> = downloads.into_iter().map(|l| Url::parse(&l)).collect();
    Ok((delta_upgrades, downloads?))
}

pub async fn sync_db(server: Url, name: Str, client: Client, _multi: MultiProgress) -> anyhow::Result<()> {
    //TODO use the same logic as the main delta downloads, including retries and progress bars
    info!("syncing {}", name);
    let max = common::find_latest_db(&name, "/var/lib/pacman/sync/")?;
    if let Some(old_ts) = max {
        debug!("upgrading {name} from {old_ts}");
        let url = server.join(&format!("{name}/{old_ts}"))?;
        let response = client.get(url).send().await?;
        if response.status() == StatusCode::NOT_MODIFIED {
            info!("{name} is unchanged");
            return Ok(());
        }
        assert!(response.status().is_success(), "{}", response.status());
        let header = response
            .headers()
            .get("content-disposition")
            .context("missing content disposition header")?;
        let patchname = ContentDisposition::try_from(header.as_bytes())?
            .filename
            .context("response has no filename")?;
        let (_, new_ts) = patchname.rsplit_once('-').context("malformed http filename")?;
        let new_ts: u64 = new_ts.parse()?;

        debug!("new ts for {name}: {new_ts}");
        //TODO server side should be sending 304 for this
        if new_ts != old_ts {
            // Patches are relatively small, can afford to just load it into memory
            let patch = response.bytes().await?;
            let patch = std::io::Cursor::new(patch);
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                debug!("patching {name} from {old_ts} to {new_ts}");
                let mut patch = zstd::Decoder::new(patch)?;

                let old_p = format!("/var/lib/pacman/sync/{name}-{old_ts}");
                let old = std::fs::File::open(&old_p)?;
                let mut old = flate2::read::GzDecoder::new(old);
                let mut oldv = Vec::new();
                old.read_to_end(&mut oldv)?;
                let mut old = std::io::Cursor::new(oldv);

                let new_p = format!("/var/lib/pacman/sync/{name}-{new_ts}");
                let new = std::fs::File::create(&new_p)?;
                let mut new = flate2::write::GzEncoder::new(new, flate2::Compression::default());

                ddelta::apply_chunked(&mut old, &mut new, &mut patch)?;

                info!("finished patching {name} to {new_ts}");

                trace!("linking {new_p} to db");
                let db_p = format!("/var/lib/pacman/sync/{name}.db");
                std::fs::remove_file(&db_p)?;
                std::os::unix::fs::symlink(new_p, db_p)?;

                trace!("deleting obsolete db {old_p}");
                std::fs::remove_file(old_p)?;

                Ok(())
            })
            .await??;
        } else {
            info!("{name}: old ({old_ts}) == new ({new_ts}), nothing to do");
        }
    } else {
        info!("no timestamped db found, do full download");
        //TODO: maybe share this block between the two branches
        let url = server.join(&format!("{name}"))?;
        let mut response = client.get(url).send().await?;
        assert!(response.status().is_success());
        let header = response
            .headers()
            .get("content-disposition")
            .context("missing content disposition header")?;
        let patchname = ContentDisposition::try_from(header.as_bytes())?
            .filename
            .context("response has no filename")?;
        let (_, new_ts) = patchname.rsplit_once('-').context("malformed http filename")?;
        let new_ts: u64 = new_ts.parse()?;

        let new_p = format!("/var/lib/pacman/sync/{name}-{new_ts}");
        let mut new = tokio::fs::File::create(&new_p).await?;
        while let Some(chunk) = response.chunk().await? {
            new.write_all(&chunk).await?;
        }

        let db_p = format!("/var/lib/pacman/sync/{name}.db");
        trace!("linking {new_p} to {db_p}");
        let res = std::fs::remove_file(&db_p);
        if let Err(e) = res {
            // Not found is fine, just means the db does not exist yet
            if e.kind() != std::io::ErrorKind::NotFound {
                bail!(e);
            }
        }
        std::os::unix::fs::symlink(new_p, db_p)?;
        info!("finished downloading {name} at {new_ts}");
    }
    Ok(())
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

/// Calculates and prints some stats about bandwidth savings
pub(crate) fn calc_stats(count: usize) -> std::io::Result<()> {
    let mut deltas: HashMap<(Str, Str, Str), u64> = HashMap::new();
    let mut pkgs: HashMap<(Str, Str, Str), u64> = HashMap::new();
    let mut pairs: HashMap<Str, (u64, u64, u64)> = HashMap::new();
    for line in std::fs::read_dir(PACMAN_CACHE)? {
        let line = line?;
        if !line.file_type()?.is_file() {
            continue;
        }
        let filename = line.file_name();
        let filename: String = filename.into_string().unwrap();
        if filename.ends_with(".delta") {
            let (_old, new) = filename.rsplit_once(":to:").unwrap();
            let pkg = Package::try_from(&*new).unwrap();
            let (name, version, arch, trailer) = pkg.destructure();
            assert_eq!(&*trailer, "delta");
            let len = line.metadata()?.len();
            deltas.insert((name.into(), version.into(), arch.into()), len);
        } else if filename.ends_with(".pkg.tar.zst") {
            let (name, version, arch, _trailer) = Package::try_from(filename.as_str())
                .expect("weird pkg file name")
                .destructure();
            let len = line.metadata()?.len();
            if let Some(((name, _version, _arch), dlen)) =
                deltas.remove_entry(&(name.clone(), version.clone(), arch.clone()))
            {
                pairs
                    .entry(name)
                    .and_modify(|e| {
                        e.0 += 1;
                        e.1 += len;
                        e.2 += dlen;
                    })
                    .or_insert((1, len, dlen));
            } else {
                pkgs.insert((name, version, arch), len);
            }
        } else if filename.ends_with(".sig") {
            continue;
        } else {
            debug!("unmatched filename {:?}", filename);
            continue;
        }
    }
    let mut unmatched = 0;
    for ((name, version, arch), len) in pkgs.drain() {
        if log::log_enabled!(log::Level::Trace) {
            if !deltas.contains_key(&(name.clone(), version.clone(), arch.clone())) {
                trace!("pkg: {name} {version} {arch} unmatched");
            }
        }
        if let Some(((name, _version, _arch), dlen)) = deltas.remove_entry(&(name, version, arch)) {
            pairs
                .entry(name)
                .and_modify(|e| {
                    e.0 += 1;
                    e.1 += len;
                    e.2 += dlen;
                })
                .or_insert((1, len, dlen));
        } else {
            unmatched += 1;
        }
    }
    info!("{unmatched} packages did not have an associated delta");
    info!("{} unmatched deltas", deltas.len());

    if log::log_enabled!(log::Level::Debug) {
        for ((name, version, arch), _dlen) in deltas.drain() {
            debug!("delta: {name} {version} {arch} unmatched");
        }
    }

    let mut pairs = pairs
        .drain()
        .map(|(name, (count, len, dlen))| {
            let ratio = dlen as f64 / len as f64;
            let abs = len.abs_diff(dlen) / count;
            (ratio, name, count, len, dlen, abs)
        })
        .collect_vec();
    // todo: create stats type instead of having a big tuple
    pairs.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

    for (_, name, _, len, dlen, _) in pairs.iter() {
        if len < dlen {
            debug!("big delta in {}: {}:{}", name, ByteSize::b(*len), ByteSize::b(*dlen))
        }
    }

    info!("worst ratios:");
    for (i, (ratio, name, _count, len, dlen, abs)) in pairs.iter().rev().take(count).enumerate() {
        info!(
            "{}. {:.2}% {}: {} {} each update",
            i + 1,
            ratio * 100.,
            name,
            ByteSize::b(*abs),
            if dlen < len { "saved" } else { "wasted" }
        )
    }

    info!("top ratios:");
    for (i, (ratio, name, _count, len, dlen, abs)) in pairs.iter().take(count).enumerate() {
        info!(
            "{}. {:.2}% {}: {} {} each update",
            i + 1,
            ratio * 100.,
            name,
            ByteSize::b(*abs),
            if dlen < len { "saved" } else { "wasted" }
        )
    }
    pairs.sort_by_key(|e| e.5 * e.2);
    info!("top size saves");
    for (i, (ratio, name, count, len, dlen, abs)) in pairs.iter().rev().take(count).enumerate() {
        info!(
            "{}. {:.2}% {}: {} {} in {} updates",
            i + 1,
            ratio * 100.,
            name,
            ByteSize::b(*abs * count),
            if dlen < len { "saved" } else { "wasted" },
            count
        )
    }

    // todo: do this in one iteration
    let total_len: u64 = pairs.iter().map(|e| e.3).sum();
    let total_dlen: u64 = pairs.iter().map(|e| e.4).sum();

    info!("{} saved in total", ByteSize::b(total_len - total_dlen));
    let ratio = total_dlen as f64 / total_len as f64;
    info!("total ratio: {:.2}%", ratio * 100.);
    // On my machine I get 84.47% bandwidth saved.
    info!("saved {:.2}% of bandwidth", (1. - ratio) * 100.);

    Ok(())
}

#[test]
#[ignore = "expensive to run, needs actual packages"]
fn test_dec_sizes() {
    use std::io::Seek;
    use std::os::unix::ffi::OsStrExt;

    let dirents: Vec<_> = std::fs::read_dir(PACMAN_CACHE)
        .unwrap()
        .map(Result::unwrap)
        .filter(|line| line.file_type().unwrap().is_file() && line.file_name().as_bytes().ends_with(b".tar.zst"))
        .collect();

    let mut actual_size = 0u64;
    let mut guessed_size = 0u64;
    let mut count = 0u64;
    let mut compressed = 0u64;
    std::thread::scope(|s| {
        let (ab, cd) = dirents.split_at(dirents.len() / 2);
        let (a, b) = ab.split_at(ab.len() / 2);
        let (c, d) = cd.split_at(cd.len() / 2);

        let a = s.spawn(|| calc_stuff(a));
        let b = s.spawn(|| calc_stuff(b));
        let c = s.spawn(|| calc_stuff(c));
        let d = s.spawn(|| calc_stuff(d));

        for j in [a, b, c, d].into_iter() {
            let (a_s, g_s, c, comp) = j.join().unwrap();
            actual_size += a_s;
            guessed_size += g_s;
            count += c;
            compressed += comp;
        }
    });

    fn calc_stuff(entries: &[std::fs::DirEntry]) -> (u64, u64, u64, u64) {
        use std::os::unix::fs::MetadataExt;
        let mut buf = vec![0; 2048];
        let mut actual_size = 0u64;
        let mut guessed_size = 0u64;
        let mut count = 0u64;
        let mut compressed = 0u64;
        for line in entries {
            compressed += line.metadata().unwrap().size();
            let mut file = std::fs::File::open(line.path()).expect("io error on local disk");
            let mmapfile = unsafe { Mmap::map(&file).expect("mmap failed") };
            guessed_size += zstd::bulk::Decompressor::upper_bound(&mmapfile).unwrap() as u64;
            file.rewind().unwrap();
            let mut dec = zstd::Decoder::new(file).unwrap();
            while let Ok(read) = dec.read(&mut buf) {
                if read == 0 {
                    break;
                }
                actual_size += read as u64;
            }
            count += 1;
        }

        (actual_size, guessed_size, count, compressed)
    }

    println!("real size is {}", ByteSize::b(actual_size));
    println!("guess is {}", ByteSize::b(guessed_size));
    println!("average package size is {}", ByteSize::b(actual_size / count));
    println!(
        "average compression ratio: {}",
        (compressed as f64) / (actual_size as f64)
    );
}
