mod util;

use anyhow::Context;
use bytesize::ByteSize;
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, error, info};
use parsing::Package;
use reqwest::Url;
use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{BufRead, Cursor},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
    time::Duration,
};
use tokio::task::JoinSet;

static PACMAN_CACHE: &str = "/var/cache/pacman/pkg/";

#[derive(Parser, Debug)]
#[command(version, about)]
enum Commands {
    #[cfg(feature = "diff")]
    Delta {
        orig: PathBuf,
        new: PathBuf,
        patch: PathBuf,
    },
    Patch {
        orig: PathBuf,
        patch: PathBuf,
        new: PathBuf,
    },
    /// Download the newest packages to the provided delta_cache path
    ///
    /// If delta_cache is somewhere you can write, no sudo is needed.
    ///
    /// ```bash
    /// $ pacman -Sy
    ///
    /// $ deltaclient download http://bogen.moeh.re/arch path
    /// $ sudo cp path/*.pkg path/*.sig /var/cache/pacman/pkg/
    /// ## or
    /// $ sudo deltaclient download http://bogen.moeh.re/arch /var/cache/pacman/pkg/
    ///
    /// $ sudo pacman -Su
    /// ```
    ///
    /// If you are doing a full sysupgrade try the upgrade subcommand for more comfort.
    #[command(verbatim_doc_comment)]
    Download { server: Url, delta_cache: PathBuf },
    /// run an entire upgrade, calling pacman interally, needs sudo to run.
    Upgrade {
        server: Url,
        #[arg(default_values_t = [Box::<str>::from("linux")])]
        blacklist: Vec<Box<str>>,
    },
}

fn main() {
    // set up a logger that does not conflict with progress bars
    let logger = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).build();
    let multi = MultiProgress::new();

    indicatif_log_bridge::LogWrapper::new(multi.clone(), logger)
        .try_init()
        .expect("initializing logger failed");

    let args = Commands::parse();

    match args {
        #[cfg(feature = "diff")]
        Commands::Delta { orig, new, patch } => {
            gen_delta(&orig, &new, &patch).unwrap();
        }
        Commands::Patch { orig, patch, new } => {
            let pb = multi.add(ProgressBar::new(0));
            let orig = OpenOptions::new().read(true).open(orig).unwrap();
            let orig = zstd::decode_all(orig).unwrap();
            apply_patch(&orig, &patch, &new, pb).unwrap();
        }
        Commands::Upgrade { server, blacklist } => {
            info!("running pacman -Sy");
            let exit = Command::new("pacman")
                .arg("-Sy")
                .spawn()
                .expect("could not run pacman -Sy")
                .wait()
                .expect("error waiting for pacman -Sy");
            if !exit.success() {
                panic!("pacman -Sy failed, aborting");
            }

            let cachepath = PathBuf::from_str("/var/cache/pacman/pkg").unwrap();
            let (deltasize, newsize) = match upgrade(server, blacklist, cachepath, multi) {
                Ok((d, n)) => (d, n),
                Err(e) => {
                    error!("{e}");
                    panic!("{e}")
                }
            };

            info!("running pacman -Su to install updates");
            let exit = Command::new("pacman")
                .args(["-Su", "--noconfirm"])
                .spawn()
                .expect("could not run pacman -Su")
                .wait()
                .expect("error waiting for pacman -Su");

            let saved = newsize - deltasize;

            let deltasize = ByteSize::b(deltasize);
            let newsize = ByteSize::b(newsize);
            let saved = ByteSize::b(saved);
            info!("downloaded   {deltasize}");
            info!("upgrades are {newsize}");
            info!("saved you    {saved}");

            if !exit.success() {
                panic!("pacman -Su failed, aborting. Fix errors and try again");
            }

            info!("have a great day!");
        }
        Commands::Download { server, delta_cache } => {
            std::fs::create_dir_all(&delta_cache).unwrap();
            upgrade(server, vec![], delta_cache, multi).unwrap();
        }
    }
}

fn upgrade(
    server: Url,
    blacklist: Vec<Box<str>>,
    delta_cache: PathBuf,
    multi: MultiProgress,
) -> anyhow::Result<(u64, u64)> {
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
            if blacklist.iter().any(|e| **e == *name) {
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
                use memmap2::Mmap;
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
                Some((pkg, oldpkg, oldfile, (dec_size as u64)))
            } else {
                info!("no cached package found, leaving {} for pacman", filename);
                None
            }
        })
        .collect();
    lines.sort_unstable_by_key(|(_, _, _, size)| std::cmp::Reverse(*size));

    info!("downloading {} updates", lines.len());

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("could not build async runtime")
        .block_on(async {
            let maxpar = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
            let maxpar_req = std::sync::Arc::new(tokio::sync::Semaphore::new(5));
            let maxpar_inflight = std::sync::Arc::new(tokio::sync::Semaphore::new(20));
            let client = reqwest::Client::new();

            let total_pg = ProgressBar::new(0)
                .with_style(
                    ProgressStyle::with_template(
                        "#### {msg} [{wide_bar}] ~{bytes}/{total_bytes} elapsed: {elapsed} ####",
                    )
                    .unwrap(),
                )
                .with_message("total progress:");
            let total_pg = multi.add(total_pg);
            total_pg.tick();
            total_pg.enable_steady_tick(Duration::from_millis(100));

            let mut set = JoinSet::new();
            for (newpkg, oldpkg, oldfile, mut dec_size) in lines {
                let client = client.clone();
                let maxpar = maxpar.clone();
                let maxpar_req = maxpar_req.clone();
                let maxpar_inflight = maxpar_inflight.clone();
                let server = server.clone();
                let multi = multi.clone();
                let delta_cache = delta_cache.clone();
                let total_pg = total_pg.clone();
                set.spawn(async move {
                    debug!("spawned task for {}", newpkg.get_name());
                    let filefut = async {
                        total_pg.inc_length(dec_size);

                        // delta download
                        let mut file_name = delta_cache.clone();
                        file_name.push(format!("{newpkg}"));

                        let delta = parsing::Delta::try_from((oldpkg.clone(), newpkg.clone()))?;

                        let mut delta = delta.to_string();
                        delta.push_str(".delta");
                        let mut deltafile_name = file_name.clone();
                        deltafile_name.set_file_name(delta);

                        let mut deltafile = tokio::fs::File::create(deltafile_name.clone()).await?;

                        let url = format!("{server}/{oldpkg}/{newpkg}");

                        let inflight_guard = maxpar_inflight.acquire().await;
                        let pg = util::do_download(
                            multi.clone(),
                            &newpkg,
                            client.clone(),
                            maxpar_req,
                            maxpar,
                            url,
                            &mut deltafile,
                        )
                        .await?;

                        info!(
                            "downloaded {} in {} seconds",
                            deltafile_name.display(),
                            pg.elapsed().as_secs_f64()
                        );
                        total_pg.inc(dec_size / 2);
                        dec_size -= dec_size / 2;

                        {
                            let file_name = file_name.clone();
                            let p_pg = ProgressBar::new(0);
                            let p_pg = multi.insert_after(&pg, p_pg);
                            pg.finish_and_clear();
                            multi.remove(&pg);
                            tokio::task::spawn_blocking(move || -> Result<_, _> {
                                let oldfile = Cursor::new(oldfile);
                                let oldfile = zstd::decode_all(oldfile).unwrap();
                                apply_patch(&oldfile, &deltafile_name, &file_name, p_pg)
                            })
                            .await??;
                        }
                        use std::os::unix::fs::MetadataExt;
                        let deltasize = deltafile.metadata().await?.size();
                        let newsize = tokio::fs::File::open(&file_name).await?.metadata().await?.size();

                        total_pg.inc(dec_size);
                        drop(inflight_guard);
                        Ok::<_, anyhow::Error>((deltasize, newsize))
                    };

                    let sigfut = async {
                        /*
                        let mut sigfile = delta_cache.clone();
                        sigfile.push(format!("{newpkg}"));
                        sigfile.as_mut_os_string().push(".sig");
                        let mut sigfile = tokio::fs::File::create(sigfile).await?;

                        let mut sigline = line.to_owned();
                        sigline.push_str(".sig");
                        debug!("getting signature from {}", sigline);
                        let mut res = client.get(&sigline).send().await?.error_for_status()?;
                        while let Some(chunk) = res.chunk().await? {
                            sigfile.write_all(&chunk).await?
                        }
                        */
                        Ok::<(), anyhow::Error>(())
                    };

                    let (f, s) = tokio::join!(filefut, sigfut);
                    let f = f.context("creating delta file failed")?;
                    s.context("creating signature file failed")?;
                    Ok::<_, anyhow::Error>(f)
                });
            }

            let mut deltasize = 0;
            let mut newsize = 0;
            let mut lasterror = None;
            while let Some(res) = set.join_next().await {
                match res.unwrap() {
                    Err(e) => {
                        error!("{}", e);
                        error!("if the error is temporary, you can try running the command again");
                        lasterror = Some(e);
                    }
                    Ok((d, n)) => {
                        deltasize += d;
                        newsize += n;
                    }
                }
            }
            lasterror.ok_or((deltasize, newsize)).swap()
        })
}

trait ResultSwap<T, E> {
    fn swap(self: Self) -> Result<E, T>;
}

impl<T, E> ResultSwap<T, E> for Result<T, E> {
    fn swap(self: Self) -> Result<E, T> {
        match self {
            Ok(t) => Err(t),
            Err(e) => Ok(e),
        }
    }
}

type Str = Box<str>;
type PackageVersions = HashMap<Str, Vec<(Str, Str, Str, PathBuf)>>;

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

#[cfg(feature = "diff")]
fn gen_delta(orig: &Path, new: &Path, patch: &Path) -> Result<(), std::io::Error> {
    let orig = OpenOptions::new().read(true).open(orig).unwrap();
    let mut orig = zstd::Decoder::new(orig)?;
    let new = OpenOptions::new().read(true).open(new).unwrap();
    let mut new = zstd::Decoder::new(new)?;

    let patch = OpenOptions::new().write(true).create(true).open(patch).unwrap();
    let mut patch = zstd::Encoder::new(patch, 22)?;

    ddelta::generate_chunked(&mut orig, &mut new, &mut patch, None, |_| {}).unwrap();
    patch.do_finish()?;

    Ok(())
}

fn apply_patch(orig: &[u8], patch: &Path, new: &Path, pb: ProgressBar) -> Result<(), std::io::Error> {
    let style = ProgressStyle::with_template("{prefix} {wide_bar} {bytes}/{total_bytes} {binary_bytes_per_sec} {eta}")
        .unwrap()
        .progress_chars("█▇▆▅▄▃▂▁  ");

    pb.set_style(style);
    pb.set_prefix(format!("patching {}", new.file_name().unwrap().to_string_lossy()));

    pb.set_length(orig.len().try_into().unwrap());
    let orig = Cursor::new(orig);
    let mut orig = pb.wrap_read(orig);
    orig.progress.tick();

    let patch = OpenOptions::new().read(true).open(patch)?;
    //let patchlen = patch.metadata()?.len();
    let mut patch = zstd::Decoder::new(patch)?;

    // fixme: while the patches are perfect the compression is not bit identical
    // can try to set parallelity in zstd lib?
    // cat patched.tar | zstd -c -T0 --ultra -20 > patched.tar.zstd
    {
        let z = Command::new("zstd")
            .args(["-c", "-T0", "--ultra", "-20"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;

        // gotta drain stdout into file while at the same time writing into stdin
        // otherwise the internal buffer of stdin/out is full and it deadlocks
        let mut new_f = OpenOptions::new().write(true).create(true).open(new)?;
        let handle = std::thread::spawn(move || -> std::io::Result<()> {
            // fixme: maybe this is why the zstd process stays around as zombie?
            std::io::copy(&mut z.stdout.unwrap(), &mut new_f)?;
            Ok(())
        });
        ddelta::apply_chunked(&mut orig, &mut z.stdin.unwrap(), &mut patch).unwrap();
        handle.join().unwrap()?;
    }
    orig.progress.finish_and_clear();
    info!("patched {}", new.file_name().unwrap().to_string_lossy());

    Ok(())
}
