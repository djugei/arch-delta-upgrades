use std::{
    fs::OpenOptions,
    io::{BufRead, Cursor},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
    time::Duration,
};

use anyhow::Context;

use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, error, info};
use parsing::Package;
use reqwest::{StatusCode, Url};
use tokio::{
    io::{AsyncSeekExt, AsyncWriteExt},
    task::JoinSet,
};

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
    /// $ deltaclient http://bogen.moeh.re/arch path
    /// $ sudo cp path/*.pkg path/*.sig /var/cache/pacman/pkg/
    /// ## or
    /// $ sudo deltaclient http://bogen.moeh.re/arch /var/cache/pacman/pkg/
    ///
    /// $ sudo pacman -Su
    /// ```
    ///
    /// If you are doing a full sysupgrade try the upgrade subcommand for more comfort.
    #[command(verbatim_doc_comment)]
    Download { server: Url, delta_cache: PathBuf },
    /// run an entire upgrade, calling pacman interally, needs sudo to run.
    Upgrade { server: Url },
}

fn main() {
    // set up a logger that does not conflict with progress bars
    let logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).build();
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
        Commands::Upgrade { server } => {
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
            upgrade(server, cachepath, multi).unwrap();

            info!("running pacman -Su to install updates");
            let exit = Command::new("pacman")
                .args(["-Su", "--noconfirm"])
                .spawn()
                .expect("could not run pacman -Su")
                .wait()
                .expect("error waiting for pacman -Su");
            if !exit.success() {
                panic!("pacman -Su failed, aborting. Fix errors and try again");
            }
        }
        Commands::Download {
            server,
            delta_cache,
        } => {
            std::fs::create_dir_all(&delta_cache).unwrap();
            upgrade(server, delta_cache, multi).unwrap()
        }
    }
}

fn upgrade(server: Url, delta_cache: PathBuf, multi: MultiProgress) -> anyhow::Result<()> {
    let upgrades = Command::new("pacman").args(["-Sup"]).output()?.stdout;
    let lines: Vec<_> = upgrades
        .lines()
        .map(|l| l.unwrap())
        .filter(|l| !l.starts_with("file"))
        .inspect(|l| debug!("{}", l))
        .collect();

    info!("downloading {} updates", lines.len());

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("could not build async runtime")
        .block_on(async {
            let maxpar = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
            let maxpar_req = std::sync::Arc::new(tokio::sync::Semaphore::new(5));
            let client = reqwest::Client::new();

            let total_pg = ProgressBar::new(0)
                .with_style(ProgressStyle::with_template("#### {msg} [{wide_bar}] {bytes}/{total_bytes} elapsed: {elapsed} eta: {eta} ####").unwrap())
                .with_message("total progress:")
                ;
            total_pg.enable_steady_tick(Duration::from_millis(100));
            let total_pg = multi.add(total_pg);
            total_pg.tick();


            let mut set = JoinSet::new();
            for line in lines {
                let client = client.clone();
                let maxpar = maxpar.clone();
                let maxpar_req = maxpar_req.clone();
                let server = server.clone();
                let multi = multi.clone();
                let delta_cache = delta_cache.clone();
                let total_pg = total_pg.clone();
                set.spawn(async move {
                    debug!("spawned thread for {}", &line);
                    let (_, filename) = line.rsplit_once('/').unwrap();
                    let filefut = async {
                    if let Some((oldname, oldpath)) = newest_cached(filename)? {
                        // try to find the decompressed size for better progress monitoring
                        use memmap2::Mmap;
                        let orig = std::fs::File::open(oldpath)?;
                        // i promise to not open the same file as writable at the same time
                        let orig = unsafe { Mmap::map(&orig)?};
                        // 16 megabytes seems like an ok average size
                        // todo: find the actual average size of a decompressed package
                        let default_size = 16*1024*1024;
                        let dec_size = zstd::bulk::Decompressor::upper_bound(&orig)
                            .unwrap_or_else(|| {
                                debug!("using default size for {oldname}");
                                default_size
                            });
                        total_pg.inc_length(dec_size.try_into().unwrap());

                        // delta download
                        let mut file_name = delta_cache.clone();
                        file_name.push(filename);
                        let mut deltafile_name = file_name.clone();
                        deltafile_name.as_mut_os_string().push(".delta");

                        let mut deltafile = tokio::fs::File::create(deltafile_name.clone())
                            .await?;

                        let style = ProgressStyle::with_template(
                        "{prefix}{msg} [{wide_bar}] {bytes}/{total_bytes} {binary_bytes_per_sec} {eta}").unwrap()
        .progress_chars("█▇▆▅▄▃▂▁  ");

                        let guard = maxpar_req.acquire().await;
                        let pg = ProgressBar::new(0)
                            .with_style(style)
                            .with_prefix(format!("deltadownload {}", filename))
                            .with_message(": waiting for server, this may take up to a few minutes")
                            ;
                        let pg = multi.add(pg);
                        pg.tick();

                        let mut url: String = server.into();
                        url.push('/');
                        url.push_str(&oldname);
                        url.push('/');
                        url.push_str(filename);
                        let mut delta = {
                            loop {
                                // catch both client and server timeouts and simply retry
                                match client.get(&url).send().await {
                                    Ok(d) => { match d.status() {
                                        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => {debug!("retrying request {}", url)}
                                        status if !status.is_success() => panic!("request failed with{}", status),
                                        _=> break d,
                                        }
                                    }
                                    Err(e) => if !e.is_timeout(){
                                        panic!("{}", e);
                                        },
                            }
                            debug!("timeout; retrying {}", &url);
                            }
                        };
                        std::mem::drop(guard);
                        pg.set_length(delta.content_length().unwrap_or(0));
                        pg.set_message("");
                        pg.tick();

                        // acquire guard after sending request but before using the body
                        // so the deltas can get generated on the server as parallel as possible
                        // but the download does not get fragmented/overwhelmed
                        let guard = maxpar.acquire().await;
                        let mut tries = 0;
                            'retry: loop {
                                loop {
                                    match delta.chunk().await {
                                        Ok(Some(chunk)) => {
                                            let len = chunk.len();
                                            deltafile.write_all(&chunk).await?;
                                            pg.inc(len.try_into().unwrap());
                                        },
                                        Ok(None) => break 'retry,
                                        Err(e) => {
                                            if tries <3 {
                                                deltafile.seek(std::io::SeekFrom::Start(0)).await?;
                                                pg.set_position(0);
                                                debug!("download failed {tries}/3");
                                                tries +=1;
                                                continue 'retry;
                                            } else {
                                                anyhow::bail!(e);
                                            }
                                        },
                                    }
                                }
                        }
                        drop(guard);
                        info!("downloaded {} in {} seconds", deltafile_name.display(), pg.elapsed().as_secs_f64());

                        {
                            let p_pg = ProgressBar::new(0);
                            let p_pg = multi.insert_after(&pg, p_pg);
                            pg.finish_and_clear();
                            multi.remove(&pg);
                            tokio::task::spawn_blocking(move || -> Result<_,_> {
                                let orig = Cursor::new(orig);
                                let orig = zstd::decode_all(orig).unwrap();
                                apply_patch(&orig, &deltafile_name, &file_name, p_pg)
                            })
                            .await??;
                        }
                        total_pg.inc(dec_size.try_into().unwrap());
                    } else {
                        info!("no cached package found, leaving {} for pacman", filename);
                    }
                        Ok::<(),anyhow::Error>(())
                    };

                    let sigfut = async {
                        let mut sigfile = delta_cache.clone();
                        sigfile.push(filename);
                        sigfile.as_mut_os_string().push(".sig");
                        let mut sigfile = tokio::fs::File::create(sigfile).await?;

                        let mut sigline = line.to_owned();
                        sigline.push_str(".sig");
                        debug!("getting signature from {}", sigline);
                        let mut res = client.get(&sigline).send().await?;
                        while let Some(chunk) = res.chunk().await? {
                            sigfile.write_all(&chunk).await?
                        }
                        Ok::<(),anyhow::Error>(())
                    };

                    let (f, s) = tokio::join!(filefut, sigfut);
                    f.context("creating delat file failed")?;
                    s.context("creating signature file failed")?;
                    Ok::<(),anyhow::Error>(())
                });
            }

            while let Some(res) = set.join_next().await {
                if let Err(e) = res {
                    error!("{}", e);
                    error!("if the error is temporary, you can try running the command again");

                }
            }
        });
    Ok(())
}

/// returns the newest package in /var/cache/pacman/pkg that
/// fits the passed package and is not identical to it in version
fn newest_cached(filename: &str) -> std::io::Result<Option<(String, PathBuf)>> {
    let package = Package::try_from(filename).unwrap();
    let mut local: Vec<_> = std::fs::read_dir("/var/cache/pacman/pkg/")?
        .map(|e| e.unwrap())
        .filter(|e| e.file_type().unwrap().is_file())
        .map(|e| (e.file_name().into_string().unwrap(), e.path()))
        .filter(|e| e.0.ends_with(".zst"))
        .filter(|e| {
            let p = Package::try_from(e.0.as_str()).unwrap();
            p.get_name() == package.get_name()
        })
        .collect();
    local.sort();
    if local.iter().any(|(name, _)| *name == *filename) {
        info!("{} already downloaded", filename);
        return Ok(None);
    }
    Ok(local.pop())
}

#[cfg(feature = "diff")]
fn gen_delta(orig: &Path, new: &Path, patch: &Path) -> Result<(), std::io::Error> {
    let orig = OpenOptions::new().read(true).open(orig).unwrap();
    let mut orig = zstd::Decoder::new(orig)?;
    let new = OpenOptions::new().read(true).open(new).unwrap();
    let mut new = zstd::Decoder::new(new)?;

    let patch = OpenOptions::new()
        .write(true)
        .create(true)
        .open(patch)
        .unwrap();
    let mut patch = zstd::Encoder::new(patch, 22)?;

    ddelta::generate_chunked(&mut orig, &mut new, &mut patch, None, |_| {}).unwrap();
    patch.do_finish()?;

    Ok(())
}

fn apply_patch(
    orig: &[u8],
    patch: &Path,
    new: &Path,
    pb: ProgressBar,
) -> Result<(), std::io::Error> {
    let style = ProgressStyle::with_template(
        "{prefix} {wide_bar} {bytes}/{total_bytes} {binary_bytes_per_sec} {eta}",
    )
    .unwrap()
    .progress_chars("█▇▆▅▄▃▂▁  ");

    pb.set_style(style);
    pb.set_prefix(format!(
        "patching {}",
        new.file_name().unwrap().to_string_lossy()
    ));

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
