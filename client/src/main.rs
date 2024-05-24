mod util;

use anyhow::Context;
use bytesize::ByteSize;
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, error, info, trace};
use reqwest::Url;
use std::{
    fs::OpenOptions,
    io::{Cursor, ErrorKind},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
    time::Duration,
};
use tokio::{io::AsyncWriteExt, task::JoinSet};

type Str = Box<str>;

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
            let (deltasize, newsize, comptime) = match upgrade(server, blacklist, cachepath, multi) {
                Ok((d, n, c)) => (d, n, c),
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
            if let Some(comptime) = comptime {
                debug!(
                    "wasted {:?} on compression since arch does not provide uncompressed signatures yet",
                    comptime
                );
            }

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
    blacklist: Vec<Str>,
    delta_cache: PathBuf,
    multi: MultiProgress,
) -> anyhow::Result<(u64, u64, Option<Duration>)> {
    let upgrade_candidates = util::find_deltaupgrade_candidates(&blacklist)?;
    info!("downloading {} updates", upgrade_candidates.len());

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("could not build async runtime")
        .block_on(async {
            let maxpar_req = std::sync::Arc::new(tokio::sync::Semaphore::new(5));
            let maxpar_dl = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
            let parallel = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
            trace!("setting cpu parallelity to {parallel}");
            let maxpar_cpu = std::sync::Arc::new(tokio::sync::Semaphore::new(parallel));
            let client = reqwest::Client::new();

            let total_pg = ProgressBar::new(0).with_style(
                ProgressStyle::with_template("#### total: [{wide_bar}] ~{bytes}/{total_bytes} elapsed: {elapsed} ####")
                    .unwrap(),
            );
            let total_pg = multi.add(total_pg);
            total_pg.tick();
            total_pg.enable_steady_tick(Duration::from_millis(100));

            let mut set = JoinSet::new();
            for (url, newpkg, oldpkg, oldfile, mut dec_size) in upgrade_candidates {
                let multi = multi.clone();
                let total_pg = total_pg.clone();

                let maxpar_req = maxpar_req.clone();
                let maxpar_dl = maxpar_dl.clone();
                let maxpar_cpu = maxpar_cpu.clone();

                let client = client.clone();
                let delta_cache = delta_cache.clone();
                let server = server.clone();

                set.spawn(async move {
                    debug!("spawned task for {}", newpkg.get_name());
                    let filefut = async {
                        total_pg.inc_length(dec_size);

                        // delta download
                        let mut file_name = delta_cache.clone();
                        file_name.push(format!("{newpkg}"));

                        let delta = parsing::Delta::try_from((oldpkg.clone(), newpkg.clone()))?;
                        let deltafile_name = file_name.with_file_name(format!("{delta}.delta"));

                        let mut deltafile = tokio::fs::File::create(deltafile_name.clone()).await?;

                        let url = format!("{server}/{oldpkg}/{newpkg}");

                        let pg = util::do_download(
                            multi.clone(),
                            &newpkg,
                            client.clone(),
                            maxpar_req,
                            maxpar_dl,
                            url.clone(),
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

                        let comptime;
                        {
                            let file_name = file_name.clone();
                            let p_pg = ProgressBar::new(0);
                            let p_pg = multi.insert_after(&pg, p_pg);
                            pg.finish_and_clear();
                            multi.remove(&pg);
                            let guard_cpu = maxpar_cpu.acquire_owned().await;
                            comptime = tokio::task::spawn_blocking(move || -> Result<_, _> {
                                let oldfile = Cursor::new(oldfile);
                                let oldfile = zstd::decode_all(oldfile).unwrap();
                                apply_patch(&oldfile, &deltafile_name, &file_name, p_pg)
                            })
                            .await??;
                            drop(guard_cpu);
                        }
                        use std::os::unix::fs::MetadataExt;
                        let deltasize = deltafile.metadata().await?.size();
                        let newsize = tokio::fs::File::open(&file_name).await?.metadata().await?.size();

                        total_pg.inc(dec_size);
                        Ok::<_, anyhow::Error>((deltasize, newsize, comptime))
                    };

                    let sigfut = async {
                        let mut sigfile = delta_cache.clone();
                        sigfile.push(format!("{newpkg}.sig"));
                        let mut sigfile = match tokio::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(sigfile)
                            .await
                        {
                            // the sigfile already exists, probably from a previous run, do nothing
                            Err(e) if e.kind() == ErrorKind::AlreadyExists => return Ok(()),
                            Err(e) => return Err(anyhow::Error::from(e)),
                            Ok(f) => f,
                        };

                        let sigurl = format!("{url}.sig");
                        debug!("getting signature from {}", url);
                        let mut res = client.get(&sigurl).send().await?.error_for_status()?;
                        while let Some(chunk) = res.chunk().await? {
                            sigfile.write_all(&chunk).await?
                        }
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
            let mut comptime = Some(Duration::from_secs(0));
            let mut lasterror = None;
            while let Some(res) = set.join_next().await {
                match res.unwrap() {
                    Err(e) => {
                        error!("{:?}", e);
                        for cause in e.chain() {
                            error!("caused by: {:#}", cause);
                        }
                        error!("if the error is temporary, you can try running the command again");
                        lasterror = Some(e);
                    }
                    Ok((d, n, c)) => {
                        deltasize += d;
                        newsize += n;
                        if let Some(c) = c {
                            comptime.as_mut().map(|sum: &mut Duration| *sum += c);
                        }
                    }
                }
            }
            lasterror.ok_or((deltasize, newsize, comptime)).swap()
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

fn apply_patch(orig: &[u8], patch: &Path, new: &Path, pb: ProgressBar) -> anyhow::Result<Option<Duration>> {
    pb.set_style(util::progress_style());
    pb.set_prefix("patching");
    pb.set_message(new.file_name().unwrap().to_string_lossy().into_owned());

    pb.set_length(orig.len().try_into().unwrap());
    let orig = Cursor::new(orig);
    let mut orig = pb.wrap_read(orig).with_hold_max(true);
    orig.progress.tick();

    let patch = OpenOptions::new().read(true).open(patch)?;
    //let patchlen = patch.metadata()?.len();
    let mut patch = zstd::Decoder::new(patch)?;

    // fixme: while the patches are perfect the compression is not bit identical
    // can try to set parallelity in zstd lib?
    // cat patched.tar | zstd -c -T0 --ultra -20 > patched.tar.zstd
    let comptime;
    {
        let mut z = Command::new("zstd")
            .args(["-c", "-T0", "--ultra", "-20", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;

        let mut stdin = z.stdin.take().unwrap();
        let mut stdout = z.stdout.take().unwrap();

        // gotta drain stdout into file while at the same time writing into stdin
        // otherwise the internal buffer of stdin/out is full and it deadlocks
        let mut new_f = OpenOptions::new().write(true).create(true).open(new)?;
        let handle = std::thread::spawn(move || -> std::io::Result<()> {
            std::io::copy(&mut stdout, &mut new_f)?;
            Ok(())
        });
        ddelta::apply_chunked(&mut orig, &mut stdin, &mut patch).unwrap();
        drop(stdin);
        use command_rusage::Error::*;
        use command_rusage::GetRUsage;
        comptime = match z.wait_for_rusage() {
            Ok(rusage) => Some(rusage.utime + rusage.stime),
            Err(UnsupportedPlatform) => None,
            Err(Unavailable) => anyhow::bail!("zstd compression command failed"),
            Err(NoSuchProcess | NotChild) => panic!("programming error"),
        };
        trace!("zstd compression took {:?}", comptime);
        handle.join().unwrap()?;
    }
    orig.progress.finish_and_clear();
    info!("patched {}", new.file_name().unwrap().to_string_lossy());

    Ok(comptime)
}
