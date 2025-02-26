mod util;

use anyhow::Context;
use bytesize::ByteSize;
use clap::Parser;
use common::Package;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, error, info, trace};
use memmap2::Mmap;
use reqwest::{Client, Url};
use std::{
    fs::OpenOptions,
    io::{Cursor, ErrorKind},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::{io::AsyncWriteExt, sync::Semaphore, task::JoinSet};

type Str = Box<str>;

#[derive(Parser, Debug)]
#[command(version, about)]
enum Commands {
    /// Run an entire upgrade, calling pacman interally, needs sudo to run.
    Upgrade {
        server: Url,
        #[arg(default_values_t = [Str::from("linux"), Str::from("blas"), Str::from("lapack")])]
        blacklist: Vec<Box<str>>,
        /// Use pacman for syncing the databases,
        /// if unset/by default uses delta-enabled sync.
        #[arg(long)]
        pacman_sync: bool,
        /// Disable fuzzy search if an exact package can not be found.
        /// This might find renames like -qt5 to -qt6
        #[arg(long)]
        no_fuz: bool,
        /// Only do delta, downloads,
        /// leave other upgrades to pacman
        #[arg(long)]
        only_delta: bool,
    },
    /// Upgrade the databases using deltas, ~= pacman -Sy
    //TODO: target directory/rootless mode
    Sync { server: Url },
    /// Download the newest packages to the provided delta_cache path
    ///
    /// If delta_cache is somewhere you can write, no sudo is needed.
    ///
    /// ```bash
    /// // todo make this rootless
    /// $ sudo deltaclient sync http://bogen.moeh.re/
    /// ## or
    /// $ sudo pacman -Sy
    ///
    /// $ deltaclient download http://bogen.moeh.re/ path
    /// $ sudo cp path/*.pkg path/*.sig /var/cache/pacman/pkg/
    /// ## or
    /// $ sudo deltaclient download http://bogen.moeh.re/ /var/cache/pacman/pkg/
    ///
    /// $ sudo pacman -Su
    /// ```
    ///
    /// If you are doing a full sysupgrade try the upgrade subcommand for more comfort.
    #[command(verbatim_doc_comment)]
    Download {
        server: Url,
        delta_cache: PathBuf,
        /// Disable fuzzy search if an exact package can not be found.
        /// This might find renames like -qt5 to -qt6
        #[arg(long)]
        no_fuz: bool,
        /// Only do delta, downloads,
        /// leave other upgrades to pacman
        #[arg(long)]
        only_delta: bool,
    },
    /// Calculate and display some statistics about effectiveness of the delta-encoding
    Stats {
        /// Number of best/worst entries to display
        number: Option<usize>,
    },
    #[cfg(feature = "diff")]
    /// Debug: generate a delta
    Delta {
        orig: PathBuf,
        new: PathBuf,
        patch: PathBuf,
    },
    /// Debug: manually apply a patch
    Patch {
        orig: PathBuf,
        patch: PathBuf,
        new: PathBuf,
    },
}

fn main() {
    //TODO: if set use the environment variable, else check the command line for -v flags (-v=debug -vv=trace -q=warn -qq=error).
    //      make a struct Cmd { verbosity: u8, task: Command}.
    //      Can utilize clap_verbosity_flag for this.
    //
    // Set up a logger that does not conflict with progress bars
    let logger = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).build();
    let multi = MultiProgress::new();
    let level = logger.filter();

    indicatif_log_bridge::LogWrapper::new(multi.clone(), logger)
        .try_init()
        .expect("initializing logger failed");
    log::set_max_level(level);

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
        Commands::Upgrade {
            server,
            pacman_sync,
            blacklist,
            no_fuz,
            only_delta,
        } => full_upgrade(multi, server, pacman_sync, blacklist, no_fuz, only_delta),
        Commands::Download {
            server,
            delta_cache,
            no_fuz,
            only_delta,
        } => {
            std::fs::create_dir_all(&delta_cache).unwrap();
            mkruntime()
                .block_on(do_upgrade(server, vec![], delta_cache, multi, !no_fuz, only_delta))
                .unwrap();
        }
        Commands::Sync { server } => {
            mkruntime().block_on(sync(server, multi)).unwrap();
        }
        Commands::Stats { number: count } => util::calc_stats(count.unwrap_or(5)).unwrap(),
    }
}

async fn sync(server: Url, multi: MultiProgress) -> anyhow::Result<()> {
    let server = server.join("archdb/")?;
    let client = Client::new();
    //TODO sync databases configured in /etc/pacman.conf
    let (_core, _extra, _multilib) = tokio::try_join!(
        util::sync_db(server.clone(), "core".into(), client.clone(), multi.clone()),
        util::sync_db(server.clone(), "extra".into(), client.clone(), multi.clone()),
        util::sync_db(server.clone(), "multilib".into(), client, multi),
    )?;
    Ok(())
}

fn full_upgrade(
    multi: MultiProgress,
    server: Url,
    pacman_sync: bool,
    blacklist: Vec<Box<str>>,
    no_fuz: bool,
    only_delta: bool,
) {
    let runtime = mkruntime();
    if pacman_sync {
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
    } else {
        info!("syncing databases");
        runtime.block_on(sync(server.clone(), multi.clone())).unwrap();
    }
    let cachepath = PathBuf::from_str("/var/cache/pacman/pkg").unwrap();
    let (deltasize, newsize, comptime) = match runtime
        .block_on(async move { do_upgrade(server, blacklist, cachepath, multi.clone(), !no_fuz, only_delta).await })
    {
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

fn mkruntime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("could not build async runtime")
}

async fn do_upgrade(
    server: Url,
    blacklist: Vec<Str>,
    delta_cache: PathBuf,
    multi: MultiProgress,
    fuz: bool,
    only_delta: bool,
) -> anyhow::Result<(u64, u64, Option<Duration>)> {
    let (upgrade_candidates, downloads) = util::find_deltaupgrade_candidates(&blacklist, fuz)?;
    info!("downloading {} updates", upgrade_candidates.len());

    let maxpar_req = Arc::new(Semaphore::new(5));
    let maxpar_dl = Arc::new(Semaphore::new(1));
    let parallel = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    trace!("setting cpu parallelity to {parallel}");
    let maxpar_cpu = Arc::new(Semaphore::new(parallel));
    let client = reqwest::Client::new();

    let total_pg = ProgressBar::new(0).with_style(
        ProgressStyle::with_template("#### total: [{wide_bar}] ~{bytes}/{total_bytes} elapsed: {elapsed} ####")
            .unwrap(),
    );
    let total_pg = multi.add(total_pg);
    total_pg.tick();
    total_pg.enable_steady_tick(Duration::from_millis(100));

    let global = GlobalState {
        multi,
        total_pg,
        maxpar_req,
        maxpar_dl,
        maxpar_cpu,
        client: client.clone(),
    };

    let localset = tokio::task::LocalSet::new();
    let mut set = JoinSet::new();
    for (url, newpkg, oldpkg, oldfile, dec_size) in upgrade_candidates {
        let get_delta_f = get_delta(
            global.clone(),
            newpkg.clone(),
            oldpkg.clone(),
            oldfile,
            dec_size.clone(),
            delta_cache.clone(),
            server.clone(),
        );
        let get_sig_f = get_signature(url.into(), client.clone(), delta_cache.clone(), newpkg.clone());

        set.spawn_local_on(
            async move {
                let (f, s) = tokio::join!(get_delta_f, get_sig_f);
                let f = f.context("creating delta file failed")?;
                if let Err(e) = s.context("creating signature file failed") {
                    error!("{e}");
                    error!("continuing")
                };
                Ok::<_, anyhow::Error>(f)
            },
            &localset,
        );
        debug!("spawned task for {}", newpkg.get_name());
    }

    let mut dlset = JoinSet::new();
    if !only_delta {
        for url in downloads {
            let boring_dl = util::do_boring_download(global.clone(), url.clone(), delta_cache.clone());
            let name = url.path_segments().and_then(Iterator::last).context("malformed url")?;
            let pkg = Package::try_from(name).unwrap();
            let get_sig_f = get_signature(url.as_str().into(), client.clone(), delta_cache.clone(), pkg);
            dlset.spawn_local_on(
                async move {
                    let (f, s) = tokio::join!(boring_dl, get_sig_f);
                    let f = f.context("boring dl failed")?;
                    if let Err(e) = s.context("creating signature file failed") {
                        error!("{e}");
                        error!("continuing")
                    };
                    Ok::<_, anyhow::Error>(f)
                },
                &localset,
            );
        }
    }

    let mut deltasize = 0;
    let mut newsize = 0;
    let mut comptime = Some(Duration::from_secs(0));
    let mut lasterror = None;

    localset.await;

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
    while let Some(res) = dlset.join_next().await {
        match res.unwrap() {
            Ok(s) => {
                newsize += s;
                deltasize += s
            }
            Err(e) => {
                error!("{:?}", e);
                for cause in e.chain() {
                    error!("caused by: {:#}", cause);
                }
                error!("if the error is temporary, you can try running the command again");
                lasterror = Some(e);
            }
        }
    }

    lasterror.ok_or((deltasize, newsize, comptime)).swap()
}

#[derive(Clone)]
pub(crate) struct GlobalState {
    multi: MultiProgress,
    total_pg: ProgressBar,
    maxpar_req: Arc<Semaphore>,
    maxpar_dl: Arc<Semaphore>,
    maxpar_cpu: Arc<Semaphore>,
    client: Client,
}

//TODO: maybe have less parameters somehow?
async fn get_delta(
    global: GlobalState,
    newpkg: Package,
    oldpkg: Package,
    oldfile: Mmap,
    mut dec_size: u64,
    delta_cache: PathBuf,
    server: Url,
) -> Result<(u64, u64, Option<Duration>), anyhow::Error> {
    global.total_pg.inc_length(dec_size);
    let mut file_name = delta_cache.clone();
    file_name.push(newpkg.to_string());
    let delta = common::Delta::try_from((oldpkg.clone(), newpkg.clone()))?;
    let deltafile_name = file_name.with_file_name(format!("{delta}.delta"));
    let mut deltafile = tokio::fs::File::options()
        .write(true)
        .truncate(false)
        .create(true)
        .open(deltafile_name.clone())
        .await
        .unwrap();
    let url = server.join(&format!("/arch/{oldpkg}/{newpkg}"))?;
    let pg = util::do_delta_download(global.clone(), &newpkg, url.clone(), &mut deltafile).await?;
    info!(
        "downloaded {} in {} seconds",
        deltafile_name.display(),
        pg.elapsed().as_secs_f64()
    );
    global.total_pg.inc(dec_size / 2);
    // To not lose 1 in case of integer division round-down
    dec_size -= dec_size / 2;
    let comptime;
    {
        let file_name = file_name.clone();
        let p_pg = ProgressBar::new(0);
        let p_pg = global.multi.insert_after(&pg, p_pg);
        pg.finish_and_clear();
        global.multi.remove(&pg);
        let guard_cpu = global.maxpar_cpu.acquire_owned().await;
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
    global.total_pg.inc(dec_size);
    Ok((deltasize, newsize, comptime))
}

async fn get_signature(url: Str, client: Client, delta_cache: PathBuf, newpkg: Package) -> Result<(), anyhow::Error> {
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
    Ok(())
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

    ddelta::generate_chunked(&mut orig, &mut new, &mut patch, None, |p| debug!("{p:?}")).unwrap();
    patch.do_finish()?;

    Ok(())
}

fn apply_patch(orig: &[u8], patch: &Path, new: &Path, pb: ProgressBar) -> anyhow::Result<Option<Duration>> {
    pb.set_style(util::progress_style());
    pb.set_prefix("patching");
    pb.set_message(new.file_name().unwrap().to_string_lossy().into_owned());

    pb.set_length(orig.len().try_into().unwrap());
    let orig = Cursor::new(orig);
    let mut orig = pb.wrap_read(orig);
    orig.progress.tick();

    let patch = OpenOptions::new().read(true).open(patch)?;
    //let patchlen = patch.metadata()?.len();
    let mut patch = zstd::Decoder::new(patch)?;

    // There is one arch maintainer that sometimes changes the compression options within the PKGBUILD
    // for minor compression gains, usually in the order of a few bytes to a kilobyte.
    // Cost is this special casing and higher memory and compute costs for compression _and_ decompression.
    // tbh besides this code being annoying to add
    // I am a little confused about there being a big mailing list thread with research and data
    // about the specific compression parameters and space time trade-offs.
    // Which then gets overridden without so much as a mention in the commit message.
    // This is also the only time the compression parameters are altered.
    // That's pretty hefty.
    let filename = new.file_name().unwrap().to_string_lossy();
    let pkg = Package::try_from(filename.as_ref()).unwrap();
    let mut command = Command::new("zstd");
    if [
        "rust",
        "lib32-rust-libs",
        "rust-musl",
        "rust-aarch64-gnu",
        "rust-aarch64-musl",
        "rust-wasm",
        "rust-src",
        "js91",
        "js115",
        "js128",
    ]
    .contains(&pkg.get_name())
    {
        debug!("using alternative zstd compression parameters for {}", pkg.get_name());
        command.args(["-c", "-T0", "--ultra", "-20", "--long", "-"]);
    } else {
        command.args(["-c", "-T0", "--ultra", "-20", "-"]);
    };

    // fixme: while the patches are perfect the compression is not bit identical
    // can try to set parallelity in zstd lib?
    // cat patched.tar | zstd -c -T0 --ultra -20 > patched.tar.zstd
    let comptime;
    {
        let mut z = command.stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;

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

#[test]
fn testurl() {
    let url = Url::parse("http://bogen.moeh.re/").unwrap();
    let url = url.join("arch/").unwrap();
    let url = url.join("a/").unwrap();
    dbg!(&url);
    assert_eq!(url.path(), "/arch/a/");

    let url = Url::parse("http://bogen.moeh.re/").unwrap();
    let url = url.join("arch").unwrap();
    let url = url.join("a/").unwrap();
    dbg!(&url);
    assert_eq!(url.path(), "/a/");
    dbg!(&url.join("file"));
}
