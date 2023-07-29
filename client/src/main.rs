use std::{
    fs::OpenOptions,
    io::{BufRead, Cursor},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
    sync::Arc,
};

use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, error, info};
use parsing::Package;
use reqwest::Url;
use tokio::{io::AsyncWriteExt, task::JoinSet};

#[derive(Parser, Debug)]
#[command(version, about)]
enum Commands {
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
    Upgrade {
        server: Url,
    },
}

fn main() {
    // set up a logger that does not conflict with progress bars
    let (read, write) = pipe::pipe();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .target(env_logger::Target::Pipe(Box::new(write)))
        .init();

    let multi = Arc::new(MultiProgress::new());

    {
        let m = multi.clone();
        std::thread::spawn(move || {
            let br = std::io::BufReader::new(read);
            for line in br.lines() {
                let line = line.unwrap();
                m.println(&line).unwrap();
            }
        });
    }

    let args = Commands::parse();

    match args {
        Commands::Delta { orig, new, patch } => {
            gen_delta(&orig, &new, &patch).unwrap();
        }
        Commands::Patch { orig, patch, new } => {
            apply_patch(&orig, &patch, &new, multi).unwrap();
        }
        Commands::Upgrade { server } => upgrade(server, multi).unwrap(),
    }
}

fn upgrade(server: Url, multi: Arc<MultiProgress>) -> std::io::Result<()> {
    let upgrades = Command::new("pacman").args(["-Sup"]).output()?.stdout;
    let lines: Vec<_> = upgrades
        .lines()
        .map(|l| l.unwrap())
        .filter(|l| !l.starts_with("file"))
        .inspect(|l| debug!("{}", l))
        .collect();

    info!("downloading {} updates", lines.len());

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let maxpar = std::sync::Arc::new(tokio::sync::Semaphore::new(4));
            let client = reqwest::Client::new();

            let mut set = JoinSet::new();
            for line in lines {
                let client = client.clone();
                let maxpar = maxpar.clone();
                let server = server.clone();
                let multi = multi.clone();
                set.spawn(async move {
                    debug!("spawned thread for {}", &line);
                    let (_, filename) = line.rsplit_once('/').unwrap();
                    let filefut = async {
                    if let Some((oldname, oldpath)) = newest_cached(filename).unwrap() {
                        // delta download
                        let mut file_name = PathBuf::from_str("/var/cache/pacman/pkg/").unwrap();
                        file_name.push(filename);
                        let mut deltafile_name = file_name.clone();
                        deltafile_name.as_mut_os_string().push(".delta");

                        let mut deltafile = tokio::fs::File::create(deltafile_name.clone())
                            .await
                            .unwrap();

                        let style = ProgressStyle::with_template(
                        "{prefix}{message} {wide_bar} {bytes}/{total_bytes} {binary_bytes_per_sec} {eta}").unwrap()
        .progress_chars("█▇▆▅▄▃▂▁  ");

                        let pg = ProgressBar::new(0)
                            .with_style(style)
                            .with_prefix(format!("deltadownload {}", filename))
                            // fixme: message is not being displayed?
                            .with_message(": waiting for server, this may take up to a few minutes")
                            ;
                        let pg = multi.add(pg);
                        pg.tick();

                        let mut url: String = server.into();
                        url.push('/');
                        url.push_str(&oldname);
                        url.push('/');
                        url.push_str(filename);
                        let guard = maxpar.acquire().await;
                        let mut delta = client.get(url).send().await.unwrap();
                        pg.set_length(delta.content_length().unwrap_or(0));
                        pg.set_message("");
                        pg.tick();

                        while let Some(chunk) = delta.chunk().await.unwrap() {
                            let len = chunk.len();
                            deltafile.write_all(&chunk).await.unwrap();
                            pg.inc(len.try_into().unwrap());
                        }
                        drop(guard);
                        info!("downloaded {} in {} seconds", deltafile_name.display(), pg.elapsed().as_secs_f64());
                        pg.finish_and_clear();
                        multi.remove(&pg);

                        tokio::task::spawn(async move {
                            apply_patch(&oldpath, &deltafile_name, &file_name, multi).unwrap()
                        })
                        .await
                        .unwrap();
                    } else {
                        //conventional download
                        let mut file = PathBuf::from_str("/var/cache/pacman/pkg/").unwrap();
                        file.push(filename);
                        let mut file = tokio::fs::File::create(file).await.unwrap();


                        // just one conventional download
                        let guard = maxpar.acquire_many(3).await;
                        let mut res = client.get(&line).send().await.unwrap();

                        let style = ProgressStyle::with_template(
                        "{prefix} {wide_bar} {bytes}/{total_bytes} {binary_bytes_per_sec} {eta}").unwrap()
        .progress_chars("█▇▆▅▄▃▂▁  ");

                        let pg = ProgressBar::new(res.content_length().unwrap_or(0))
                            .with_style(style)
                            .with_prefix(format!("(regular) download {}", filename));
                        let pg = multi.add(pg);
                        pg.tick();

                        while let Some(chunk) = res.chunk().await.unwrap() {
                            let len = chunk.len();
                            file.write_all(&chunk).await.unwrap();
                            pg.inc(len.try_into().unwrap());
                        }
                        drop(guard);

                        info!("downloaded {} in {} seconds", filename, pg.elapsed().as_secs_f64());
                        pg.finish_and_clear();
                        multi.remove(&pg);
                    }
                    };

                    let sigfut = async {
                        // todo: could do this in parallel
                        let mut sigfile = PathBuf::from_str("/var/cache/pacman/pkg/").unwrap();
                        sigfile.push(filename);
                        sigfile.as_mut_os_string().push(".sig");
                        let mut sigfile = tokio::fs::File::create(sigfile).await.unwrap();

                        let mut sigline = line.to_owned();
                        sigline.push_str(".sig");
                        info!("getting signature from {}", sigline);
                        let mut res = client.get(&sigline).send().await.unwrap();
                        while let Some(chunk) = res.chunk().await.unwrap() {
                            sigfile.write_all(&chunk).await.unwrap()
                        }
                    };

                    tokio::join!(filefut, sigfut);
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
    let mut patch = zstd::Encoder::new(patch, 20)?;

    ddelta::generate_chunked(&mut orig, &mut new, &mut patch, None, |_| {}).unwrap();
    patch.do_finish()?;

    Ok(())
}

fn apply_patch(
    orig: &Path,
    patch: &Path,
    new: &Path,
    multi: Arc<MultiProgress>,
) -> Result<(), std::io::Error> {
    let style = ProgressStyle::with_template(
        "{prefix} {wide_bar} {bytes}/{total_bytes} {binary_bytes_per_sec} {eta}",
    )
    .unwrap()
    .progress_chars("█▇▆▅▄▃▂▁  ");

    let pg = ProgressBar::new(0).with_style(style).with_prefix(format!(
        "patching {}",
        new.file_name().unwrap().to_string_lossy()
    ));
    let pg = multi.add(pg);

    let orig = OpenOptions::new().read(true).open(orig)?;
    let orig = zstd::decode_all(orig)?;
    pg.set_length(orig.len().try_into().unwrap());
    let orig = Cursor::new(orig);
    let mut orig = pg.wrap_read(orig);
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
    multi.remove(&orig.progress);
    info!("patched {}", new.file_name().unwrap().to_string_lossy());

    Ok(())
}
