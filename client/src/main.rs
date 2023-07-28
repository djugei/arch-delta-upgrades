use std::{
    fs::OpenOptions,
    io::Cursor,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
};

use clap::Parser;
use log::{debug, info};
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
    pretty_env_logger::init();
    let args = Commands::parse();

    match args {
        Commands::Delta { orig, new, patch } => {
            gen_delta(&orig, &new, &patch).unwrap();
        }
        Commands::Patch { orig, patch, new } => {
            apply_patch(&orig, &patch, &new).unwrap();
        }
        Commands::Upgrade { server } => upgrade(server).unwrap(),
    }
}

fn upgrade(server: Url) -> std::io::Result<()> {
    let upgrades = Command::new("pacman").args(["-Sup"]).output()?.stdout;
    use std::io::BufRead;
    let lines: Vec<_> = upgrades
        .lines()
        .map(|l| l.unwrap())
        .filter(|l| !l.starts_with("file"))
        .inspect(|l| debug!("{}", l))
        .collect();

    debug!("downloading {} updates", lines.len());

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
                set.spawn(async move {
                    debug!("spawned thread for {}", &line);
                    let (_, filename) = line.rsplit_once('/').unwrap();
                    if let Some((oldname, oldpath)) = newest_cached(filename).unwrap() {
                        info!("delta-download {} to {}", oldname, filename);
                        // delta download
                        let mut file_name = PathBuf::from_str("/var/cache/pacman/pkg/").unwrap();
                        file_name.push(filename);
                        let mut deltafile_name = file_name.clone();
                        deltafile_name.as_mut_os_string().push(".delta");

                        let mut deltafile = tokio::fs::File::create(deltafile_name.clone())
                            .await
                            .unwrap();

                        let mut url: String = server.into();
                        url.push('/');
                        url.push_str(&oldname);
                        url.push('/');
                        url.push_str(filename);
                        let guard = maxpar.acquire().await;
                        let mut delta = client.get(url).send().await.unwrap();

                        while let Some(chunk) = delta.chunk().await.unwrap() {
                            deltafile.write_all(&chunk).await.unwrap()
                        }
                        drop(guard);
                        tokio::task::spawn(async move {
                            debug!(
                                "applying {:?} to {:?} to get {:?}",
                                &deltafile_name, &oldpath, &file_name
                            );
                            apply_patch(&oldpath, &deltafile_name, &file_name).unwrap()
                        })
                        .await
                        .unwrap();
                    } else {
                        //conventional download
                        info!("conventional-download {}", filename);
                        let mut file = PathBuf::from_str("/var/cache/pacman/pkg/").unwrap();
                        file.push(filename);
                        let mut file = tokio::fs::File::create(file).await.unwrap();

                        // just one conventional download
                        let guard = maxpar.acquire_many(3).await;
                        let mut res = client.get(&line).send().await.unwrap();
                        while let Some(chunk) = res.chunk().await.unwrap() {
                            file.write_all(&chunk).await.unwrap()
                        }
                        drop(guard);
                    }
                    let mut sigfile = PathBuf::from_str("/var/cache/pacman/pkg/").unwrap();
                    sigfile.push(filename);
                    sigfile.as_mut_os_string().push(".sig");
                    let mut sigfile = tokio::fs::File::create(sigfile).await.unwrap();

                    let guard = maxpar.acquire().await;
                    let mut sigline = line.to_owned();
                    sigline.push_str(".sig");
                    info!("getting signature from {}", sigline);
                    let mut res = client.get(&sigline).send().await.unwrap();
                    while let Some(chunk) = res.chunk().await.unwrap() {
                        sigfile.write_all(&chunk).await.unwrap()
                    }
                    drop(guard);
                });
            }

            while let Some(res) = set.join_next().await {
                // todo: error handling: don't panic in the parallel part, instead return errors
                // and retry here
                res.unwrap()
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

fn apply_patch(orig: &Path, patch: &Path, new: &Path) -> Result<(), std::io::Error> {
    let orig = OpenOptions::new().read(true).open(orig)?;
    let orig = zstd::decode_all(orig)?;
    let mut orig = Cursor::new(orig);

    let patch = OpenOptions::new().read(true).open(patch)?;
    //let patchlen = patch.metadata()?.len();
    let mut patch = zstd::Decoder::new(patch)?;

    // fixme: while the patches are perfect the compression is not bit identical
    // can try to set parallelity in zstd lib?
    // cat patched.tar | zstd -c -T0 --ultra -20 > patched.tar.zstd
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

    Ok(())
}
