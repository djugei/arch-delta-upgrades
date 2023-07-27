use std::{
    fs::OpenOptions,
    io::Cursor,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use log::info;
use parsing::Package;
use reqwest::Url;

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
    Arch {
        server: Url,
        req: Url,
        tmpfile: PathBuf,
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
        Commands::Arch {
            server,
            req,
            tmpfile,
        } => arch(server, req, tmpfile).unwrap(),
    }
}

fn arch(server: Url, req: Url, tmpfile: PathBuf) -> Result<(), std::io::Error> {
    let template = "{msg} {wide_bar} {percent}% eta: {eta}";
    let style = ProgressStyle::with_template(template).unwrap();
    let message: String;

    // do regular requests to the regular mirrors
    if req.path().ends_with(".db") | req.path().ends_with(".sig") {
        message = format!("Downloading {}", req.path());
    } else if req.path().ends_with(".tar.zst") {
        let (_, filename) = req.path().rsplit_once('/').unwrap();
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
            return Ok(());
        }
        let newest = local.pop();
        if let Some((old, oldpath)) = newest {
            info!("requesting {} to {}", old, filename);

            let mut url: String = server.into();
            url.push('/');
            url.push_str(&old);
            url.push('/');
            url.push_str(filename);
            let client = reqwest::blocking::ClientBuilder::new()
                .timeout(None)
                .build()
                .unwrap();
            let delta = client.get(url).send().unwrap();

            let mut deltapath = tmpfile.clone();
            deltapath.set_extension("delta");
            let mut deltafile = std::fs::File::create(&deltapath)?;

            let pb = ProgressBar::new(delta.content_length().unwrap_or(0));
            pb.set_message(format!("Downloading {} to {}", old, filename));
            pb.set_style(style.clone());
            let mut pbdelta = pb.wrap_read(delta);
            std::io::copy(&mut pbdelta, &mut deltafile)?;
            pb.finish();
            drop(deltafile);

            apply_patch(&oldpath, &deltapath, &tmpfile).unwrap();
            return Ok(());
        } else {
            message = format!("No cache: downloading {}", filename);
        }
    } else {
        panic!("trying to download weird extension {:?}", req)
    }

    let client = reqwest::blocking::ClientBuilder::new()
        .timeout(None)
        .build()
        .unwrap();
    let body = client.get(req.clone()).send().unwrap();
    let mut file = std::fs::File::create(tmpfile)?;

    let pb = ProgressBar::new(body.content_length().unwrap_or(0));
    pb.set_message(message);
    pb.set_style(style.clone());
    let mut pbbody = pb.wrap_read(body);
    std::io::copy(&mut pbbody, &mut file)?;
    pb.finish();
    Ok(())
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
    let patchlen = patch.metadata()?.len();
    let pb = indicatif::ProgressBar::new(patchlen);
    let template = "{msg} {wide_bar} {percent}% eta: {eta}";
    let style = ProgressStyle::with_template(template).unwrap();
    pb.set_style(style);
    pb.set_message("Applying delta");
    let patch = pb.wrap_read(patch);
    let mut patch = zstd::Decoder::new(patch)?;

    // fixme: while the patches are perfect the compression is not bit identical
    // can try to set parallelity in zstd lib?
    // cat patched.tar | zstd -c -T0 --ultra -20 > patched.tar.zstd
    let z = Command::new("zstd")
        .args(["-c", "-T0", "--ultra", "-20"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    ddelta::apply_chunked(&mut orig, &mut z.stdin.unwrap(), &mut patch).unwrap();

    let mut new_f = OpenOptions::new().write(true).create(true).open(new)?;
    std::io::copy(&mut z.stdout.unwrap(), &mut new_f)?;
    pb.finish();

    Ok(())
}
