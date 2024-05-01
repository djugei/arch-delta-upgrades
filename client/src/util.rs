use anyhow::bail;
use http::StatusCode;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, trace};
use parsing::Package;
use reqwest::Client;
use tokio::{
    io::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    pin,
    sync::Semaphore,
};

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
