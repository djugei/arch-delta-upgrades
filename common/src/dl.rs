use std::{borrow::Cow, sync::Arc};

use indicatif::ProgressBar;
use reqwest::{header::CONTENT_RANGE, Client, StatusCode, Url};
use thiserror::Error;
use tokio::{
    io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    sync::{AcquireError, Semaphore},
};

/// Retries Op,
/// if it errors Check determines if another try should happen
pub async fn retry<OpState, Op, CheckState, Check, Value, Error>(
    mut op_state: &mut OpState,
    mut op: Op,
    check_state: &mut CheckState,
    mut check: Check,
) -> Result<Value, Error>
where
    Op: AsyncFnMut(&mut OpState) -> Result<Value, Error>,
    Check: FnMut(&mut CheckState, &Error) -> bool,
{
    loop {
        match (op)(&mut op_state).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if !(check)(check_state, &e) {
                    return Err(e);
                }
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum DLError {
    #[error("{0}")]
    Network(#[from] reqwest::Error),
    #[error("{0}")]
    IO(#[from] std::io::Error),
    #[error("{0}")]
    Concurency(#[from] AcquireError),
    #[error("{0}")]
    Other(Cow<'static, str>),
}

impl From<String> for DLError {
    fn from(value: String) -> Self {
        Self::Other(value.into())
    }
}
impl From<&'static str> for DLError {
    fn from(value: &'static str) -> Self {
        Self::Other(value.into())
    }
}

pub struct Limits {
    pub maxpar_dl: Arc<Semaphore>,
    pub maxpar_req: Arc<Semaphore>,
}

#[test]
fn test_dl_body() {
    let limit = Limits {
        maxpar_dl: Semaphore::new(1).into(),
        maxpar_req: Semaphore::new(1).into(),
    };
    let client = reqwest::Client::new();
    let pg = ProgressBar::new(0);
    let url: Url = Url::parse("http://localhost:1/index.html").unwrap();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("could not build async runtime")
        .block_on(async {
            let file = tokio::fs::File::create("lol").await.unwrap();
            let dl = dl_body(limit, client, pg, url, file).await;
            assert!(dl.is_err())
        })
}

/// downloads a file from url, writes into target
//TODO: return number of bytes loaded
pub async fn dl_body<W>(global: Limits, client: Client, pg: ProgressBar, url: Url, target: W) -> Result<(), DLError>
where
    W: AsyncWrite + AsyncSeek + Unpin,
{
    async fn try_dl_body<W>(
        &mut (ref global, client, pg, ref url, ref mut target): &mut (Limits, &Client, &ProgressBar, Url, W),
    ) -> Result<(), DLError>
    where
        W: AsyncWrite + AsyncSeek + Unpin,
    {
        //TODO: progress bar handling
        let write_offset = target.seek(std::io::SeekFrom::End(0)).await?;
        pg.set_position(write_offset);
        let mut body = get_header(global, client, pg, url, 0).await?;
        pg.set_length(body.content_length().map(|c| c + write_offset).unwrap_or(0));

        pg.set_prefix("ratelimit(d)");

        // Acquire guard after sending request but before using the body
        // so the deltas can get generated on the server as parallel as possible
        // but the download does not get fragmented/overwhelmed
        let guard = global.maxpar_dl.acquire().await?;
        pg.set_prefix("downloading");

        match body.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (),
            StatusCode::NOT_MODIFIED => return Ok(()),
            StatusCode::RANGE_NOT_SATISFIABLE => {
                if let Some(r) = body.headers().get("content-range") {
                    let s = r.to_str().map_err(|_e| "invalid header bytes")?;
                    let (e, bytes) = s.split_once("bytes */").ok_or(format!("invalid range, s: {}", s))?;
                    if !e.is_empty() {
                        return Err(format!("invalid range, e: {}", e).into());
                    }
                    let bytes: u64 = bytes.parse().map_err(|_e| "invalid header bytes")?;
                    if bytes == write_offset {
                        return Ok(());
                    }
                }

                return Err(format!("invalid range, h: {:#?}", body.headers()).into());
            }
            s => return Err(format!("got unknown status code {}, bailing", s).into()),
        }
        let h = body.headers().get(CONTENT_RANGE);
        match (write_offset == 0, h) {
            (true, None) => (),
            (_at_start, Some(_h)) => {
                //TODO: maybe parse the range header and check that it starts at the write_offset
            }
            (false, None) => {
                target.seek(std::io::SeekFrom::Start(0)).await.unwrap();
                pg.set_position(0);
            }
        }

        // Write all chunks from the network to the disk
        loop {
            match body.chunk().await {
                Ok(Some(chunk)) => {
                    let len = chunk.len();
                    target.write_all(&chunk).await?;
                    pg.inc(len as u64);
                }
                Ok(None) => {
                    // done
                    drop(guard);
                    return Ok(());
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }
    fn dl_body_check(tries_left: &mut u8, _err: &DLError) -> bool {
        //TODO cancel early on some errors
        if *tries_left == 0 {
            return false;
        }
        *tries_left -= 1;
        true
    }

    let mut op_state = (global, &client, &pg, url, target);
    let mut tries_left = 3u8;
    retry(&mut op_state, try_dl_body, &mut tries_left, dl_body_check).await?;
    Ok(())
}

pub async fn get_header(
    global: &Limits,
    client: &Client,
    pg: &ProgressBar,
    url: &Url,
    write_offset: u64,
) -> Result<reqwest::Response, reqwest::Error> {
    async fn try_get_header(
        &mut (global, client, pg, url, ref write_offset): &mut (&Limits, &Client, &ProgressBar, &Url, u64),
    ) -> Result<reqwest::Response, reqwest::Error> {
        pg.set_prefix("ratelimit(h)");
        let guard = global.maxpar_req.acquire().await.unwrap();
        pg.set_prefix("waiting for server");
        let mut req = client.get(url.clone());
        if *write_offset != 0 {
            let range = format!("bytes={write_offset}-");
            req = req.header(reqwest::header::RANGE, range);
        }
        let resp = req.send().await;
        std::mem::drop(guard);
        resp?.error_for_status()
    }
    fn header_continue((): &mut (), err: &reqwest::Error) -> bool {
        if let Some(status) = err.status() {
            status == StatusCode::GATEWAY_TIMEOUT
                || status == StatusCode::REQUEST_TIMEOUT
                || status == StatusCode::RANGE_NOT_SATISFIABLE
                || status == StatusCode::NOT_MODIFIED
        } else {
            err.is_timeout()
        }
    }

    let mut op_state = (global, client, pg, url, write_offset);
    let mut nothing = ();
    retry(&mut op_state, try_get_header, &mut nothing, header_continue).await
}
