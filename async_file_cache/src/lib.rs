use core::future::Future;
use std::ffi::OsString;
use std::{hash::Hash, io::ErrorKind, path::PathBuf, pin::pin};
use tracing::{debug, trace};

use hashbrown::HashMap;
use tokio::io::AsyncSeekExt;
use tokio::{fs::File, sync::Mutex, sync::Semaphore};

/// State of the [FileCache]
/// you can store identifiers for having different instances of the same kind of cache
/// or HTTP-clients for connection reuse in here.
pub trait CacheState {
    /// Identifies the value to be generated,
    /// Could for example be a package name or a URL
    type Key: Eq + Hash + Clone;
    type Error;

    /// Turns the Key into a path where the computation is cached
    fn key_to_path(&self, key: &Self::Key) -> PathBuf;

    /// Generate your Value identified by the key,
    /// write it into file.
    ///
    /// You May panic.
    /// Panics are propagated and will probably crash the program,
    /// but the cache state should be unaffected.
    ///
    /// Errors and panics are assumed to be spurious and will be retried by the next task attempting to access the file.
    fn gen_value(&self, key: &Self::Key, file: File) -> impl Future<Output = Result<File, Self::Error>>;
}

/**
    File-Backed Cache:
    Execute an expensive operation to generate a file and store it in on disk.
    If the same file is requested again it will be read from disk.
    If the same file is requested while being generated it will be generated only
    once (dog-piling is prevented).

    It is possible to set a max parallelism level.
    todo: streaming, currently the whole thing is generated at once
*/
pub struct FileCache<State>
where
    State: CacheState,
{
    in_flight: tokio::sync::Mutex<HashMap<State::Key, tokio::sync::watch::Receiver<()>>>,
    state: State,
    max_para: Option<Semaphore>,
}

impl<State: CacheState> FileCache<State> {
    /// Be mindful when crating multiple instances based on the same CacheState.
    /// They will access the same folder and interfere with each others operations.
    /// # Parameters:
    /// max_para: maximum number of generating operations in flight.
    pub fn new(init_state: State, max_para: Option<usize>) -> Self {
        let max_para = max_para.map(Semaphore::new);
        let in_flight = Mutex::new(HashMap::new());
        Self {
            state: init_state,
            in_flight,
            max_para,
        }
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    /// Open an already generated file or,
    /// in case it does not exist yet,
    /// generate it.
    ///
    /// The outer result is from io operations conducted by FileCache,
    /// the inner one is for [StateCache::Error].
    pub async fn get_or_generate(&self, key: State::Key) -> std::io::Result<Result<File, State::Error>> {
        let mut oo = tokio::fs::OpenOptions::new();
        let read = oo.read(true).write(false).create(false);
        let mut oo = tokio::fs::OpenOptions::new();
        let create = oo.read(true).write(true).create(true);

        loop {
            let mut in_flight = self.in_flight.lock().await;
            use hashbrown::hash_map::EntryRef;
            match (*in_flight).entry_ref(&key) {
                EntryRef::Occupied(mut entry) => {
                    let mut e = entry.get_mut().clone();
                    drop(in_flight);
                    let path = self.state.key_to_path(&key);
                    debug!("waiting {:?}", path);
                    match e.changed().await {
                        Ok(()) => {
                            //TODO: only open the file once, ever
                            // this is a slight race-condition where the file could be deleted from the fs in between this and the previous line currently
                            // weird but ok(ok()))
                            return Ok(Ok(read.open(path).await?));
                        }
                        Err(_) => {
                            // Sender has been dropped which means either the generation function
                            // panicked internally or the whole future has been dropped in flight.
                            // We need to remove the entry and file and try again.

                            //FIXME: doesn't this file need to be deleted while locking the hashmap?
                            if let Err(e) = tokio::fs::remove_file(path).await {
                                // Not found is fine that is what we want
                                if e.kind() != ErrorKind::NotFound {
                                    return Err(e);
                                }
                            };
                            // I hope this locking logic is sound:
                            // if multiple threads detect a crashed channel then only the first
                            // once has an equal channel
                            // other ones get either an empty entry or a new entry with a different
                            // channel
                            let mut in_flight = self.in_flight.lock().await;
                            if let EntryRef::Occupied(entry) = (*in_flight).entry_ref(&key) {
                                if e.same_channel(entry.get()) {
                                    entry.remove();
                                }
                            }
                            drop(in_flight);
                            continue;
                        }
                    }
                }
                EntryRef::Vacant(entry) => {
                    let path = self.state.key_to_path(&key);
                    match read.open(&path).await {
                        Ok(f) => {
                            debug!("exists {:?}", path);
                            return Ok(Ok(f));
                        }
                        Err(e) => {
                            if e.kind() != ErrorKind::NotFound {
                                debug!("io error {:?}", path);
                                return Err(e);
                            } else {
                                debug!("generating {:?}", path);
                                let (tx, rx) = tokio::sync::watch::channel(());
                                entry.insert_with_key(key.clone(), rx);

                                let mut part_path = path.clone();
                                part_path.as_mut_os_string().push(OsString::from(".part"));

                                let w = create.open(&part_path).await?;
                                drop(in_flight);

                                let perm = if let Some(sem) = &self.max_para {
                                    trace!("sem get {:?}", path);
                                    let s = Some(sem.acquire().await);
                                    trace!("sem gotten {:?}", path);
                                    s
                                } else {
                                    None
                                };

                                // do the expensive operation
                                let f = self.state.gen_value(&key, w);
                                let f = std::pin::pin!(f);
                                let f = f.await;
                                let _f = match f {
                                    Ok(mut f) => {
                                        let pf = pin!(f.rewind());
                                        pf.await?;
                                        f
                                    }
                                    Err(e) => {
                                        debug!("generating failed, removing {}", part_path.display());
                                        tokio::fs::remove_file(part_path).await?;
                                        return Ok(Err(e));
                                    }
                                };
                                // Operation succeeded, we move the part file to its permanent place
                                tokio::fs::rename(part_path, &path).await?;
                                let file = read.open(path).await?;

                                drop(perm);

                                // This can not panic as we hold a receiver ourselves
                                tx.send(()).expect("threading failure (snd)");
                                self.in_flight.lock().await.remove(&key);
                                return Ok(Ok(file));
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn cache_simple() {
    use futures_util::future::join_all;

    tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).init();

    struct TestState;
    impl CacheState for TestState {
        type Key = String;
        type Error = std::io::Error;

        fn key_to_path(&self, k: &Self::Key) -> PathBuf {
            PathBuf::from(k)
        }

        async fn gen_value(&self, key: &Self::Key, mut file: File) -> Result<File, Self::Error> {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if key.ends_with('f') {
                return Err(std::io::ErrorKind::TimedOut.into());
            }
            use tokio::io::AsyncWriteExt;
            file.write_all(key.as_bytes()).await?;
            Ok(file)
        }
    }

    let c = FileCache::new(TestState, None);

    let tmpdir = tempfile::TempDir::new().unwrap();
    let basepath = tmpdir.path();
    debug!("basepath: {:?}", basepath);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut futs = Vec::new();
            debug!("expecting generate");
            for i in 0..3 {
                let p = basepath.join(i.to_string());
                let fut = c.get_or_generate(p.to_str().unwrap().to_owned());
                futs.push(fut);
            }
            let f = join_all(futs);
            for e in f.await {
                e.unwrap().unwrap();
            }

            debug!("expecting cached");
            let mut futs = Vec::new();
            for i in 0..3 {
                let p = basepath.join(i.to_string());
                let fut = c.get_or_generate(p.to_str().unwrap().to_owned());
                futs.push(fut);
            }
            let f = join_all(futs);
            for e in f.await {
                e.unwrap().unwrap();
            }

            debug!("running 3 concurrent futures");
            debug!("expecting gen-wait-wait");
            let mut futs = Vec::new();
            for i in 3..6 {
                let p = basepath.join(i.to_string());
                let fut = c.get_or_generate(p.to_str().unwrap().to_owned());
                futs.push(fut);

                let p = basepath.join(i.to_string());
                let fut = c.get_or_generate(p.to_str().unwrap().to_owned());
                futs.push(fut);

                let p = basepath.join(i.to_string());
                let fut = c.get_or_generate(p.to_str().unwrap().to_owned());
                futs.push(fut);
            }
            let f = join_all(futs);
            for e in f.await {
                e.unwrap().unwrap();
            }

            debug!("running 3 concurrent futures on a failing generator");
            debug!("expecting:");
            debug!("gen wait wait fail");
            debug!("gen wait fail");
            debug!("gen fail");
            let mut futs = Vec::new();
            for i in 0..3 {
                let mut i = i.to_string();
                i.push('f');
                let p = basepath.join(i);
                let p = p.to_str().unwrap().to_owned();

                let fut = c.get_or_generate(p.clone());
                futs.push(fut);
                let fut = c.get_or_generate(p.clone());
                futs.push(fut);
                let fut = c.get_or_generate(p.clone());
                futs.push(fut);
            }
            let f = join_all(futs);
            for e in f.await {
                assert!(e.unwrap().is_err());
            }
        });
    drop(c);
    drop(tmpdir);
    //debug!("{:?}", tmpdir.into_path());
}
