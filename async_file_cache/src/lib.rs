use core::future::Future;
use log::{debug, trace};
use std::ffi::OsString;
use std::{collections::HashMap, hash::Hash, io::ErrorKind, path::PathBuf, pin::pin};

use tokio::io::AsyncSeekExt;
use tokio::{fs::File, sync::Mutex, sync::Semaphore};
/**
    File-Backed Cache:
    Execute an expensive opertion to generate a Resource and store the results in a file on disk.
    If the same resource is requested again it will be produced from disk.
    If the same resource is requested while it is also being generated it will be generated only
    once (dogpiling is prevented).

    It is possible to set a max parallelism level.
    todo: streaming, currently the whole thing is generated at once
*/
pub struct FileCache<Key, S, KF, F, FF, E>
where
    Key: Hash + PartialEq + Eq + Clone + Send,
    S: Clone + Send,
    KF: Send + Fn(&S, &Key) -> PathBuf,
    F: Send + Fn(S, Key, File) -> FF,
    FF: Future<Output = Result<File, E>> + Send,
    E: Send,
{
    in_flight: tokio::sync::Mutex<HashMap<Key, tokio::sync::watch::Receiver<()>>>,
    state: S,
    max_para: Option<Semaphore>,
    kf: KF,
    f: F,
}

impl<Key, S, KF, F, FF, E> FileCache<Key, S, KF, F, FF, E>
where
    Key: Hash + PartialEq + Eq + Clone + Send,
    S: Clone + Send,
    KF: Send + Fn(&S, &Key) -> PathBuf,
    F: Send + Fn(S, Key, File) -> FF,
    // fixme: why does this need to return a file? why not just a result
    FF: Future<Output = Result<File, E>> + Send,
    E: Send,
{
    /**
     * # Parameters:
     * kf: function that turns the Key into a path where the result is cached
     * f: function returning the future that does the expensive operation.
     *    The future should not be blocking either by long calculations or io operations
     *    use async_runtime::spawn for calculations and async io for io
     *    f needs to take care of serializing the value to the file on its own
     *    f should not panic, but measures are taken to not leave inconsistent state
     * max_para: maximum number of expensive operations in flight at the same time
     */
    pub fn new(init_state: S, kf: KF, f: F, max_para: Option<usize>) -> Self {
        let max_para = max_para.map(Semaphore::new);
        let in_flight = Mutex::new(HashMap::new());
        Self {
            state: init_state,
            in_flight,
            max_para,
            kf,
            f,
        }
    }

    pub async fn get_or_generate(&self, key: Key) -> std::io::Result<Result<File, E>> {
        let mut oo = tokio::fs::OpenOptions::new();
        let read = oo.read(true).write(false).create(false);
        let mut oo = tokio::fs::OpenOptions::new();
        let create = oo.read(true).write(true).create(true);

        loop {
            let mut in_flight = self.in_flight.lock().await;
            use std::collections::hash_map::Entry;
            match (*in_flight).entry(key.clone()) {
                Entry::Occupied(mut entry) => {
                    let mut e = entry.get_mut().clone();
                    drop(in_flight);
                    let path = (self.kf)(&self.state, &key);
                    debug!("waiting {:?}", path);
                    match e.changed().await {
                        Ok(()) => {
                            // weird but ok(ok()))
                            return Ok(Ok(read.open(path).await?));
                        }
                        Err(_) => {
                            // sender has been dropped which means either the generation function
                            // paniced internally or the whole future has been dropped in flight.
                            // we need to remove the entry and file and try again.

                            if let Err(e) = tokio::fs::remove_file(path).await {
                                // not found is fine thats what we want
                                if e.kind() != ErrorKind::NotFound {
                                    return Err(e);
                                }
                            };
                            // i hope this locking logic is sound:
                            // if multiple threads detect a crashed channel then only the first
                            // once has an equal channel
                            // other ones get either an empty entry or a new entry with a different
                            // channel
                            let mut in_flight = self.in_flight.lock().await;
                            if let Entry::Occupied(entry) = (*in_flight).entry(key.clone()) {
                                if e.same_channel(entry.get()) {
                                    entry.remove();
                                }
                            }
                            drop(in_flight);
                            continue;
                        }
                    }
                }
                Entry::Vacant(entry) => {
                    let path = (self.kf)(&self.state, &key);
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
                                entry.insert(rx);

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

                                let f = (self.f)(self.state.clone(), key.clone(), w);
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
                                // operation succeeded, we move the .part file to its permanent place
                                tokio::fs::rename(part_path, &path).await?;
                                let file = read.open(path).await?;

                                drop(perm);

                                // this can not panic as we hold a reciever ourselves
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
    env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init()
        .unwrap();
    use futures_util::future::join_all;
    let kf = |_: &(), s: &String| PathBuf::from(s);
    async fn inner_f(_s: (), key: String, mut file: File) -> Result<File, std::io::Error> {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        use tokio::io::AsyncWriteExt;
        file.write_all(key.as_bytes()).await?;
        Ok(file)
    }

    let c = FileCache::new((), kf, inner_f, None);

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
        });
    drop(c);
    drop(tmpdir);
    //debug!("{:?}", tmpdir.into_path());
}
