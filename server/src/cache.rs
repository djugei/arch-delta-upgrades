use core::future::Future;
use log::debug;
use std::{collections::HashMap, hash::Hash, io::ErrorKind, path::PathBuf, pin::Pin};

use tokio::{fs::File, io::AsyncSeekExt, sync::Mutex, sync::Semaphore};
/**
    File-Backed Cache:
    Execute an expensive opertion to generate a Resource and store the results in a file on disk.
    If the same resource is requested again it will be produced from disk.
    If the same resource is requested while it is also being generated it will be generated only
    once (dogpiling is prevented).

    It is possible to set a max parallelism level.
    todo: streaming, currently the whole thing is generated at once
*/
pub struct FileCache<Key, S, KF, F, E>
where
    Key: Hash + PartialEq + Eq + Clone + Send,
    S: Clone + Send,
    KF: Send + Fn(&S, &Key) -> PathBuf,
    // would love for this to not use boxes but thats just how things are currently with rust and
    // futures
    F: Send + Fn(S, Key, File) -> Pin<Box<dyn Future<Output = Result<File, E>> + Send>>,
    E: Send,
{
    in_flight: tokio::sync::Mutex<HashMap<Key, tokio::sync::watch::Receiver<()>>>,
    state: S,
    max_para: Option<Semaphore>,
    kf: KF,
    f: F,
}

impl<Key, S, KF, F, E> FileCache<Key, S, KF, F, E>
where
    Key: Hash + PartialEq + Eq + Clone + Send,
    S: Clone + Send,
    KF: Send + Fn(&S, &Key) -> PathBuf,
    F: Send + Fn(S, Key, File) -> Pin<Box<dyn Future<Output = Result<File, E>> + Send>>,
    E: Send,
{
    /**
     * # Parameters:
     * kf: function that turns the Key into a path where the result is cached
     * f: function returning the future that does the expensive operation.
     *    The future should not be blocking either by long calculations or io operations
     *    use async_runtime::spawn for calculations and async io for io
     *    f needs to take care of serializing the value to the file on its own
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

        let mut in_flight = self.in_flight.lock().await;
        use std::collections::hash_map::Entry;
        match (*in_flight).entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let mut e = entry.get_mut().clone();
                drop(in_flight);
                // threading errors are unrecoverable for now
                let path = (self.kf)(&self.state, &key);
                debug!("waiting {:?}", path);
                e.changed().await.unwrap();
                // weird but ok(ok()))
                Ok(Ok(read.open(path).await?))
            }
            Entry::Vacant(entry) => {
                let path = (self.kf)(&self.state, &key);
                match read.open(&path).await {
                    Ok(f) => {
                        debug!("exists {:?}", path);
                        Ok(Ok(f))
                    }
                    Err(e) => {
                        if e.kind() != ErrorKind::NotFound {
                            debug!("io error {:?}", path);
                            Err(e)
                        } else {
                            debug!("generating {:?}", path);
                            let (tx, rx) = tokio::sync::watch::channel(());
                            entry.insert(rx);
                            let w = create.open(&path).await?;
                            drop(in_flight);

                            let perm = if let Some(sem) = &self.max_para {
                                debug!("sem get {:?}", path);
                                let s = Some(sem.acquire().await);
                                debug!("sem gotten {:?}", path);
                                s
                            } else {
                                None
                            };
                            // do the expensive operation

                            let f = (self.f)(self.state.clone(), key.clone(), w);
                            let f = Box::pin(f);
                            let mut f = f.await;
                            match &mut f {
                                Ok(ref mut f) => {
                                    f.rewind().await?;
                                }
                                Err(_) => {
                                    panic!("add error handling")
                                }
                            }

                            drop(perm);

                            // todo: error handling?
                            tx.send(()).unwrap();
                            self.in_flight.lock().await.remove(&key);
                            Ok(f)
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn cache_simple() {
    pretty_env_logger::formatted_builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init()
        .unwrap();
    use futures_util::future::join_all;
    let kf = |_: &(), s: &String| PathBuf::from(s);
    async fn inner_f(s: (), key: String, mut file: File) -> Result<File, std::io::Error> {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        use tokio::io::AsyncWriteExt;
        file.write_all(key.as_bytes()).await?;
        Ok(file)
    }

    let f = |s: (), key: String, file: File| Box::pin(inner_f(s, key, file)) as _;

    let c = FileCache::new((), kf, f, None);

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
