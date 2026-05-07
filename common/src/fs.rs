use log::{debug, trace};
pub fn find_latest_db<P: AsRef<std::path::Path>>(name: &str, path: P) -> std::io::Result<Option<u64>> {
    debug!("finding latest {name} in {:?}", path.as_ref());
    let mut max = None;
    for line in std::fs::read_dir(path)? {
        let line = line?;
        if !line.file_type()?.is_file() {
            continue;
        }
        let filename = line.file_name();
        let filename = filename.to_string_lossy();
        trace!("considering {filename}");
        if !filename.starts_with(name) {
            continue;
        }
        trace!("{filename} belongs to correct db");
        // 3 kinds of files in this folder:
        // core.db | (symlink to) the database
        // core-timestamp | version of the database
        // core-timestamp-timestamp | patch
        // we are only looking for core-timestamp
        if filename.matches('-').count() != 1 {
            continue;
        }
        trace!("{filename} has correct number of dashes");
        let (_, ts) = if let Some(s) = filename.split_once('-') {
            s
        } else {
            panic!("no dashes though we just checked for dashes");
        };
        let ts: u64 = if let Ok(ts) = ts.parse() { ts } else { continue };
        trace!("found timestamp {ts} for {name} in {filename}");
        let ts = Some(ts);
        if ts > max {
            max = ts;
        }
    }
    Ok(max)
}
