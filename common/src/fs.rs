pub fn find_latest_db<P: AsRef<std::path::Path>>(name: &str, path: P) -> std::io::Result<Option<u64>> {
    let mut max = None;
    for line in std::fs::read_dir(path)? {
        let line = line?;
        if !line.file_type()?.is_file() {
            continue;
        }
        let filename = line.file_name();
        let filename = filename.to_string_lossy();
        if !filename.starts_with(name) {
            continue;
        }
        // 3 kinds of files in this folder:
        // core.db | (symlink to) the database
        // core-timestamp | version of the database
        // core-timestamp-timestamp | patch
        // we are only looking for core-timestamp
        if filename.matches('-').count() != 1 {
            continue;
        }
        let (_, ts) = if let Some(s) = filename.split_once('-') {
            s
        } else {
            continue;
        };
        let ts: u64 = if let Ok(ts) = ts.parse() { ts } else { continue };
        let ts = Some(ts);
        if ts > max {
            max = ts;
        }
    }
    Ok(max)
}
