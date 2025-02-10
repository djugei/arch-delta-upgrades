use thiserror::Error;

type Str = Box<str>;

#[derive(Debug, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct Package {
    name: Str,
    version: Str,
    arch: Str,
    trailer: Str,
}

impl Package {
    pub fn get_name(&self) -> &str {
        &self.name
    }
    pub fn get_version(&self) -> &str {
        &self.version
    }
    // name, version, arch, trailer
    pub fn destructure(self) -> (Str, Str, Str, Str) {
        (self.name, self.version, self.arch, self.trailer)
    }
    pub fn from_parts((name, version, arch, trailer): (Str, Str, Str, Str)) -> Self {
        Self {
            name,
            version,
            arch,
            trailer,
        }
    }
}

impl PartialEq for Package {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.version == other.version && self.arch == other.arch
    }
}

#[derive(Error, Debug)]
pub enum PackageParseError {
    #[error("{0:?}")]
    Parse(&'static str),
    #[error("invalid character in input")]
    Invalid,
}
impl From<&'static str> for PackageParseError {
    fn from(value: &'static str) -> Self {
        Self::Parse(value)
    }
}

impl<'s> TryFrom<&'s str> for Package {
    type Error = PackageParseError;

    fn try_from(value: &'s str) -> Result<Self, Self::Error> {
        if value.contains('/') {
            return Err(PackageParseError::Invalid);
        }
        let (left, trailer) = value.rsplit_once('-').ok_or("name, version | arch trailer")?;
        let mut idx = left.rmatch_indices('-');
        let _ = idx.next();
        let (idx, _) = idx.next().ok_or("name | version")?;
        let (name, version) = left.split_at(idx);
        let version = &version[1..];
        let (arch, trailer) = trailer.split_once('.').ok_or("arch | trailer")?;

        Ok(Package {
            name: name.into(),
            version: version.into(),
            arch: arch.into(),
            trailer: trailer.into(),
        })
    }
}

impl std::fmt::Display for Package {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}-{}.{}", self.name, self.version, self.arch, self.trailer)
    }
}

#[test]
fn package_parse() {
    let s = "6tunnel-0.13-1-x86_64.pkg.tar.xz";
    let p = Package::try_from(s).unwrap();

    assert_eq!(&*p.name, "6tunnel");
    assert_eq!(&*p.version, "0.13-1");
    assert_eq!(&*p.arch, "x86_64");
    assert_eq!(&*p.trailer, "pkg.tar.xz");

    assert_eq!(p.to_string(), s);
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct Delta {
    old: Package,
    new: Package,
}

impl Delta {
    pub fn get_old(self) -> Package {
        self.old
    }
    pub fn get_new(self) -> Package {
        self.new
    }
    pub fn get_both(self) -> (Package, Package) {
        (self.old, self.new)
    }
}

#[derive(Error, Debug)]
pub enum DeltaError {
    #[error("equal version")]
    Version,
}
impl TryFrom<(Package, Package)> for Delta {
    type Error = DeltaError;

    fn try_from((old, new): (Package, Package)) -> Result<Self, Self::Error> {
        if new.version == old.version {
            Err(DeltaError::Version)
        } else {
            Ok(Delta { old, new })
        }
    }
}

impl std::fmt::Display for Delta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}:to:{}-{}-{}",
            self.old.name, self.old.version, self.old.arch, self.new.name, self.new.version, self.new.arch,
        )
    }
}

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
