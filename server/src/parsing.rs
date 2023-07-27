type Str = Box<str>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct Package {
    name: Str,
    version: Str,
    arch: Str,
    trailer: Str,
}

impl<'s> TryFrom<&'s str> for Package {
    type Error = ();

    fn try_from(value: &'s str) -> Result<Self, Self::Error> {
        if value.contains('/') {
            return Err(());
        }
        let (left, trailer) = value.rsplit_once('-').ok_or(())?;
        let mut idx = left.rmatch_indices('-');
        let _ = idx.next();
        let (idx, _) = idx.next().ok_or(())?;
        let (name, version) = left.split_at(idx);
        let version = &version[1..];
        let (arch, trailer) = trailer.split_once('.').ok_or(())?;

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
        write!(
            f,
            "{}-{}-{}.{}",
            self.name, self.version, self.arch, self.trailer
        )
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
    name: Str,
    old: Str,
    new: Str,
    arch: Str,
    trailer: Str,
}

impl Delta {
    pub fn get_old(self) -> Package {
        Package {
            name: self.name,
            arch: self.arch,
            trailer: self.trailer,
            version: self.old,
        }
    }
    pub fn get_new(self) -> Package {
        Package {
            name: self.name,
            arch: self.arch,
            trailer: self.trailer,
            version: self.new,
        }
    }
}

impl TryFrom<(Package, Package)> for Delta {
    type Error = ();

    fn try_from((p1, p2): (Package, Package)) -> Result<Self, Self::Error> {
        #[allow(clippy::if_same_then_else)]
        if (&p1.name, &p1.arch, &p1.trailer) != (&p2.name, &p2.arch, &p2.trailer) {
            Err(())
        } else if p1.version >= p2.version {
            Err(())
        } else {
            Ok(Delta {
                name: p1.name,
                arch: p1.arch,
                trailer: p1.trailer,
                old: p1.version,
                new: p2.version,
            })
        }
    }
}

impl std::fmt::Display for Delta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}_to_{}-{}.{}",
            self.name, self.old, self.new, self.arch, self.trailer
        )
    }
}
