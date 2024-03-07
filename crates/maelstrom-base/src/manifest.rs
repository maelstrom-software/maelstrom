use crate::{Sha256Digest, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::fmt;

struct OctalFmt<T>(T);

impl<T> fmt::Debug for OctalFmt<T>
where
    T: fmt::Octal,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Octal::fmt(&self.0, f)
    }
}

#[derive(Clone, Copy, Deserialize, PartialEq, Serialize)]
pub struct Mode(pub u32);

impl fmt::Debug for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Mode").field(&OctalFmt(self.0)).finish()
    }
}

impl From<Mode> for u32 {
    fn from(m: Mode) -> u32 {
        m.0
    }
}

#[test]
fn mode_fmt() {
    assert_eq!(format!("{:?}", Mode(0o1755)), "Mode(1755)");
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct UnixTimestamp(pub i64);

impl From<UnixTimestamp> for i64 {
    fn from(t: UnixTimestamp) -> Self {
        t.0
    }
}

impl From<UnixTimestamp> for std::time::SystemTime {
    fn from(t: UnixTimestamp) -> Self {
        if t.0 < 0 {
            std::time::UNIX_EPOCH - std::time::Duration::from_secs(u64::try_from(-t.0).unwrap())
        } else {
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(u64::try_from(t.0).unwrap())
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ManifestEntryMetadata {
    pub size: u64,
    pub mode: Mode,
    pub mtime: UnixTimestamp,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ManifestEntryData {
    Directory,
    File(Option<Sha256Digest>),
    Symlink(Vec<u8>),
    Hardlink(Utf8PathBuf),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ManifestEntry {
    pub path: Utf8PathBuf,
    pub metadata: ManifestEntryMetadata,
    pub data: ManifestEntryData,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u32)]
pub enum ManifestVersion {
    #[default]
    V0 = 0,
}
