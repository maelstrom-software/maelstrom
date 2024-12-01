use crate::{Sha256Digest, Utf8PathBuf};
use derive_more::Debug;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::fmt;

#[derive(Debug)]
#[debug("{_0:o}")]
struct OctalFmt<T>(T);

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
    assert_eq!(format!("{:05?}", Mode(0o1755)), "Mode(01755)");
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct UnixTimestamp(pub i64);

impl UnixTimestamp {
    pub const EPOCH: Self = Self(0);
}

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
pub enum ManifestFileData {
    Digest(Sha256Digest),
    Inline(Vec<u8>),
    Empty,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ManifestEntryData {
    Directory { opaque: bool },
    File(ManifestFileData),
    Symlink(Vec<u8>),
    Hardlink(Utf8PathBuf),
    Whiteout,
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
    V0 = 0,
    #[default]
    V1 = 1,
}
