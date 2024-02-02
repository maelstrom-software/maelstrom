use crate::{proto, Sha256Digest, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::{fmt, io};

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

pub struct ManifestReader<ReadT> {
    r: ReadT,
    stream_end: u64,
}

impl<ReadT: io::Read + io::Seek> ManifestReader<ReadT> {
    pub fn new(mut r: ReadT) -> io::Result<Self> {
        let stream_start = r.stream_position()?;
        r.seek(io::SeekFrom::End(0))?;
        let stream_end = r.stream_position()?;
        r.seek(io::SeekFrom::Start(stream_start))?;

        let version: ManifestVersion =
            proto::deserialize_from(&mut r).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if version != ManifestVersion::default() {
            return Err(io::Error::new(io::ErrorKind::Other, "bad manifest version"));
        }

        Ok(Self { r, stream_end })
    }

    fn next_inner(&mut self) -> io::Result<Option<ManifestEntry>> {
        if self.r.stream_position()? == self.stream_end {
            return Ok(None);
        }
        Ok(Some(
            proto::deserialize_from(&mut self.r)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        ))
    }
}

impl<ReadT: io::Read + io::Seek> Iterator for ManifestReader<ReadT> {
    type Item = io::Result<ManifestEntry>;

    fn next(&mut self) -> Option<io::Result<ManifestEntry>> {
        self.next_inner().transpose()
    }
}

pub struct ManifestWriter<WriteT> {
    w: WriteT,
}

impl<WriteT: io::Write> ManifestWriter<WriteT> {
    pub fn new(mut w: WriteT) -> io::Result<Self> {
        proto::serialize_into(&mut w, &ManifestVersion::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(Self { w })
    }

    pub fn write_entry(&mut self, entry: &ManifestEntry) -> io::Result<()> {
        proto::serialize_into(&mut self.w, entry)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn write_entries<'entry>(
        &mut self,
        entries: impl IntoIterator<Item = &'entry ManifestEntry>,
    ) -> io::Result<()> {
        for entry in entries {
            self.write_entry(entry)?
        }
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u32)]
pub enum ManifestVersion {
    #[default]
    V0 = 0,
}
