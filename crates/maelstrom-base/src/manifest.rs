use crate::{Sha256Digest, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::io;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Identity {
    Name(String),
    Id(u64),
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct Mode(pub u32);

impl From<Mode> for u32 {
    fn from(m: Mode) -> u32 {
        m.0
    }
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
    pub user: Identity,
    pub group: Identity,
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

        let version: ManifestVersion = bincode::deserialize_from(&mut r)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
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
            bincode::deserialize_from(&mut self.r)
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
        bincode::serialize_into(&mut w, &ManifestVersion::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(Self { w })
    }

    pub fn write_entry(&mut self, entry: &ManifestEntry) -> io::Result<()> {
        bincode::serialize_into(&mut self.w, entry)
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
