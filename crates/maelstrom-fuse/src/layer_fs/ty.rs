use crate::fuse::{ErrnoResult, FileType};
use anyhow::Result;
use derive_more::Into;
use maelstrom_base::manifest::{Mode, UnixTimestamp};
use maelstrom_linux::Errno;
use maelstrom_util::async_fs::Fs;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU64;
use std::path::Path;
use std::path::PathBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Copy, Clone, Debug, Hash, Deserialize, Serialize, PartialEq, Eq)]
pub struct LayerId(u64);

impl LayerId {
    pub const ZERO: Self = Self(0);
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LayerSuper {
    layer_id: LayerId,
    lower_layers: HashMap<LayerId, PathBuf>,
}

impl Default for LayerSuper {
    fn default() -> Self {
        Self {
            layer_id: LayerId::ZERO,
            lower_layers: Default::default(),
        }
    }
}

impl LayerSuper {
    pub async fn read_from_path(fs: &Fs, path: &Path) -> Result<Self> {
        decode(fs.open_file(path).await?).await
    }

    pub async fn write_to_path(&self, fs: &Fs, path: &Path) -> Result<()> {
        encode(fs.create_file(path).await?, self).await
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FileId(NonZeroU64);

impl FileId {
    #[allow(dead_code)]
    pub const ROOT: Self = Self(NonZeroU64::MIN);

    pub fn as_u64(&self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for FileId {
    type Error = std::num::TryFromIntError;

    fn try_from(v: u64) -> std::result::Result<Self, Self::Error> {
        Ok(Self(NonZeroU64::try_from(v)?))
    }
}

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Copy, Clone, Debug, Into, Deserialize, Serialize)]
pub struct DirectoryOffset(u64);

impl TryFrom<i64> for DirectoryOffset {
    type Error = Errno;

    fn try_from(o: i64) -> ErrnoResult<Self> {
        Ok(Self(o.try_into().map_err(|_| Errno::EINVAL)?))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DirectoryEntryData {
    pub file_id: FileId,
    pub kind: FileType,
}

#[derive(Copy, Clone, Default, Debug, Deserialize_repr, Serialize_repr)]
#[repr(u32)]
pub enum LayerFsVersion {
    #[default]
    V0 = 0,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct AttributesId(NonZeroU64);

impl AttributesId {
    pub fn as_u64(&self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for AttributesId {
    type Error = std::num::TryFromIntError;

    fn try_from(v: u64) -> std::result::Result<Self, Self::Error> {
        Ok(Self(NonZeroU64::try_from(v)?))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileAttributes {
    pub size: u64,
    pub mode: Mode,
    pub mtime: UnixTimestamp,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FileData {
    Empty,
    Inline(Vec<u8>),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileTableEntry {
    pub kind: FileType,
    pub data: FileData,
    pub attr_id: AttributesId,
}

pub async fn decode<T: DeserializeOwned>(mut stream: impl AsyncRead + Unpin) -> Result<T> {
    let len = stream.read_u64().await?;
    let mut buffer = vec![0; len as usize];
    stream.read_exact(&mut buffer).await?;
    Ok(maelstrom_base::proto::deserialize(&buffer)?)
}

#[allow(dead_code)]
pub async fn encode<T: Serialize>(mut stream: impl AsyncWrite + Unpin, t: &T) -> Result<()> {
    let mut buffer = vec![0; 8];
    maelstrom_base::proto::serialize_into(&mut buffer, t).unwrap();
    let len = buffer.len() as u64 - 8;
    std::io::Cursor::new(&mut buffer[..8])
        .write_u64(len)
        .await
        .unwrap();
    stream.write_all(&buffer).await?;
    Ok(())
}
