pub use maelstrom_fuse::FileType;

use anyhow::{bail, Context as _, Result};
use derive_more::{From, Into};
use maelstrom_base::{
    manifest::{Mode, UnixTimestamp},
    Sha256Digest,
};
use maelstrom_fuse::ErrnoResult;
use maelstrom_linux::Errno;
use maelstrom_util::async_fs::{Fs, GetPath};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Identifier for a layer within a given LayerFS stacking.
///
/// It is a number which increases by one for each layer in a stacking and `0` is the bottom.
#[derive(Copy, Clone, Debug, From, Hash, Deserialize, Serialize, PartialEq, Eq)]
pub struct LayerId(u32);

impl LayerId {
    pub const BOTTOM: Self = Self(0);

    pub fn as_u32(&self) -> u32 {
        self.0
    }

    pub fn inc(&self) -> Self {
        Self(self.0 + 1)
    }
}

/// The data stored in `super.bin` for a LayerFS layer.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LayerSuper {
    pub layer_id: LayerId,
    pub lower_layers: HashMap<LayerId, PathBuf>,
}

impl Default for LayerSuper {
    fn default() -> Self {
        Self {
            layer_id: LayerId::BOTTOM,
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

/// Identifies a LayerFS file. Basically a tuple of [`LayerId`] and file-table offset
#[derive(Copy, Clone, Debug, Hash, Deserialize, Serialize, PartialEq, Eq)]
pub struct FileId {
    layer_id: LayerId,
    offset: NonZeroU32,
}

impl FileId {
    pub fn new(layer_id: LayerId, offset: NonZeroU32) -> Self {
        Self { layer_id, offset }
    }

    pub fn root(layer_id: LayerId) -> Self {
        Self {
            layer_id,
            offset: NonZeroU32::MIN,
        }
    }

    pub fn as_u64(&self) -> u64 {
        ((self.layer_id.as_u32() as u64) << 32) | self.offset.get() as u64
    }

    pub fn layer(&self) -> LayerId {
        self.layer_id
    }

    pub fn offset(&self) -> NonZeroU32 {
        self.offset
    }

    pub fn offset_u64(&self) -> u64 {
        self.offset.get() as u64
    }

    pub fn is_root(&self) -> bool {
        self.offset.get() == 1
    }
}

impl TryFrom<u64> for FileId {
    type Error = std::num::TryFromIntError;

    fn try_from(v: u64) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            layer_id: LayerId::from((v >> 32) as u32),
            offset: NonZeroU32::try_from(v as u32)?,
        })
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
pub struct DirectoryEntryFileData {
    pub file_id: FileId,
    pub kind: FileType,
    pub opaque_dir: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, From)]
pub enum DirectoryEntryData {
    Whiteout,
    FileData(DirectoryEntryFileData),
}

impl DirectoryEntryData {
    pub fn as_dir(&self) -> Option<(FileId, bool)> {
        match self {
            Self::FileData(DirectoryEntryFileData {
                kind: FileType::Directory,
                file_id,
                opaque_dir,
            }) => Some((*file_id, *opaque_dir)),
            _ => None,
        }
    }

    pub fn into_file_data(self) -> Option<DirectoryEntryFileData> {
        match self {
            Self::FileData(data) => Some(data),
            _ => None,
        }
    }
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
    pub fn offset(&self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for AttributesId {
    type Error = std::num::TryFromIntError;

    fn try_from(v: u64) -> std::result::Result<Self, Self::Error> {
        Ok(Self(NonZeroU64::try_from(v)?))
    }
}

/// The attributes we store in attribute-table about each file.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileAttributes {
    pub size: u64,
    pub mode: Mode,
    pub mtime: UnixTimestamp,
}

#[test]
fn file_attributes_encoding_size_remains_same() {
    let mut a = FileAttributes {
        size: 1,
        mode: Mode(1),
        mtime: UnixTimestamp(1),
    };
    let start_size = crate::fixint_serialized_size(&a).unwrap();
    a.size = u64::MAX;
    a.mode = Mode(u32::MAX);
    a.mtime = UnixTimestamp(i64::MAX);
    let end_size = crate::fixint_serialized_size(&a).unwrap();
    assert_eq!(start_size, end_size);
}

/// The data for a file
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FileData {
    Empty,
    Inline {
        offset: u64,
        length: u64,
    },
    Digest {
        digest: Sha256Digest,
        offset: u64,
        length: u64,
    },
}

/// What is stored in the file-table about each file.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileTableEntry {
    pub kind: FileType,
    pub data: FileData,
    pub attr_id: AttributesId,
}

const MAX_ENCODE_SIZE: u64 = 1024 * 1024 * 1024; // 1 GiB

pub async fn decode<T: DeserializeOwned>(mut stream: impl AsyncRead + Unpin) -> Result<T> {
    let len = stream.read_u64().await?;
    if len > MAX_ENCODE_SIZE {
        bail!("item to decode too large {len} > {MAX_ENCODE_SIZE}");
    }
    let mut buffer = vec![0; len as usize];
    stream.read_exact(&mut buffer).await?;
    Ok(crate::fixint_deserialize(&buffer)?)
}

/// Decode from a stream where we can get the path so we can include it in the error message
pub async fn decode_with_rich_error<T: DeserializeOwned>(
    stream: &mut (impl AsyncRead + GetPath + Unpin),
) -> Result<T> {
    let res = decode(&mut *stream).await;
    res.with_context(|| {
        format!(
            "error decoding {} while reading from {:?}",
            std::any::type_name::<T>(),
            stream.path()
        )
    })
}

pub async fn encode<T: Serialize>(mut stream: impl AsyncWrite + Unpin, t: &T) -> Result<()> {
    let mut buffer = vec![0; 8];
    crate::fixint_serialize_into(&mut buffer, t).unwrap();
    let len = buffer.len() as u64 - 8;
    if len > MAX_ENCODE_SIZE {
        bail!("item to encode too large {len} > {MAX_ENCODE_SIZE}");
    }
    std::io::Cursor::new(&mut buffer[..8])
        .write_u64(len)
        .await
        .unwrap();
    stream.write_all(&buffer).await?;
    Ok(())
}

/// Encode into a stream where we can get the path so we can include it in the error message
pub async fn encode_with_rich_error<T: Serialize>(
    stream: &mut (impl AsyncWrite + GetPath + Unpin),
    t: &T,
) -> Result<()> {
    let res = encode(&mut *stream, t).await;
    res.with_context(|| {
        format!(
            "error encoding {} while writing to {:?}",
            std::any::type_name::<T>(),
            stream.path()
        )
    })
}
