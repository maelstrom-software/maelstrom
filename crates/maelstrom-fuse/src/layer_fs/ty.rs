use crate::fuse::{ErrnoResult, FileType};
use anyhow::Result;
use derive_more::{From, Into};
use maelstrom_linux::Errno;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::fmt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Copy, Clone, Debug, From, Into, Deserialize, Serialize)]
pub struct FileId(u64);

impl FileId {
    pub const ROOT: Self = Self(1);

    pub fn as_u64(&self) -> u64 {
        self.0
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
