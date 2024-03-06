use crate::fuse;
use crate::layer_fs::ty::{decode, DirectoryEntry, DirectoryOffset, FileId};
use crate::layer_fs::{to_eio, LayerFs};
use anyhow::Result;
use maelstrom_util::async_fs::File;
use std::io::SeekFrom;
use std::pin::Pin;
use tokio::io::AsyncSeekExt;

pub struct DirectoryDataReader<'fs> {
    stream: File<'fs>,
    length: u64,
}

impl<'fs> DirectoryDataReader<'fs> {
    pub async fn new(layer_fs: &'fs LayerFs, file_id: FileId) -> Result<Self> {
        let stream = layer_fs
            .data_fs
            .open_file(layer_fs.dir_data_path(file_id))
            .await?;
        let length = stream.metadata().await?.len();
        Ok(Self { stream, length })
    }

    async fn next_entry(&mut self) -> Result<Option<fuse::DirEntry>> {
        if self.stream.stream_position().await? == self.length {
            return Ok(None);
        }
        let entry: DirectoryEntry = decode(&mut self.stream).await?;
        let offset = self.stream.stream_position().await?.try_into().unwrap();
        Ok(Some(fuse::DirEntry {
            ino: entry.file_id.into(),
            offset,
            kind: entry.kind,
            name: entry.name,
        }))
    }

    pub async fn into_stream(mut self, offset: DirectoryOffset) -> Result<DirectoryStream<'fs>> {
        self.stream.seek(SeekFrom::Start(offset.into())).await?;
        Ok(Box::pin(futures::stream::unfold(self, |mut self_| async {
            to_eio(self_.next_entry().await)
                .transpose()
                .map(|v| (v, self_))
        })))
    }
}

pub type DirectoryStream<'fs> =
    Pin<Box<dyn futures::Stream<Item = fuse::ErrnoResult<fuse::DirEntry>> + Send + 'fs>>;
