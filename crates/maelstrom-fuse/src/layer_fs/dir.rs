use crate::fuse;
use crate::layer_fs::avl::{AvlNode, AvlPtr, AvlStorage, AvlTree, FlatAvlPtrOption};
use crate::layer_fs::ty::{
    decode, encode, DirectoryEntryData, DirectoryOffset, FileId, LayerFsVersion,
};
use crate::layer_fs::{to_eio, LayerFs};
use anyhow::Result;
use async_trait::async_trait;
use maelstrom_util::async_fs::File;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, FromInto};
use std::io::SeekFrom;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt as _, AsyncWrite};

pub struct DirectoryDataReader<'fs> {
    stream: File<'fs>,
    entry_begin: u64,
    length: u64,
}

impl<'fs> DirectoryDataReader<'fs> {
    pub async fn new(layer_fs: &'fs LayerFs, file_id: FileId) -> Result<Self> {
        let mut stream = layer_fs
            .data_fs
            .open_file(layer_fs.dir_data_path(file_id)?)
            .await?;
        let length = stream.metadata().await?.len();
        let _header: DirectoryEntryStorageHeader = decode(&mut stream).await?;
        let entry_begin = stream.stream_position().await?;
        Ok(Self {
            stream,
            entry_begin,
            length,
        })
    }

    pub async fn look_up(&mut self, entry_name: &str) -> Result<Option<FileId>> {
        let mut tree = AvlTree::new(DirectoryEntryStorage::new(&mut self.stream));
        Ok(tree.get(&entry_name.into()).await?.map(|e| e.file_id))
    }

    async fn next_entry(&mut self) -> Result<Option<fuse::DirEntry>> {
        if self.stream.stream_position().await? == self.length {
            return Ok(None);
        }
        let entry: DirectoryEntry = decode(&mut self.stream).await?;
        let offset = i64::try_from(self.stream.stream_position().await?).unwrap();
        Ok(Some(fuse::DirEntry {
            ino: entry.value.file_id.as_u64(),
            offset: offset - i64::try_from(self.entry_begin).unwrap(),
            kind: entry.value.kind,
            name: entry.key,
        }))
    }

    pub async fn into_stream(mut self, offset: DirectoryOffset) -> Result<DirectoryStream<'fs>> {
        self.stream
            .seek(SeekFrom::Start(self.entry_begin + u64::from(offset)))
            .await?;
        Ok(Box::pin(futures::stream::unfold(self, |mut self_| async {
            to_eio(self_.next_entry().await)
                .transpose()
                .map(|v| (v, self_))
        })))
    }
}

#[serde_as]
#[derive(Copy, Clone, Default, Debug, Deserialize, Serialize)]
pub struct DirectoryEntryStorageHeader {
    pub version: LayerFsVersion,
    #[serde_as(as = "FromInto<FlatAvlPtrOption>")]
    pub root: Option<AvlPtr>,
}

struct DirectoryEntryStorage<StreamT> {
    stream: StreamT,
}

impl<StreamT> DirectoryEntryStorage<StreamT> {
    fn new(stream: StreamT) -> Self {
        Self { stream }
    }
}

type DirectoryEntry = AvlNode<String, DirectoryEntryData>;

#[async_trait]
impl<StreamT: AsyncRead + AsyncWrite + AsyncSeek + Unpin + Send> AvlStorage
    for DirectoryEntryStorage<StreamT>
{
    type Key = String;
    type Value = DirectoryEntryData;

    async fn root(&mut self) -> Result<Option<AvlPtr>> {
        self.stream.seek(SeekFrom::Start(0)).await?;
        let header: DirectoryEntryStorageHeader = decode(&mut self.stream).await?;
        Ok(header.root)
    }

    async fn set_root(&mut self, root: AvlPtr) -> Result<()> {
        self.stream.seek(SeekFrom::Start(0)).await?;
        let header = DirectoryEntryStorageHeader {
            root: Some(root),
            ..Default::default()
        };
        encode(&mut self.stream, &header).await?;
        Ok(())
    }

    async fn look_up(&mut self, key: AvlPtr) -> Result<DirectoryEntry> {
        self.stream.seek(SeekFrom::Start(key.as_u64())).await?;
        Ok(decode(&mut self.stream).await?)
    }

    async fn update(&mut self, key: AvlPtr, value: DirectoryEntry) -> Result<()> {
        self.stream.seek(SeekFrom::Start(key.as_u64())).await?;

        #[cfg(debug_assertions)]
        let old_len = {
            use tokio::io::AsyncReadExt as _;
            let old_len = self.stream.read_u64().await?;
            self.stream.seek(SeekFrom::Start(key.as_u64())).await?;
            old_len
        };

        encode(&mut self.stream, &value).await?;

        #[cfg(debug_assertions)]
        {
            use tokio::io::AsyncReadExt as _;
            self.stream.seek(SeekFrom::Start(key.as_u64())).await?;
            let new_len = self.stream.read_u64().await?;
            assert_eq!(old_len, new_len);
        }

        Ok(())
    }

    async fn insert(&mut self, node: DirectoryEntry) -> Result<AvlPtr> {
        self.stream.seek(SeekFrom::End(0)).await?;
        let new_ptr = self.stream.stream_position().await?;
        encode(&mut self.stream, &node).await?;
        Ok(AvlPtr::new(new_ptr).unwrap())
    }
}

pub type DirectoryStream<'fs> =
    Pin<Box<dyn futures::Stream<Item = fuse::ErrnoResult<fuse::DirEntry>> + Send + 'fs>>;

#[allow(dead_code)]
pub struct DirectoryDataWriter<'fs> {
    tree: AvlTree<DirectoryEntryStorage<File<'fs>>>,
}

#[allow(dead_code)]
impl<'fs> DirectoryDataWriter<'fs> {
    pub async fn new(layer_fs: &'fs LayerFs, file_id: FileId) -> Result<Self> {
        let path = layer_fs.dir_data_path(file_id)?;
        let existing = layer_fs.data_fs.exists(&path).await;
        let mut stream = layer_fs.data_fs.open_or_create_file(path).await?;
        if !existing {
            encode(&mut stream, &DirectoryEntryStorageHeader::default()).await?;
        }
        Ok(Self {
            tree: AvlTree::new(DirectoryEntryStorage::new(stream)),
        })
    }

    pub async fn insert_entry(
        &mut self,
        entry_name: &str,
        entry_data: DirectoryEntryData,
    ) -> Result<bool> {
        self.tree
            .insert_if_not_exists(entry_name.into(), entry_data)
            .await
    }
}
