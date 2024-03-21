use crate::fuse;
use crate::layer_fs::avl::{AvlNode, AvlPtr, AvlStorage, AvlTree, FlatAvlPtrOption};
use crate::layer_fs::ty::{
    decode_file, encode_file, DirectoryEntryData, DirectoryOffset, FileId, LayerFsVersion,
};
use crate::layer_fs::{to_eio, LayerFs};
use anyhow::Result;
use async_trait::async_trait;
use maelstrom_util::async_fs::File;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, FromInto};
use std::borrow::BorrowMut;
use std::io::SeekFrom;
use std::pin::Pin;
use tokio::io::{AsyncSeekExt as _, AsyncWriteExt as _};

pub struct DirectoryDataReader<'fs> {
    stream: File<'fs>,
    entry_begin: u64,
    length: u64,
}

impl<'fs> DirectoryDataReader<'fs> {
    pub async fn new(layer_fs: &'fs LayerFs, file_id: FileId) -> Result<Self> {
        let mut stream = layer_fs
            .data_fs
            .open_file(layer_fs.dir_data_path(file_id).await?)
            .await?;
        let length = stream.metadata().await?.len();
        let _header: DirectoryEntryStorageHeader = decode_file(&mut stream).await?;
        let entry_begin = stream.stream_position().await?;
        Ok(Self {
            stream,
            entry_begin,
            length,
        })
    }

    pub async fn look_up(&mut self, entry_name: &str) -> Result<Option<FileId>> {
        Ok(self.look_up_entry(entry_name).await?.map(|e| e.file_id))
    }

    pub async fn look_up_entry(&mut self, entry_name: &str) -> Result<Option<DirectoryEntryData>> {
        let mut tree = AvlTree::new(DirectoryEntryStorage::new(&mut self.stream));
        tree.get(&entry_name.into()).await
    }

    pub async fn next_entry(&mut self) -> Result<Option<DirectoryEntry>> {
        if self.stream.stream_position().await? == self.length {
            return Ok(None);
        }
        let entry: DirectoryEntry = decode_file(&mut self.stream).await?;
        Ok(Some(entry))
    }

    async fn next_fuse_entry(&mut self) -> Result<Option<fuse::DirEntry>> {
        let Some(entry) = self.next_entry().await? else {
            return Ok(None);
        };
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
            to_eio(self_.next_fuse_entry().await)
                .transpose()
                .map(|v| (v, self_))
        })))
    }

    pub async fn into_ordered_stream(self) -> Result<OrderedDirectoryStream<'fs>> {
        Ok(Box::pin(
            AvlTree::new(DirectoryEntryStorage::new(self.stream))
                .into_stream()
                .await?,
        ))
    }
}

#[serde_as]
#[derive(Copy, Clone, Default, Debug, Deserialize, Serialize)]
pub struct DirectoryEntryStorageHeader {
    pub version: LayerFsVersion,
    #[serde_as(as = "FromInto<FlatAvlPtrOption>")]
    pub root: Option<AvlPtr>,
}

struct DirectoryEntryStorage<FileT> {
    stream: FileT,
}

impl<FileT> DirectoryEntryStorage<FileT> {
    fn new(stream: FileT) -> Self {
        Self { stream }
    }
}

type DirectoryEntry = AvlNode<String, DirectoryEntryData>;

#[async_trait]
impl<'fs, FileT: BorrowMut<File<'fs>> + Send> AvlStorage for DirectoryEntryStorage<FileT> {
    type Key = String;
    type Value = DirectoryEntryData;

    async fn root(&mut self) -> Result<Option<AvlPtr>> {
        self.stream.borrow_mut().seek(SeekFrom::Start(0)).await?;
        let header: DirectoryEntryStorageHeader = decode_file(self.stream.borrow_mut()).await?;
        Ok(header.root)
    }

    async fn set_root(&mut self, root: AvlPtr) -> Result<()> {
        self.stream.borrow_mut().seek(SeekFrom::Start(0)).await?;
        let header = DirectoryEntryStorageHeader {
            root: Some(root),
            ..Default::default()
        };
        encode_file(self.stream.borrow_mut(), &header).await?;
        Ok(())
    }

    async fn look_up(&mut self, key: AvlPtr) -> Result<DirectoryEntry> {
        self.stream
            .borrow_mut()
            .seek(SeekFrom::Start(key.as_u64()))
            .await?;
        Ok(decode_file(self.stream.borrow_mut()).await?)
    }

    async fn update(&mut self, key: AvlPtr, value: DirectoryEntry) -> Result<()> {
        self.stream
            .borrow_mut()
            .seek(SeekFrom::Start(key.as_u64()))
            .await?;

        #[cfg(debug_assertions)]
        let old_len = {
            use tokio::io::AsyncReadExt as _;
            let old_len = self.stream.borrow_mut().read_u64().await?;
            self.stream
                .borrow_mut()
                .seek(SeekFrom::Start(key.as_u64()))
                .await?;
            old_len
        };

        encode_file(self.stream.borrow_mut(), &value).await?;

        #[cfg(debug_assertions)]
        {
            use tokio::io::AsyncReadExt as _;
            self.stream
                .borrow_mut()
                .seek(SeekFrom::Start(key.as_u64()))
                .await?;
            let new_len = self.stream.borrow_mut().read_u64().await?;
            assert_eq!(old_len, new_len);
        }

        Ok(())
    }

    async fn insert(&mut self, node: DirectoryEntry) -> Result<AvlPtr> {
        self.stream.borrow_mut().seek(SeekFrom::End(0)).await?;
        let new_ptr = self.stream.borrow_mut().stream_position().await?;
        encode_file(self.stream.borrow_mut(), &node).await?;
        Ok(AvlPtr::new(new_ptr).unwrap())
    }

    async fn flush(&mut self) -> Result<()> {
        self.stream.borrow_mut().flush().await?;
        Ok(())
    }
}

pub type DirectoryStream<'fs> =
    Pin<Box<dyn futures::Stream<Item = fuse::ErrnoResult<fuse::DirEntry>> + Send + 'fs>>;

pub type OrderedDirectoryStream<'fs> =
    Pin<Box<dyn futures::Stream<Item = Result<(String, DirectoryEntryData)>> + Send + 'fs>>;

#[allow(dead_code)]
pub struct DirectoryDataWriter<'fs> {
    tree: AvlTree<DirectoryEntryStorage<File<'fs>>>,
}

#[allow(dead_code)]
impl<'fs> DirectoryDataWriter<'fs> {
    pub async fn new(layer_fs: &'fs LayerFs, file_id: FileId) -> Result<Self> {
        let path = layer_fs.dir_data_path(file_id).await?;
        let existing = layer_fs.data_fs.exists(&path).await;
        let mut stream = layer_fs.data_fs.open_or_create_file(path).await?;
        if !existing {
            encode_file(&mut stream, &DirectoryEntryStorageHeader::default()).await?;
        }
        Ok(Self {
            tree: AvlTree::new(DirectoryEntryStorage::new(stream)),
        })
    }

    pub async fn write_empty(layer_fs: &'fs LayerFs, file_id: FileId) -> Result<()> {
        let mut s = Self::new(layer_fs, file_id).await?;
        s.flush().await?;
        Ok(())
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

    pub async fn flush(&mut self) -> Result<()> {
        self.tree.flush().await?;
        Ok(())
    }
}
