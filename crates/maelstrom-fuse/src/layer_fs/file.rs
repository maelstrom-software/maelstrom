use crate::layer_fs::ty::{
    decode, encode, AttributesId, FileAttributes, FileData, FileId, FileTableEntry, LayerFsVersion,
};
use crate::layer_fs::LayerFs;
use crate::FileType;
use anyhow::Result;
use maelstrom_util::async_fs::File;
use serde::{Deserialize, Serialize};
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt as _;

pub struct FileMetadataReader<'fs> {
    file_table: File<'fs>,
    file_table_start: u64,
    attr_table: File<'fs>,
    attr_table_start: u64,
}

impl<'fs> FileMetadataReader<'fs> {
    pub async fn new(layer_fs: &'fs LayerFs) -> Result<Self> {
        let mut file_table = layer_fs
            .data_fs
            .open_file(layer_fs.file_table_path())
            .await?;
        let _header: FileTableHeader = decode(&mut file_table).await?;
        let file_table_start = file_table.stream_position().await?;

        let mut attr_table = layer_fs
            .data_fs
            .open_file(layer_fs.attributes_table_path())
            .await?;
        let _header: AttributesTableHeader = decode(&mut attr_table).await?;
        let attr_table_start = attr_table.stream_position().await?;
        Ok(Self {
            file_table,
            file_table_start,
            attr_table,
            attr_table_start,
        })
    }

    pub async fn get_attr(&mut self, id: FileId) -> Result<(FileType, FileAttributes)> {
        self.file_table
            .seek(SeekFrom::Start(self.file_table_start + id.as_u64() - 1))
            .await?;
        let entry: FileTableEntry = decode(&mut self.file_table).await?;

        self.attr_table
            .seek(SeekFrom::Start(
                self.attr_table_start + entry.attr_id.as_u64() - 1,
            ))
            .await?;
        let attrs: FileAttributes = decode(&mut self.attr_table).await?;

        Ok((entry.kind, attrs))
    }

    pub async fn get_data(&mut self, id: FileId) -> Result<(FileType, FileData)> {
        self.file_table
            .seek(SeekFrom::Start(self.file_table_start + id.as_u64() - 1))
            .await?;
        let entry: FileTableEntry = decode(&mut self.file_table).await?;

        Ok((entry.kind, entry.data))
    }
}

#[derive(Copy, Clone, Default, Debug, Deserialize, Serialize)]
pub struct FileTableHeader {
    pub version: LayerFsVersion,
}

#[derive(Copy, Clone, Default, Debug, Deserialize, Serialize)]
pub struct AttributesTableHeader {
    pub version: LayerFsVersion,
}

#[allow(dead_code)]
pub struct FileMetadataWriter<'fs> {
    file_table: File<'fs>,
    file_table_start: u64,
    attr_table: File<'fs>,
    attr_table_start: u64,
}

#[allow(dead_code)]
impl<'fs> FileMetadataWriter<'fs> {
    pub async fn new(layer_fs: &'fs LayerFs) -> Result<Self> {
        let mut file_table = layer_fs
            .data_fs
            .create_file_read_write(layer_fs.file_table_path())
            .await?;
        encode(&mut file_table, &FileTableHeader::default()).await?;
        let file_table_start = file_table.stream_position().await?;

        let mut attr_table = layer_fs
            .data_fs
            .create_file_read_write(layer_fs.attributes_table_path())
            .await?;
        encode(&mut attr_table, &AttributesTableHeader::default()).await?;
        let attr_table_start = file_table.stream_position().await?;

        Ok(Self {
            file_table,
            file_table_start,
            attr_table,
            attr_table_start,
        })
    }

    pub async fn insert_file(
        &mut self,
        kind: FileType,
        attrs: FileAttributes,
        data: FileData,
    ) -> Result<FileId> {
        let attr_id = AttributesId::try_from(
            self.attr_table.stream_position().await? - self.attr_table_start + 1,
        )
        .unwrap();
        encode(&mut self.attr_table, &attrs).await?;

        let entry = FileTableEntry {
            kind,
            data,
            attr_id,
        };

        let file_id =
            FileId::try_from(self.file_table.stream_position().await? - self.file_table_start + 1)
                .unwrap();
        encode(&mut self.file_table, &entry).await?;

        Ok(file_id)
    }
}
