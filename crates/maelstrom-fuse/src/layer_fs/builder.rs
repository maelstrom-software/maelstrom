use crate::layer_fs::dir::{DirectoryDataReader, DirectoryDataWriter};
use crate::layer_fs::file::FileMetadataWriter;
use crate::layer_fs::ty::{
    DirectoryEntryData, FileAttributes, FileData, FileId, FileType, LayerId,
};
use crate::layer_fs::LayerFs;
use anyhow::{anyhow, Result};
use maelstrom_base::{
    manifest::{Mode, UnixTimestamp},
    Utf8Component, Utf8Path,
};
use maelstrom_util::async_fs::Fs;
use maelstrom_util::ext::BoolExt as _;
use std::path::Path;

pub struct BottomLayerBuilder<'fs> {
    layer_fs: LayerFs,
    file_writer: FileMetadataWriter<'fs>,
    time: UnixTimestamp,
}

impl<'fs> BottomLayerBuilder<'fs> {
    pub async fn new(data_fs: &'fs Fs, data_dir: &Path, time: UnixTimestamp) -> Result<Self> {
        let layer_fs = LayerFs::new(data_dir).await?;
        let file_table_path = layer_fs.file_table_path(LayerId::BOTTOM)?;
        let attribute_table_path = layer_fs.attributes_table_path(LayerId::BOTTOM)?;

        let mut file_writer =
            FileMetadataWriter::new(data_fs, &file_table_path, &attribute_table_path).await?;
        let root = file_writer
            .insert_file(
                FileType::Directory,
                FileAttributes {
                    size: 0,
                    mode: Mode(0o777),
                    mtime: time,
                },
                FileData::Empty,
            )
            .await?;
        assert_eq!(root, FileId::root(LayerId::BOTTOM));
        DirectoryDataWriter::write_empty(&layer_fs, root).await?;

        Ok(Self {
            layer_fs,
            file_writer,
            time,
        })
    }

    async fn look_up(&mut self, dir_id: FileId, name: &str) -> Result<Option<FileId>> {
        let mut dir_reader = DirectoryDataReader::new(&self.layer_fs, dir_id).await?;
        dir_reader.look_up(name).await
    }

    async fn ensure_path(&mut self, path: &Utf8Path) -> Result<FileId> {
        let mut comp_iter = path.components();
        if comp_iter.next() != Some(Utf8Component::RootDir) {
            return Err(anyhow!("relative path {path}"));
        }

        let mut dir_id = FileId::root(LayerId::BOTTOM);
        for comp in comp_iter {
            let Utf8Component::Normal(comp) = comp else {
                return Err(anyhow!("unsupported path {path}"));
            };
            match self.look_up(dir_id, comp).await? {
                Some(new_dir_id) => dir_id = new_dir_id,
                None => dir_id = self.add_dir(dir_id, comp).await?,
            }
        }
        Ok(dir_id)
    }

    async fn add_dir(&mut self, parent: FileId, name: &str) -> Result<FileId> {
        let attrs = FileAttributes {
            size: 0,
            mode: Mode(0o777),
            mtime: self.time,
        };
        let file_id = self
            .file_writer
            .insert_file(FileType::Directory, attrs, FileData::Empty)
            .await?;
        self.add_link(parent, name, file_id, FileType::Directory)
            .await?
            .assert_is_true();
        DirectoryDataWriter::write_empty(&self.layer_fs, file_id).await?;

        Ok(file_id)
    }

    async fn add_link(
        &mut self,
        parent: FileId,
        name: &str,
        file_id: FileId,
        kind: FileType,
    ) -> Result<bool> {
        let mut dir_writer = DirectoryDataWriter::new(&self.layer_fs, parent).await?;
        dir_writer
            .insert_entry(name, DirectoryEntryData { file_id, kind })
            .await
    }

    pub async fn add_file_path(
        &mut self,
        path: &Utf8Path,
        attrs: FileAttributes,
        data: FileData,
    ) -> Result<FileId> {
        let file_id = self
            .file_writer
            .insert_file(FileType::RegularFile, attrs, data)
            .await?;

        let parent_id = if let Some(parent) = path.parent() {
            self.ensure_path(parent).await?
        } else {
            FileId::root(LayerId::BOTTOM)
        };
        let name = path.file_name().ok_or(anyhow!("missing file name"))?;
        let inserted = self
            .add_link(parent_id, name, file_id, FileType::RegularFile)
            .await?;
        if !inserted {
            return Err(anyhow!("file already exists at {path}"));
        }

        Ok(file_id)
    }

    pub fn finish(self) -> LayerFs {
        self.layer_fs
    }
}
