use crate::dir::{DirectoryDataReader, DirectoryDataWriter, OrderedDirectoryStream};
use crate::file::FileMetadataWriter;
use crate::ty::{
    DirectoryEntryData, DirectoryEntryFileData, FileAttributes, FileData, FileId, FileType,
    LayerId, LayerSuper,
};
use crate::{BlobDir, LayerFs};
use anyhow::bail;
use anyhow::{anyhow, Result};
use anyhow_trace::anyhow_trace;
use futures::stream::{Peekable, StreamExt as _};
use lru::LruCache;
use maelstrom_base::{
    manifest::{ManifestEntryData, Mode, UnixTimestamp},
    Sha256Digest, Utf8Component, Utf8Path,
};
use maelstrom_util::{async_fs::Fs, ext::BoolExt as _, manifest::AsyncManifestReader, root::Root};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::path::Path;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncSeek};
use tokio_tar::{Archive, EntryType};

struct DirectoryDataWriterCache<'fs> {
    data_fs: &'fs Fs,
    cache: LruCache<FileId, DirectoryDataWriter>,
}

impl<'fs> DirectoryDataWriterCache<'fs> {
    const CACHE_SIZE: usize = 100;

    fn new(data_fs: &'fs Fs) -> Self {
        Self {
            data_fs,
            cache: LruCache::new(Self::CACHE_SIZE.try_into().unwrap()),
        }
    }

    async fn get_writer(
        &mut self,
        layer_fs: &LayerFs,
        file_id: FileId,
    ) -> Result<&mut DirectoryDataWriter> {
        if !self.cache.contains(&file_id) {
            let writer = DirectoryDataWriter::new(layer_fs, self.data_fs, file_id).await?;
            if let Some((_, mut old)) = self.cache.push(file_id, writer) {
                old.flush().await?;
            }
        }

        Ok(self.cache.get_mut(&file_id).unwrap())
    }

    async fn flush(&mut self) -> Result<()> {
        for (_, writer) in &mut self.cache {
            writer.flush().await?;
        }
        Ok(())
    }
}

/// Creates a LayerFS bottom layer using a manifest or a tar file as input.
pub struct BottomLayerBuilder<'fs> {
    layer_fs: LayerFs,
    file_writer: FileMetadataWriter,
    time: UnixTimestamp,
    dir_writer_cache: DirectoryDataWriterCache<'fs>,
}

#[anyhow_trace]
impl<'fs> BottomLayerBuilder<'fs> {
    /// Build the new bottom layer in `data_dir`.
    /// `cache_path` should contain a path to a directory that can be used to look-up file-data via
    /// digest.
    /// The given timestamp will be used for the atttributes of files or directories created which
    /// don't otherwise have their timestamp specified by the manifest or tar.
    pub async fn new(
        _log: slog::Logger,
        data_fs: &'fs Fs,
        data_dir: &Path,
        blob_dir: &Root<BlobDir>,
        time: UnixTimestamp,
    ) -> Result<Self> {
        let layer_fs = LayerFs::new(data_dir, blob_dir, LayerSuper::default()).await?;
        let file_table_path = layer_fs.file_table_path(LayerId::BOTTOM).await?;
        let attribute_table_path = layer_fs.attributes_table_path(LayerId::BOTTOM).await?;

        let mut file_writer = FileMetadataWriter::new(
            data_fs,
            LayerId::BOTTOM,
            &file_table_path,
            &attribute_table_path,
        )
        .await?;
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
        file_writer.flush().await?;

        Ok(Self {
            layer_fs,
            file_writer,
            time,
            dir_writer_cache: DirectoryDataWriterCache::new(data_fs),
        })
    }

    async fn look_up(&mut self, dir_id: FileId, name: &str) -> Result<Option<FileId>> {
        let dir_reader = self
            .dir_writer_cache
            .get_writer(&self.layer_fs, dir_id)
            .await?;
        dir_reader.look_up(name).await
    }

    async fn set_opaque_dir(&mut self, dir_id: FileId, name: &str, opaque: bool) -> Result<()> {
        let dir_writer = self
            .dir_writer_cache
            .get_writer(&self.layer_fs, dir_id)
            .await?;
        dir_writer.set_opaque_dir(name, opaque).await
    }

    async fn look_up_entry(
        &mut self,
        dir_id: FileId,
        name: &str,
    ) -> Result<Option<DirectoryEntryData>> {
        let dir_reader = self
            .dir_writer_cache
            .get_writer(&self.layer_fs, dir_id)
            .await?;
        dir_reader.look_up_entry(name).await
    }

    async fn ensure_path(&mut self, path: &Utf8Path) -> Result<FileId> {
        let comp_iter = path.components();

        let mut dir_id = FileId::root(LayerId::BOTTOM);
        for comp in comp_iter {
            if let Utf8Component::RootDir = comp {
                continue;
            };
            let Utf8Component::Normal(comp) = comp else {
                return Err(anyhow!("unsupported path {path}"));
            };
            match self.look_up(dir_id, comp).await? {
                Some(new_dir_id) => dir_id = new_dir_id,
                None => {
                    dir_id = {
                        let attrs = FileAttributes {
                            size: 0,
                            mode: Mode(0o777),
                            mtime: self.time,
                        };
                        self.add_dir(dir_id, comp, attrs).await?
                    }
                }
            }
        }
        Ok(dir_id)
    }

    async fn add_dir(
        &mut self,
        parent: FileId,
        name: &str,
        attrs: FileAttributes,
    ) -> Result<FileId> {
        let file_id = self
            .file_writer
            .insert_file(FileType::Directory, attrs, FileData::Empty)
            .await?;
        self.add_link(parent, name, file_id, FileType::Directory)
            .await?
            .assert_is_true();
        self.dir_writer_cache
            .get_writer(&self.layer_fs, file_id)
            .await?;

        Ok(file_id)
    }

    async fn add_link(
        &mut self,
        parent: FileId,
        name: &str,
        file_id: FileId,
        kind: FileType,
    ) -> Result<bool> {
        let dir_writer = self
            .dir_writer_cache
            .get_writer(&self.layer_fs, parent)
            .await?;
        let inserted = dir_writer
            .insert_entry(
                name,
                DirectoryEntryFileData {
                    file_id,
                    kind,
                    opaque_dir: false,
                },
            )
            .await?;
        Ok(inserted)
    }

    async fn add_whiteout(&mut self, parent: FileId, name: &str) -> Result<bool> {
        let dir_writer = self
            .dir_writer_cache
            .get_writer(&self.layer_fs, parent)
            .await?;
        let inserted = dir_writer
            .insert_entry(name, DirectoryEntryData::Whiteout)
            .await?;
        Ok(inserted)
    }

    /// Add a regular file at the given path in the new layer. Creates any intermediate directories
    /// that don't exist.
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

    /// Add a whiteout entry at the given path in the new layer. Creates any intermediate
    /// directories that don't exist.
    pub async fn add_whiteout_path(&mut self, path: &Utf8Path) -> Result<()> {
        let parent_id = if let Some(parent) = path.parent() {
            self.ensure_path(parent).await?
        } else {
            FileId::root(LayerId::BOTTOM)
        };
        let name = path.file_name().ok_or(anyhow!("missing file name"))?;
        let inserted = self.add_whiteout(parent_id, name).await?;
        if !inserted {
            return Err(anyhow!("file already exists at {path}"));
        }
        Ok(())
    }

    /// Set the attributes for the given existing file in the new layer.
    pub async fn set_attr(&mut self, id: FileId, attrs: FileAttributes) -> Result<()> {
        self.file_writer.update_attributes(id, attrs).await
    }

    /// Add a directory at the given path in the new layer. Creates any intermediate directories
    /// that don't exist.
    pub async fn add_dir_path(&mut self, path: &Utf8Path, attrs: FileAttributes) -> Result<FileId> {
        let parent_id = if let Some(parent) = path.parent() {
            self.ensure_path(parent).await?
        } else {
            FileId::root(LayerId::BOTTOM)
        };
        let name = path.file_name().ok_or(anyhow!("missing file name"))?;
        if let Some(existing) = self.look_up(parent_id, name).await? {
            self.set_attr(existing, attrs).await?;
            Ok(existing)
        } else {
            self.add_dir(parent_id, name, attrs).await
        }
    }

    /// Set the directory at the given path to be opaque. If the given path doesn't exist, a
    /// directory will be created there with any intermediate directories being created also.
    ///
    /// Errors if the given path points to something that isn't a directory.
    pub async fn set_opaque_dir_path(&mut self, path: &Utf8Path) -> Result<()> {
        let parent_id = if let Some(parent) = path.parent() {
            self.ensure_path(parent).await?
        } else {
            FileId::root(LayerId::BOTTOM)
        };
        let name = path.file_name().ok_or(anyhow!("missing file name"))?;

        if self.look_up(parent_id, name).await?.is_none() {
            let attrs = FileAttributes {
                size: 0,
                mode: Mode(0o777),
                mtime: self.time,
            };
            self.add_dir(parent_id, name, attrs).await?;
        }

        self.set_opaque_dir(parent_id, name, true).await?;

        Ok(())
    }

    /// Add a symlink at the given path in the new layer. Creates any intermediate directories
    /// that don't exist.
    pub async fn add_symlink_path(
        &mut self,
        path: &Utf8Path,
        target: impl Into<Vec<u8>>,
    ) -> Result<FileId> {
        let attrs = FileAttributes {
            size: 0,
            mode: Mode(0o777),
            mtime: self.time,
        };
        let file_id = self
            .file_writer
            .insert_file(FileType::Symlink, attrs, FileData::Inline(target.into()))
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

    /// Add a hardlink at the given path in the new layer. Creates any intermediate directories
    /// that don't exist.
    pub async fn add_link_path(&mut self, path: &Utf8Path, target: &Utf8Path) -> Result<FileId> {
        let parent_id = if let Some(parent) = path.parent() {
            self.ensure_path(parent).await?
        } else {
            FileId::root(LayerId::BOTTOM)
        };
        let name = path.file_name().ok_or(anyhow!("missing file name"))?;

        let target_parent_id = if let Some(parent) = target.parent() {
            self.ensure_path(parent).await?
        } else {
            FileId::root(LayerId::BOTTOM)
        };
        let target_name = target.file_name().ok_or(anyhow!("missing file name"))?;

        let existing = self
            .look_up_entry(target_parent_id, target_name)
            .await?
            .ok_or(anyhow!("link target not found {target:?}"))?;
        let Some(existing) = existing.into_file_data() else {
            bail!("hardlink to whiteout entry not allowed {target:?}")
        };

        if existing.kind == FileType::Directory {
            bail!("hardlink to directory not allowed {target:?}")
        }

        self.add_link(parent_id, name, existing.file_id, existing.kind)
            .await?;

        Ok(existing.file_id)
    }

    /// Add all of the entries from the given tar data to the new layer. Data for the new files
    /// will point to the given digest and contain the offset and length from the tar entry.
    pub async fn add_from_tar(
        &mut self,
        digest: Sha256Digest,
        tar_stream: impl AsyncRead + Unpin,
    ) -> Result<()> {
        let mut ar = Archive::new(tar_stream);
        let mut entries = ar.entries()?;
        while let Some(entry) = entries.next().await {
            let entry = entry?;
            let header = entry.header();
            let entry_path = entry.path()?;
            let utf8_path: &Utf8Path = entry_path
                .to_str()
                .ok_or(anyhow!("non-UTF8 path in tar"))?
                .as_ref();
            let path = Utf8Path::new("/").join(utf8_path);
            match header.entry_type() {
                EntryType::Regular => {
                    let file_name = path.file_name().unwrap();
                    if let Some(stripped_name) = file_name.strip_prefix(".wh.") {
                        if file_name == ".wh..wh..opq" {
                            let parent = path.parent().unwrap();
                            self.set_opaque_dir_path(parent).await?;
                            continue;
                        }

                        let mut path = path.clone();
                        path.set_file_name(stripped_name);
                        self.add_whiteout_path(&path).await?;
                        continue;
                    }

                    self.add_file_path(
                        &path,
                        FileAttributes {
                            size: header.size()?,
                            mode: Mode(header.mode()?),
                            mtime: UnixTimestamp(header.mtime()?.try_into()?),
                        },
                        FileData::Digest {
                            digest: digest.clone(),
                            offset: entry.raw_file_position(),
                            length: header.entry_size()?,
                        },
                    )
                    .await?;
                }
                EntryType::Directory => {
                    self.add_dir_path(
                        &path,
                        FileAttributes {
                            size: header.size()?,
                            mode: Mode(header.mode()?),
                            mtime: UnixTimestamp(header.mtime()?.try_into()?),
                        },
                    )
                    .await?;
                }
                EntryType::Symlink => {
                    self.add_symlink_path(
                        &path,
                        header.link_name_bytes().expect("empty symlink in tar"),
                    )
                    .await?;
                }
                EntryType::Link => {
                    self.add_link_path(
                        &path,
                        std::str::from_utf8(
                            &header.link_name_bytes().expect("empty symlink in tar"),
                        )?
                        .into(),
                    )
                    .await?;
                }
                other => {
                    bail!("unsupported tar entry type {other:?}")
                }
            }
        }
        self.file_writer.flush().await?;

        Ok(())
    }

    /// Add all of the entries from the given manifest data to the new layer.
    pub async fn add_from_manifest(
        &mut self,
        manifest_stream: impl AsyncRead + AsyncSeek + Unpin,
    ) -> Result<()> {
        let mut reader = AsyncManifestReader::new(manifest_stream).await?;

        while let Some(entry) = reader.next().await? {
            let attrs = FileAttributes {
                size: entry.metadata.size,
                mode: entry.metadata.mode,
                mtime: entry.metadata.mtime,
            };
            let path = Utf8Path::new("/").join(&entry.path);
            match entry.data {
                ManifestEntryData::Directory { opaque } => {
                    self.add_dir_path(&path, attrs).await?;
                    if opaque {
                        self.set_opaque_dir_path(&path).await?;
                    }
                }
                ManifestEntryData::File(data) => {
                    let data = match data {
                        Some(digest) => FileData::Digest {
                            digest,
                            offset: 0,
                            length: entry.metadata.size,
                        },
                        None => FileData::Empty,
                    };
                    self.add_file_path(&path, attrs, data).await?;
                }
                ManifestEntryData::Symlink(data) => {
                    self.add_symlink_path(&path, data).await?;
                }
                ManifestEntryData::Hardlink(target) => {
                    self.add_link_path(&path, &target).await?;
                }
                ManifestEntryData::Whiteout => {
                    self.add_whiteout_path(&path).await?;
                }
            }
        }

        Ok(())
    }

    /// Finish building. Flush all caches to disk. Returns a `LayerFs` instance for the built layer
    /// for convenience.
    pub async fn finish(mut self) -> Result<LayerFs> {
        self.file_writer.flush().await?;
        self.dir_writer_cache.flush().await?;

        Ok(self.layer_fs)
    }
}

/// Walks the `right_fs` and yields together with it any matching entries from `left_fs`
pub struct DoubleFsWalk<'fs> {
    streams: Vec<(Option<WalkStream>, WalkStream)>,
    left_fs: &'fs LayerFs,
    right_fs: &'fs LayerFs,
}

#[derive(Debug)]
enum LeftRight<T> {
    Left(T),
    Right(T),
    Both(T, T),
}

struct WalkStream {
    stream: Peekable<OrderedDirectoryStream>,
    right_parent: FileId,
}

#[anyhow_trace]
impl WalkStream {
    async fn new(fs: &LayerFs, file_id: FileId, right_parent: FileId) -> Result<Self> {
        Ok(Self {
            stream: DirectoryDataReader::new(fs, file_id)
                .await?
                .into_ordered_stream()
                .await?
                .peekable(),
            right_parent,
        })
    }

    async fn next(&mut self) -> Result<Option<WalkEntry>> {
        Ok(self
            .stream
            .next()
            .await
            .transpose()?
            .map(|(key, data)| WalkEntry {
                key,
                data,
                right_parent: self.right_parent,
            }))
    }
}

#[derive(Debug)]
struct WalkEntry {
    key: String,
    data: DirectoryEntryData,
    right_parent: FileId,
}

#[anyhow_trace]
impl<'fs> DoubleFsWalk<'fs> {
    async fn new(left_fs: &'fs LayerFs, right_fs: &'fs LayerFs) -> Result<Self> {
        let streams = vec![(
            Some(WalkStream::new(left_fs, left_fs.root().await?, right_fs.root().await?).await?),
            WalkStream::new(right_fs, right_fs.root().await?, right_fs.root().await?).await?,
        )];
        Ok(Self {
            streams,
            left_fs,
            right_fs,
        })
    }

    async fn next(&mut self) -> Result<Option<LeftRight<WalkEntry>>> {
        let res = loop {
            let Some((left, right)) = self.streams.last_mut() else {
                return Ok(None);
            };
            let Some(left) = left else {
                if let Some(entry) = right.next().await? {
                    break LeftRight::Right(entry);
                }
                self.streams.pop();
                continue;
            };

            let left_entry = Pin::new(&mut left.stream).peek().await;
            let right_entry = Pin::new(&mut right.stream).peek().await;

            break match (left_entry, right_entry) {
                (Some(_), None) | (Some(_), Some(Err(_))) => {
                    LeftRight::Left(left.next().await?.unwrap())
                }
                (None, Some(_)) | (Some(Err(_)), Some(_)) => {
                    LeftRight::Right(right.next().await?.unwrap())
                }
                (Some(Ok((left_key, _))), Some(Ok((right_key, _)))) => {
                    match left_key.cmp(right_key) {
                        Ordering::Less => LeftRight::Left(left.next().await?.unwrap()),
                        Ordering::Greater => LeftRight::Right(right.next().await?.unwrap()),
                        Ordering::Equal => LeftRight::Both(
                            left.next().await?.unwrap(),
                            right.next().await?.unwrap(),
                        ),
                    }
                }
                (None, None) => {
                    self.streams.pop();
                    continue;
                }
            };
        };

        match &res {
            LeftRight::Both(WalkEntry { data: left, .. }, WalkEntry { data: right, .. }) => {
                if let Some((right_file_id, right_opaque)) = right.as_dir() {
                    let left_stream = match left.as_dir() {
                        Some((left_file_id, _)) if !right_opaque => {
                            Some(WalkStream::new(self.left_fs, left_file_id, right_file_id).await?)
                        }
                        _ => None,
                    };
                    let right_stream =
                        WalkStream::new(self.right_fs, right_file_id, right_file_id).await?;
                    self.streams.push((left_stream, right_stream));
                }
            }
            LeftRight::Right(WalkEntry { data: right, .. }) => {
                if let Some((right_file_id, _)) = right.as_dir() {
                    self.streams.push((
                        None,
                        WalkStream::new(self.right_fs, right_file_id, right_file_id).await?,
                    ));
                }
            }
            _ => (),
        }

        Ok(Some(res))
    }
}

struct DirectoryDataWriterStack<'fs> {
    layer_fs: &'fs LayerFs,
    writers: Vec<(FileId, DirectoryDataWriter)>,
    seen: HashSet<FileId>,
}

#[anyhow_trace]
impl<'fs> DirectoryDataWriterStack<'fs> {
    fn new(layer_fs: &'fs LayerFs) -> Self {
        Self {
            layer_fs,
            writers: vec![],
            seen: HashSet::new(),
        }
    }

    async fn get_writer(&mut self, file_id: FileId) -> Result<&mut DirectoryDataWriter> {
        if self.seen.contains(&file_id) {
            while self.writers.last().unwrap().0 != file_id {
                let (old_id, mut writer) = self.writers.pop().unwrap();
                writer.flush().await?;
                self.seen.remove(&old_id).assert_is_true();
            }
            return Ok(&mut self.writers.last_mut().unwrap().1);
        }

        self.writers.push((
            file_id,
            DirectoryDataWriter::new(self.layer_fs, &self.layer_fs.data_fs, file_id).await?,
        ));
        self.seen.insert(file_id).assert_is_true();

        return Ok(&mut self.writers.last_mut().unwrap().1);
    }

    async fn flush(&mut self) -> Result<()> {
        for (_, writer) in &mut self.writers {
            writer.flush().await?;
        }
        Ok(())
    }
}

/// Builds an upper layer LayerFS layer using a bottom layer as input.
pub struct UpperLayerBuilder<'fs> {
    upper: LayerFs,
    lower: &'fs LayerFs,
}

#[anyhow_trace]
impl<'fs> UpperLayerBuilder<'fs> {
    /// Build the new bottom layer in `data_dir`
    /// `cache_path` should contain a path to a directory that can be used to look-up file-data via
    /// digest.
    /// `lower` is what we are stacking the new layer on top of.
    pub async fn new(
        _log: slog::Logger,
        data_dir: &Path,
        blob_dir: &Root<BlobDir>,
        lower: &'fs LayerFs,
    ) -> Result<Self> {
        let lower_id = lower.layer_super().await?.layer_id;
        let upper_id = lower_id.inc();
        let mut upper_super = lower.layer_super().await?;
        upper_super.layer_id = upper_id;
        upper_super
            .lower_layers
            .insert(lower_id, lower.top_layer_path.clone());

        let upper = LayerFs::new(data_dir, blob_dir, upper_super).await?;

        Ok(Self { upper, lower })
    }

    async fn hard_link_files(&mut self, other: &LayerFs) -> Result<()> {
        let other_file_table = other
            .file_table_path(other.layer_super().await?.layer_id)
            .await?;
        let upper_file_table = self
            .upper
            .file_table_path(self.upper.layer_super().await?.layer_id)
            .await?;
        if self.upper.data_fs.exists(&upper_file_table).await {
            self.upper.data_fs.remove_file(&upper_file_table).await?;
        }
        self.upper
            .data_fs
            .hard_link(other_file_table, upper_file_table)
            .await?;

        let other_attribute_table = other
            .attributes_table_path(other.layer_super().await?.layer_id)
            .await?;
        let upper_attribute_table = self
            .upper
            .attributes_table_path(self.upper.layer_super().await?.layer_id)
            .await?;
        if self.upper.data_fs.exists(&upper_attribute_table).await {
            self.upper
                .data_fs
                .remove_file(&upper_attribute_table)
                .await?;
        }
        self.upper
            .data_fs
            .hard_link(other_attribute_table, upper_attribute_table)
            .await?;

        Ok(())
    }

    /// Fill this upper layer with all of the data from the given bottom layer.
    ///
    /// This function only supports being called once per builder.
    ///
    /// The file-table and attribute-table from the given bottom layer are hardlinked into the new
    /// layer.
    ///
    /// The file-system from the given bottom layer is walked together with the file-system we are
    /// being stacked on top of. Anywhere they overlap we merge the directory contents together. If
    /// a directory doesn't appear in `other` we point directly to the existing directory in the
    /// lower layer.
    pub async fn fill_from_bottom_layer(&mut self, other: &LayerFs) -> Result<()> {
        self.hard_link_files(other).await?;
        let mut dir_writers = DirectoryDataWriterStack::new(&self.upper);
        let upper_id = self.upper.layer_super().await?.layer_id;
        let mut walker = DoubleFsWalk::new(self.lower, other).await?;
        while let Some(res) = walker.next().await? {
            match res {
                LeftRight::Left(entry) => {
                    let dir_id = FileId::new(upper_id, entry.right_parent.offset());
                    let writer = dir_writers.get_writer(dir_id).await?;
                    writer.insert_entry(&entry.key, entry.data).await?;
                }
                LeftRight::Right(entry) | LeftRight::Both(_, entry) => {
                    let Some(mut data) = entry.data.into_file_data() else {
                        continue; // whiteout means we just ignore this entry
                    };
                    let dir_id = FileId::new(upper_id, entry.right_parent.offset());
                    let writer = dir_writers.get_writer(dir_id).await?;
                    let file_id = FileId::new(upper_id, data.file_id.offset());
                    data.file_id = file_id;
                    let kind = data.kind;
                    writer.insert_entry(&entry.key, data).await?;
                    if kind == FileType::Directory {
                        dir_writers.get_writer(file_id).await?;
                    }
                }
            }
        }
        dir_writers.flush().await?;
        Ok(())
    }

    /// Finish building. Flush all caches to disk. Returns a `LayerFs` instance for the built layer
    /// for convenience.
    pub async fn finish(self) -> Result<LayerFs> {
        Ok(self.upper)
    }
}
