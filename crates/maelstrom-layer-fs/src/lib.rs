//! This crate contains a Linux FUSE file-system implementation called LayerFS.
//!
//! # Introduction
//! The file-system is based on "layers" which can stack on top of each-other. Each layer
//! represents its own complete file-system, and when stacked the file-systems are unioned
//! together. When file-systems are unioned together the one which is stacked on "top" of the other
//! is favored. This achieves a similar effect to what can be done with overlay-fs.
//!
//! # Disk Layout
//! A layer is stored on disk as a directory of files. Each directory contains the following files
//! - `super.bin` contains information about the layer including any layers it is stacked on top of
//! - `file_table.bin` contains a listing of all the files in the layer
//! - `attributes_table.bin` contains the attributes for all the files in the layer
//! - `<offset>.dir_data.bin` contains directory contents for the directory found at `<offset>` in
//!    the file table.
//!
//! Notice that none of the above bullets mention containing file-data. Most file-data is instead
//! read from files outside of the layer. This what the `cache_dir` in [`LayerFs::from_path`] is
//! used for. However, if a file contains a small amount of data it can actually be stored in the
//! attributes as "inline data" to avoid the overhead of reading from another file.
//!
//! # The Stacking
//! When a layer is stacked on top of other layers, the directory entries for that layer may point
//! to directories or files in the lower layers. This is supported because [`FileId`] contains a
//! [`LayerId`]. This allows us to create the aforementioned overlay-fs like functionality.
//!
//! When an upper layer contains files which also exist in a lower layer, the lower layer files are
//! obscured by the upper layer files. When an upper layer contains a directory that also exists in
//! the lower layer, the directory contents are unioned together.
//!
//! ```ascii art
//! +------------------------+             +-----------+
//! | /   layer_id 2         |  built from | /         |
//! | `-- a.txt => (2, 10)   | <========== | `-- a.txt |
//! | `-- b.txt => (2, 3)    |             +-----------+
//! | `-- c.txt => (1, 3)    |
//! | `-- d/ => (0, 3)       |
//! +------------------------+             +-----------+
//! | /   layer_id 1         |  built from | /         |
//! | `-- a.txt => (1, 10)   | <========== | `-- a.txt |
//! | `-- c.txt => (1, 3)    |             | `-- c.txt |
//! | `-- d/ => (0, 3)       |             +-----------+
//! +------------------------+
//! | /   layer_id 0         |
//! | `-- d/ => (0, 3)       |
//! |     `-- e.txt => (0, 4)|
//! +------------------------+
//! ```
//! figure 1. Three stacked layers
//!
//! In figure 1 we illustrate a stacking of three layers. We show the [`FileId`] each directory
//! entry has as a way to illustrate how the stacking functions. Here is a description of the
//! layers
//! - Layer 0 has a directory `/d` and file `/d/e.txt`. Since this is a bottom layer all of its
//!   directory entries have `LayerId` 0
//! - Layer 1 has two files `/a.txt` and `/c.txt`. It also has a directory entry for `/d` from
//!   layer 0. Since this layer contains no entry itself for `/d`, this was merged in.
//! - Layer 2 has files `/a.txt`, `/b.txt`, `/c.txt` and directory `/d`. It has inherited entries
//!   for `/b.txt`, `/c.txt` and `/d` This can be seen because they all have `FileId`s which
//!   point to layer 1 or 0. `/a.txt` has `LayerId` 2, meaning that this file is shadowing `/a.txt`
//!   from layer 1. This means the `/a.txt` in layer 1 is now inaccessible.
//!
//! # Building Layers
//! Conceptually there are two different kinds of layers, "bottom layers" and "upper layers".
//! Upper layers are layer which are stacked on top of other layers. Bottom layers are layers which
//! aren't stacked on top of any other layers and are thus self-sufficient. Upper layers require
//! the layers they are stacked on top of to be present in order to be used. Upper layers are
//! created by stacking a bottom layer on top of some other layer.
//!
//! ```ascii art
//! +----------------+
//! | 2 upper layer  | <-------+
//! +----------------+         |
//! +----------------+         | UpperLayerBuilder::fill_from_bottom_layer
//! | 1 upper layer  |         |
//! +----------------+         |
//! +----------------+  +---------------+
//! | 0 bottom layer |  |  bottom layer |
//! +----------------+  +---------------+
//! ```
//! figure 2. A new upper layer with id 2 is being created stacked on top of two other layers.
//!
//! When creating a bottom layer, [`BottomLayerBuilder`] must be used. It can create a bottom layer
//! using either a manifest or a tar file as input.
//!
//! When creating an upper layer, [`UpperLayerBuilder`] must be used. The path to the layer you are
//! stacking on top of is given as input to [`UpperLayerBuilder::new`]. The bottom layer we are
//! using as input to create the upper layer is passed to
//! [`UpperLayerBuilder::fill_from_bottom_layer`].
//!
//! When creating upper layers, the layers that they are stacked on top of are not modified. This
//! means that layers can be reused in multiple stacks. This is also why the layer numbering is
//! done from the bottom-up.
//!
//! # Serving the File-System
//! Serving the file-system is done via FUSE. First the path to the top of the layer stack you want
//! to serve should be passed to [`LayerFs::from_path`]. Then either [`LayerFs::mount`] or
//! [`LayerFs::run_fuse`] should be called.

mod avl;
mod builder;
mod dir;
mod file;
mod ty;

use anyhow::{anyhow, Result};
use anyhow_trace::anyhow_trace;
pub use builder::*;
pub use dir::DirectoryDataReader;
pub use file::FileMetadataReader;
use futures::stream::StreamExt as _;
use lru::LruCache;
use maelstrom_base::Sha256Digest;
use maelstrom_fuse::{
    AttrResponse, EntryResponse, ErrnoResult, FileAttr, FuseFileSystem, ReadLinkResponse,
    ReadResponse, Request,
};
use maelstrom_linux::Errno;
use maelstrom_util::async_fs::Fs;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard};
pub use ty::{FileAttributes, FileData, FileId, FileType, LayerId, LayerSuper};

const TTL: Duration = Duration::from_secs(1); // 1 second
                                              //
fn to_eio<ValueT, ErrorT: std::fmt::Debug>(
    log: slog::Logger,
    res: std::result::Result<ValueT, ErrorT>,
) -> ErrnoResult<ValueT> {
    res.map_err(|err| {
        slog::error!(log, "Got error servicing FUSE request. Returning EIO"; "error" => ?err);
        Errno::EIO
    })
}

fn to_einval<ValueT, ErrorT: std::fmt::Debug>(
    log: slog::Logger,
    res: std::result::Result<ValueT, ErrorT>,
) -> ErrnoResult<ValueT> {
    res.map_err(|err| {
        slog::error!(log, "Got error servicing FUSE request. Returning EIO"; "error" => ?err);
        Errno::EINVAL
    })
}

enum LazyLayerSuperInner {
    NotCached(PathBuf),
    Cached(LayerSuper),
}

struct LazyLayerSuper(Mutex<LazyLayerSuperInner>);

impl LazyLayerSuper {
    fn not_cached(path: PathBuf) -> Self {
        Self(Mutex::new(LazyLayerSuperInner::NotCached(path)))
    }

    fn cached(layer_super: LayerSuper) -> Self {
        Self(Mutex::new(LazyLayerSuperInner::Cached(layer_super)))
    }

    async fn read(&self, data_fs: &Fs) -> Result<MappedMutexGuard<'_, LayerSuper>> {
        let mut inner = self.0.lock().await;
        if let LazyLayerSuperInner::NotCached(path) = &*inner {
            let new_state =
                LazyLayerSuperInner::Cached(LayerSuper::read_from_path(data_fs, path).await?);
            *inner = new_state;
        }
        Ok(MutexGuard::map(inner, |r| {
            let LazyLayerSuperInner::Cached(s) = r else {
                unreachable!()
            };
            s
        }))
    }
}

/// The in-memory representation of a mountable `LayerFs` layer. This can be either a bottom layer
/// or upper layer.
pub struct LayerFs {
    data_fs: Fs,
    top_layer_path: PathBuf,
    layer_super: LazyLayerSuper,
    cache_path: PathBuf,
}

#[anyhow_trace]
impl LayerFs {
    /// Instantiate a `LayerFs` with the given path to its data structures. `cache_path` should
    /// contain a path to a directory that can be used to look-up file-data via digest.
    pub fn from_path(data_dir: &Path, cache_path: &Path) -> Result<Self> {
        let data_fs = Fs::new();
        let data_dir = data_dir.to_owned();

        Ok(Self {
            data_fs,
            layer_super: LazyLayerSuper::not_cached(data_dir.join("super.bin")),
            top_layer_path: data_dir,
            cache_path: cache_path.to_owned(),
        })
    }

    async fn new(data_dir: &Path, cache_path: &Path, layer_super: LayerSuper) -> Result<Self> {
        let data_fs = Fs::new();
        let data_dir = data_dir.to_owned();

        layer_super
            .write_to_path(&data_fs, &data_dir.join("super.bin"))
            .await?;

        Ok(Self {
            data_fs,
            top_layer_path: data_dir,
            layer_super: LazyLayerSuper::cached(layer_super),
            cache_path: cache_path.to_owned(),
        })
    }

    async fn root(&self) -> Result<FileId> {
        Ok(FileId::root(self.layer_super().await?.layer_id))
    }

    async fn data_path(&self, layer_id: LayerId) -> Result<PathBuf> {
        if layer_id == self.layer_super().await?.layer_id {
            Ok(self.top_layer_path.clone())
        } else {
            self.layer_super()
                .await?
                .lower_layers
                .get(&layer_id)
                .ok_or(anyhow!("unknown layer {layer_id:?}"))
                .cloned()
        }
    }

    async fn dir_data_path(&self, mut file_id: FileId) -> Result<PathBuf> {
        if file_id.is_root() {
            file_id = self.root().await?;
        }
        Ok(self
            .data_path(file_id.layer())
            .await?
            .join(format!("{}.dir_data.bin", file_id.offset())))
    }

    async fn file_table_path(&self, layer_id: LayerId) -> Result<PathBuf> {
        Ok(self.data_path(layer_id).await?.join("file_table.bin"))
    }

    async fn attributes_table_path(&self, layer_id: LayerId) -> Result<PathBuf> {
        Ok(self.data_path(layer_id).await?.join("attributes_table.bin"))
    }

    async fn layer_super(&self) -> Result<LayerSuper> {
        Ok(self.layer_super.read(&self.data_fs).await?.clone())
    }

    pub async fn layer_id(&self) -> Result<LayerId> {
        Ok(self.layer_super().await?.layer_id)
    }

    fn cache_entry(&self, digest: &Sha256Digest) -> PathBuf {
        self.cache_path.join(digest.to_string())
    }

    /// Mount the file-system in a child process. It can then be accessed via a path in `/proc`.
    /// See [`maelstrom_fuse::fuse_mount_namespace`] for more details.
    pub async fn mount(
        self,
        log: slog::Logger,
        cache: Arc<Mutex<ReaderCache>>,
    ) -> Result<maelstrom_fuse::FuseNamespaceHandle> {
        slog::debug!(log, "mounting FUSE file-system in namespace");
        let adapter = LayerFsFuseAdapter::new(self, log.clone(), cache);
        maelstrom_fuse::fuse_mount_namespace(adapter, log, "Maelstrom LayerFS").await
    }

    /// Serve the file-system using the given FUSE file-descriptor. The function returns when the
    /// connection has closed either with an error or due to unmounting.
    pub async fn run_fuse(
        self,
        log: slog::Logger,
        cache: Arc<Mutex<ReaderCache>>,
        fd: maelstrom_linux::OwnedFd,
    ) -> Result<()> {
        slog::debug!(log, "running FUSE file-system");
        let adapter = LayerFsFuseAdapter::new(self, log.clone(), cache);
        maelstrom_fuse::run_fuse(adapter, log, fd).await
    }
}

/// This cache contains a synchronized collection of open files and cached data.
pub struct ReaderCache {
    dir_readers: LruCache<PathBuf, Arc<Mutex<DirectoryDataReader>>>,
    file_readers: LruCache<PathBuf, Arc<Mutex<FileMetadataReader>>>,
    data_files: LruCache<Sha256Digest, Arc<std::fs::File>>,
}

impl Default for ReaderCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ReaderCache {
    pub fn new() -> Self {
        Self {
            dir_readers: LruCache::new(300.try_into().unwrap()),
            file_readers: LruCache::new(300.try_into().unwrap()),
            data_files: LruCache::new(200.try_into().unwrap()),
        }
    }

    async fn open_dir(
        &mut self,
        layer_fs: &LayerFs,
        file_id: FileId,
    ) -> Result<Arc<Mutex<DirectoryDataReader>>> {
        let path = layer_fs.dir_data_path(file_id).await?;
        if let Some(reader) = self.dir_readers.get(&path) {
            Ok(reader.clone())
        } else {
            let reader = Arc::new(Mutex::new(
                DirectoryDataReader::new(layer_fs, file_id).await?,
            ));
            self.dir_readers.push(path, reader.clone());
            Ok(reader)
        }
    }

    async fn files(
        &mut self,
        layer_fs: &LayerFs,
        layer_id: LayerId,
    ) -> Result<Arc<Mutex<FileMetadataReader>>> {
        let path = layer_fs.file_table_path(layer_id).await?;
        if let Some(reader) = self.file_readers.get(&path) {
            Ok(reader.clone())
        } else {
            let reader = Arc::new(Mutex::new(
                FileMetadataReader::new(layer_fs, layer_id).await?,
            ));
            self.file_readers.push(path, reader.clone());
            Ok(reader)
        }
    }

    async fn data_file(
        &mut self,
        layer_fs: &LayerFs,
        digest: &Sha256Digest,
    ) -> Result<Arc<std::fs::File>> {
        if let Some(file) = self.data_files.get(digest) {
            Ok(file.clone())
        } else {
            let file = layer_fs
                .data_fs
                .open_file(layer_fs.cache_entry(digest))
                .await?;
            let file = Arc::new(file.into_std().await);
            self.data_files.push(digest.clone(), file.clone());
            Ok(file)
        }
    }
}

struct LayerFsFuseAdapter {
    layer_fs: LayerFs,
    log: slog::Logger,
    cache: Arc<Mutex<ReaderCache>>,
}

impl LayerFsFuseAdapter {
    fn new(layer_fs: LayerFs, log: slog::Logger, cache: Arc<Mutex<ReaderCache>>) -> Self {
        Self {
            layer_fs,
            log,
            cache,
        }
    }
}

impl FuseFileSystem for LayerFsFuseAdapter {
    async fn look_up(&self, req: Request, parent: u64, name: &OsStr) -> ErrnoResult<EntryResponse> {
        let name = to_einval(self.log.clone(), name.to_str().ok_or("invalid name"))?;
        let parent = to_einval(self.log.clone(), FileId::try_from(parent))?;
        let reader = to_eio(
            self.log.clone(),
            self.cache
                .lock()
                .await
                .open_dir(&self.layer_fs, parent)
                .await,
        )?;
        let child_id = to_eio(self.log.clone(), reader.lock().await.look_up(name).await)?
            .ok_or(Errno::ENOENT)?;
        let attrs = self.get_attr(req, child_id.as_u64()).await?;
        Ok(EntryResponse {
            attr: attrs.attr,
            ttl: TTL,
            generation: 0,
        })
    }

    async fn get_attr(&self, _req: Request, ino: u64) -> ErrnoResult<AttrResponse> {
        let file = to_einval(self.log.clone(), FileId::try_from(ino))?;
        let reader = to_eio(
            self.log.clone(),
            self.cache
                .lock()
                .await
                .files(&self.layer_fs, file.layer())
                .await,
        )?;
        let (kind, attrs) = to_eio(self.log.clone(), reader.lock().await.get_attr(file).await)?;
        Ok(AttrResponse {
            ttl: TTL,
            attr: FileAttr {
                ino,
                size: attrs.size,
                blocks: 0,
                atime: attrs.mtime.into(),
                mtime: attrs.mtime.into(),
                ctime: attrs.mtime.into(),
                kind,
                perm: u32::from(attrs.mode) as u16,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                blksize: 512,
            },
        })
    }

    async fn read(
        &self,
        _req: Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
    ) -> ErrnoResult<ReadResponse> {
        let file = to_einval(self.log.clone(), FileId::try_from(ino))?;
        let reader = to_eio(
            self.log.clone(),
            self.cache
                .lock()
                .await
                .files(&self.layer_fs, file.layer())
                .await,
        )?;
        let (kind, data) = to_eio(self.log.clone(), reader.lock().await.get_data(file).await)?;
        if kind != FileType::RegularFile {
            return Err(Errno::EINVAL);
        }
        match data {
            FileData::Empty => Ok(ReadResponse::Buffer { data: vec![] }),
            FileData::Inline(inline) => {
                let offset = to_einval(self.log.clone(), usize::try_from(offset))?;
                if offset >= inline.len() {
                    return Err(Errno::EINVAL);
                }
                let size = std::cmp::min(size as usize, inline.len() - offset);

                Ok(ReadResponse::Buffer {
                    data: inline[offset..(offset + size)].to_vec(),
                })
            }
            FileData::Digest {
                digest,
                offset: file_start,
                length: file_length,
            } => {
                let read_start =
                    file_start + to_einval::<u64, _>(self.log.clone(), offset.try_into())?;
                let file_end = file_start + file_length;
                if read_start > file_end {
                    return Err(Errno::EINVAL);
                }

                let read_end = std::cmp::min(read_start + size as u64, file_end);
                let read_length = read_end - read_start;

                let file = to_eio(
                    self.log.clone(),
                    self.cache
                        .lock()
                        .await
                        .data_file(&self.layer_fs, &digest)
                        .await,
                )?;
                Ok(ReadResponse::Splice {
                    file,
                    offset: read_start,
                    length: read_length as usize,
                })
            }
        }
    }

    type ReadDirStream<'a> = Pin<
        Box<
            dyn futures::Stream<Item = maelstrom_fuse::ErrnoResult<maelstrom_fuse::DirEntry>>
                + Send,
        >,
    >;

    async fn read_dir(
        &self,
        _req: Request,
        ino: u64,
        _fh: u64,
        offset: i64,
    ) -> ErrnoResult<Self::ReadDirStream<'_>> {
        let file = to_einval(self.log.clone(), FileId::try_from(ino))?;
        let reader = to_eio(
            self.log.clone(),
            DirectoryDataReader::new(&self.layer_fs, file).await,
        )?;
        let stream = to_eio(
            self.log.clone(),
            reader.into_stream(offset.try_into()?).await,
        )?;
        let log = self.log.clone();
        Ok(Box::pin(stream.filter_map(move |res| {
            let log = log.clone();
            async move {
                let (offset, entry) = match to_eio(log, res) {
                    Err(error) => {
                        return Some(Err(error));
                    }
                    Ok(value) => value,
                };
                let value = entry.value.into_file_data()?;
                Some(Ok(maelstrom_fuse::DirEntry {
                    ino: value.file_id.as_u64(),
                    offset: i64::try_from(offset).unwrap(),
                    kind: value.kind,
                    name: entry.key,
                }))
            }
        })))
    }

    async fn read_link(&self, _req: Request, ino: u64) -> ErrnoResult<ReadLinkResponse> {
        let file = to_einval(self.log.clone(), FileId::try_from(ino))?;
        let reader = to_eio(
            self.log.clone(),
            self.cache
                .lock()
                .await
                .files(&self.layer_fs, file.layer())
                .await,
        )?;
        let (kind, data) = to_eio(self.log.clone(), reader.lock().await.get_data(file).await)?;
        if kind != FileType::Symlink {
            return Err(Errno::EINVAL);
        }
        match data {
            FileData::Empty => Ok(ReadLinkResponse { data: vec![] }),
            FileData::Inline(inline) => Ok(ReadLinkResponse {
                data: inline.to_vec(),
            }),
            FileData::Digest { .. } => Err(Errno::EIO),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maelstrom_base::manifest::UnixTimestamp;
    use maelstrom_base::{
        manifest::{ManifestEntry, ManifestEntryData, ManifestEntryMetadata, Mode},
        Utf8PathBuf,
    };
    use maelstrom_util::manifest::AsyncManifestWriter;
    use slog::Drain as _;
    use std::collections::HashMap;
    use std::future::Future;
    use std::os::unix::fs::MetadataExt as _;
    use std::pin::Pin;

    const ARBITRARY_TIME: UnixTimestamp = UnixTimestamp(1705000271);

    #[derive(Debug)]
    enum BuildEntryData {
        Regular {
            type_: FileType,
            data: FileData,
            mode: u32,
            opaque_dir: bool,
        },
        Link {
            target: String,
            hard: bool,
        },
        Whiteout,
    }

    struct BuildEntry {
        path: String,
        data: BuildEntryData,
    }

    impl BuildEntry {
        fn reg(path: impl Into<String>, data: impl Into<Vec<u8>>) -> Self {
            Self::reg_mode(path, FileData::Inline(data.into()), 0o555)
        }

        fn reg_digest(
            path: impl Into<String>,
            digest: Sha256Digest,
            offset: u64,
            length: u64,
        ) -> Self {
            Self::reg_mode(
                path,
                FileData::Digest {
                    digest,
                    offset,
                    length,
                },
                0o555,
            )
        }

        fn reg_empty(path: impl Into<String>) -> Self {
            Self::reg_empty_mode(path, 0o555)
        }

        fn reg_empty_mode(path: impl Into<String>, mode: u32) -> Self {
            Self {
                path: path.into(),
                data: BuildEntryData::Regular {
                    type_: FileType::RegularFile,
                    data: FileData::Empty,
                    mode,
                    opaque_dir: false,
                },
            }
        }

        fn reg_mode(path: impl Into<String>, data: FileData, mode: u32) -> Self {
            Self {
                path: path.into(),
                data: BuildEntryData::Regular {
                    type_: FileType::RegularFile,
                    data,
                    mode,
                    opaque_dir: false,
                },
            }
        }

        fn dir(path: impl Into<String>) -> Self {
            Self::dir_args(path, 0o555, false)
        }

        fn opaque_dir(path: impl Into<String>) -> Self {
            Self::dir_args(path, 0o555, true)
        }

        fn dir_args(path: impl Into<String>, mode: u32, opaque: bool) -> Self {
            Self {
                path: path.into(),
                data: BuildEntryData::Regular {
                    type_: FileType::Directory,
                    data: FileData::Empty,
                    mode,
                    opaque_dir: opaque,
                },
            }
        }

        fn sym(path: impl Into<String>, target: impl Into<String>) -> Self {
            Self {
                path: path.into(),
                data: BuildEntryData::Link {
                    target: target.into(),
                    hard: false,
                },
            }
        }

        fn link(path: impl Into<String>, target: impl Into<String>) -> Self {
            Self {
                path: path.into(),
                data: BuildEntryData::Link {
                    target: target.into(),
                    hard: true,
                },
            }
        }

        fn whiteout(path: impl Into<String>) -> Self {
            Self {
                path: path.into(),
                data: BuildEntryData::Whiteout,
            }
        }

        fn from_str(s: &str) -> Self {
            if s.starts_with("wh:") {
                Self::whiteout(&s[3..])
            } else if s.starts_with("hl:") {
                let mut split = s[3..].split(" -> ");
                Self::link(split.next().unwrap(), split.next().unwrap())
            } else if s.starts_with("sym:") {
                let mut split = s[4..].split(" -> ");
                Self::link(split.next().unwrap(), split.next().unwrap())
            } else if s.starts_with("opq:") {
                let path = &s[4..];
                assert!(path.ends_with("/"), "{path:?}");
                Self::opaque_dir(path)
            } else if s.ends_with("/") {
                Self::dir(s)
            } else {
                Self::reg_empty(s)
            }
        }
    }

    fn test_logger() -> slog::Logger {
        let decorator = slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, slog::o!())
    }

    async fn assert_entries(fs: &Fs, path: &Path, expected: Vec<&str>) {
        let mut entry_stream = fs.read_dir(path).await.unwrap();
        let mut entries = vec![];
        while let Some(e) = entry_stream.next().await {
            let e = e.unwrap();
            let mut name = e.file_name();
            if e.file_type().await.unwrap().is_dir() {
                name.push("/");
            }
            entries.push(name);
        }
        entries.sort();
        assert_eq!(
            entries,
            Vec::from_iter(expected.into_iter().map(|e| std::ffi::OsString::from(e)))
        );
    }

    #[derive(Clone, Default)]
    struct ExpectedAttrs {
        len: Option<u64>,
        mode: Option<Mode>,
        mtime: Option<UnixTimestamp>,
        ino: Option<u64>,
    }

    impl From<Mode> for ExpectedAttrs {
        fn from(m: Mode) -> Self {
            Self {
                mode: Some(m),
                ..Default::default()
            }
        }
    }

    impl ExpectedAttrs {
        async fn assert(&self, fs: &Fs, path: &Path) {
            let attrs = fs.metadata(path).await.unwrap();
            if let Some(len) = &self.len {
                assert_eq!(attrs.len(), *len);
            }
            if let Some(mode) = &self.mode {
                assert_eq!(Mode(attrs.mode() & 0o777), *mode);
            }
            if let Some(mtime) = &self.mtime {
                assert_eq!(attrs.mtime(), (*mtime).into());
            }
            if let Some(ino) = &self.ino {
                assert_eq!(attrs.ino(), *ino);
            }
        }
    }

    async fn assert_contents(fs: &Fs, path: &Path, expected: &str) {
        let actual = fs.read_to_string(path).await.unwrap();
        assert_eq!(actual, expected);
    }

    enum Expect {
        Entries(&'static str, Vec<&'static str>),
        Exists(&'static str),
        NotExists(&'static str),
        Attrs(&'static str, ExpectedAttrs),
        Contents(&'static str, &'static str),
        IsSymlink(&'static str),
    }

    async fn assert_expectations(fs: &Fs, root: &Path, expected: Vec<Expect>) {
        for expect in expected {
            match expect {
                Expect::Entries(e, entries) => assert_entries(fs, &root.join(e), entries).await,
                Expect::Exists(e) => assert!(fs.exists(&root.join(e)).await, "/{e}"),
                Expect::NotExists(e) => assert!(!fs.exists(&root.join(e)).await, "/{e}"),
                Expect::Attrs(e, attrs) => attrs.assert(fs, &root.join(e)).await,
                Expect::Contents(e, contents) => assert_contents(fs, &root.join(e), contents).await,
                Expect::IsSymlink(e) => {
                    let sym_meta = fs.symlink_metadata(root.join(e)).await.unwrap();
                    assert!(sym_meta.is_symlink(), "/{e}");
                }
            }
        }
    }

    struct Fixture {
        fs: Fs,
        temp: tempfile::TempDir,
        data_dirs: HashMap<usize, PathBuf>,
        cache_dir: PathBuf,
        log: slog::Logger,
        data_dir_index: usize,
        cache: Arc<Mutex<ReaderCache>>,
    }

    impl Fixture {
        async fn new() -> Self {
            let fs = Fs::new();
            let temp = tempfile::tempdir().unwrap();
            let cache_dir = temp.path().join("cache");
            fs.create_dir(&cache_dir).await.unwrap();
            Self {
                fs,
                temp,
                data_dirs: HashMap::new(),
                cache_dir,
                log: test_logger(),
                data_dir_index: 1,
                cache: Arc::new(Mutex::new(ReaderCache::new())),
            }
        }

        async fn add_to_cache(&self, data: &[u8]) -> Sha256Digest {
            let temp_path = self.cache_dir.join("temp");
            self.fs.write(&temp_path, data).await.unwrap();
            let digest = calc_digest(&self.fs, &temp_path).await;
            self.fs
                .rename(temp_path, self.cache_dir.join(digest.to_string()))
                .await
                .unwrap();
            digest
        }

        async fn new_data_dir(&mut self) -> PathBuf {
            let index = self.data_dir_index;
            self.data_dir_index += 1;
            let data_dir = self.temp.path().join(&format!("data{index}"));
            self.fs.create_dir(&data_dir).await.unwrap();
            self.data_dirs.insert(index, data_dir.clone());
            data_dir
        }

        async fn bottom_layer_builder(&self, data_dir: &Path) -> BottomLayerBuilder {
            BottomLayerBuilder::new(
                self.log.clone(),
                &self.fs,
                &data_dir,
                &self.cache_dir,
                ARBITRARY_TIME,
            )
            .await
            .unwrap()
        }

        async fn build_bottom_layer(&mut self, files: Vec<BuildEntry>) -> LayerFs {
            let data_dir = self.new_data_dir().await;
            let mut builder = self.bottom_layer_builder(&data_dir).await;

            for BuildEntry { path, data } in files {
                let size = match &data {
                    BuildEntryData::Regular {
                        data: ty::FileData::Inline(d),
                        ..
                    } => d.len() as u64,
                    BuildEntryData::Regular {
                        data: ty::FileData::Digest { length, .. },
                        ..
                    } => *length,
                    _ => 0,
                };
                match data {
                    BuildEntryData::Link { hard: true, target } => {
                        builder
                            .add_link_path(path.as_ref(), target.as_ref())
                            .await
                            .unwrap();
                    }
                    BuildEntryData::Link {
                        hard: false,
                        target,
                    } => {
                        builder
                            .add_symlink_path(path.as_ref(), target)
                            .await
                            .unwrap();
                    }
                    BuildEntryData::Regular {
                        type_,
                        data,
                        mode,
                        opaque_dir,
                    } => match type_ {
                        FileType::RegularFile => {
                            builder
                                .add_file_path(
                                    path.as_ref(),
                                    ty::FileAttributes {
                                        size,
                                        mode: Mode(mode),
                                        mtime: ARBITRARY_TIME,
                                    },
                                    data,
                                )
                                .await
                                .unwrap();
                        }
                        FileType::Directory => {
                            builder
                                .add_dir_path(
                                    path.as_ref(),
                                    ty::FileAttributes {
                                        size,
                                        mode: Mode(mode),
                                        mtime: ARBITRARY_TIME,
                                    },
                                )
                                .await
                                .unwrap();
                            if opaque_dir {
                                builder.set_opaque_dir_path(path.as_ref()).await.unwrap();
                            }
                        }
                        FileType::Symlink => {}
                        other => panic!("unsupported file type {other:?}"),
                    },
                    BuildEntryData::Whiteout => {
                        builder.add_whiteout_path(path.as_ref()).await.unwrap();
                    }
                }
            }

            builder.finish().await.unwrap()
        }

        async fn build_upper_layer(&mut self, lower: &LayerFs, upper: &LayerFs) -> LayerFs {
            let data_dir = self.new_data_dir().await;
            let mut builder =
                UpperLayerBuilder::new(self.log.clone(), &data_dir, &self.cache_dir, lower)
                    .await
                    .unwrap();
            builder.fill_from_bottom_layer(upper).await.unwrap();
            builder.finish().await.unwrap()
        }

        async fn build_tar(&self, files: Vec<BuildEntry>) -> (Sha256Digest, PathBuf) {
            let tar_path = self.cache_dir.join("temp.tar");
            let f = self.fs.create_file(&tar_path).await.unwrap();
            let mut ar = tokio_tar::Builder::new(f.into_inner());
            for BuildEntry { path, data } in files {
                let mut header = tokio_tar::Header::new_gnu();
                match data {
                    BuildEntryData::Regular {
                        data, type_, mode, ..
                    } => {
                        header.set_entry_type(match type_ {
                            FileType::RegularFile => tokio_tar::EntryType::Regular,
                            FileType::Directory => tokio_tar::EntryType::Directory,
                            other => panic!("unsupported entry type {other:?}"),
                        });
                        let data = match data {
                            ty::FileData::Empty => vec![],
                            ty::FileData::Inline(d) => d,
                            _ => panic!(),
                        };
                        header.set_size(data.len() as u64);
                        header.set_mode(mode);
                        ar.append_data(&mut header, path, &data[..]).await.unwrap();
                    }
                    BuildEntryData::Link { hard, target } => {
                        header.set_entry_type(if hard {
                            tokio_tar::EntryType::Link
                        } else {
                            tokio_tar::EntryType::Symlink
                        });
                        header.set_size(0);
                        header.set_link_name(target).unwrap();
                        ar.append_data(&mut header, path, tokio::io::empty())
                            .await
                            .unwrap();
                    }
                    other => panic!("unsupported builder entry for tar {other:?}"),
                }
            }
            ar.finish().await.unwrap();

            let digest = calc_digest(&self.fs, &tar_path).await;

            let final_path = self.cache_dir.join(digest.to_string());
            self.fs.rename(tar_path, &final_path).await.unwrap();

            (digest, final_path)
        }

        async fn build_manifest(&self, files: Vec<BuildEntry>) -> PathBuf {
            let manifest_path = self.cache_dir.join("temp.manifest");
            let f = self.fs.create_file(&manifest_path).await.unwrap();
            let mut builder = AsyncManifestWriter::new(f).await.unwrap();
            for BuildEntry { path, data } in files {
                let path: Utf8PathBuf = path.into();
                match data {
                    BuildEntryData::Regular {
                        data, type_, mode, ..
                    } => {
                        let size = match &data {
                            ty::FileData::Empty => 0,
                            ty::FileData::Inline(d) => d.len() as u64,
                            ty::FileData::Digest { length, .. } => *length,
                        };
                        let metadata = ManifestEntryMetadata {
                            size,
                            mode: Mode(mode),
                            mtime: ARBITRARY_TIME,
                        };
                        match type_ {
                            FileType::Directory => builder
                                .write_entry(&ManifestEntry {
                                    path,
                                    metadata,
                                    data: ManifestEntryData::Directory,
                                })
                                .await
                                .unwrap(),
                            FileType::RegularFile => {
                                let data = match data {
                                    ty::FileData::Empty => None,
                                    ty::FileData::Inline(d) => Some(self.add_to_cache(&d).await),
                                    ty::FileData::Digest { digest, offset, .. } => {
                                        assert_eq!(offset, 0);
                                        Some(digest)
                                    }
                                };
                                builder
                                    .write_entry(&ManifestEntry {
                                        path,
                                        metadata,
                                        data: ManifestEntryData::File(data),
                                    })
                                    .await
                                    .unwrap();
                            }
                            other => panic!("unsupported entry type {other:?}"),
                        }
                    }
                    BuildEntryData::Link { hard: true, target } => {
                        let metadata = ManifestEntryMetadata {
                            size: 0,
                            mode: Mode(0o777),
                            mtime: ARBITRARY_TIME,
                        };
                        builder
                            .write_entry(&ManifestEntry {
                                path,
                                metadata,
                                data: ManifestEntryData::Hardlink(target.into()),
                            })
                            .await
                            .unwrap();
                    }
                    BuildEntryData::Link {
                        hard: false,
                        target,
                    } => {
                        let metadata = ManifestEntryMetadata {
                            size: 0,
                            mode: Mode(0o777),
                            mtime: ARBITRARY_TIME,
                        };
                        builder
                            .write_entry(&ManifestEntry {
                                path,
                                metadata,
                                data: ManifestEntryData::Symlink(target.into()),
                            })
                            .await
                            .unwrap();
                    }
                    other => panic!("unsupported builder entry for manifest {other:?}"),
                }
            }
            drop(builder);

            let digest = calc_digest(&self.fs, &manifest_path).await;

            let final_path = self.cache_dir.join(digest.to_string());
            self.fs.rename(manifest_path, &final_path).await.unwrap();

            final_path
        }

        async fn build_bottom_layer_from_tar(&mut self, input: Vec<BuildEntry>) -> LayerFs {
            let data_dir = self.new_data_dir().await;
            let mut builder = self.bottom_layer_builder(&data_dir).await;

            let (tar_digest, tar_path) = self.build_tar(input).await;
            builder
                .add_from_tar(tar_digest, self.fs.open_file(tar_path).await.unwrap())
                .await
                .unwrap();

            builder.finish().await.unwrap()
        }

        async fn build_bottom_layer_from_manifest(&mut self, input: Vec<BuildEntry>) -> LayerFs {
            let data_dir = self.new_data_dir().await;
            let mut builder = self.bottom_layer_builder(&data_dir).await;

            let manifest_path = self.build_manifest(input).await;
            builder
                .add_from_manifest(self.fs.open_file(manifest_path).await.unwrap())
                .await
                .unwrap();

            builder.finish().await.unwrap()
        }

        async fn mount(&self, layer_fs: LayerFs) -> maelstrom_fuse::FuseNamespaceHandle {
            layer_fs
                .mount(self.log.clone(), self.cache.clone())
                .await
                .unwrap()
        }
    }

    #[tokio::test]
    async fn read_dir_and_look_up() {
        let mut fix = Fixture::new().await;

        let layer_fs = fix
            .build_bottom_layer(vec![
                BuildEntry::reg_empty("/Foo"),
                BuildEntry::reg_empty("/Bar"),
                BuildEntry::reg_empty("/Baz"),
                BuildEntry::whiteout("/Qux"), // should be ignored
            ])
            .await;

        let mount_handle = fix.mount(layer_fs).await;
        let mount_path = mount_handle.mount_path();

        assert_expectations(
            &fix.fs,
            &mount_path,
            vec![
                Expect::Entries("", vec!["Bar", "Baz", "Foo"]),
                Expect::Exists("Bar"),
                Expect::Exists("Baz"),
                Expect::Exists("Foo"),
                Expect::NotExists("Qux"),
            ],
        )
        .await;

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn read_dir_multi_level() {
        let mut fix = Fixture::new().await;

        let layer_fs = fix
            .build_bottom_layer(vec![
                BuildEntry::reg_empty("/Foo/Bar/Baz"),
                BuildEntry::reg_empty("/Foo/Bin"),
                BuildEntry::reg_empty("/Foo/Bar/Qux"),
            ])
            .await;

        let mount_handle = fix.mount(layer_fs).await;
        let mount_path = mount_handle.mount_path();

        assert_expectations(
            &fix.fs,
            &mount_path,
            vec![
                Expect::Entries("", vec!["Foo/"]),
                Expect::Entries("Foo", vec!["Bar/", "Bin"]),
                Expect::Entries("Foo/Bar", vec!["Baz", "Qux"]),
            ],
        )
        .await;

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn get_attr() {
        let mut fix = Fixture::new().await;

        let layer_fs = fix
            .build_bottom_layer(vec![
                BuildEntry::reg_empty("/Foo"),
                BuildEntry::reg_empty("/Bar"),
                BuildEntry::reg_empty("/Baz"),
            ])
            .await;

        let mount_handle = fix.mount(layer_fs).await;
        let mount_path = mount_handle.mount_path();

        let expected_attrs = ExpectedAttrs {
            len: Some(0),
            mode: Some(Mode(0o555)),
            mtime: Some(ARBITRARY_TIME),
            ino: None,
        };
        assert_expectations(
            &fix.fs,
            &mount_path,
            ["Foo", "Bar", "Baz"]
                .into_iter()
                .map(|d| Expect::Attrs(d, expected_attrs.clone()))
                .collect(),
        )
        .await;

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn hard_link() {
        let mut fix = Fixture::new().await;

        let layer_fs = fix
            .build_bottom_layer(vec![
                BuildEntry::reg_empty("/Foo"),
                BuildEntry::link("/Bar", "/Foo"),
                BuildEntry::link("/Baz", "/Bar"),
            ])
            .await;

        let mount_handle = fix.mount(layer_fs).await;
        let mount_path = mount_handle.mount_path();

        let foo_attrs = fix.fs.metadata(mount_path.join("Foo")).await.unwrap();

        let expected_attrs = ExpectedAttrs {
            len: Some(0),
            mode: Some(Mode(0o555)),
            mtime: Some(ARBITRARY_TIME),
            ino: Some(foo_attrs.ino()),
        };
        assert_expectations(
            &fix.fs,
            &mount_path,
            ["Foo", "Bar", "Baz"]
                .into_iter()
                .map(|d| Expect::Attrs(d, expected_attrs.clone()))
                .collect(),
        )
        .await;

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn read_inline() {
        let mut fix = Fixture::new().await;

        let layer_fs = fix
            .build_bottom_layer(vec![
                BuildEntry::reg("/Foo", b"hello world"),
                BuildEntry::reg_empty("/Bar"),
                BuildEntry::reg_empty("/Baz"),
            ])
            .await;

        let mount_handle = fix.mount(layer_fs).await;
        let mount_path = mount_handle.mount_path();

        assert_expectations(
            &fix.fs,
            &mount_path,
            vec![
                Expect::Contents("Foo", "hello world"),
                Expect::Contents("Bar", ""),
            ],
        )
        .await;

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn read_link() {
        let mut fix = Fixture::new().await;

        let layer_fs = fix
            .build_bottom_layer(vec![
                BuildEntry::reg("/Foo", b"hello world"),
                BuildEntry::sym("/Bar", "./Foo"),
            ])
            .await;

        let mount_handle = fix.mount(layer_fs).await;
        let mount_path = mount_handle.mount_path();

        let contents = fix.fs.read_to_string(mount_path.join("Foo")).await.unwrap();
        assert_eq!(contents, "hello world");

        assert_expectations(
            &fix.fs,
            &mount_path,
            vec![
                Expect::IsSymlink("Bar"),
                Expect::Contents("Bar", "hello world"),
            ],
        )
        .await;

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn read_digest() {
        let mut fix = Fixture::new().await;

        let digest = fix.add_to_cache(b"hello world").await;

        let layer_fs = fix
            .build_bottom_layer(vec![
                BuildEntry::reg_digest("/Foo", digest.clone(), 0, 5),
                BuildEntry::reg_digest("/Bar", digest.clone(), 6, 5),
            ])
            .await;

        let mount_handle = fix.mount(layer_fs).await;
        let mount_path = mount_handle.mount_path();

        assert_expectations(
            &fix.fs,
            &mount_path,
            vec![
                Expect::Contents("Foo", "hello"),
                Expect::Contents("Bar", "world"),
            ],
        )
        .await;

        mount_handle.umount_and_join().await.unwrap();
    }

    async fn calc_digest(fs: &Fs, path: &Path) -> Sha256Digest {
        let mut f = fs.open_file(path).await.unwrap();
        let mut hasher = maelstrom_util::io::Sha256Stream::new(tokio::io::sink());
        tokio::io::copy(&mut f, &mut hasher).await.unwrap();
        hasher.finalize().1
    }

    #[cfg(test)]
    async fn layer_from_tar_or_manifest(
        populate_fn: impl for<'a> FnOnce(
            &'a mut Fixture,
            Vec<BuildEntry>,
        ) -> Pin<Box<dyn Future<Output = LayerFs> + 'a>>,
    ) {
        let mut fix = Fixture::new().await;
        let input = vec![
            BuildEntry::reg("Foo", b"hello world"),
            BuildEntry::dir("Qux"),
            BuildEntry::reg_empty("Bar/Baz"),
            BuildEntry::reg_empty("Bar/Bin"),
            BuildEntry::reg_empty("Qux/Fred"),
            BuildEntry::dir_args("Bar", 0o666, false),
            BuildEntry::sym("Waldo", "Foo"),
            BuildEntry::link("Thud", "/Foo"),
        ];

        let layer_fs = populate_fn(&mut fix, input).await;

        let mount_handle = fix.mount(layer_fs).await;
        let mount_path = mount_handle.mount_path();

        let mut expectations = vec![
            Expect::Contents("Foo", "hello world"),
            Expect::Contents("Thud", "hello world"),
            Expect::IsSymlink("Waldo"),
            Expect::Contents("Waldo", "hello world"),
            Expect::Contents("Bar/Baz", ""),
            Expect::Entries("", vec!["Bar/", "Foo", "Qux/", "Thud", "Waldo"]),
            Expect::Entries("Bar", vec!["Baz", "Bin"]),
            Expect::Entries("Qux", vec!["Fred"]),
            Expect::Attrs("Bar", Mode(0o666).into()),
        ];

        for e in [
            "Foo", "Qux", "Bar/Baz", "Bar/Bin", "Qux/Fred", "Waldo", "Thud",
        ] {
            expectations.push(Expect::Attrs(e, Mode(0o555).into()));
        }

        assert_expectations(&fix.fs, &mount_path, expectations).await;

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn layer_from_tar() {
        layer_from_tar_or_manifest(|fix, input| {
            Box::pin(async move { fix.build_bottom_layer_from_tar(input).await })
        })
        .await
    }

    #[tokio::test]
    async fn layer_from_manifest() {
        layer_from_tar_or_manifest(|fix, input| {
            Box::pin(async move { fix.build_bottom_layer_from_manifest(input).await })
        })
        .await
    }

    async fn two_layer_test(lower: Vec<&str>, upper: Vec<&str>, expected: Vec<Expect>) {
        let mut fix = Fixture::new().await;

        let layer_fs1 = fix
            .build_bottom_layer(lower.into_iter().map(BuildEntry::from_str).collect())
            .await;

        let layer_fs2 = fix
            .build_bottom_layer(upper.into_iter().map(BuildEntry::from_str).collect())
            .await;

        let layer_fs = fix.build_upper_layer(&layer_fs1, &layer_fs2).await;

        let mount_handle = fix.mount(layer_fs).await;
        let mount_path = mount_handle.mount_path();

        assert_expectations(&fix.fs, &mount_path, expected).await;

        mount_handle.umount_and_join().await.unwrap();
    }

    async fn three_layer_test(
        lowest: Vec<&str>,
        lower: Vec<&str>,
        upper: Vec<&str>,
        expected: Vec<Expect>,
    ) {
        let mut fix = Fixture::new().await;

        let layer_fs1 = fix
            .build_bottom_layer(lowest.into_iter().map(BuildEntry::from_str).collect())
            .await;

        let layer_fs2 = fix
            .build_bottom_layer(lower.into_iter().map(BuildEntry::from_str).collect())
            .await;

        let layer_fs3 = fix
            .build_bottom_layer(upper.into_iter().map(BuildEntry::from_str).collect())
            .await;

        let upper_layer_fs = fix.build_upper_layer(&layer_fs1, &layer_fs2).await;

        let layer_fs = fix.build_upper_layer(&upper_layer_fs, &layer_fs3).await;

        let mount_handle = fix.mount(layer_fs).await;
        let mount_path = mount_handle.mount_path();

        assert_expectations(&fix.fs, &mount_path, expected).await;

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn two_layer_empty_bottom() {
        two_layer_test(
            vec![],
            vec!["/Pie/KeyLime", "/Cake/Birthday", "/Cookies/Snickerdoodle"],
            vec![
                Expect::Entries("", vec!["Cake/", "Cookies/", "Pie/"]),
                Expect::Entries("Cake", vec!["Birthday"]),
                Expect::Entries("Pie", vec!["KeyLime"]),
                Expect::Entries("Cookies", vec!["Snickerdoodle"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_empty_top() {
        two_layer_test(
            vec!["/Pie/KeyLime", "/Cake/Birthday", "/Cookies/Snickerdoodle"],
            vec![],
            vec![
                Expect::Entries("", vec!["Cake/", "Cookies/", "Pie/"]),
                Expect::Entries("Cake", vec!["Birthday"]),
                Expect::Entries("Pie", vec!["KeyLime"]),
                Expect::Entries("Cookies", vec!["Snickerdoodle"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_large_bottom_dir() {
        two_layer_test(
            vec!["/a/b/c/d/e/f"],
            vec!["/a/g"],
            vec![
                Expect::Entries("a", vec!["b/", "g"]),
                Expect::Entries("a/b/c/d/e", vec!["f"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_large_upper_dir() {
        two_layer_test(
            vec!["/a/g"],
            vec!["/a/b/c/d/e/f"],
            vec![
                Expect::Entries("a", vec!["b/", "g"]),
                Expect::Entries("a/b/c/d/e", vec!["f"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_complicated() {
        two_layer_test(
            vec!["/Pie/Apple", "/Cake/Chocolate", "/Cake/Cupcakes/Sprinkle"],
            vec!["/Pie/KeyLime", "/Cake/Birthday", "/Cookies/Snickerdoodle"],
            vec![
                Expect::Entries("", vec!["Cake/", "Cookies/", "Pie/"]),
                Expect::Entries("Cake", vec!["Birthday", "Chocolate", "Cupcakes/"]),
                Expect::Entries("Cake/Cupcakes", vec!["Sprinkle"]),
                Expect::Entries("Pie", vec!["Apple", "KeyLime"]),
                Expect::Entries("Cookies", vec!["Snickerdoodle"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_replace_dir_with_file() {
        two_layer_test(
            vec!["/Cake/Cupcakes/Sprinkle"],
            vec!["/Cake/Cupcakes"],
            vec![
                Expect::Entries("", vec!["Cake/"]),
                Expect::Entries("Cake", vec!["Cupcakes"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_replace_file_with_dir() {
        two_layer_test(
            vec!["/Cake/Cupcakes"],
            vec!["/Cake/Cupcakes/Sprinkle"],
            vec![
                Expect::Entries("", vec!["Cake/"]),
                Expect::Entries("Cake", vec!["Cupcakes/"]),
                Expect::Entries("Cake/Cupcakes", vec!["Sprinkle"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_with_whiteout_of_file() {
        two_layer_test(
            vec!["/Cake/Cupcakes/Sprinkle", "/Cake/Cupcakes/RedVelvet"],
            vec!["wh:/Cake/Cupcakes/Sprinkle"],
            vec![
                Expect::Entries("", vec!["Cake/"]),
                Expect::Entries("Cake", vec!["Cupcakes/"]),
                Expect::Entries("Cake/Cupcakes", vec!["RedVelvet"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_with_whiteout_of_hardlink() {
        two_layer_test(
            vec!["/Cake/Cupcakes", "hl:/Cake/Muffins -> /Cake/Cupcakes"],
            vec!["wh:/Cake/Cupcakes"],
            vec![
                Expect::Entries("", vec!["Cake/"]),
                Expect::Entries("Cake", vec!["Muffins"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_with_whiteout_of_directory() {
        two_layer_test(
            vec![
                "/Cake/Cupcakes/Sprinkle/Yellow",
                "/Cake/Cupcakes/Sprinkle/Red",
                "/Cake/Cupcakes/RedVelvet/",
            ],
            vec!["wh:/Cake/Cupcakes/Sprinkle"],
            vec![
                Expect::Entries("", vec!["Cake/"]),
                Expect::Entries("Cake", vec!["Cupcakes/"]),
                Expect::Entries("Cake/Cupcakes", vec!["RedVelvet/"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_with_opaque_directory() {
        two_layer_test(
            vec![
                "/Cake/Cupcakes/Sprinkle/Yellow",
                "/Cake/Cupcakes/Sprinkle/Red",
                "/Cake/Cupcakes/RedVelvet/",
            ],
            vec![
                "opq:/Cake/Cupcakes/",
                "/Cake/Cupcakes/CarrotCake/",
                "/Cake/Cupcakes/Sprinkle/Orange",
            ],
            vec![
                Expect::Entries("", vec!["Cake/"]),
                Expect::Entries("Cake", vec!["Cupcakes/"]),
                Expect::Entries("Cake/Cupcakes", vec!["CarrotCake/", "Sprinkle/"]),
                Expect::Entries("Cake/Cupcakes/Sprinkle", vec!["Orange"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn three_layer() {
        three_layer_test(
            vec!["/Cake/Cupcakes/"],
            vec!["/Cake/Chocolate/"],
            vec!["/Cake/Chocolate/"],
            vec![
                Expect::Entries("", vec!["Cake/"]),
                Expect::Entries("Cake", vec!["Chocolate/", "Cupcakes/"]),
            ],
        )
        .await;
    }
}
