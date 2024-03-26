mod avl;
mod builder;
mod dir;
mod file;
mod ty;

use anyhow::{anyhow, Result};
use anyhow_trace::anyhow_trace;
use async_trait::async_trait;
pub use builder::*;
pub use dir::{DirectoryDataReader, DirectoryStream};
pub use file::FileMetadataReader;
use maelstrom_base::Sha256Digest;
use maelstrom_fuse::{
    AttrResponse, EntryResponse, ErrnoResult, FileAttr, FuseFileSystem, ReadLinkResponse,
    ReadResponse, Request,
};
use maelstrom_linux::Errno;
use maelstrom_util::async_fs::Fs;
use std::ffi::OsStr;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
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

pub struct LayerFs {
    data_fs: Fs,
    top_layer_path: PathBuf,
    layer_super: LazyLayerSuper,
    cache_path: PathBuf,
    log: slog::Logger,
}

#[anyhow_trace]
impl LayerFs {
    pub fn from_path(log: slog::Logger, data_dir: &Path, cache_path: &Path) -> Result<Self> {
        let data_fs = Fs::new();
        let data_dir = data_dir.to_owned();

        Ok(Self {
            data_fs,
            layer_super: LazyLayerSuper::not_cached(data_dir.join("super.bin")),
            top_layer_path: data_dir,
            cache_path: cache_path.to_owned(),
            log,
        })
    }

    async fn new(
        log: slog::Logger,
        data_dir: &Path,
        cache_path: &Path,
        layer_super: LayerSuper,
    ) -> Result<Self> {
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
            log,
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

    fn cache_entry(&self, digest: Sha256Digest) -> PathBuf {
        self.cache_path.join(digest.to_string())
    }

    pub fn mount(self, mount_path: &Path) -> Result<maelstrom_fuse::FuseHandle> {
        slog::debug!(self.log, "mounting FUSE file-system"; "path" => ?mount_path);
        maelstrom_fuse::fuse_mount(self, mount_path, "Maelstrom LayerFS")
    }
}

#[async_trait]
impl FuseFileSystem for LayerFs {
    async fn look_up(&self, req: Request, parent: u64, name: &OsStr) -> ErrnoResult<EntryResponse> {
        let name = name.to_str().ok_or(Errno::EINVAL)?;
        let parent = FileId::try_from(parent).map_err(|_| Errno::EINVAL)?;
        let mut reader = to_eio(
            self.log.clone(),
            DirectoryDataReader::new(self, parent).await,
        )?;
        let child_id =
            to_eio(self.log.clone(), reader.look_up(name).await)?.ok_or(Errno::ENOENT)?;
        let attrs = self.get_attr(req, child_id.as_u64()).await?;
        Ok(EntryResponse {
            attr: attrs.attr,
            ttl: TTL,
            generation: 0,
        })
    }

    async fn get_attr(&self, _req: Request, ino: u64) -> ErrnoResult<AttrResponse> {
        let file = FileId::try_from(ino).map_err(|_| Errno::EINVAL)?;
        let mut reader = to_eio(
            self.log.clone(),
            FileMetadataReader::new(self, file.layer()).await,
        )?;
        let (kind, attrs) = to_eio(self.log.clone(), reader.get_attr(file).await)?;
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
        let file = FileId::try_from(ino).map_err(|_| Errno::EINVAL)?;
        let mut reader = to_eio(
            self.log.clone(),
            FileMetadataReader::new(self, file.layer()).await,
        )?;
        let (kind, data) = to_eio(self.log.clone(), reader.get_data(file).await)?;
        if kind != FileType::RegularFile {
            return Err(Errno::EINVAL);
        }
        match data {
            FileData::Empty => Ok(ReadResponse { data: vec![] }),
            FileData::Inline(inline) => {
                let offset = usize::try_from(offset).map_err(|_| Errno::EINVAL)?;
                if offset >= inline.len() {
                    return Err(Errno::EINVAL);
                }
                let size = std::cmp::min(size as usize, inline.len() - offset);

                Ok(ReadResponse {
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

                let mut file = to_eio(
                    self.log.clone(),
                    self.data_fs.open_file(self.cache_entry(digest)).await,
                )?;
                to_eio(
                    self.log.clone(),
                    file.seek(SeekFrom::Start(read_start)).await,
                )?;
                let mut buffer = vec![0; read_length as usize];
                to_eio(self.log.clone(), file.read_exact(&mut buffer).await)?;
                Ok(ReadResponse { data: buffer })
            }
        }
    }

    type ReadDirStream<'a> = DirectoryStream<'a>;

    async fn read_dir<'a>(
        &'a self,
        _req: Request,
        ino: u64,
        _fh: u64,
        offset: i64,
    ) -> ErrnoResult<Self::ReadDirStream<'a>> {
        let file = FileId::try_from(ino).map_err(|_| Errno::EINVAL)?;
        let reader = to_eio(self.log.clone(), DirectoryDataReader::new(self, file).await)?;
        Ok(to_eio(
            self.log.clone(),
            reader
                .into_stream(self.log.clone(), offset.try_into()?)
                .await,
        )?)
    }

    async fn read_link(&self, _req: Request, ino: u64) -> ErrnoResult<ReadLinkResponse> {
        let file = FileId::try_from(ino).map_err(|_| Errno::EINVAL)?;
        let mut reader = to_eio(
            self.log.clone(),
            FileMetadataReader::new(self, file.layer()).await,
        )?;
        let (kind, data) = to_eio(self.log.clone(), reader.get_data(file).await)?;
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
    use futures::StreamExt as _;
    use maelstrom_base::{
        manifest::{ManifestEntry, ManifestEntryData, ManifestEntryMetadata, Mode},
        Utf8PathBuf,
    };
    use maelstrom_util::manifest::AsyncManifestWriter;
    use slog::Drain as _;
    use std::future::Future;
    use std::os::unix::{ffi::OsStrExt as _, fs::MetadataExt as _};
    use std::pin::Pin;
    use tokio::io::AsyncWriteExt as _;

    const ARBITRARY_TIME: maelstrom_base::manifest::UnixTimestamp =
        maelstrom_base::manifest::UnixTimestamp(1705000271);

    struct BuildEntry {
        path: String,
        type_: FileType,
        data: FileData,
        mode: u32,
        is_hard_link: bool,
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
                type_: FileType::RegularFile,
                data: FileData::Empty,
                mode,
                is_hard_link: false,
            }
        }

        fn reg_mode(path: impl Into<String>, data: FileData, mode: u32) -> Self {
            Self {
                path: path.into(),
                type_: FileType::RegularFile,
                data,
                mode,
                is_hard_link: false,
            }
        }

        fn dir(path: impl Into<String>) -> Self {
            Self::dir_mode(path, 0o555)
        }

        fn dir_mode(path: impl Into<String>, mode: u32) -> Self {
            Self {
                path: path.into(),
                type_: FileType::Directory,
                data: FileData::Empty,
                mode,
                is_hard_link: false,
            }
        }

        fn sym(path: impl Into<String>, target: impl Into<Vec<u8>>) -> Self {
            Self {
                path: path.into(),
                type_: FileType::Symlink,
                data: FileData::Inline(target.into()),
                mode: 0o777,
                is_hard_link: false,
            }
        }

        fn link(path: impl Into<String>, target: impl Into<Vec<u8>>) -> Self {
            Self {
                path: path.into(),
                type_: FileType::RegularFile,
                data: FileData::Inline(target.into()),
                mode: 0o777,
                is_hard_link: true,
            }
        }

        fn from_str(s: &str) -> Self {
            if s.ends_with("/") {
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

    async fn build_fs(
        fs: &Fs,
        data_dir: &Path,
        cache_path: &Path,
        files: Vec<BuildEntry>,
    ) -> LayerFs {
        let mut builder =
            BottomLayerBuilder::new(test_logger(), fs, data_dir, cache_path, ARBITRARY_TIME)
                .await
                .unwrap();

        for BuildEntry {
            path,
            type_,
            data,
            mode,
            is_hard_link,
        } in files
        {
            let size = match &data {
                ty::FileData::Empty => 0,
                ty::FileData::Inline(d) => d.len() as u64,
                ty::FileData::Digest { length, .. } => *length,
            };
            if is_hard_link {
                let FileData::Inline(data) = data else {
                    unreachable!()
                };

                builder
                    .add_link_path(path.as_ref(), std::str::from_utf8(&data).unwrap().as_ref())
                    .await
                    .unwrap();
                continue;
            }

            match type_ {
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
                }
                FileType::Symlink => {
                    let FileData::Inline(data) = data else {
                        unreachable!()
                    };
                    builder.add_symlink_path(path.as_ref(), data).await.unwrap();
                }
                other => panic!("unsupported file type {other:?}"),
            }
        }

        builder.finish().await.unwrap()
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

    #[tokio::test]
    async fn read_dir_and_look_up() {
        let temp = tempfile::tempdir().unwrap();
        let mount_point = temp.path().join("mount");
        let data_dir = temp.path().join("data");
        let cache_dir = temp.path().join("cache");

        let fs = Fs::new();
        fs.create_dir(&mount_point).await.unwrap();
        fs.create_dir(&data_dir).await.unwrap();
        fs.create_dir(&cache_dir).await.unwrap();

        let layer_fs = build_fs(
            &fs,
            &data_dir,
            &cache_dir,
            vec![
                BuildEntry::reg_empty("/Foo"),
                BuildEntry::reg_empty("/Bar"),
                BuildEntry::reg_empty("/Baz"),
            ],
        )
        .await;

        let mount_handle = layer_fs.mount(&mount_point).unwrap();

        assert_entries(&fs, &mount_point, vec!["Bar", "Baz", "Foo"]).await;

        fs.metadata(mount_point.join("Bar")).await.unwrap();
        fs.metadata(mount_point.join("Baz")).await.unwrap();
        fs.metadata(mount_point.join("Foo")).await.unwrap();

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn read_dir_multi_level() {
        let temp = tempfile::tempdir().unwrap();
        let mount_point = temp.path().join("mount");
        let data_dir = temp.path().join("data");
        let cache_dir = temp.path().join("cache");

        let fs = Fs::new();
        fs.create_dir(&mount_point).await.unwrap();
        fs.create_dir(&data_dir).await.unwrap();
        fs.create_dir(&cache_dir).await.unwrap();

        let layer_fs = build_fs(
            &fs,
            &data_dir,
            &cache_dir,
            vec![
                BuildEntry::reg_empty("/Foo/Bar/Baz"),
                BuildEntry::reg_empty("/Foo/Bin"),
                BuildEntry::reg_empty("/Foo/Bar/Qux"),
            ],
        )
        .await;

        let mount_handle = layer_fs.mount(&mount_point).unwrap();

        assert_entries(&fs, &mount_point, vec!["Foo/"]).await;
        assert_entries(&fs, &mount_point.join("Foo"), vec!["Bar/", "Bin"]).await;
        assert_entries(&fs, &mount_point.join("Foo/Bar"), vec!["Baz", "Qux"]).await;

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn get_attr() {
        let temp = tempfile::tempdir().unwrap();
        let mount_point = temp.path().join("mount");
        let data_dir = temp.path().join("data");
        let cache_dir = temp.path().join("cache");

        let fs = Fs::new();
        fs.create_dir(&mount_point).await.unwrap();
        fs.create_dir(&data_dir).await.unwrap();
        fs.create_dir(&cache_dir).await.unwrap();

        let layer_fs = build_fs(
            &fs,
            &data_dir,
            &cache_dir,
            vec![
                BuildEntry::reg_empty("/Foo"),
                BuildEntry::reg_empty("/Bar"),
                BuildEntry::reg_empty("/Baz"),
            ],
        )
        .await;

        let mount_handle = layer_fs.mount(&mount_point).unwrap();

        for name in ["Foo", "Bar", "Baz"] {
            let attrs = fs.metadata(mount_point.join(name)).await.unwrap();
            assert_eq!(attrs.len(), 0);
            assert_eq!(Mode(attrs.mode()), Mode(0o100555));
            assert_eq!(attrs.mtime(), ARBITRARY_TIME.into());
        }

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn hard_link() {
        let temp = tempfile::tempdir().unwrap();
        let mount_point = temp.path().join("mount");
        let data_dir = temp.path().join("data");
        let cache_dir = temp.path().join("cache");

        let fs = Fs::new();
        fs.create_dir(&mount_point).await.unwrap();
        fs.create_dir(&data_dir).await.unwrap();
        fs.create_dir(&cache_dir).await.unwrap();

        let layer_fs = build_fs(
            &fs,
            &data_dir,
            &cache_dir,
            vec![
                BuildEntry::reg_empty("/Foo"),
                BuildEntry::link("/Bar", "/Foo"),
                BuildEntry::link("/Baz", "/Bar"),
            ],
        )
        .await;

        let mount_handle = layer_fs.mount(&mount_point).unwrap();

        let foo_attrs = fs.metadata(mount_point.join("Foo")).await.unwrap();

        for name in ["Foo", "Bar", "Baz"] {
            let attrs = fs.metadata(mount_point.join(name)).await.unwrap();
            assert_eq!(attrs.len(), 0);
            assert_eq!(Mode(attrs.mode()), Mode(0o100555));
            assert_eq!(attrs.mtime(), ARBITRARY_TIME.into());
            assert_eq!(attrs.ino(), foo_attrs.ino());
        }

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn read_inline() {
        let temp = tempfile::tempdir().unwrap();
        let mount_point = temp.path().join("mount");
        let data_dir = temp.path().join("data");
        let cache_dir = temp.path().join("cache");

        let fs = Fs::new();
        fs.create_dir(&mount_point).await.unwrap();
        fs.create_dir(&data_dir).await.unwrap();
        fs.create_dir(&cache_dir).await.unwrap();

        let layer_fs = build_fs(
            &fs,
            &data_dir,
            &cache_dir,
            vec![
                BuildEntry::reg("/Foo", b"hello world"),
                BuildEntry::reg_empty("/Bar"),
                BuildEntry::reg_empty("/Baz"),
            ],
        )
        .await;

        let mount_handle = layer_fs.mount(&mount_point).unwrap();

        let contents = fs.read_to_string(mount_point.join("Foo")).await.unwrap();
        assert_eq!(contents, "hello world");

        let contents = fs.read_to_string(mount_point.join("Bar")).await.unwrap();
        assert_eq!(contents, "");

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn read_link() {
        let temp = tempfile::tempdir().unwrap();
        let mount_point = temp.path().join("mount");
        let data_dir = temp.path().join("data");
        let cache_dir = temp.path().join("cache");

        let fs = Fs::new();
        fs.create_dir(&mount_point).await.unwrap();
        fs.create_dir(&data_dir).await.unwrap();
        fs.create_dir(&cache_dir).await.unwrap();

        let layer_fs = build_fs(
            &fs,
            &data_dir,
            &cache_dir,
            vec![
                BuildEntry::reg("/Foo", b"hello world"),
                BuildEntry::sym("/Bar", "./Foo"),
            ],
        )
        .await;

        let mount_handle = layer_fs.mount(&mount_point).unwrap();

        let contents = fs.read_to_string(mount_point.join("Foo")).await.unwrap();
        assert_eq!(contents, "hello world");

        assert!(fs
            .symlink_metadata(mount_point.join("Bar"))
            .await
            .unwrap()
            .is_symlink());
        let contents = fs.read_to_string(mount_point.join("Bar")).await.unwrap();
        assert_eq!(contents, "hello world");

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn read_digest() {
        let temp = tempfile::tempdir().unwrap();
        let mount_point = temp.path().join("mount");
        let data_dir = temp.path().join("data");
        let cache_dir = temp.path().join("cache");

        let fs = Fs::new();
        fs.create_dir(&mount_point).await.unwrap();
        fs.create_dir(&data_dir).await.unwrap();
        fs.create_dir(&cache_dir).await.unwrap();

        let temp_path = cache_dir.join("temp");
        let mut f = fs.create_file(&temp_path).await.unwrap();
        f.write_all(b"hello world").await.unwrap();
        drop(f);
        let digest = calc_digest(&fs, &temp_path).await;
        fs.rename(temp_path, cache_dir.join(digest.to_string()))
            .await
            .unwrap();

        let layer_fs = build_fs(
            &fs,
            &data_dir,
            &cache_dir,
            vec![
                BuildEntry::reg_digest("/Foo", digest.clone(), 0, 5),
                BuildEntry::reg_digest("/Bar", digest.clone(), 6, 5),
            ],
        )
        .await;

        let mount_handle = layer_fs.mount(&mount_point).unwrap();

        let contents = fs.read_to_string(mount_point.join("Foo")).await.unwrap();
        assert_eq!(contents, "hello");

        let contents = fs.read_to_string(mount_point.join("Bar")).await.unwrap();
        assert_eq!(contents, "world");

        mount_handle.umount_and_join().await.unwrap();
    }

    async fn calc_digest(fs: &Fs, path: &Path) -> Sha256Digest {
        let mut f = fs.open_file(path).await.unwrap();
        let mut hasher = maelstrom_util::io::Sha256Stream::new(tokio::io::sink());
        tokio::io::copy(&mut f, &mut hasher).await.unwrap();
        hasher.finalize().1
    }

    async fn build_tar(fs: &Fs, cache_path: &Path, files: Vec<BuildEntry>) -> Sha256Digest {
        let tar_path = cache_path.join("temp.tar");
        let f = fs.create_file(&tar_path).await.unwrap();
        let mut ar = tokio_tar::Builder::new(f.into_inner());
        for BuildEntry {
            path,
            type_,
            data,
            mode,
            is_hard_link,
        } in files
        {
            let mut header = tokio_tar::Header::new_gnu();
            let data = match data {
                ty::FileData::Empty => vec![],
                ty::FileData::Inline(d) => d,
                _ => panic!(),
            };
            header.set_entry_type(match (is_hard_link, type_) {
                (true, _) => tokio_tar::EntryType::Link,
                (_, FileType::RegularFile) => tokio_tar::EntryType::Regular,
                (_, FileType::Directory) => tokio_tar::EntryType::Directory,
                (_, FileType::Symlink) => tokio_tar::EntryType::Symlink,
                (_, other) => panic!("unsupported entry type {other:?}"),
            });
            if type_ == FileType::Symlink || is_hard_link {
                header.set_size(0);
                header
                    .set_link_name(std::ffi::OsStr::from_bytes(&data))
                    .unwrap();
                ar.append_data(&mut header, path, tokio::io::empty())
                    .await
                    .unwrap();
            } else {
                header.set_size(data.len() as u64);
                header.set_mode(mode);
                ar.append_data(&mut header, path, &data[..]).await.unwrap();
            }
        }
        ar.finish().await.unwrap();

        let digest = calc_digest(fs, &tar_path).await;

        fs.rename(tar_path, cache_path.join(digest.to_string()))
            .await
            .unwrap();

        digest
    }

    async fn build_manifest(fs: &Fs, cache_path: &Path, files: Vec<BuildEntry>) -> PathBuf {
        let manifest_path = cache_path.join("temp.manifest");
        let f = fs.create_file(&manifest_path).await.unwrap();
        let mut builder = AsyncManifestWriter::new(f).await.unwrap();
        for BuildEntry {
            path,
            type_,
            data,
            mode,
            is_hard_link,
        } in files
        {
            let path: Utf8PathBuf = path.into();
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
            if is_hard_link {
                let ty::FileData::Inline(data) = data else {
                    unimplemented!()
                };
                builder
                    .write_entry(&ManifestEntry {
                        path,
                        metadata,
                        data: ManifestEntryData::Hardlink(
                            std::str::from_utf8(&data).unwrap().into(),
                        ),
                    })
                    .await
                    .unwrap();
                continue;
            }
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
                        ty::FileData::Inline(d) => {
                            let temp_path = cache_path.join("temp.bin");
                            fs.write(&temp_path, d).await.unwrap();
                            let digest = calc_digest(fs, &temp_path).await;
                            fs.rename(temp_path, cache_path.join(digest.to_string()))
                                .await
                                .unwrap();
                            Some(digest)
                        }
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
                FileType::Symlink => {
                    let ty::FileData::Inline(data) = data else {
                        unimplemented!()
                    };
                    builder
                        .write_entry(&ManifestEntry {
                            path,
                            metadata,
                            data: ManifestEntryData::Symlink(data),
                        })
                        .await
                        .unwrap();
                }
                other => panic!("unsupported entry type {other:?}"),
            }
        }
        drop(builder);

        let digest = calc_digest(fs, &manifest_path).await;

        let final_path = cache_path.join(digest.to_string());
        fs.rename(manifest_path, &final_path).await.unwrap();

        final_path
    }

    #[cfg(test)]
    async fn layer_from_tar_or_manifest(
        populate_fn: impl for<'a> FnOnce(
            &'a Fs,
            &'a Path,
            Vec<BuildEntry>,
            &'a mut BottomLayerBuilder,
        ) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let mount_point = temp.path().join("mount");
        let data_dir = temp.path().join("data");
        let cache_dir = temp.path().join("cache");

        let fs = Fs::new();
        fs.create_dir(&mount_point).await.unwrap();
        fs.create_dir(&data_dir).await.unwrap();
        fs.create_dir(&cache_dir).await.unwrap();

        let input = vec![
            BuildEntry::reg("Foo", b"hello world"),
            BuildEntry::dir("Qux"),
            BuildEntry::reg_empty("Bar/Baz"),
            BuildEntry::reg_empty("Bar/Bin"),
            BuildEntry::reg_empty("Qux/Fred"),
            BuildEntry::dir_mode("Bar", 0o666),
            BuildEntry::sym("Waldo", b"Foo"),
            BuildEntry::link("Thud", "/Foo"),
        ];

        let mut builder =
            BottomLayerBuilder::new(test_logger(), &fs, &data_dir, &cache_dir, ARBITRARY_TIME)
                .await
                .unwrap();

        populate_fn(&fs, &cache_dir, input, &mut builder).await;

        let layer_fs = builder.finish().await.unwrap();

        let mount_handle = layer_fs.mount(&mount_point).unwrap();

        let contents = fs.read_to_string(mount_point.join("Foo")).await.unwrap();
        assert_eq!(contents, "hello world");

        let contents = fs.read_to_string(mount_point.join("Thud")).await.unwrap();
        assert_eq!(contents, "hello world");

        assert!(fs
            .symlink_metadata(mount_point.join("Waldo"))
            .await
            .unwrap()
            .is_symlink());
        let contents = fs.read_to_string(mount_point.join("Waldo")).await.unwrap();
        assert_eq!(contents, "hello world");

        let contents = fs
            .read_to_string(mount_point.join("Bar/Baz"))
            .await
            .unwrap();
        assert_eq!(contents, "");

        assert_entries(
            &fs,
            &mount_point,
            vec!["Bar/", "Foo", "Qux/", "Thud", "Waldo"],
        )
        .await;
        assert_entries(&fs, &mount_point.join("Bar"), vec!["Baz", "Bin"]).await;
        assert_entries(&fs, &mount_point.join("Qux"), vec!["Fred"]).await;

        for p in [
            "Foo", "Qux", "Bar/Baz", "Bar/Bin", "Qux/Fred", "Waldo", "Thud",
        ] {
            let mode = fs.metadata(mount_point.join(p)).await.unwrap().mode();
            assert_eq!(Mode(mode & 0o777), Mode(0o555));
        }

        let mode = fs.metadata(mount_point.join("Bar")).await.unwrap().mode();
        assert_eq!(Mode(mode & 0o777), Mode(0o666));

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn layer_from_tar() {
        layer_from_tar_or_manifest(|fs, cache_dir, input, builder| {
            Box::pin(async move {
                let tar_digest = build_tar(&fs, &cache_dir, input).await;
                let tar_path = cache_dir.join(tar_digest.to_string());

                builder
                    .add_from_tar(tar_digest, fs.open_file(tar_path).await.unwrap())
                    .await
                    .unwrap();
            })
        })
        .await
    }

    #[tokio::test]
    async fn layer_from_manifest() {
        layer_from_tar_or_manifest(|fs, cache_dir, input, builder| {
            Box::pin(async move {
                let manifest_path = build_manifest(&fs, &cache_dir, input).await;
                builder
                    .add_from_manifest(fs.open_file(manifest_path).await.unwrap())
                    .await
                    .unwrap();
            })
        })
        .await
    }

    async fn two_layer_test(lower: Vec<&str>, upper: Vec<&str>, expected: Vec<(&str, Vec<&str>)>) {
        let temp = tempfile::tempdir().unwrap();
        let mount_point = temp.path().join("mount");
        let data_dir1 = temp.path().join("data1");
        let data_dir2 = temp.path().join("data2");
        let data_dir3 = temp.path().join("data3");
        let cache_dir = temp.path().join("cache");

        let fs = Fs::new();
        fs.create_dir(&mount_point).await.unwrap();
        fs.create_dir(&data_dir1).await.unwrap();
        fs.create_dir(&data_dir2).await.unwrap();
        fs.create_dir(&data_dir3).await.unwrap();
        fs.create_dir(&cache_dir).await.unwrap();

        let layer_fs1 = build_fs(
            &fs,
            &data_dir1,
            &cache_dir,
            lower.into_iter().map(BuildEntry::from_str).collect(),
        )
        .await;

        let layer_fs2 = build_fs(
            &fs,
            &data_dir2,
            &cache_dir,
            upper.into_iter().map(BuildEntry::from_str).collect(),
        )
        .await;

        let mut builder = UpperLayerBuilder::new(test_logger(), &data_dir3, &cache_dir, &layer_fs1)
            .await
            .unwrap();
        builder.fill_from_bottom_layer(&layer_fs2).await.unwrap();
        let layer_fs = builder.finish().await.unwrap();

        let mount_handle = layer_fs.mount(&mount_point).unwrap();

        for (d, entries) in expected {
            assert_entries(&fs, &mount_point.join(d), entries).await;
        }

        mount_handle.umount_and_join().await.unwrap();
    }

    async fn three_layer_test(
        lowest: Vec<&str>,
        lower: Vec<&str>,
        upper: Vec<&str>,
        expected: Vec<(&str, Vec<&str>)>,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let mount_point = temp.path().join("mount");
        let data_dir1 = temp.path().join("data1");
        let data_dir2 = temp.path().join("data2");
        let data_dir3 = temp.path().join("data3");
        let data_dir4 = temp.path().join("data4");
        let data_dir5 = temp.path().join("data5");
        let cache_dir = temp.path().join("cache");

        let fs = Fs::new();
        fs.create_dir(&mount_point).await.unwrap();
        fs.create_dir(&data_dir1).await.unwrap();
        fs.create_dir(&data_dir2).await.unwrap();
        fs.create_dir(&data_dir3).await.unwrap();
        fs.create_dir(&data_dir4).await.unwrap();
        fs.create_dir(&data_dir5).await.unwrap();
        fs.create_dir(&cache_dir).await.unwrap();

        let layer_fs1 = build_fs(
            &fs,
            &data_dir1,
            &cache_dir,
            lowest.into_iter().map(BuildEntry::from_str).collect(),
        )
        .await;

        let layer_fs2 = build_fs(
            &fs,
            &data_dir2,
            &cache_dir,
            lower.into_iter().map(BuildEntry::from_str).collect(),
        )
        .await;

        let layer_fs3 = build_fs(
            &fs,
            &data_dir3,
            &cache_dir,
            upper.into_iter().map(BuildEntry::from_str).collect(),
        )
        .await;

        let log = test_logger();

        let mut builder = UpperLayerBuilder::new(log.clone(), &data_dir4, &cache_dir, &layer_fs1)
            .await
            .unwrap();
        builder.fill_from_bottom_layer(&layer_fs2).await.unwrap();
        let upper_layer_fs = builder.finish().await.unwrap();

        let mut builder = UpperLayerBuilder::new(log, &data_dir5, &cache_dir, &upper_layer_fs)
            .await
            .unwrap();
        builder.fill_from_bottom_layer(&layer_fs3).await.unwrap();
        let layer_fs = builder.finish().await.unwrap();

        let mount_handle = layer_fs.mount(&mount_point).unwrap();

        for (d, entries) in expected {
            assert_entries(&fs, &mount_point.join(d), entries).await;
        }

        mount_handle.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn two_mounts_test() {
        let temp = tempfile::tempdir().unwrap();
        let mount_point1 = temp.path().join("mount1");
        let mount_point2 = temp.path().join("mount2");

        let data_dir1 = temp.path().join("data1");
        let data_dir2 = temp.path().join("data2");
        let cache_dir = temp.path().join("cache");

        let fs = Fs::new();
        fs.create_dir(&mount_point1).await.unwrap();
        fs.create_dir(&mount_point2).await.unwrap();
        fs.create_dir(&data_dir1).await.unwrap();
        fs.create_dir(&data_dir2).await.unwrap();
        fs.create_dir(&cache_dir).await.unwrap();

        let layer_fs1 = build_fs(
            &fs,
            &data_dir1,
            &cache_dir,
            ["/Apple"].into_iter().map(BuildEntry::from_str).collect(),
        )
        .await;

        let layer_fs2 = build_fs(
            &fs,
            &data_dir2,
            &cache_dir,
            ["/Birthday"]
                .into_iter()
                .map(BuildEntry::from_str)
                .collect(),
        )
        .await;

        let mount_handle1 = layer_fs1.mount(&mount_point1).unwrap();
        let mount_handle2 = layer_fs2.mount(&mount_point2).unwrap();

        assert_entries(&fs, &mount_point1, vec!["Apple"]).await;
        assert_entries(&fs, &mount_point2, vec!["Birthday"]).await;

        mount_handle1.umount_and_join().await.unwrap();
        mount_handle2.umount_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn two_layer_empty_bottom() {
        two_layer_test(
            vec![],
            vec!["/Pie/KeyLime", "/Cake/Birthday", "/Cookies/Snickerdoodle"],
            vec![
                ("", vec!["Cake/", "Cookies/", "Pie/"]),
                ("Cake", vec!["Birthday"]),
                ("Pie", vec!["KeyLime"]),
                ("Cookies", vec!["Snickerdoodle"]),
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
                ("", vec!["Cake/", "Cookies/", "Pie/"]),
                ("Cake", vec!["Birthday"]),
                ("Pie", vec!["KeyLime"]),
                ("Cookies", vec!["Snickerdoodle"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_large_bottom_dir() {
        two_layer_test(
            vec!["/a/b/c/d/e/f"],
            vec!["/a/g"],
            vec![("a", vec!["b/", "g"]), ("a/b/c/d/e", vec!["f"])],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_large_upper_dir() {
        two_layer_test(
            vec!["/a/g"],
            vec!["/a/b/c/d/e/f"],
            vec![("a", vec!["b/", "g"]), ("a/b/c/d/e", vec!["f"])],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_complicated() {
        two_layer_test(
            vec!["/Pie/Apple", "/Cake/Chocolate", "/Cake/Cupcakes/Sprinkle"],
            vec!["/Pie/KeyLime", "/Cake/Birthday", "/Cookies/Snickerdoodle"],
            vec![
                ("", vec!["Cake/", "Cookies/", "Pie/"]),
                ("Cake", vec!["Birthday", "Chocolate", "Cupcakes/"]),
                ("Cake/Cupcakes", vec!["Sprinkle"]),
                ("Pie", vec!["Apple", "KeyLime"]),
                ("Cookies", vec!["Snickerdoodle"]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_replace_dir_with_file() {
        two_layer_test(
            vec!["/Cake/Cupcakes/Sprinkle"],
            vec!["/Cake/Cupcakes"],
            vec![("", vec!["Cake/"]), ("Cake", vec!["Cupcakes"])],
        )
        .await;
    }

    #[tokio::test]
    async fn two_layer_replace_file_with_dir() {
        two_layer_test(
            vec!["/Cake/Cupcakes"],
            vec!["/Cake/Cupcakes/Sprinkle"],
            vec![
                ("", vec!["Cake/"]),
                ("Cake", vec!["Cupcakes/"]),
                ("Cake/Cupcakes", vec!["Sprinkle"]),
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
                ("", vec!["Cake/"]),
                ("Cake", vec!["Chocolate/", "Cupcakes/"]),
            ],
        )
        .await;
    }
}
