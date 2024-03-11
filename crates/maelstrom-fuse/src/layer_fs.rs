mod avl;
mod builder;
mod dir;
mod file;
mod ty;

use crate::{
    AttrResponse, EntryResponse, ErrnoResult, FileAttr, FileType, FuseFileSystem, ReadResponse,
    Request,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
pub use builder::*;
use dir::{DirectoryDataReader, DirectoryStream};
use file::FileMetadataReader;
use maelstrom_linux::Errno;
use maelstrom_util::async_fs::Fs;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::Duration;
pub use ty::{FileAttributes, FileData, FileId};
use ty::{LayerId, LayerSuper};

fn to_eio<T>(res: Result<T>) -> ErrnoResult<T> {
    res.map_err(|_| Errno::EIO)
}

const TTL: Duration = Duration::from_secs(1); // 1 second

pub struct LayerFs {
    data_fs: Fs,
    top_layer_path: PathBuf,
    layer_super: LayerSuper,
}

impl LayerFs {
    pub async fn from_path(data_dir: &Path) -> Result<Self> {
        let data_fs = Fs::new();
        let data_dir = data_dir.to_owned();

        let layer_super = LayerSuper::read_from_path(&data_fs, &data_dir.join("super.bin")).await?;

        Ok(Self {
            data_fs,
            top_layer_path: data_dir,
            layer_super,
        })
    }

    pub async fn new(data_dir: &Path) -> Result<Self> {
        let data_fs = Fs::new();
        let data_dir = data_dir.to_owned();

        let layer_super = LayerSuper::default();
        layer_super
            .write_to_path(&data_fs, &data_dir.join("super.bin"))
            .await?;

        Ok(Self {
            data_fs,
            top_layer_path: data_dir,
            layer_super,
        })
    }

    fn data_path(&self, layer_id: LayerId) -> Result<&PathBuf> {
        if layer_id == self.layer_super.layer_id {
            Ok(&self.top_layer_path)
        } else {
            self.layer_super
                .lower_layers
                .get(&layer_id)
                .ok_or(anyhow!("unknown layer {layer_id:?}"))
        }
    }

    fn dir_data_path(&self, file_id: FileId) -> Result<PathBuf> {
        Ok(self
            .data_path(file_id.layer())?
            .join(format!("{}.dir_data.bin", file_id.offset())))
    }

    fn file_table_path(&self, layer_id: LayerId) -> Result<PathBuf> {
        Ok(self.data_path(layer_id)?.join("file_table.bin"))
    }

    fn attributes_table_path(&self, layer_id: LayerId) -> Result<PathBuf> {
        Ok(self.data_path(layer_id)?.join("attributes_table.bin"))
    }

    pub async fn mount(self, mount_path: &Path) -> Result<crate::FuseHandle> {
        crate::fuse_mount(self, mount_path, "Maelstrom LayerFS").await
    }
}

#[async_trait]
impl FuseFileSystem for LayerFs {
    async fn look_up(&self, req: Request, parent: u64, name: &OsStr) -> ErrnoResult<EntryResponse> {
        let name = name.to_str().ok_or(Errno::EINVAL)?;
        let parent = FileId::try_from(parent).map_err(|_| Errno::EINVAL)?;
        let mut reader = to_eio(DirectoryDataReader::new(self, parent).await)?;
        let child_id = to_eio(reader.look_up(name).await)?.ok_or(Errno::ENOENT)?;
        let attrs = self.get_attr(req, child_id.as_u64()).await?;
        Ok(EntryResponse {
            attr: attrs.attr,
            ttl: TTL,
            generation: 0,
        })
    }

    async fn get_attr(&self, _req: Request, ino: u64) -> ErrnoResult<AttrResponse> {
        let file = FileId::try_from(ino).map_err(|_| Errno::EINVAL)?;
        let mut reader = to_eio(FileMetadataReader::new(self, file.layer()).await)?;
        let (kind, attrs) = to_eio(reader.get_attr(file).await)?;
        Ok(AttrResponse {
            ttl: TTL,
            attr: FileAttr {
                ino,
                size: attrs.size,
                blocks: 0,
                atime: attrs.mtime.into(),
                mtime: attrs.mtime.into(),
                ctime: attrs.mtime.into(),
                crtime: attrs.mtime.into(),
                kind,
                perm: u32::from(attrs.mode) as u16,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                flags: 0,
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
        let mut reader = to_eio(FileMetadataReader::new(self, file.layer()).await)?;
        let (kind, data) = to_eio(reader.get_data(file).await)?;
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
        let reader = to_eio(DirectoryDataReader::new(self, file).await)?;
        Ok(to_eio(reader.into_stream(offset.try_into()?).await)?)
    }
}

#[cfg(test)]
const ARBITRARY_TIME: maelstrom_base::manifest::UnixTimestamp =
    maelstrom_base::manifest::UnixTimestamp(1705000271);

#[cfg(test)]
async fn build_fs(layer_fs: &LayerFs, files: Vec<(&str, FileData)>) {
    use maelstrom_base::manifest::Mode;

    let mut builder = BottomLayerBuilder::new(layer_fs, ARBITRARY_TIME)
        .await
        .unwrap();

    for (path, data) in files {
        let size = match &data {
            ty::FileData::Empty => 0,
            ty::FileData::Inline(d) => d.len() as u64,
        };
        builder
            .add_file_path(
                path.as_ref(),
                ty::FileAttributes {
                    size,
                    mode: Mode(0o555),
                    mtime: ARBITRARY_TIME,
                },
                data,
            )
            .await
            .unwrap();
    }
}

#[cfg(test)]
async fn assert_entries(fs: &Fs, path: &Path, expected: Vec<&str>) {
    use futures::StreamExt as _;

    let entry_stream = fs.read_dir(path).await.unwrap();
    let mut entries: Vec<_> = entry_stream.map(|e| e.unwrap().file_name()).collect().await;
    entries.sort();
    assert_eq!(
        entries,
        Vec::from_iter(expected.into_iter().map(|e| std::ffi::OsString::from(e)))
    );
}

#[tokio::test]
async fn read_dir_and_look_up() {
    use ty::FileData::*;

    let temp = tempfile::tempdir().unwrap();
    let mount_point = temp.path().join("mount");
    let data_dir = temp.path().join("data");

    let fs = Fs::new();
    fs.create_dir(&mount_point).await.unwrap();
    fs.create_dir(&data_dir).await.unwrap();

    let layer_fs = LayerFs::new(&data_dir).await.unwrap();
    build_fs(
        &layer_fs,
        vec![("/Foo", Empty), ("/Bar", Empty), ("/Baz", Empty)],
    )
    .await;

    let mount_handle = layer_fs.mount(&mount_point).await.unwrap();

    assert_entries(&fs, &mount_point, vec!["Bar", "Baz", "Foo"]).await;

    fs.metadata(mount_point.join("Bar")).await.unwrap();
    fs.metadata(mount_point.join("Baz")).await.unwrap();
    fs.metadata(mount_point.join("Foo")).await.unwrap();

    mount_handle.umount_and_join().await.unwrap();
}

#[tokio::test]
async fn read_dir_multi_level() {
    use ty::FileData::*;

    let temp = tempfile::tempdir().unwrap();
    let mount_point = temp.path().join("mount");
    let data_dir = temp.path().join("data");

    let fs = Fs::new();
    fs.create_dir(&mount_point).await.unwrap();
    fs.create_dir(&data_dir).await.unwrap();

    let layer_fs = LayerFs::new(&data_dir).await.unwrap();
    build_fs(
        &layer_fs,
        vec![
            ("/Foo/Bar/Baz", Empty),
            ("/Foo/Bin", Empty),
            ("/Foo/Bar/Qux", Empty),
        ],
    )
    .await;

    let mount_handle = layer_fs.mount(&mount_point).await.unwrap();

    assert_entries(&fs, &mount_point, vec!["Foo"]).await;
    assert_entries(&fs, &mount_point.join("Foo"), vec!["Bar", "Bin"]).await;
    assert_entries(&fs, &mount_point.join("Foo/Bar"), vec!["Baz", "Qux"]).await;

    mount_handle.umount_and_join().await.unwrap();
}

#[tokio::test]
async fn get_attr() {
    use maelstrom_base::manifest::Mode;
    use std::os::unix::fs::MetadataExt as _;
    use ty::FileData::*;

    let temp = tempfile::tempdir().unwrap();
    let mount_point = temp.path().join("mount");
    let data_dir = temp.path().join("data");

    let fs = Fs::new();
    fs.create_dir(&mount_point).await.unwrap();
    fs.create_dir(&data_dir).await.unwrap();

    let layer_fs = LayerFs::new(&data_dir).await.unwrap();
    build_fs(
        &layer_fs,
        vec![("/Foo", Empty), ("/Bar", Empty), ("/Baz", Empty)],
    )
    .await;

    let mount_handle = layer_fs.mount(&mount_point).await.unwrap();

    for name in ["Foo", "Bar", "Baz"] {
        let attrs = fs.metadata(mount_point.join(name)).await.unwrap();
        assert_eq!(attrs.len(), 0);
        assert_eq!(Mode(attrs.mode()), Mode(0o100555));
        assert_eq!(attrs.mtime(), ARBITRARY_TIME.into());
    }

    mount_handle.umount_and_join().await.unwrap();
}

#[tokio::test]
async fn read_inline() {
    use ty::FileData::*;

    let temp = tempfile::tempdir().unwrap();
    let mount_point = temp.path().join("mount");
    let data_dir = temp.path().join("data");

    let fs = Fs::new();
    fs.create_dir(&mount_point).await.unwrap();
    fs.create_dir(&data_dir).await.unwrap();

    let layer_fs = LayerFs::new(&data_dir).await.unwrap();
    build_fs(
        &layer_fs,
        vec![
            ("/Foo", Inline(b"hello world".into())),
            ("/Bar", Empty),
            ("/Baz", Empty),
        ],
    )
    .await;

    let mount_handle = layer_fs.mount(&mount_point).await.unwrap();

    let contents = fs.read_to_string(mount_point.join("Foo")).await.unwrap();
    assert_eq!(contents, "hello world");

    let contents = fs.read_to_string(mount_point.join("Bar")).await.unwrap();
    assert_eq!(contents, "");

    mount_handle.umount_and_join().await.unwrap();
}
