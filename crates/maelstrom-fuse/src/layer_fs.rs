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
use maelstrom_base::Sha256Digest;
use maelstrom_linux::Errno;
use maelstrom_util::async_fs::Fs;
use std::ffi::OsStr;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard};
pub use ty::{FileAttributes, FileData, FileId};
use ty::{LayerId, LayerSuper};

fn to_eio<ValueT, ErrorT>(res: std::result::Result<ValueT, ErrorT>) -> ErrnoResult<ValueT> {
    res.map_err(|_| Errno::EIO)
}

fn to_einval<ValueT, ErrorT>(res: std::result::Result<ValueT, ErrorT>) -> ErrnoResult<ValueT> {
    res.map_err(|_| Errno::EINVAL)
}

const TTL: Duration = Duration::from_secs(1); // 1 second

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
                LazyLayerSuperInner::Cached(LayerSuper::read_from_path(&data_fs, path).await?);
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
}

impl LayerFs {
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

    fn cache_entry(&self, digest: Sha256Digest) -> PathBuf {
        self.cache_path.join(digest.to_string())
    }

    pub fn mount(self, mount_path: &Path) -> Result<crate::FuseHandle> {
        crate::fuse_mount(self, mount_path, "Maelstrom LayerFS")
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
            FileData::Digest {
                digest,
                offset: file_start,
                length: file_length,
            } => {
                let read_start = file_start + to_einval::<u64, _>(offset.try_into())?;
                let file_end = file_start + file_length;
                if read_start > file_end {
                    return Err(Errno::EINVAL);
                }

                let read_end = std::cmp::min(read_start + size as u64, file_end);
                let read_length = read_end - read_start;

                let mut file = to_eio(self.data_fs.open_file(self.cache_entry(digest)).await)?;
                to_eio(file.seek(SeekFrom::Start(read_start)).await)?;
                let mut buffer = vec![0; read_length as usize];
                to_eio(file.read_exact(&mut buffer).await)?;
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
        let reader = to_eio(DirectoryDataReader::new(self, file).await)?;
        Ok(to_eio(reader.into_stream(offset.try_into()?).await)?)
    }
}

#[cfg(test)]
const ARBITRARY_TIME: maelstrom_base::manifest::UnixTimestamp =
    maelstrom_base::manifest::UnixTimestamp(1705000271);

#[cfg(test)]
async fn build_fs(
    fs: &Fs,
    data_dir: &Path,
    cache_path: &Path,
    files: Vec<(&str, FileData)>,
) -> LayerFs {
    use maelstrom_base::manifest::Mode;

    let mut builder = BottomLayerBuilder::new(fs, data_dir, cache_path, ARBITRARY_TIME)
        .await
        .unwrap();

    for (path, data) in files {
        let size = match &data {
            ty::FileData::Empty => 0,
            ty::FileData::Inline(d) => d.len() as u64,
            ty::FileData::Digest { length, .. } => *length,
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

    builder.finish()
}

#[cfg(test)]
async fn assert_entries(fs: &Fs, path: &Path, expected: Vec<&str>) {
    use futures::StreamExt as _;

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
    use ty::FileData::*;

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
        vec![("/Foo", Empty), ("/Bar", Empty), ("/Baz", Empty)],
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
    use ty::FileData::*;

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
            ("/Foo/Bar/Baz", Empty),
            ("/Foo/Bin", Empty),
            ("/Foo/Bar/Qux", Empty),
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
    use maelstrom_base::manifest::Mode;
    use std::os::unix::fs::MetadataExt as _;
    use ty::FileData::*;

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
        vec![("/Foo", Empty), ("/Bar", Empty), ("/Baz", Empty)],
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
async fn read_inline() {
    use ty::FileData::*;

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
            ("/Foo", Inline(b"hello world".into())),
            ("/Bar", Empty),
            ("/Baz", Empty),
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
async fn read_digest() {
    use tokio::io::AsyncWriteExt as _;
    use ty::FileData::*;

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
            (
                "/Foo",
                Digest {
                    digest: digest.clone(),
                    offset: 0,
                    length: 5,
                },
            ),
            (
                "/Bar",
                Digest {
                    digest,
                    offset: 6,
                    length: 5,
                },
            ),
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

#[cfg(test)]
async fn calc_digest(fs: &Fs, path: &Path) -> Sha256Digest {
    let mut f = fs.open_file(path).await.unwrap();
    let mut hasher = maelstrom_util::io::Sha256Stream::new(tokio::io::sink());
    tokio::io::copy(&mut f, &mut hasher).await.unwrap();
    hasher.finalize().1
}

#[cfg(test)]
async fn build_tar(fs: &Fs, cache_path: &Path, files: Vec<(&str, FileData)>) -> Sha256Digest {
    let tar_path = cache_path.join("temp.tar");
    let f = fs.create_file(&tar_path).await.unwrap();
    let mut ar = tokio_tar::Builder::new(f.into_inner());
    let mut header = tokio_tar::Header::new_gnu();
    for (p, d) in files {
        let data = match d {
            ty::FileData::Empty => vec![],
            ty::FileData::Inline(d) => d,
            _ => panic!(),
        };
        header.set_size(data.len() as u64);
        header.set_mode(0o555);
        ar.append_data(&mut header, p, &data[..]).await.unwrap();
    }
    ar.finish().await.unwrap();

    let digest = calc_digest(fs, &tar_path).await;

    fs.rename(tar_path, cache_path.join(digest.to_string()))
        .await
        .unwrap();

    digest
}

#[tokio::test]
async fn layer_from_tar() {
    use ty::FileData::*;

    let temp = tempfile::tempdir().unwrap();
    let mount_point = temp.path().join("mount");
    let data_dir = temp.path().join("data");
    let cache_dir = temp.path().join("cache");

    let fs = Fs::new();
    fs.create_dir(&mount_point).await.unwrap();
    fs.create_dir(&data_dir).await.unwrap();
    fs.create_dir(&cache_dir).await.unwrap();

    let tar_digest = build_tar(
        &fs,
        &cache_dir,
        vec![
            ("Foo", Inline(b"hello world".into())),
            ("Bar/Baz", Empty),
            ("Bar/Bin", Empty),
        ],
    )
    .await;
    let tar_path = cache_dir.join(tar_digest.to_string());

    let mut builder = BottomLayerBuilder::new(&fs, &data_dir, &cache_dir, ARBITRARY_TIME)
        .await
        .unwrap();
    builder
        .add_from_tar(tar_digest, fs.open_file(tar_path).await.unwrap())
        .await
        .unwrap();
    let layer_fs = builder.finish();

    let mount_handle = layer_fs.mount(&mount_point).unwrap();

    let contents = fs.read_to_string(mount_point.join("Foo")).await.unwrap();
    assert_eq!(contents, "hello world");

    let contents = fs
        .read_to_string(mount_point.join("Bar/Baz"))
        .await
        .unwrap();
    assert_eq!(contents, "");

    assert_entries(&fs, &mount_point.join("Bar"), vec!["Baz", "Bin"]).await;

    mount_handle.umount_and_join().await.unwrap();
}

#[cfg(test)]
async fn two_layer_test(lower: Vec<&str>, upper: Vec<&str>, expected: Vec<(&str, Vec<&str>)>) {
    use ty::FileData::*;

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
        lower.into_iter().map(|e| (e, Empty)).collect(),
    )
    .await;

    let layer_fs2 = build_fs(
        &fs,
        &data_dir2,
        &cache_dir,
        upper.into_iter().map(|e| (e, Empty)).collect(),
    )
    .await;

    let mut builder = UpperLayerBuilder::new(&data_dir3, &cache_dir, &layer_fs1)
        .await
        .unwrap();
    builder.fill_from_bottom_layer(&layer_fs2).await.unwrap();
    let layer_fs = builder.finish();

    let mount_handle = layer_fs.mount(&mount_point).unwrap();

    for (d, entries) in expected {
        assert_entries(&fs, &mount_point.join(d), entries).await;
    }

    mount_handle.umount_and_join().await.unwrap();
}

#[tokio::test]
async fn two_mounts_test() {
    use ty::FileData::*;

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
        ["/Apple"].into_iter().map(|e| (e, Empty)).collect(),
    )
    .await;

    let layer_fs2 = build_fs(
        &fs,
        &data_dir2,
        &cache_dir,
        ["/Birthday"].into_iter().map(|e| (e, Empty)).collect(),
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
