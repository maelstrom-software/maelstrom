#[allow(dead_code)]
mod avl;
mod dir;
mod ty;

use crate::{
    AttrResponse, EntryResponse, ErrnoResult, FileAttr, FileType, FuseFileSystem, ReadResponse,
    Request,
};
use anyhow::Result;
use async_trait::async_trait;
use dir::{DirectoryDataReader, DirectoryStream};
use maelstrom_linux::Errno;
use maelstrom_util::async_fs::Fs;
use std::ffi::OsStr;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};
use ty::FileId;

fn to_eio<T>(res: Result<T>) -> ErrnoResult<T> {
    res.map_err(|_| Errno::EIO)
}

const TTL: Duration = Duration::from_secs(1); // 1 second

const TEST_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
    blksize: 512,
};

pub struct LayerFs {
    data_fs: Fs,
    data_dir: PathBuf,
}

impl LayerFs {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            data_fs: Fs::new(),
            data_dir: data_dir.to_owned(),
        }
    }

    fn dir_data_path(&self, file_id: FileId) -> PathBuf {
        self.data_dir.join(format!("{file_id}.dir_data"))
    }

    pub async fn mount<RetT>(
        self,
        mount_path: &Path,
        body: impl Future<Output = RetT>,
    ) -> Result<RetT> {
        let handle = crate::fuse_mount(self, mount_path, "Maelstrom LayerFS").await?;
        let ret = body.await;
        handle.join().await?;
        Ok(ret)
    }
}

#[async_trait]
impl FuseFileSystem for LayerFs {
    async fn look_up(
        &self,
        _req: Request,
        _parent: u64,
        _name: &OsStr,
    ) -> ErrnoResult<EntryResponse> {
        Err(Errno::ENOENT)
    }

    async fn get_attr(&self, _req: Request, ino: u64) -> ErrnoResult<AttrResponse> {
        Ok(AttrResponse {
            ttl: TTL,
            attr: FileAttr { ino, ..TEST_ATTR },
        })
    }

    async fn read(
        &self,
        _req: Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        _flags: i32,
        _lock: Option<u64>,
    ) -> ErrnoResult<ReadResponse> {
        unimplemented!()
    }

    type ReadDirStream<'a> = DirectoryStream<'a>;

    async fn read_dir<'a>(
        &'a self,
        _req: Request,
        ino: u64,
        _fh: u64,
        offset: i64,
    ) -> ErrnoResult<Self::ReadDirStream<'a>> {
        let reader = to_eio(DirectoryDataReader::new(self, FileId::from(ino)).await)?;
        Ok(to_eio(reader.into_stream(offset.try_into()?).await)?)
    }
}

#[tokio::test]
async fn read_dir() {
    use futures::StreamExt as _;

    let temp = tempfile::tempdir().unwrap();
    let mount_point = temp.path().join("mount");
    let data_dir = temp.path().join("data");

    let fs = Fs::new();
    fs.create_dir(&mount_point).await.unwrap();
    fs.create_dir(&data_dir).await.unwrap();

    let mut stream = fs.create_file(data_dir.join("1.dir_data")).await.unwrap();
    let entries = vec![ty::DirectoryEntry {
        name: "Foo".into(),
        file_id: 2.into(),
        kind: FileType::RegularFile,
    }];
    for entry in &entries {
        ty::encode(&mut stream, entry).await.unwrap();
    }

    let layer_fs = LayerFs::new(&data_dir);
    layer_fs
        .mount(&mount_point, async {
            let entry_stream = fs.read_dir(&mount_point).await.unwrap();
            let mut entries: Vec<_> = entry_stream.map(|e| e.unwrap().file_name()).collect().await;
            entries.sort();
            assert_eq!(entries, vec![std::ffi::OsString::from("Foo")]);
        })
        .await
        .unwrap()
}
