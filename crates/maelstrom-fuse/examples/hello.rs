///! This example is taken from the fuser project
use anyhow::Result;
use maelstrom_fuse::{
    AttrResponse, DirEntry, EntryResponse, ErrnoResult, FileAttr, FileType, FuseFileSystem,
    ReadResponse, Request,
};
use maelstrom_linux::Errno;
use slog::{o, Drain, Logger};
use slog_async::Async;
use slog_term::{FullFormat, TermDecorator};
use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};
use tokio::io::AsyncBufReadExt as _;

const TTL: Duration = Duration::from_secs(1); // 1 second

const HELLO_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    blksize: 512,
};

const HELLO_TXT_CONTENT: &str = "Hello World!\n";

const HELLO_TXT_ATTR: FileAttr = FileAttr {
    ino: 2,
    size: 13,
    blocks: 1,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 501,
    gid: 20,
    rdev: 0,
    blksize: 512,
};

struct HelloFs;

impl FuseFileSystem for HelloFs {
    async fn look_up(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
    ) -> ErrnoResult<EntryResponse> {
        if parent == 1 && name.to_str() == Some("hello.txt") {
            Ok(EntryResponse {
                ttl: TTL,
                attr: HELLO_TXT_ATTR.clone(),
                generation: 0,
            })
        } else {
            Err(Errno::ENOENT)
        }
    }

    async fn get_attr(&self, _req: Request, ino: u64) -> ErrnoResult<AttrResponse> {
        match ino {
            1 => Ok(AttrResponse {
                ttl: TTL,
                attr: HELLO_DIR_ATTR,
            }),
            2 => Ok(AttrResponse {
                ttl: TTL,
                attr: HELLO_TXT_ATTR,
            }),
            _ => Err(Errno::ENOENT),
        }
    }

    async fn read(
        &self,
        _req: Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        _flags: i32,
        _lock: Option<u64>,
    ) -> ErrnoResult<ReadResponse> {
        if ino == 2 {
            Ok(ReadResponse::Buffer {
                data: HELLO_TXT_CONTENT.as_bytes()[offset as usize..].to_owned(),
            })
        } else {
            Err(Errno::ENOENT)
        }
    }

    type ReadDirStream<'a> = futures::stream::Iter<std::vec::IntoIter<ErrnoResult<DirEntry>>>;

    async fn read_dir<'a>(
        &'a self,
        _req: Request,
        ino: u64,
        _fh: u64,
        offset: i64,
    ) -> ErrnoResult<Self::ReadDirStream<'a>> {
        if ino != 1 {
            return Err(Errno::ENOENT);
        }

        let entries = vec![
            (1, FileType::Directory, "."),
            (1, FileType::Directory, ".."),
            (2, FileType::RegularFile, "hello.txt"),
        ];

        let mut resp = vec![];
        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 means the index of the next entry
            resp.push(Ok(DirEntry {
                ino: entry.0,
                offset: (i + 1) as i64,
                kind: entry.1,
                name: entry.2.into(),
            }));
        }
        Ok(futures::stream::iter(resp))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!());

    let handle = maelstrom_fuse::fuse_mount_namespace(HelloFs, log, "hello").await?;
    println!("mounted at {}", handle.mount_path().display());

    // wait for newline on stdin
    println!("press enter to exit");
    let _ = tokio::io::BufReader::new(tokio::io::stdin())
        .lines()
        .next_line()
        .await;

    handle.umount_and_join().await
}
