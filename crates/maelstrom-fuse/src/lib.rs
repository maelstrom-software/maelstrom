use anyhow::Result;
use async_trait::async_trait;
pub use fuser::{FileAttr, FileType};
use fuser::{MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry};
use futures::stream::{Stream, StreamExt};
use maelstrom_linux::Errno;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{channel, error::SendError, Receiver, Sender};
use tokio::sync::Semaphore;

const MAX_INFLIGHT: usize = 1000;

pub struct FuseHandle {
    fuser_session: fuser::BackgroundSession,
    receiver: tokio::task::JoinHandle<Result<()>>,
}

impl FuseHandle {
    pub async fn umount_and_join(self) -> Result<()> {
        drop(self.fuser_session);
        self.receiver.await?
    }
}

pub fn fuse_mount(
    handler: impl FuseFileSystem + Send + Sync + 'static,
    mount_point: &Path,
    name: &str,
) -> Result<FuseHandle> {
    let mount_point = mount_point.to_owned();
    let name = name.into();
    let (send, recv) = channel(MAX_INFLIGHT / 2);

    let fuser_session = fuse_mount_sender(send, mount_point, name)?;
    let receiver = tokio::task::spawn(async move { fuse_mount_receiver(handler, recv).await });

    Ok(FuseHandle {
        fuser_session,
        receiver,
    })
}

fn fuse_mount_sender(
    send: Sender<FsMessage>,
    mount_point: PathBuf,
    name: String,
) -> Result<fuser::BackgroundSession> {
    let options = vec![MountOption::RO, MountOption::FSName(name)];
    Ok(fuser::spawn_mount2(SendingFs(send), mount_point, &options)?)
}

trait ErrorResponse {
    fn error(self, e: i32);
}

impl ErrorResponse for ReplyAttr {
    fn error(self, e: i32) {
        ReplyAttr::error(self, e)
    }
}

impl ErrorResponse for ReplyEntry {
    fn error(self, e: i32) {
        ReplyEntry::error(self, e)
    }
}

impl ErrorResponse for ReplyData {
    fn error(self, e: i32) {
        ReplyData::error(self, e)
    }
}

trait Response {
    type Reply: ErrorResponse;
    fn send(self, reply: Self::Reply);
}

async fn handle_resp<RespT: Response>(res: ErrnoResult<RespT>, reply: RespT::Reply)
where
    RespT: Send + 'static,
    RespT::Reply: Send + 'static,
{
    tokio::task::spawn_blocking(move || match res {
        Ok(resp) => resp.send(reply),
        Err(e) => reply.error(e.as_i32()),
    })
    .await
    .unwrap()
}

async fn handle_read_dir_resp(
    res: ErrnoResult<impl Stream<Item = ErrnoResult<DirEntry>>>,
    mut reply: ReplyDirectory,
) {
    let (send, mut recv) = channel::<ErrnoResult<DirEntry>>(100);
    let blocking_handle = tokio::task::spawn_blocking(move || loop {
        match recv.blocking_recv() {
            Some(Ok(entry)) => {
                if entry.add(&mut reply) {
                    return;
                }
            }
            Some(Err(e)) => {
                reply.error(e.as_i32());
                return;
            }
            None => {
                reply.ok();
                return;
            }
        }
    });
    match res {
        Ok(stream) => {
            let mut pinned_stream = pin!(stream);
            while let Some(entry) = pinned_stream.next().await {
                let is_err = entry.is_err();
                if send.send(entry).await.is_err() {
                    break;
                }
                if is_err {
                    break;
                }
            }
        }
        Err(e) => {
            send.send(Err(e)).await.unwrap();
        }
    }
    drop(send);
    blocking_handle.await.unwrap();
}

async fn fuse_mount_receiver(
    handler: impl FuseFileSystem + Send + Sync + 'static,
    mut recv: Receiver<FsMessage>,
) -> Result<()> {
    let handler = Arc::new(handler);
    let sem = Arc::new(Semaphore::new(MAX_INFLIGHT / 2));
    while let Some(msg) = recv.recv().await {
        let handler = handler.clone();
        let permit = sem.clone().acquire_owned().await.unwrap();
        match msg {
            FsMessage::LookUp {
                request,
                parent,
                name,
                reply,
            } => tokio::task::spawn(async move {
                handle_resp(handler.look_up(request, parent, &name).await, reply).await;
                drop(permit);
            }),
            FsMessage::GetAttr {
                request,
                ino,
                reply,
            } => tokio::task::spawn(async move {
                handle_resp(handler.get_attr(request, ino).await, reply).await;
                drop(permit);
            }),
            FsMessage::Read {
                request,
                ino,
                fh,
                offset,
                size,
                flags,
                lock_owner,
                reply,
            } => tokio::task::spawn(async move {
                handle_resp(
                    handler
                        .read(request, ino, fh, offset, size, flags, lock_owner)
                        .await,
                    reply,
                )
                .await;
                drop(permit);
            }),
            FsMessage::ReadDir {
                request,
                ino,
                fh,
                offset,
                reply,
            } => tokio::task::spawn(async move {
                handle_read_dir_resp(handler.read_dir(request, ino, fh, offset).await, reply).await;
                drop(permit);
            }),
            FsMessage::ReadLink {
                request,
                ino,
                reply,
            } => tokio::task::spawn(async move {
                handle_resp(handler.read_link(request, ino).await, reply).await;
                drop(permit);
            }),
        };
    }
    sem.acquire_many(MAX_INFLIGHT as u32 / 2).await?.forget();
    Ok(())
}

enum FsMessage {
    LookUp {
        request: Request,
        parent: u64,
        name: OsString,
        reply: ReplyEntry,
    },
    GetAttr {
        request: Request,
        ino: u64,
        reply: ReplyAttr,
    },
    Read {
        request: Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    },
    ReadDir {
        request: Request,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: ReplyDirectory,
    },
    ReadLink {
        request: Request,
        ino: u64,
        reply: ReplyData,
    },
}

struct SendingFs(Sender<FsMessage>);

macro_rules! op {
    ($op1:ident, $op2:ident { $($arg:ident: $ty:ty),* }) => {
        fn $op1(&mut self, req: &fuser::Request, $($arg: $ty),*) {
            let msg = FsMessage::$op2 {
                request: req.into(),
                $($arg: $arg.into()),*
            };
            if let Err(SendError(FsMessage::$op2 { reply, .. })) = self.0.blocking_send(msg) {
                reply.error(Errno::EIO.as_i32())
            }
        }
    }
}

impl fuser::Filesystem for SendingFs {
    op!(
        lookup,
        LookUp {
            parent: u64,
            name: &OsStr,
            reply: ReplyEntry
        }
    );
    op!(
        getattr,
        GetAttr {
            ino: u64,
            reply: ReplyAttr
        }
    );
    op!(
        read,
        Read {
            ino: u64,
            fh: u64,
            offset: i64,
            size: u32,
            flags: i32,
            lock_owner: Option<u64>,
            reply: ReplyData
        }
    );
    op!(
        readdir,
        ReadDir {
            ino: u64,
            fh: u64,
            offset: i64,
            reply: ReplyDirectory
        }
    );
    op!(
        readlink,
        ReadLink {
            ino: u64,
            reply: ReplyData
        }
    );
}

pub struct Request {
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
}

impl From<&fuser::Request<'_>> for Request {
    fn from(r: &fuser::Request) -> Self {
        Self {
            uid: r.uid(),
            gid: r.gid(),
            pid: r.pid(),
        }
    }
}

pub type ErrnoResult<T> = std::result::Result<T, Errno>;

pub struct EntryResponse {
    pub ttl: Duration,
    pub attr: FileAttr,
    pub generation: u64,
}

impl Response for EntryResponse {
    type Reply = ReplyEntry;

    fn send(self, reply: ReplyEntry) {
        reply.entry(&self.ttl, &self.attr, self.generation)
    }
}

pub struct AttrResponse {
    pub ttl: Duration,
    pub attr: FileAttr,
}

impl Response for AttrResponse {
    type Reply = ReplyAttr;

    fn send(self, reply: ReplyAttr) {
        reply.attr(&self.ttl, &self.attr)
    }
}

pub struct ReadResponse {
    pub data: Vec<u8>,
}

impl Response for ReadResponse {
    type Reply = ReplyData;

    fn send(self, reply: ReplyData) {
        reply.data(&self.data)
    }
}

pub struct ReadLinkResponse {
    pub data: Vec<u8>,
}

impl Response for ReadLinkResponse {
    type Reply = ReplyData;

    fn send(self, reply: ReplyData) {
        reply.data(&self.data)
    }
}

pub struct DirEntry {
    pub ino: u64,
    pub offset: i64,
    pub kind: FileType,
    pub name: String,
}

impl DirEntry {
    fn add(self, reply: &mut ReplyDirectory) -> bool {
        reply.add(self.ino, self.offset, self.kind, self.name)
    }
}

#[async_trait]
pub trait FuseFileSystem {
    async fn look_up(
        &self,
        _req: Request,
        _parent: u64,
        _name: &OsStr,
    ) -> ErrnoResult<EntryResponse> {
        Err(Errno::ENOSYS)
    }

    async fn get_attr(&self, _req: Request, _ino: u64) -> ErrnoResult<AttrResponse> {
        Err(Errno::ENOSYS)
    }

    async fn read_link(&self, _req: Request, _ino: u64) -> ErrnoResult<ReadLinkResponse> {
        Err(Errno::ENOSYS)
    }

    /*
    fn open(&mut self, _req: Request, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }
    */

    #[allow(clippy::too_many_arguments)]
    async fn read(
        &self,
        _req: Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> ErrnoResult<ReadResponse> {
        Err(Errno::ENOSYS)
    }

    /*
    fn release(
        &mut self,
        _req: Request,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn opendir(&mut self, _req: Request, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }
    */

    type ReadDirStream<'a>: Stream<Item = ErrnoResult<DirEntry>> + Send + 'a
    where
        Self: 'a;

    async fn read_dir<'a>(
        &'a self,
        _req: Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
    ) -> ErrnoResult<Self::ReadDirStream<'a>> {
        Err(Errno::ENOSYS)
    }

    /*
    fn readdirplus(
        &mut self,
        _req: Request,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: ReplyDirectoryPlus,
    ) {
        debug!(
            "[Not Implemented] readdirplus(ino: {:#x?}, fh: {}, offset: {})",
            ino, fh, offset
        );
        reply.error(ENOSYS);
    }

    fn releasedir(
        &mut self,
        _req: Request,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&mut self, _req: Request, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn access(&mut self, _req: Request, ino: u64, mask: i32, reply: ReplyEmpty) {
        debug!("[Not Implemented] access(ino: {:#x?}, mask: {})", ino, mask);
        reply.error(ENOSYS);
    }
    */
}
