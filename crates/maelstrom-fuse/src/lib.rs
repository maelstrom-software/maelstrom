pub mod fuser;

use anyhow::Result;
use async_trait::async_trait;
pub use fuser::{FileAttr, FileType};
use fuser::{MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry};
use futures::stream::{Stream, StreamExt};
use maelstrom_linux::Errno;
use std::ffi::{OsStr, OsString};
use std::future::Future;
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
    fn error(self, e: i32) -> impl Future<Output = ()>;
}

impl ErrorResponse for ReplyAttr {
    fn error(self, e: i32) -> impl Future<Output = ()> {
        ReplyAttr::error(self, e)
    }
}

impl ErrorResponse for ReplyEntry {
    fn error(self, e: i32) -> impl Future<Output = ()> {
        ReplyEntry::error(self, e)
    }
}

impl ErrorResponse for ReplyData {
    fn error(self, e: i32) -> impl Future<Output = ()> {
        ReplyData::error(self, e)
    }
}

trait Response {
    type Reply: ErrorResponse;
    fn send(self, reply: Self::Reply) -> impl Future<Output = ()>;
}

async fn handle_resp<RespT: Response>(res: ErrnoResult<RespT>, reply: RespT::Reply)
where
    RespT: Send + 'static,
    RespT::Reply: Send + std::fmt::Debug + 'static,
{
    match res {
        Ok(resp) => resp.send(reply).await,
        Err(e) => reply.error(e.as_i32()).await,
    }
}

async fn handle_read_dir_resp(
    res: ErrnoResult<impl Stream<Item = ErrnoResult<DirEntry>>>,
    mut reply: ReplyDirectory,
) {
    match res {
        Ok(stream) => {
            let mut pinned_stream = pin!(stream);
            while let Some(entry) = pinned_stream.next().await {
                match entry {
                    Ok(entry) => {
                        if entry.add(&mut reply) {
                            break;
                        }
                    }
                    Err(e) => {
                        reply.error(e.as_i32()).await;
                        return;
                    }
                }
            }
            reply.ok().await;
        }
        Err(e) => {
            reply.error(e.as_i32()).await;
        }
    }
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

#[async_trait]
impl fuser::Filesystem for SendingFs {
    async fn lookup(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: ReplyEntry,
    ) {
        let msg = FsMessage::LookUp {
            request: req.into(),
            parent: parent,
            name: name.into(),
            reply,
        };
        if let Err(SendError(FsMessage::LookUp { reply, .. })) = self.0.send(msg).await {
            reply.error(Errno::EIO.as_i32()).await
        }
    }
    async fn getattr(&mut self, req: &fuser::Request<'_>, ino: u64, reply: ReplyAttr) {
        let msg = FsMessage::GetAttr {
            request: req.into(),
            ino,
            reply,
        };
        if let Err(SendError(FsMessage::GetAttr { reply, .. })) = self.0.send(msg).await {
            reply.error(Errno::EIO.as_i32()).await
        }
    }
    async fn read(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let msg = FsMessage::Read {
            request: req.into(),
            ino,
            fh,
            offset,
            size,
            flags,
            lock_owner,
            reply,
        };
        if let Err(SendError(FsMessage::Read { reply, .. })) = self.0.send(msg).await {
            reply.error(Errno::EIO.as_i32()).await
        }
    }
    async fn readdir(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: ReplyDirectory,
    ) {
        let msg = FsMessage::ReadDir {
            request: req.into(),
            ino,
            fh,
            offset,
            reply,
        };
        if let Err(SendError(FsMessage::ReadDir { reply, .. })) = self.0.send(msg).await {
            reply.error(Errno::EIO.as_i32()).await
        }
    }
    async fn readlink(&mut self, req: &fuser::Request<'_>, ino: u64, reply: ReplyData) {
        let msg = FsMessage::ReadLink {
            request: req.into(),
            ino,
            reply,
        };
        if let Err(SendError(FsMessage::ReadLink { reply, .. })) = self.0.send(msg).await {
            reply.error(Errno::EIO.as_i32()).await
        }
    }
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

#[derive(Debug)]
pub struct EntryResponse {
    pub ttl: Duration,
    pub attr: FileAttr,
    pub generation: u64,
}

impl Response for EntryResponse {
    type Reply = ReplyEntry;

    async fn send(self, reply: ReplyEntry) {
        reply.entry(&self.ttl, &self.attr, self.generation).await
    }
}

#[derive(Debug)]
pub struct AttrResponse {
    pub ttl: Duration,
    pub attr: FileAttr,
}

impl Response for AttrResponse {
    type Reply = ReplyAttr;

    async fn send(self, reply: ReplyAttr) {
        reply.attr(&self.ttl, &self.attr).await
    }
}

#[derive(Debug)]
pub struct ReadResponse {
    pub data: Vec<u8>,
}

impl Response for ReadResponse {
    type Reply = ReplyData;

    async fn send(self, reply: ReplyData) {
        reply.data(&self.data).await
    }
}

#[derive(Debug)]
pub struct ReadLinkResponse {
    pub data: Vec<u8>,
}

impl Response for ReadLinkResponse {
    type Reply = ReplyData;

    async fn send(self, reply: ReplyData) {
        reply.data(&self.data).await
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
