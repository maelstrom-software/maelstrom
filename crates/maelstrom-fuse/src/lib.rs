pub mod fuser;

use anyhow::Result;
use async_trait::async_trait;
pub use fuser::{FileAttr, FileType};
use fuser::{MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry};
use futures::stream::{Stream, StreamExt};
use maelstrom_linux::{self as linux, Errno};
use std::ffi::OsStr;
use std::future::Future;
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::{self, JoinHandle};

const MAX_INFLIGHT: usize = 1000;

pub struct FuseHandle {
    fuser_session: fuser::BackgroundSession,
}

impl FuseHandle {
    pub async fn umount_and_join(self) -> Result<()> {
        self.fuser_session.join().await;
        Ok(())
    }
}

pub fn fuse_mount(
    handler: impl FuseFileSystem + Send + Sync + 'static,
    mount_point: &Path,
    name: &str,
) -> Result<FuseHandle> {
    let mount_point = mount_point.to_owned();
    let name = name.into();

    let fuser_session = fuse_mount_dispatcher(handler, mount_point, name)?;

    Ok(FuseHandle { fuser_session })
}

pub struct FuseNamespaceHandle {
    stream: linux::UnixStream,
    handle: JoinHandle<std::io::Result<()>>,
    mount_path: PathBuf,
}

impl FuseNamespaceHandle {
    pub async fn umount_and_join(self) -> Result<()> {
        drop(self.stream);
        self.handle.await.unwrap()?;
        Ok(())
    }

    pub fn mount_path(&self) -> &Path {
        &self.mount_path
    }
}

fn run_fuse_child(b: linux::UnixStream, fsname: String, uid: linux::Uid, gid: linux::Gid) -> ! {
    let fs = maelstrom_util::fs::Fs::new();
    fs.write("/proc/self/setgroups", b"deny").unwrap();
    fs.write("/proc/self/uid_map", format!("0 {} 1", uid.as_u32()))
        .unwrap();
    fs.write("/proc/self/gid_map", format!("0 {} 1", gid.as_u32()))
        .unwrap();

    let (file, _) = crate::fuser::fuse_mount_pure(
        Path::new("/mnt").as_os_str(),
        &[MountOption::RO, MountOption::FSName(fsname)],
    )
    .unwrap();

    let fd = linux::Fd::from_raw(file.as_raw_fd());
    b.send_with_fd(b"a", fd).unwrap();

    let mut buf = [0; 1];
    b.recv_with_fd(&mut buf).unwrap();

    std::process::exit(0)
}

pub async fn fuse_mount_namespace(
    handler: impl FuseFileSystem + Send + Sync + 'static,
    name: &str,
) -> Result<FuseNamespaceHandle> {
    let name = name.to_owned();
    let (a, child, fd) = task::spawn_blocking(move || -> Result<_> {
        let (a, b) = linux::UnixStream::pair()?;
        let uid = linux::getuid();
        let gid = linux::getgid();
        let mut clone_args = linux::CloneArgs::default().flags(
            linux::CloneFlags::NEWCGROUP
                | linux::CloneFlags::NEWIPC
                | linux::CloneFlags::NEWNET
                | linux::CloneFlags::NEWNS
                | linux::CloneFlags::NEWPID
                | linux::CloneFlags::NEWUSER,
        );
        let Some(child) = linux::clone3(&mut clone_args)? else {
            drop(a);
            run_fuse_child(b, name, uid, gid);
        };

        let mut buf = [0; 1];
        let (_, fd) = a.recv_with_fd(&mut buf)?;
        Ok((a, child, fd))
    })
    .await??;

    let handle = task::spawn(async move {
        let mut session = crate::fuser::Session::from_fd(
            DispatchingFs::new(handler),
            fd.unwrap().into(),
            crate::fuser::SessionACL::All,
        )?;
        session.run().await
    });
    Ok(FuseNamespaceHandle {
        stream: a,
        handle,
        mount_path: format!("/proc/{}/root/mnt", child.as_i32()).into(),
    })
}

pub async fn run_fuse(
    handler: impl FuseFileSystem + Send + Sync + 'static,
    fd: linux::OwnedFd,
) -> Result<()> {
    let mut session = crate::fuser::Session::from_fd(
        DispatchingFs::new(handler),
        fd.into(),
        crate::fuser::SessionACL::All,
    )?;
    session.run().await?;
    Ok(())
}

fn fuse_mount_dispatcher(
    handler: impl FuseFileSystem + Send + Sync + 'static,
    mount_point: PathBuf,
    name: String,
) -> Result<fuser::BackgroundSession> {
    let options = vec![MountOption::RO, MountOption::FSName(name)];
    Ok(fuser::spawn_mount2(
        DispatchingFs::new(handler),
        mount_point,
        &options,
    )?)
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

struct DispatchingFs<FileSystemT> {
    handler: Arc<FileSystemT>,
    sem: Arc<Semaphore>,
}

impl<FileSystemT> DispatchingFs<FileSystemT> {
    fn new(handler: FileSystemT) -> Self {
        Self {
            handler: Arc::new(handler),
            sem: Arc::new(Semaphore::new(MAX_INFLIGHT / 2)),
        }
    }
}

#[async_trait]
impl<FileSystemT: FuseFileSystem + Send + Sync + 'static> fuser::Filesystem
    for DispatchingFs<FileSystemT>
{
    async fn destroy(&mut self) {
        self.sem
            .acquire_many(MAX_INFLIGHT as u32 / 2)
            .await
            .unwrap()
            .forget();
    }

    async fn lookup(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: ReplyEntry,
    ) {
        let handler = self.handler.clone();
        let permit = self.sem.clone().acquire_owned().await.unwrap();
        let request = req.into();
        let name = name.to_owned();
        tokio::task::spawn(async move {
            handle_resp(handler.look_up(request, parent, &name).await, reply).await;
            drop(permit);
        });
    }

    async fn getattr(&mut self, req: &fuser::Request<'_>, ino: u64, reply: ReplyAttr) {
        let handler = self.handler.clone();
        let permit = self.sem.clone().acquire_owned().await.unwrap();
        let request = req.into();
        tokio::task::spawn(async move {
            handle_resp(handler.get_attr(request, ino).await, reply).await;
            drop(permit);
        });
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
        let handler = self.handler.clone();
        let permit = self.sem.clone().acquire_owned().await.unwrap();
        let request = req.into();
        tokio::task::spawn(async move {
            handle_resp(
                handler
                    .read(request, ino, fh, offset, size, flags, lock_owner)
                    .await,
                reply,
            )
            .await;
            drop(permit);
        });
    }

    async fn readdir(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: ReplyDirectory,
    ) {
        let handler = self.handler.clone();
        let permit = self.sem.clone().acquire_owned().await.unwrap();
        let request = req.into();
        tokio::task::spawn(async move {
            handle_read_dir_resp(handler.read_dir(request, ino, fh, offset).await, reply).await;
            drop(permit);
        });
    }

    async fn readlink(&mut self, req: &fuser::Request<'_>, ino: u64, reply: ReplyData) {
        let handler = self.handler.clone();
        let permit = self.sem.clone().acquire_owned().await.unwrap();
        let request = req.into();
        tokio::task::spawn(async move {
            handle_resp(handler.read_link(request, ino).await, reply).await;
            drop(permit);
        });
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
