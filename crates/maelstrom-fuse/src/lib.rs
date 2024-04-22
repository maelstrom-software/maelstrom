//! This crate contains code to help implement the Linux FUSE API using async Rust.
//!
//! The guts were adapted from the fuser crate and can be found in the [`fuser`] module
pub mod fuser;

use anyhow::Result;
pub use fuser::{FileAttr, FileType};
use fuser::{MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry};
use futures::stream::{Stream, StreamExt};
use maelstrom_linux::{self as linux, Errno};
use maelstrom_util::r#async::await_and_every_sec;
use std::ffi::OsStr;
use std::fs::File;
use std::future::Future;
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::{self, JoinHandle};

/// The number of requests the [`DispatchingFs`] allow in-flight. When this limit is reached it
/// will block the next FUSE request until an existing one finishes.
///
/// This limit is put in place to try to have some control on the amount of tasks and memory being
/// used by a FUSE connection.
const MAX_INFLIGHT: usize = 1000;

/// The handle returned by [`fuse_mount_namespace`]. It can be used make the child exit (thus
/// removing the mount) also it provides a way to get the path to the mount (via `/proc/`)
pub struct FuseNamespaceHandle {
    stream: linux::UnixStream,
    handle: JoinHandle<std::io::Result<()>>,
    log: slog::Logger,
    child: linux::Pid,
    mount_path: PathBuf,
}

impl FuseNamespaceHandle {
    /// Make the child exit and wait for it. This makes the mount go away.
    pub async fn umount_and_join(self) -> Result<()> {
        let _ = self.stream.shutdown();
        drop(self.stream);
        await_and_every_sec(
            tokio::task::spawn_blocking(move || linux::waitpid(self.child)),
            || slog::debug!(self.log, "waiting for child"; "child" => ?self.child),
        )
        .await
        .unwrap()?;
        await_and_every_sec(
            self.handle,
            || slog::debug!(self.log, "waiting for FUSE"; "child" => ?self.child),
        )
        .await
        .unwrap()?;
        Ok(())
    }

    /// The path to the mount (via `/proc/`)
    pub fn mount_path(&self) -> &Path {
        &self.mount_path
    }
}

fn run_fuse_child(
    b: linux::UnixStream,
    mount_path: &Path,
    fsname: String,
    uid: linux::Uid,
    gid: linux::Gid,
) -> ! {
    let fs = maelstrom_util::fs::Fs::new();
    fs.write("/proc/self/setgroups", b"deny").unwrap();
    fs.write("/proc/self/uid_map", format!("0 {} 1", uid.as_u32()))
        .unwrap();
    fs.write("/proc/self/gid_map", format!("0 {} 1", gid.as_u32()))
        .unwrap();

    let file = crate::fuser::fuse_mount_sys(
        mount_path.as_os_str(),
        &[MountOption::RO, MountOption::FSName(fsname)],
    )
    .unwrap();

    let fd = linux::Fd::from_raw(file.as_raw_fd());
    b.send_with_fd(b"a", fd).unwrap();

    let mut buf = [0; 1];
    b.recv_with_fd(&mut buf).unwrap();

    linux::_exit(linux::ExitCode::from_u8(0))
}

/// Serve a FUSE connection using the provided handler and name. The FUSE connection will be
/// mounted in a child process in its own namespace.
///
/// This is useful because it doesn't require root access to mount the namespace (or the fusermount
/// binary). The mount can be accessed via a path in `/proc`. The returned handle gives a way to
/// get the path to the mount and exit the child process.
///
/// The child process hangs around waiting on a UNIX domain socket so if the parent dies it should
/// cleanly exit on its own. (Crucially it also doesn't attempt to access the mount after the
/// parent is gone)
pub async fn fuse_mount_namespace(
    handler: impl FuseFileSystem + Send + Sync + 'static,
    log: slog::Logger,
    name: &str,
) -> Result<FuseNamespaceHandle> {
    let mount_path = std::env::current_dir()?;
    let name = name.to_owned();
    let mount_path2 = mount_path.clone();
    let (a, child, fd) = task::spawn_blocking(move || -> Result<_> {
        let (a, b) = linux::UnixStream::pair()?;
        let uid = linux::getuid();
        let gid = linux::getgid();
        let mut clone_args = linux::CloneArgs::default()
            .flags(
                linux::CloneFlags::NEWCGROUP
                    | linux::CloneFlags::NEWIPC
                    | linux::CloneFlags::NEWNET
                    | linux::CloneFlags::NEWNS
                    | linux::CloneFlags::NEWPID
                    | linux::CloneFlags::NEWUSER,
            )
            .exit_signal(linux::Signal::CHLD);
        let Some(child) = linux::clone3(&mut clone_args)? else {
            drop(a);
            run_fuse_child(b, &mount_path2, name, uid, gid);
        };

        let mut buf = [0; 1];
        let (_, fd) = a.recv_with_fd(&mut buf)?;
        Ok((a, child, fd))
    })
    .await??;

    let other_log = log.clone();
    let handle = task::spawn(async move {
        let mut session = crate::fuser::Session::from_fd(
            DispatchingFs::new(handler),
            fd.unwrap().into(),
            crate::fuser::SessionACL::All,
            other_log,
        )?;
        session.run().await
    });
    Ok(FuseNamespaceHandle {
        stream: a,
        handle,
        child,
        log,
        mount_path: format!("/proc/{}/root{}", child.as_i32(), mount_path.display()).into(),
    })
}

/// Serve a FUSE connection using the provided handler and file-descriptor. The file-descriptor
/// must have been obtained by opening `/dev/fuse`.
///
/// The function returns when the FUSE connection has been closed either via an error or cleanly
/// unmounting.
pub async fn run_fuse(
    handler: impl FuseFileSystem + Send + Sync + 'static,
    log: slog::Logger,
    fd: linux::OwnedFd,
) -> Result<()> {
    let mut session = crate::fuser::Session::from_fd(
        DispatchingFs::new(handler),
        fd.into(),
        crate::fuser::SessionACL::All,
        log,
    )?;
    session.run().await?;
    Ok(())
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

/// Passed to all the [`FuseFileSystem`] request functions and contains information about who is
/// doing the request.
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

/// Response from a [`FuseFileSystem::look_up`] request
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

/// Response from a [`FuseFileSystem::get_attr`] request
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
pub enum ReadResponse {
    Buffer {
        data: Vec<u8>,
    },
    Splice {
        file: Arc<File>,
        offset: u64,
        length: usize,
    },
}

impl Response for ReadResponse {
    type Reply = ReplyData;

    async fn send(self, reply: ReplyData) {
        match self {
            Self::Buffer { data } => reply.data(&data).await,
            Self::Splice {
                file,
                offset,
                length,
            } => {
                let fd = linux::Fd::from_raw(file.as_raw_fd());
                reply.data_splice(fd, offset, length).await
            }
        }
    }
}

/// Response from a [`FuseFileSystem::read_link`] request
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

/// Directory entry, used in [`FuseFileSystem::read_dir`] request
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

/// The Linux FUSE API as a trait for async Rust code.
pub trait FuseFileSystem {
    fn look_up(
        &self,
        _req: Request,
        _parent: u64,
        _name: &OsStr,
    ) -> impl Future<Output = ErrnoResult<EntryResponse>> + Send {
        async move { Err(Errno::ENOSYS) }
    }

    fn get_attr(
        &self,
        _req: Request,
        _ino: u64,
    ) -> impl Future<Output = ErrnoResult<AttrResponse>> + Send {
        async move { Err(Errno::ENOSYS) }
    }

    fn read_link(
        &self,
        _req: Request,
        _ino: u64,
    ) -> impl Future<Output = ErrnoResult<ReadLinkResponse>> + Send {
        async move { Err(Errno::ENOSYS) }
    }

    /*
    fn open(&mut self, _req: Request, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }
    */

    #[allow(clippy::too_many_arguments)]
    fn read(
        &self,
        _req: Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> impl Future<Output = ErrnoResult<ReadResponse>> + Send {
        async move { Err(Errno::ENOSYS) }
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

    fn read_dir(
        &self,
        _req: Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
    ) -> impl Future<Output = ErrnoResult<Self::ReadDirStream<'_>>> + Send {
        async move { Err(Errno::ENOSYS) }
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
