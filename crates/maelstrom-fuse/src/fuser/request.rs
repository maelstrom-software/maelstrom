//! Filesystem operation request
//!
//! A request represents information about a filesystem operation the kernel driver wants us to
//! perform.
//!
//! TODO: This module is meant to go away soon in favor of `ll::Request`.

use crate::fuser::ll::{Errno, Response};
use std::convert::TryFrom;
use std::convert::TryInto;
use std::path::Path;

use crate::fuser::channel::ChannelSender;
use crate::fuser::ll::Request as _;
use crate::fuser::reply::ReplyDirectoryPlus;
use crate::fuser::reply::{Reply, ReplyDirectory, ReplySender};
use crate::fuser::session::{Session, SessionACL};
use crate::fuser::Filesystem;
use crate::fuser::{ll, KernelConfig};

/// Request data structure
#[derive(Debug)]
pub struct Request<'a> {
    /// Channel sender for sending the reply
    ch: ChannelSender,
    /// Request raw data
    #[allow(unused)]
    data: &'a [u8],
    /// Parsed request
    request: ll::AnyRequest<'a>,
}

impl<'a> Request<'a> {
    /// Create a new request from the given data
    pub(crate) fn new(ch: ChannelSender, data: &'a [u8]) -> Option<Request<'a>> {
        let request = match ll::AnyRequest::try_from(data) {
            Ok(request) => request,
            Err(_) => {
                return None;
            }
        };

        Some(Self { ch, data, request })
    }

    /// Dispatch request to the given filesystem.
    /// This calls the appropriate filesystem operation method for the
    /// request and sends back the returned reply to the kernel
    pub(crate) async fn dispatch<FS: Filesystem>(&self, se: &mut Session<FS>) {
        let unique = self.request.unique();

        let resp = match self.dispatch_req(se).await {
            Ok(Some(resp)) => resp,
            Ok(None) => return,
            Err(errno) => self.request.reply_err(errno),
        };
        let header = resp.header(unique);
        let iov = resp.as_iovec(&header);
        self.ch.send(&iov).await.ok();
    }

    async fn dispatch_req<FS: Filesystem>(
        &self,
        se: &mut Session<FS>,
    ) -> Result<Option<Response<'_>>, Errno> {
        let op = self.request.operation().map_err(|_| Errno::ENOSYS)?;
        // Implement allow_root & access check for auto_unmount
        if (se.allowed == SessionACL::RootAndOwner
            && self.request.uid() != se.session_owner
            && self.request.uid() != 0)
            || (se.allowed == SessionACL::Owner && self.request.uid() != se.session_owner)
        {
            match op {
                // Only allow operations that the kernel may issue without a uid set
                ll::Operation::Init(_)
                | ll::Operation::Destroy(_)
                | ll::Operation::Read(_)
                | ll::Operation::ReadDir(_)
                | ll::Operation::ReadDirPlus(_)
                | ll::Operation::BatchForget(_)
                | ll::Operation::Forget(_)
                | ll::Operation::Write(_)
                | ll::Operation::FSync(_)
                | ll::Operation::FSyncDir(_)
                | ll::Operation::Release(_)
                | ll::Operation::ReleaseDir(_) => {}
                _ => {
                    return Err(Errno::EACCES);
                }
            }
        }
        match op {
            // Filesystem initialization
            ll::Operation::Init(x) => {
                // We don't support ABI versions before 7.6
                let v = x.version();
                if v < ll::Version(7, 6) {
                    return Err(Errno::EPROTO);
                }
                // Remember ABI version supported by kernel
                se.proto_major = v.major();
                se.proto_minor = v.minor();

                let mut config = KernelConfig::new(x.capabilities(), x.max_readahead());
                // Call filesystem init method and give it a chance to return an error
                se.filesystem
                    .init(self, &mut config)
                    .map_err(Errno::from_i32)?;

                // Reply with our desired version and settings. If the kernel supports a
                // larger major version, it'll re-send a matching init message. If it
                // supports only lower major versions, we replied with an error above.
                se.initialized = true;
                return Ok(Some(x.reply(&config)));
            }
            // Any operation is invalid before initialization
            _ if !se.initialized => {
                return Err(Errno::EIO);
            }
            // Filesystem destroyed
            ll::Operation::Destroy(x) => {
                se.filesystem.destroy();
                se.destroyed = true;
                return Ok(Some(x.reply()));
            }
            // Any operation is invalid after destroy
            _ if se.destroyed => {
                return Err(Errno::EIO);
            }

            ll::Operation::Interrupt(_) => {
                // TODO: handle FUSE_INTERRUPT
                return Err(Errno::ENOSYS);
            }

            ll::Operation::Lookup(x) => {
                se.filesystem
                    .lookup(
                        self,
                        self.request.nodeid().into(),
                        x.name().as_ref(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::Forget(x) => {
                se.filesystem
                    .forget(self, self.request.nodeid().into(), x.nlookup()); // no reply
            }
            ll::Operation::GetAttr(_) => {
                se.filesystem
                    .getattr(self, self.request.nodeid().into(), self.reply())
                    .await;
            }
            ll::Operation::SetAttr(x) => {
                se.filesystem
                    .setattr(
                        self,
                        self.request.nodeid().into(),
                        x.mode(),
                        x.uid(),
                        x.gid(),
                        x.size(),
                        x.atime(),
                        x.mtime(),
                        x.ctime(),
                        x.file_handle().map(|fh| fh.into()),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::ReadLink(_) => {
                se.filesystem
                    .readlink(self, self.request.nodeid().into(), self.reply())
                    .await;
            }
            ll::Operation::MkNod(x) => {
                se.filesystem
                    .mknod(
                        self,
                        self.request.nodeid().into(),
                        x.name().as_ref(),
                        x.mode(),
                        x.umask(),
                        x.rdev(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::MkDir(x) => {
                se.filesystem
                    .mkdir(
                        self,
                        self.request.nodeid().into(),
                        x.name().as_ref(),
                        x.mode(),
                        x.umask(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::Unlink(x) => {
                se.filesystem
                    .unlink(
                        self,
                        self.request.nodeid().into(),
                        x.name().as_ref(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::RmDir(x) => {
                se.filesystem
                    .rmdir(
                        self,
                        self.request.nodeid().into(),
                        x.name().as_ref(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::SymLink(x) => {
                se.filesystem
                    .symlink(
                        self,
                        self.request.nodeid().into(),
                        x.link_name().as_ref(),
                        Path::new(x.target()),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::Rename(x) => {
                se.filesystem
                    .rename(
                        self,
                        self.request.nodeid().into(),
                        x.src().name.as_ref(),
                        x.dest().dir.into(),
                        x.dest().name.as_ref(),
                        0,
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::Link(x) => {
                se.filesystem
                    .link(
                        self,
                        x.inode_no().into(),
                        self.request.nodeid().into(),
                        x.dest().name.as_ref(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::Open(x) => {
                se.filesystem
                    .open(self, self.request.nodeid().into(), x.flags(), self.reply())
                    .await;
            }
            ll::Operation::Read(x) => {
                se.filesystem
                    .read(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.offset(),
                        x.size(),
                        x.flags(),
                        x.lock_owner().map(|l| l.into()),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::Write(x) => {
                se.filesystem
                    .write(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.offset(),
                        x.data(),
                        x.write_flags(),
                        x.flags(),
                        x.lock_owner().map(|l| l.into()),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::Flush(x) => {
                se.filesystem
                    .flush(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.lock_owner().into(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::Release(x) => {
                se.filesystem
                    .release(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.flags(),
                        x.lock_owner().map(|x| x.into()),
                        x.flush(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::FSync(x) => {
                se.filesystem
                    .fsync(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.fdatasync(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::OpenDir(x) => {
                se.filesystem
                    .opendir(self, self.request.nodeid().into(), x.flags(), self.reply())
                    .await;
            }
            ll::Operation::ReadDir(x) => {
                se.filesystem
                    .readdir(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.offset(),
                        ReplyDirectory::new(
                            self.request.unique().into(),
                            self.ch.clone(),
                            x.size() as usize,
                        ),
                    )
                    .await;
            }
            ll::Operation::ReleaseDir(x) => {
                se.filesystem
                    .releasedir(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.flags(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::FSyncDir(x) => {
                se.filesystem
                    .fsyncdir(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.fdatasync(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::StatFs(_) => {
                se.filesystem
                    .statfs(self, self.request.nodeid().into(), self.reply())
                    .await;
            }
            ll::Operation::SetXAttr(x) => {
                se.filesystem
                    .setxattr(
                        self,
                        self.request.nodeid().into(),
                        x.name(),
                        x.value(),
                        x.flags(),
                        x.position(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::GetXAttr(x) => {
                se.filesystem
                    .getxattr(
                        self,
                        self.request.nodeid().into(),
                        x.name(),
                        x.size_u32(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::ListXAttr(x) => {
                se.filesystem
                    .listxattr(self, self.request.nodeid().into(), x.size(), self.reply())
                    .await;
            }
            ll::Operation::RemoveXAttr(x) => {
                se.filesystem
                    .removexattr(self, self.request.nodeid().into(), x.name(), self.reply())
                    .await;
            }
            ll::Operation::Access(x) => {
                se.filesystem
                    .access(self, self.request.nodeid().into(), x.mask(), self.reply())
                    .await;
            }
            ll::Operation::Create(x) => {
                se.filesystem
                    .create(
                        self,
                        self.request.nodeid().into(),
                        x.name().as_ref(),
                        x.mode(),
                        x.umask(),
                        x.flags(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::GetLk(x) => {
                se.filesystem
                    .getlk(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.lock_owner().into(),
                        x.lock().range.0,
                        x.lock().range.1,
                        x.lock().typ,
                        x.lock().pid,
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::SetLk(x) => {
                se.filesystem
                    .setlk(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.lock_owner().into(),
                        x.lock().range.0,
                        x.lock().range.1,
                        x.lock().typ,
                        x.lock().pid,
                        false,
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::SetLkW(x) => {
                se.filesystem
                    .setlk(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.lock_owner().into(),
                        x.lock().range.0,
                        x.lock().range.1,
                        x.lock().typ,
                        x.lock().pid,
                        true,
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::BMap(x) => {
                se.filesystem
                    .bmap(
                        self,
                        self.request.nodeid().into(),
                        x.block_size(),
                        x.block(),
                        self.reply(),
                    )
                    .await;
            }

            ll::Operation::IoCtl(x) => {
                if x.unrestricted() {
                    return Err(Errno::ENOSYS);
                } else {
                    se.filesystem
                        .ioctl(
                            self,
                            self.request.nodeid().into(),
                            x.file_handle().into(),
                            x.flags(),
                            x.command(),
                            x.in_data(),
                            x.out_size(),
                            self.reply(),
                        )
                        .await;
                }
            }
            ll::Operation::Poll(x) => {
                se.filesystem
                    .poll(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.kernel_handle(),
                        x.events(),
                        x.flags(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::BatchForget(x) => {
                se.filesystem.batch_forget(self, x.nodes()); // no reply
            }
            ll::Operation::FAllocate(x) => {
                se.filesystem
                    .fallocate(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.offset(),
                        x.len(),
                        x.mode(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::ReadDirPlus(x) => {
                se.filesystem
                    .readdirplus(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.offset(),
                        ReplyDirectoryPlus::new(
                            self.request.unique().into(),
                            self.ch.clone(),
                            x.size() as usize,
                        ),
                    )
                    .await;
            }
            ll::Operation::Rename2(x) => {
                se.filesystem
                    .rename(
                        self,
                        x.from().dir.into(),
                        x.from().name.as_ref(),
                        x.to().dir.into(),
                        x.to().name.as_ref(),
                        x.flags(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::Lseek(x) => {
                se.filesystem
                    .lseek(
                        self,
                        self.request.nodeid().into(),
                        x.file_handle().into(),
                        x.offset(),
                        x.whence(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::CopyFileRange(x) => {
                let (i, o) = (x.src(), x.dest());
                se.filesystem
                    .copy_file_range(
                        self,
                        i.inode.into(),
                        i.file_handle.into(),
                        i.offset,
                        o.inode.into(),
                        o.file_handle.into(),
                        o.offset,
                        x.len(),
                        x.flags().try_into().unwrap(),
                        self.reply(),
                    )
                    .await;
            }
            ll::Operation::CuseInit(_) => {
                // TODO: handle CUSE_INIT
                return Err(Errno::ENOSYS);
            }
        }
        Ok(None)
    }

    /// Create a reply object for this request that can be passed to the filesystem
    /// implementation and makes sure that a request is replied exactly once
    fn reply<T: Reply>(&self) -> T {
        Reply::new(self.request.unique().into(), self.ch.clone())
    }

    /// Returns the unique identifier of this request
    #[inline]
    pub fn unique(&self) -> u64 {
        self.request.unique().into()
    }

    /// Returns the uid of this request
    #[inline]
    pub fn uid(&self) -> u32 {
        self.request.uid()
    }

    /// Returns the gid of this request
    #[inline]
    pub fn gid(&self) -> u32 {
        self.request.gid()
    }

    /// Returns the pid of this request
    #[inline]
    pub fn pid(&self) -> u32 {
        self.request.pid()
    }
}
