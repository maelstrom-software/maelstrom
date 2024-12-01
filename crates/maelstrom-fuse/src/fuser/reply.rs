//! Filesystem operation reply
//!
//! A reply is passed to filesystem operation implementations and must be used to send back the
//! result of an operation. The reply can optionally be sent to another thread to asynchronously
//! work on an operation and provide the result later. Also it allows replying with a block of
//! data without cloning the data. A reply *must always* be used (by calling either ok() or
//! error() exactly once).

use crate::fuser::ll::fuse_abi as abi;
use crate::fuser::ll::{
    self,
    reply::{DirEntPlusList, DirEntryPlus},
    Generation,
};
use crate::fuser::ll::{
    reply::{DirEntList, DirEntOffset, DirEntry},
    INodeNo,
};
use crate::fuser::{FileAttr, FileType};
use async_trait::async_trait;
use derive_more::Debug;
use libc::c_int;
use maelstrom_linux::Fd;
use std::convert::AsRef;
use std::ffi::OsStr;
use std::io::IoSlice;
use std::time::Duration;

/// Generic reply callback to send data
#[async_trait]
pub trait ReplySender: Send + Sync + Unpin + 'static {
    /// Send data.
    async fn send(&self, data: &[IoSlice<'_>]) -> std::io::Result<()>;

    /// Send data using splice
    async fn send_splice(
        &self,
        fd: Fd,
        header: abi::fuse_out_header,
        offset: u64,
        length: usize,
    ) -> std::io::Result<()>;
}

/// Generic reply trait
pub trait Reply {
    /// Create a new reply for the given request
    fn new<S: ReplySender>(unique: u64, sender: S) -> Self;
}

///
/// Raw reply
///
#[derive(Debug)]
pub(crate) struct ReplyRaw {
    /// Unique id of the request to reply to
    unique: ll::RequestId,
    /// Closure to call for sending the reply
    #[debug(skip)]
    sender: Option<Box<dyn ReplySender>>,
}

impl Reply for ReplyRaw {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyRaw {
        let sender = Box::new(sender);
        ReplyRaw {
            unique: ll::RequestId(unique),
            sender: Some(sender),
        }
    }
}

impl ReplyRaw {
    /// Reply to a request with the given error code and data. Must be called
    /// only once (the `ok` and `error` methods ensure this by consuming `self`)
    async fn send_ll_mut(&mut self, response: &ll::Response<'_>) {
        assert!(self.sender.is_some());
        response
            .send(self.unique, &*self.sender.take().unwrap())
            .await
            .ok();
    }
    async fn send_ll(mut self, response: &ll::Response<'_>) {
        self.send_ll_mut(response).await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        assert_ne!(err, 0);
        self.send_ll(&ll::Response::new_error(ll::Errno::from_i32(err)))
            .await;
    }
}

impl Drop for ReplyRaw {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let unique = self.unique;

            tokio::task::spawn(async move {
                let response = ll::Response::new_error(ll::Errno::EIO);
                response.send(unique, &*sender).await.ok();
            });
        }
    }
}

///
/// Empty reply
///
#[derive(Debug)]
pub struct ReplyEmpty {
    reply: ReplyRaw,
}

impl Reply for ReplyEmpty {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyEmpty {
        ReplyEmpty {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyEmpty {
    /// Reply to a request with nothing
    pub async fn ok(self) {
        self.reply.send_ll(&ll::Response::new_empty()).await;
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Data reply
///
#[derive(Debug)]
pub struct ReplyData {
    reply: ReplyRaw,
}

impl Reply for ReplyData {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyData {
        ReplyData {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyData {
    /// Reply to a request with the given data
    pub async fn data(self, data: &[u8]) {
        self.reply.send_ll(&ll::Response::new_slice(data)).await;
    }

    /// Reply to a request by splicing data from the given fd
    pub async fn data_splice(self, fd: Fd, offset: u64, length: usize) {
        self.reply
            .send_ll(&ll::Response::new_splice(fd, offset, length))
            .await;
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Entry reply
///
#[derive(Debug)]
pub struct ReplyEntry {
    reply: ReplyRaw,
}

impl Reply for ReplyEntry {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyEntry {
        ReplyEntry {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyEntry {
    /// Reply to a request with the given entry
    pub async fn entry(self, ttl: &Duration, attr: &FileAttr, generation: u64) {
        self.reply
            .send_ll(&ll::Response::new_entry(
                ll::INodeNo(attr.ino),
                ll::Generation(generation),
                &attr.into(),
                *ttl,
                *ttl,
            ))
            .await;
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Attribute Reply
///
#[derive(Debug)]
pub struct ReplyAttr {
    reply: ReplyRaw,
}

impl Reply for ReplyAttr {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyAttr {
        ReplyAttr {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyAttr {
    /// Reply to a request with the given attribute
    pub async fn attr(self, ttl: &Duration, attr: &FileAttr) {
        self.reply
            .send_ll(&ll::Response::new_attr(ttl, &attr.into()))
            .await;
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Open Reply
///
#[derive(Debug)]
pub struct ReplyOpen {
    reply: ReplyRaw,
}

impl Reply for ReplyOpen {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyOpen {
        ReplyOpen {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyOpen {
    /// Reply to a request with the given open result
    pub async fn opened(self, fh: u64, flags: u32) {
        self.reply
            .send_ll(&ll::Response::new_open(ll::FileHandle(fh), flags))
            .await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Write Reply
///
#[derive(Debug)]
pub struct ReplyWrite {
    reply: ReplyRaw,
}

impl Reply for ReplyWrite {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyWrite {
        ReplyWrite {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyWrite {
    /// Reply to a request with the given open result
    pub async fn written(self, size: u32) {
        self.reply.send_ll(&ll::Response::new_write(size)).await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Statfs Reply
///
#[derive(Debug)]
pub struct ReplyStatfs {
    reply: ReplyRaw,
}

impl Reply for ReplyStatfs {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyStatfs {
        ReplyStatfs {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyStatfs {
    /// Reply to a request with the given open result
    #[allow(clippy::too_many_arguments)]
    pub async fn statfs(
        self,
        blocks: u64,
        bfree: u64,
        bavail: u64,
        files: u64,
        ffree: u64,
        bsize: u32,
        namelen: u32,
        frsize: u32,
    ) {
        self.reply
            .send_ll(&ll::Response::new_statfs(
                blocks, bfree, bavail, files, ffree, bsize, namelen, frsize,
            ))
            .await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Create reply
///
#[derive(Debug)]
pub struct ReplyCreate {
    reply: ReplyRaw,
}

impl Reply for ReplyCreate {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyCreate {
        ReplyCreate {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyCreate {
    /// Reply to a request with the given entry
    pub async fn created(
        self,
        ttl: &Duration,
        attr: &FileAttr,
        generation: u64,
        fh: u64,
        flags: u32,
    ) {
        self.reply
            .send_ll(&ll::Response::new_create(
                ttl,
                &attr.into(),
                ll::Generation(generation),
                ll::FileHandle(fh),
                flags,
            ))
            .await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Lock Reply
///
#[derive(Debug)]
pub struct ReplyLock {
    reply: ReplyRaw,
}

impl Reply for ReplyLock {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyLock {
        ReplyLock {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyLock {
    /// Reply to a request with the given open result
    pub async fn locked(self, start: u64, end: u64, typ: i32, pid: u32) {
        self.reply
            .send_ll(&ll::Response::new_lock(&ll::Lock {
                range: (start, end),
                typ,
                pid,
            }))
            .await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Bmap Reply
///
#[derive(Debug)]
pub struct ReplyBmap {
    reply: ReplyRaw,
}

impl Reply for ReplyBmap {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyBmap {
        ReplyBmap {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyBmap {
    /// Reply to a request with the given open result
    pub async fn bmap(self, block: u64) {
        self.reply.send_ll(&ll::Response::new_bmap(block)).await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Ioctl Reply
///
#[derive(Debug)]
pub struct ReplyIoctl {
    reply: ReplyRaw,
}

impl Reply for ReplyIoctl {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyIoctl {
        ReplyIoctl {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyIoctl {
    /// Reply to a request with the given open result
    pub async fn ioctl(self, result: i32, data: &[u8]) {
        self.reply
            .send_ll(&ll::Response::new_ioctl(result, &[IoSlice::new(data)]))
            .await;
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Poll Reply
///
#[derive(Debug)]
pub struct ReplyPoll {
    reply: ReplyRaw,
}

impl Reply for ReplyPoll {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyPoll {
        ReplyPoll {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyPoll {
    /// Reply to a request with the given poll result
    pub async fn poll(self, revents: u32) {
        self.reply.send_ll(&ll::Response::new_poll(revents)).await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Directory reply
///
#[derive(Debug)]
pub struct ReplyDirectory {
    reply: ReplyRaw,
    data: DirEntList,
}

impl ReplyDirectory {
    /// Creates a new ReplyDirectory with a specified buffer size.
    pub fn new<S: ReplySender>(unique: u64, sender: S, size: usize) -> ReplyDirectory {
        ReplyDirectory {
            reply: Reply::new(unique, sender),
            data: DirEntList::new(size),
        }
    }

    /// Add an entry to the directory reply buffer. Returns true if the buffer is full.
    /// A transparent offset value can be provided for each entry. The kernel uses these
    /// value to request the next entries in further readdir calls
    #[must_use]
    pub fn add<T: AsRef<OsStr>>(&mut self, ino: u64, offset: i64, kind: FileType, name: T) -> bool {
        let name = name.as_ref();
        self.data.push(&DirEntry::new(
            INodeNo(ino),
            DirEntOffset(offset),
            kind,
            name,
        ))
    }

    /// Reply to a request with the filled directory buffer
    pub async fn ok(self) {
        self.reply.send_ll(&self.data.into()).await;
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// DirectoryPlus reply
///
#[derive(Debug)]
pub struct ReplyDirectoryPlus {
    reply: ReplyRaw,
    buf: DirEntPlusList,
}

impl ReplyDirectoryPlus {
    /// Creates a new ReplyDirectory with a specified buffer size.
    pub fn new<S: ReplySender>(unique: u64, sender: S, size: usize) -> ReplyDirectoryPlus {
        ReplyDirectoryPlus {
            reply: Reply::new(unique, sender),
            buf: DirEntPlusList::new(size),
        }
    }

    /// Add an entry to the directory reply buffer. Returns true if the buffer is full.
    /// A transparent offset value can be provided for each entry. The kernel uses these
    /// value to request the next entries in further readdir calls
    pub fn add<T: AsRef<OsStr>>(
        &mut self,
        ino: u64,
        offset: i64,
        name: T,
        ttl: &Duration,
        attr: &FileAttr,
        generation: u64,
    ) -> bool {
        let name = name.as_ref();
        self.buf.push(&DirEntryPlus::new(
            INodeNo(ino),
            Generation(generation),
            DirEntOffset(offset),
            name,
            *ttl,
            attr.into(),
            *ttl,
        ))
    }

    /// Reply to a request with the filled directory buffer
    pub async fn ok(self) {
        self.reply.send_ll(&self.buf.into()).await;
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Xattr reply
///
#[derive(Debug)]
pub struct ReplyXattr {
    reply: ReplyRaw,
}

impl Reply for ReplyXattr {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyXattr {
        ReplyXattr {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyXattr {
    /// Reply to a request with the size of the xattr.
    pub async fn size(self, size: u32) {
        self.reply
            .send_ll(&ll::Response::new_xattr_size(size))
            .await
    }

    /// Reply to a request with the data in the xattr.
    pub async fn data(self, data: &[u8]) {
        self.reply.send_ll(&ll::Response::new_data(data)).await
    }

    /// Reply to a request with the given error code.
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

///
/// Lseek Reply
///
#[derive(Debug)]
pub struct ReplyLseek {
    reply: ReplyRaw,
}

impl Reply for ReplyLseek {
    fn new<S: ReplySender>(unique: u64, sender: S) -> ReplyLseek {
        ReplyLseek {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyLseek {
    /// Reply to a request with seeked offset
    pub async fn offset(self, offset: i64) {
        self.reply.send_ll(&ll::Response::new_lseek(offset)).await
    }

    /// Reply to a request with the given error code
    pub async fn error(self, err: c_int) {
        self.reply.error(err).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{Debug, *};
    use crate::fuser::{FileAttr, FileType};
    use std::io::IoSlice;
    use std::time::{Duration, UNIX_EPOCH};
    use tokio::sync::mpsc::{channel, Sender};
    use zerocopy::AsBytes;

    #[derive(Debug, AsBytes)]
    #[repr(C)]
    struct Data {
        a: u8,
        b: u8,
        c: u16,
    }

    #[test]
    fn serialize_empty() {
        assert!(().as_bytes().is_empty());
    }

    #[test]
    fn serialize_slice() {
        let data: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
        assert_eq!(data.as_bytes(), [0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn serialize_struct() {
        let data = Data {
            a: 0x12,
            b: 0x34,
            c: 0x5678,
        };
        assert_eq!(data.as_bytes(), [0x12, 0x34, 0x78, 0x56]);
    }

    struct AssertSender {
        expected: Vec<u8>,
    }

    #[async_trait]
    impl super::ReplySender for AssertSender {
        async fn send(&self, data: &[IoSlice<'_>]) -> std::io::Result<()> {
            let mut v = vec![];
            for x in data {
                v.extend_from_slice(x)
            }
            assert_eq!(self.expected, v);
            Ok(())
        }

        async fn send_splice(
            &self,
            _fd: Fd,
            _header: abi::fuse_out_header,
            _offset: u64,
            _length: usize,
        ) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn reply_raw() {
        let data = Data {
            a: 0x12,
            b: 0x34,
            c: 0x5678,
        };
        let sender = AssertSender {
            expected: vec![
                0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x12, 0x34, 0x78, 0x56,
            ],
        };
        let reply: ReplyRaw = Reply::new(0xdeadbeef, sender);
        reply
            .send_ll(&ll::Response::new_data(data.as_bytes()))
            .await;
    }

    #[tokio::test]
    async fn reply_error() {
        let sender = AssertSender {
            expected: vec![
                0x10, 0x00, 0x00, 0x00, 0xbe, 0xff, 0xff, 0xff, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00,
            ],
        };
        let reply: ReplyRaw = Reply::new(0xdeadbeef, sender);
        reply.error(66).await;
    }

    #[tokio::test]
    async fn reply_empty() {
        let sender = AssertSender {
            expected: vec![
                0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00,
            ],
        };
        let reply: ReplyEmpty = Reply::new(0xdeadbeef, sender);
        reply.ok().await;
    }

    #[tokio::test]
    async fn reply_data() {
        let sender = AssertSender {
            expected: vec![
                0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0xde, 0xad, 0xbe, 0xef,
            ],
        };
        let reply: ReplyData = Reply::new(0xdeadbeef, sender);
        reply.data(&[0xde, 0xad, 0xbe, 0xef]).await;
    }

    #[tokio::test]
    async fn reply_entry() {
        let mut expected = vec![
            0x88, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
            0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x87,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
            0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
            0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00, 0x66, 0x00,
            0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
        ];

        expected.extend(vec![0xbb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        expected[0] = (expected.len()) as u8;

        let sender = AssertSender { expected };
        let reply: ReplyEntry = Reply::new(0xdeadbeef, sender);
        let time = UNIX_EPOCH + Duration::new(0x1234, 0x5678);
        let ttl = Duration::new(0x8765, 0x4321);
        let attr = FileAttr {
            ino: 0x11,
            size: 0x22,
            blocks: 0x33,
            atime: time,
            mtime: time,
            ctime: time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 0x55,
            uid: 0x66,
            gid: 0x77,
            rdev: 0x88,
            blksize: 0xbb,
        };
        reply.entry(&ttl, &attr, 0xaa).await;
    }

    #[tokio::test]
    async fn reply_attr() {
        let mut expected = vec![
            0x70, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
            0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
            0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00,
            0x00, 0x00, 0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
        ];

        expected.extend_from_slice(&[0xbb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        expected[0] = expected.len() as u8;

        let sender = AssertSender { expected };
        let reply: ReplyAttr = Reply::new(0xdeadbeef, sender);
        let time = UNIX_EPOCH + Duration::new(0x1234, 0x5678);
        let ttl = Duration::new(0x8765, 0x4321);
        let attr = FileAttr {
            ino: 0x11,
            size: 0x22,
            blocks: 0x33,
            atime: time,
            mtime: time,
            ctime: time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 0x55,
            uid: 0x66,
            gid: 0x77,
            rdev: 0x88,
            blksize: 0xbb,
        };
        reply.attr(&ttl, &attr).await;
    }

    #[tokio::test]
    async fn reply_open() {
        let sender = AssertSender {
            expected: vec![
                0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x22, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
            ],
        };
        let reply: ReplyOpen = Reply::new(0xdeadbeef, sender);
        reply.opened(0x1122, 0x33).await;
    }

    #[tokio::test]
    async fn reply_write() {
        let sender = AssertSender {
            expected: vec![
                0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x22, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ],
        };
        let reply: ReplyWrite = Reply::new(0xdeadbeef, sender);
        reply.written(0x1122).await;
    }

    #[tokio::test]
    async fn reply_statfs() {
        let sender = AssertSender {
            expected: vec![
                0x60, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ],
        };
        let reply: ReplyStatfs = Reply::new(0xdeadbeef, sender);
        reply
            .statfs(0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88)
            .await;
    }

    #[tokio::test]
    async fn reply_create() {
        let mut expected = vec![
            0x98, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
            0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x87,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
            0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
            0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00, 0x66, 0x00,
            0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00, 0xbb, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0xcc, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let insert_at = expected.len() - 16;
        expected.splice(
            insert_at..insert_at,
            vec![0xdd, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        );
        expected[0] = (expected.len()) as u8;

        let sender = AssertSender { expected };
        let reply: ReplyCreate = Reply::new(0xdeadbeef, sender);
        let time = UNIX_EPOCH + Duration::new(0x1234, 0x5678);
        let ttl = Duration::new(0x8765, 0x4321);
        let attr = FileAttr {
            ino: 0x11,
            size: 0x22,
            blocks: 0x33,
            atime: time,
            mtime: time,
            ctime: time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 0x55,
            uid: 0x66,
            gid: 0x77,
            rdev: 0x88,
            blksize: 0xdd,
        };
        reply.created(&ttl, &attr, 0xaa, 0xbb, 0xcc).await;
    }

    #[tokio::test]
    async fn reply_lock() {
        let sender = AssertSender {
            expected: vec![
                0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x44, 0x00, 0x00, 0x00,
            ],
        };
        let reply: ReplyLock = Reply::new(0xdeadbeef, sender);
        reply.locked(0x11, 0x22, 0x33, 0x44).await;
    }

    #[tokio::test]
    async fn reply_bmap() {
        let sender = AssertSender {
            expected: vec![
                0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ],
        };
        let reply: ReplyBmap = Reply::new(0xdeadbeef, sender);
        reply.bmap(0x1234).await;
    }

    #[tokio::test]
    async fn reply_directory() {
        let sender = AssertSender {
            expected: vec![
                0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0xbb, 0xaa, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x68, 0x65,
                0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0xdd, 0xcc, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x08, 0x00,
                0x00, 0x00, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x2e, 0x72, 0x73,
            ],
        };
        let mut reply = ReplyDirectory::new(0xdeadbeef, sender, 4096);
        assert!(!reply.add(0xaabb, 1, FileType::Directory, "hello"));
        assert!(!reply.add(0xccdd, 2, FileType::RegularFile, "world.rs"));
        reply.ok().await;
    }

    #[tokio::test]
    async fn reply_xattr_size() {
        let sender = AssertSender {
            expected: vec![
                0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF, 0xBE, 0xAD, 0xDE, 0x00, 0x00,
                0x00, 0x00, 0x78, 0x56, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00,
            ],
        };
        let reply = ReplyXattr::new(0xdeadbeef, sender);
        reply.size(0x12345678).await;
    }

    #[tokio::test]
    async fn reply_xattr_data() {
        let sender = AssertSender {
            expected: vec![
                0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF, 0xBE, 0xAD, 0xDE, 0x00, 0x00,
                0x00, 0x00, 0x11, 0x22, 0x33, 0x44,
            ],
        };
        let reply = ReplyXattr::new(0xdeadbeef, sender);
        reply.data(&[0x11, 0x22, 0x33, 0x44]).await;
    }

    #[async_trait]
    impl super::ReplySender for Sender<()> {
        async fn send(&self, _: &[IoSlice<'_>]) -> std::io::Result<()> {
            self.send(()).await.unwrap();
            Ok(())
        }

        async fn send_splice(
            &self,
            _fd: Fd,
            _header: abi::fuse_out_header,
            _offset: u64,
            _length: usize,
        ) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn async_reply() {
        let (tx, mut rx) = channel::<()>(1);
        let reply: ReplyEmpty = Reply::new(0xdeadbeef, tx);
        tokio::task::spawn(async move {
            reply.ok().await;
        });
        rx.recv().await.unwrap();
    }
}
