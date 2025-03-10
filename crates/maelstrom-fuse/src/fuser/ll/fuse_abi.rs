//! FUSE kernel interface.
//!
//! Types and definitions used for communication between the kernel driver and the userspace
//! part of a FUSE filesystem. Since the kernel driver may be installed independently, the ABI
//! interface is versioned and capabilities are exchanged during the initialization (mounting)
//! of a filesystem.
//!
//! libfuse (Linux/BSD): <https://github.com/libfuse/libfuse/blob/master/include/fuse_kernel.h>
//! - supports ABI 7.8 since FUSE 2.6.0
//! - supports ABI 7.12 since FUSE 2.8.0
//! - supports ABI 7.18 since FUSE 2.9.0
//! - supports ABI 7.19 since FUSE 2.9.1
//! - supports ABI 7.26 since FUSE 3.0.0
//!

use crate::fuser::consts::{FATTR_ATIME_NOW, FATTR_MTIME_NOW};
use std::convert::TryFrom;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub const FUSE_KERNEL_VERSION: u32 = 7;

pub const FUSE_KERNEL_MINOR_VERSION: u32 = 31;

pub const FUSE_ROOT_ID: u64 = 1;

#[repr(C)]
#[derive(Debug, Clone, Copy, Immutable, IntoBytes, FromBytes)]
pub struct fuse_attr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is treated as signed
    // to match stat.st_atime
    pub atime: i64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is treated as signed
    // to match stat.st_mtime
    pub mtime: i64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is treated as signed
    // to match stat.st_ctime
    pub ctime: i64,
    pub atimensec: u32,
    pub mtimensec: u32,
    pub ctimensec: u32,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_kstatfs {
    pub blocks: u64,  // Total blocks (in units of frsize)
    pub bfree: u64,   // Free blocks
    pub bavail: u64,  // Free blocks for unprivileged users
    pub files: u64,   // Total inodes
    pub ffree: u64,   // Free inodes
    pub bsize: u32,   // Filesystem block size
    pub namelen: u32, // Maximum filename length
    pub frsize: u32,  // Fundamental file system block size
    pub padding: u32,
    pub spare: [u32; 6],
}

#[repr(C)]
#[derive(Debug, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct fuse_file_lock {
    pub start: u64,
    pub end: u64,
    // NOTE: this field is defined as u32 in fuse_kernel.h in libfuse. However, it is treated as signed
    pub typ: i32,
    pub pid: u32,
}

pub mod consts {
    // Bitmasks for fuse_setattr_in.valid
    pub const FATTR_MODE: u32 = 1 << 0;
    pub const FATTR_UID: u32 = 1 << 1;
    pub const FATTR_GID: u32 = 1 << 2;
    pub const FATTR_SIZE: u32 = 1 << 3;
    pub const FATTR_ATIME: u32 = 1 << 4;
    pub const FATTR_MTIME: u32 = 1 << 5;
    pub const FATTR_FH: u32 = 1 << 6;
    pub const FATTR_ATIME_NOW: u32 = 1 << 7;
    pub const FATTR_MTIME_NOW: u32 = 1 << 8;
    pub const FATTR_LOCKOWNER: u32 = 1 << 9;
    pub const FATTR_CTIME: u32 = 1 << 10;

    // Flags returned by the open request
    pub const FOPEN_DIRECT_IO: u32 = 1 << 0; // bypass page cache for this open file
    pub const FOPEN_KEEP_CACHE: u32 = 1 << 1; // don't invalidate the data cache on open
    pub const FOPEN_NONSEEKABLE: u32 = 1 << 2; // the file is not seekable
    pub const FOPEN_CACHE_DIR: u32 = 1 << 3; // allow caching this directory
    pub const FOPEN_STREAM: u32 = 1 << 4; // the file is stream-like (no file position at all)

    // Init request/reply flags
    pub const FUSE_ASYNC_READ: u32 = 1 << 0; // asynchronous read requests
    pub const FUSE_POSIX_LOCKS: u32 = 1 << 1; // remote locking for POSIX file locks
    pub const FUSE_FILE_OPS: u32 = 1 << 2; // kernel sends file handle for fstat, etc...
    pub const FUSE_ATOMIC_O_TRUNC: u32 = 1 << 3; // handles the O_TRUNC open flag in the filesystem
    pub const FUSE_EXPORT_SUPPORT: u32 = 1 << 4; // filesystem handles lookups of "." and ".."
    pub const FUSE_BIG_WRITES: u32 = 1 << 5; // filesystem can handle write size larger than 4kB
    pub const FUSE_DONT_MASK: u32 = 1 << 6; // don't apply umask to file mode on create operations
    pub const FUSE_SPLICE_WRITE: u32 = 1 << 7; // kernel supports splice write on the device
    pub const FUSE_SPLICE_MOVE: u32 = 1 << 8; // kernel supports splice move on the device
    pub const FUSE_SPLICE_READ: u32 = 1 << 9; // kernel supports splice read on the device
    pub const FUSE_FLOCK_LOCKS: u32 = 1 << 10; // remote locking for BSD style file locks
    pub const FUSE_HAS_IOCTL_DIR: u32 = 1 << 11; // kernel supports ioctl on directories
    pub const FUSE_AUTO_INVAL_DATA: u32 = 1 << 12; // automatically invalidate cached pages
    pub const FUSE_DO_READDIRPLUS: u32 = 1 << 13; // do READDIRPLUS (READDIR+LOOKUP in one)
    pub const FUSE_READDIRPLUS_AUTO: u32 = 1 << 14; // adaptive readdirplus
    pub const FUSE_ASYNC_DIO: u32 = 1 << 15; // asynchronous direct I/O submission
    pub const FUSE_WRITEBACK_CACHE: u32 = 1 << 16; // use writeback cache for buffered writes
    pub const FUSE_NO_OPEN_SUPPORT: u32 = 1 << 17; // kernel supports zero-message opens
    pub const FUSE_PARALLEL_DIROPS: u32 = 1 << 18; // allow parallel lookups and readdir
    pub const FUSE_HANDLE_KILLPRIV: u32 = 1 << 19; // fs handles killing suid/sgid/cap on write/chown/trunc
    pub const FUSE_POSIX_ACL: u32 = 1 << 20; // filesystem supports posix acls
    pub const FUSE_ABORT_ERROR: u32 = 1 << 21; // reading the device after abort returns ECONNABORTED
    pub const FUSE_MAX_PAGES: u32 = 1 << 22; // init_out.max_pages contains the max number of req pages
    pub const FUSE_CACHE_SYMLINKS: u32 = 1 << 23; // cache READLINK responses
    pub const FUSE_NO_OPENDIR_SUPPORT: u32 = 1 << 24; // kernel supports zero-message opendir
    pub const FUSE_EXPLICIT_INVAL_DATA: u32 = 1 << 25; // only invalidate cached pages on explicit request

    // CUSE init request/reply flags
    pub const CUSE_UNRESTRICTED_IOCTL: u32 = 1 << 0; // use unrestricted ioctl

    // Release flags
    pub const FUSE_RELEASE_FLUSH: u32 = 1 << 0;
    pub const FUSE_RELEASE_FLOCK_UNLOCK: u32 = 1 << 1;

    // Getattr flags
    pub const FUSE_GETATTR_FH: u32 = 1 << 0;

    // Lock flags
    pub const FUSE_LK_FLOCK: u32 = 1 << 0;

    // Write flags
    pub const FUSE_WRITE_CACHE: u32 = 1 << 0; // delayed write from page cache, file handle is guessed
    pub const FUSE_WRITE_LOCKOWNER: u32 = 1 << 1; // lock_owner field is valid
    pub const FUSE_WRITE_KILL_PRIV: u32 = 1 << 2; // kill suid and sgid bits

    // Read flags
    pub const FUSE_READ_LOCKOWNER: u32 = 1 << 1;

    // IOCTL flags
    pub const FUSE_IOCTL_COMPAT: u32 = 1 << 0; // 32bit compat ioctl on 64bit machine
    pub const FUSE_IOCTL_UNRESTRICTED: u32 = 1 << 1; // not restricted to well-formed ioctls, retry allowed
    pub const FUSE_IOCTL_RETRY: u32 = 1 << 2; // retry with new iovecs
    pub const FUSE_IOCTL_32BIT: u32 = 1 << 3; // 32bit ioctl
    pub const FUSE_IOCTL_DIR: u32 = 1 << 4; // is a directory
    pub const FUSE_IOCTL_COMPAT_X32: u32 = 1 << 5; // x32 compat ioctl on 64bit machine (64bit time_t)
    pub const FUSE_IOCTL_MAX_IOV: u32 = 256; // maximum of in_iovecs + out_iovecs

    // Poll flags
    pub const FUSE_POLL_SCHEDULE_NOTIFY: u32 = 1 << 0; // request poll notify

    // fsync flags
    pub const FUSE_FSYNC_FDATASYNC: u32 = 1 << 0; // Sync data only, not metadata

    // The read buffer is required to be at least 8k, but may be much larger
    pub const FUSE_MIN_READ_BUFFER: usize = 8192;
}

/// Invalid opcode error.
#[derive(Debug)]
pub struct InvalidOpcodeError;

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub enum fuse_opcode {
    FUSE_LOOKUP = 1,
    FUSE_FORGET = 2, // no reply
    FUSE_GETATTR = 3,
    FUSE_SETATTR = 4,
    FUSE_READLINK = 5,
    FUSE_SYMLINK = 6,
    FUSE_MKNOD = 8,
    FUSE_MKDIR = 9,
    FUSE_UNLINK = 10,
    FUSE_RMDIR = 11,
    FUSE_RENAME = 12,
    FUSE_LINK = 13,
    FUSE_OPEN = 14,
    FUSE_READ = 15,
    FUSE_WRITE = 16,
    FUSE_STATFS = 17,
    FUSE_RELEASE = 18,
    FUSE_FSYNC = 20,
    FUSE_SETXATTR = 21,
    FUSE_GETXATTR = 22,
    FUSE_LISTXATTR = 23,
    FUSE_REMOVEXATTR = 24,
    FUSE_FLUSH = 25,
    FUSE_INIT = 26,
    FUSE_OPENDIR = 27,
    FUSE_READDIR = 28,
    FUSE_RELEASEDIR = 29,
    FUSE_FSYNCDIR = 30,
    FUSE_GETLK = 31,
    FUSE_SETLK = 32,
    FUSE_SETLKW = 33,
    FUSE_ACCESS = 34,
    FUSE_CREATE = 35,
    FUSE_INTERRUPT = 36,
    FUSE_BMAP = 37,
    FUSE_DESTROY = 38,
    FUSE_IOCTL = 39,
    FUSE_POLL = 40,
    FUSE_BATCH_FORGET = 42,
    FUSE_FALLOCATE = 43,
    FUSE_READDIRPLUS = 44,
    FUSE_RENAME2 = 45,
    FUSE_LSEEK = 46,
    FUSE_COPY_FILE_RANGE = 47,

    CUSE_INIT = 4096,
}

impl TryFrom<u32> for fuse_opcode {
    type Error = InvalidOpcodeError;

    fn try_from(n: u32) -> Result<Self, Self::Error> {
        match n {
            1 => Ok(fuse_opcode::FUSE_LOOKUP),
            2 => Ok(fuse_opcode::FUSE_FORGET),
            3 => Ok(fuse_opcode::FUSE_GETATTR),
            4 => Ok(fuse_opcode::FUSE_SETATTR),
            5 => Ok(fuse_opcode::FUSE_READLINK),
            6 => Ok(fuse_opcode::FUSE_SYMLINK),
            8 => Ok(fuse_opcode::FUSE_MKNOD),
            9 => Ok(fuse_opcode::FUSE_MKDIR),
            10 => Ok(fuse_opcode::FUSE_UNLINK),
            11 => Ok(fuse_opcode::FUSE_RMDIR),
            12 => Ok(fuse_opcode::FUSE_RENAME),
            13 => Ok(fuse_opcode::FUSE_LINK),
            14 => Ok(fuse_opcode::FUSE_OPEN),
            15 => Ok(fuse_opcode::FUSE_READ),
            16 => Ok(fuse_opcode::FUSE_WRITE),
            17 => Ok(fuse_opcode::FUSE_STATFS),
            18 => Ok(fuse_opcode::FUSE_RELEASE),
            20 => Ok(fuse_opcode::FUSE_FSYNC),
            21 => Ok(fuse_opcode::FUSE_SETXATTR),
            22 => Ok(fuse_opcode::FUSE_GETXATTR),
            23 => Ok(fuse_opcode::FUSE_LISTXATTR),
            24 => Ok(fuse_opcode::FUSE_REMOVEXATTR),
            25 => Ok(fuse_opcode::FUSE_FLUSH),
            26 => Ok(fuse_opcode::FUSE_INIT),
            27 => Ok(fuse_opcode::FUSE_OPENDIR),
            28 => Ok(fuse_opcode::FUSE_READDIR),
            29 => Ok(fuse_opcode::FUSE_RELEASEDIR),
            30 => Ok(fuse_opcode::FUSE_FSYNCDIR),
            31 => Ok(fuse_opcode::FUSE_GETLK),
            32 => Ok(fuse_opcode::FUSE_SETLK),
            33 => Ok(fuse_opcode::FUSE_SETLKW),
            34 => Ok(fuse_opcode::FUSE_ACCESS),
            35 => Ok(fuse_opcode::FUSE_CREATE),
            36 => Ok(fuse_opcode::FUSE_INTERRUPT),
            37 => Ok(fuse_opcode::FUSE_BMAP),
            38 => Ok(fuse_opcode::FUSE_DESTROY),
            39 => Ok(fuse_opcode::FUSE_IOCTL),
            40 => Ok(fuse_opcode::FUSE_POLL),
            42 => Ok(fuse_opcode::FUSE_BATCH_FORGET),
            43 => Ok(fuse_opcode::FUSE_FALLOCATE),
            44 => Ok(fuse_opcode::FUSE_READDIRPLUS),
            45 => Ok(fuse_opcode::FUSE_RENAME2),
            46 => Ok(fuse_opcode::FUSE_LSEEK),
            47 => Ok(fuse_opcode::FUSE_COPY_FILE_RANGE),

            4096 => Ok(fuse_opcode::CUSE_INIT),

            _ => Err(InvalidOpcodeError),
        }
    }
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_entry_out {
    pub nodeid: u64,
    pub generation: u64,
    pub entry_valid: u64,
    pub attr_valid: u64,
    pub entry_valid_nsec: u32,
    pub attr_valid_nsec: u32,
    pub attr: fuse_attr,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_forget_in {
    pub nlookup: u64,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable)]
pub struct fuse_forget_one {
    pub nodeid: u64,
    pub nlookup: u64,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_batch_forget_in {
    pub count: u32,
    pub dummy: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, IntoBytes)]
pub struct fuse_attr_out {
    pub attr_valid: u64,
    pub attr_valid_nsec: u32,
    pub dummy: u32,
    pub attr: fuse_attr,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_mknod_in {
    pub mode: u32,
    pub rdev: u32,
    pub umask: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_mkdir_in {
    pub mode: u32,
    pub padding: u32,
    pub umask: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_rename_in {
    pub newdir: u64,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_rename2_in {
    pub newdir: u64,
    pub flags: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_link_in {
    pub oldnodeid: u64,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_setattr_in {
    pub valid: u32,
    pub padding: u32,
    pub fh: u64,
    pub size: u64,
    pub lock_owner: u64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is treated as signed
    // to match stat.st_atime
    pub atime: i64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is treated as signed
    // to match stat.st_mtime
    pub mtime: i64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is treated as signed
    // to match stat.st_ctime
    pub ctime: i64,
    pub atimensec: u32,
    pub mtimensec: u32,
    pub ctimensec: u32,
    pub mode: u32,
    pub unused4: u32,
    pub uid: u32,
    pub gid: u32,
    pub unused5: u32,
}

impl fuse_setattr_in {
    pub fn atime_now(&self) -> bool {
        self.valid & FATTR_ATIME_NOW != 0
    }

    pub fn mtime_now(&self) -> bool {
        self.valid & FATTR_MTIME_NOW != 0
    }
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_open_in {
    // NOTE: this field is defined as u32 in fuse_kernel.h in libfuse. However, it is then cast
    // to an i32 when invoking the filesystem's open method and this matches the open() syscall
    pub flags: i32,
    pub unused: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_create_in {
    // NOTE: this field is defined as u32 in fuse_kernel.h in libfuse. However, it is then cast
    // to an i32 when invoking the filesystem's create method and this matches the open() syscall
    pub flags: i32,
    pub mode: u32,
    pub umask: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_create_out(pub fuse_entry_out, pub fuse_open_out);

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_open_out {
    pub fh: u64,
    pub open_flags: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_release_in {
    pub fh: u64,
    // NOTE: this field is defined as u32 in fuse_kernel.h in libfuse. However, it is then cast
    // to an i32 when invoking the filesystem's read method
    pub flags: i32,
    pub release_flags: u32,
    pub lock_owner: u64,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_flush_in {
    pub fh: u64,
    pub unused: u32,
    pub padding: u32,
    pub lock_owner: u64,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_read_in {
    pub fh: u64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is then cast
    // to an i64 when invoking the filesystem's read method
    pub offset: i64,
    pub size: u32,
    pub read_flags: u32,
    pub lock_owner: u64,
    // NOTE: this field is defined as u32 in fuse_kernel.h in libfuse. However, it is then cast
    // to an i32 when invoking the filesystem's read method
    pub flags: i32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_write_in {
    pub fh: u64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is then cast
    // to an i64 when invoking the filesystem's write method
    pub offset: i64,
    pub size: u32,
    pub write_flags: u32,
    pub lock_owner: u64,
    // NOTE: this field is defined as u32 in fuse_kernel.h in libfuse. However, it is then cast
    // to an i32 when invoking the filesystem's read method
    pub flags: i32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_write_out {
    pub size: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_statfs_out {
    pub st: fuse_kstatfs,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_fsync_in {
    pub fh: u64,
    pub fsync_flags: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_setxattr_in {
    pub size: u32,
    // NOTE: this field is defined as u32 in fuse_kernel.h in libfuse. However, it is then cast
    // to an i32 when invoking the filesystem's setxattr method
    pub flags: i32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_getxattr_in {
    pub size: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_getxattr_out {
    pub size: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_lk_in {
    pub fh: u64,
    pub owner: u64,
    pub lk: fuse_file_lock,
    pub lk_flags: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_lk_out {
    pub lk: fuse_file_lock,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_access_in {
    // NOTE: this field is defined as u32 in fuse_kernel.h in libfuse. However, it is then cast
    // to an i32 when invoking the filesystem's access method
    pub mask: i32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_init_in {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_init_out {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub max_write: u32,
    pub time_gran: u32,
    pub max_pages: u16,
    pub unused2: u16,
    pub reserved: [u32; 8],
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_interrupt_in {
    pub unique: u64,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_bmap_in {
    pub block: u64,
    pub blocksize: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_bmap_out {
    pub block: u64,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_ioctl_in {
    pub fh: u64,
    pub flags: u32,
    pub cmd: u32,
    pub arg: u64, // TODO: this is currently unused, but is defined as a void* in libfuse
    pub in_size: u32,
    pub out_size: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_ioctl_out {
    pub result: i32,
    pub flags: u32,
    pub in_iovs: u32,
    pub out_iovs: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_poll_in {
    pub fh: u64,
    pub kh: u64,
    pub flags: u32,
    pub events: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_poll_out {
    pub revents: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_fallocate_in {
    pub fh: u64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is treated as signed
    pub offset: i64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is treated as signed
    pub length: i64,
    // NOTE: this field is defined as u32 in fuse_kernel.h in libfuse. However, it is treated as signed
    pub mode: i32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_in_header {
    pub len: u32,
    pub opcode: u32,
    pub unique: u64,
    pub nodeid: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_out_header {
    pub len: u32,
    pub error: i32,
    pub unique: u64,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_dirent {
    pub ino: u64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is treated as signed
    pub off: i64,
    pub namelen: u32,
    pub typ: u32,
    // followed by name of namelen bytes
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_direntplus {
    pub entry_out: fuse_entry_out,
    pub dirent: fuse_dirent,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_lseek_in {
    pub fh: u64,
    pub offset: i64,
    // NOTE: this field is defined as u32 in fuse_kernel.h in libfuse. However, it is treated as signed
    pub whence: i32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Immutable, IntoBytes)]
pub struct fuse_lseek_out {
    pub offset: i64,
}

#[repr(C)]
#[derive(Debug, FromBytes, Immutable, KnownLayout)]
pub struct fuse_copy_file_range_in {
    pub fh_in: u64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is treated as signed
    pub off_in: i64,
    pub nodeid_out: u64,
    pub fh_out: u64,
    // NOTE: this field is defined as u64 in fuse_kernel.h in libfuse. However, it is treated as signed
    pub off_out: i64,
    pub len: u64,
    pub flags: u64,
}
