//! Function wrappers for Linux syscalls.
#![no_std]

use core::{
    ffi::{c_int, CStr},
    mem, ptr,
};
use derive_more::{BitOr, From};
use nc::syscalls;

pub type Errno = nc::Errno;

#[derive(Clone, Copy, Default, From)]
pub struct Fd(usize);

impl Fd {
    pub const STDIN: Self = Self(0);
    pub const STDOUT: Self = Self(1);
    pub const STDERR: Self = Self(2);
    pub const FIRST_NON_SPECIAL: Self = Self(3);
    pub const LAST: Self = Self(!0);
}

impl From<c_int> for Fd {
    fn from(fd: c_int) -> Fd {
        Fd(fd as usize)
    }
}

#[repr(C)]
pub struct NetlinkSocketAddr {
    sin_family: nc::sa_family_t,
    nl_pad: u16,
    nl_pid: u32,
    nl_groups: u32,
}

impl Default for NetlinkSocketAddr {
    fn default() -> Self {
        NetlinkSocketAddr {
            sin_family: nc::AF_NETLINK as nc::sa_family_t,
            nl_pad: 0,
            nl_pid: 0, // the kernel
            nl_groups: 0,
        }
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct OpenFlags(usize);

impl OpenFlags {
    pub const WRONLY: Self = Self(nc::O_WRONLY as usize);
    pub const TRUNC: Self = Self(nc::O_TRUNC as usize);
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct FileMode(usize);

impl FileMode {
    pub const SUID: Self = Self(0o4000);
    pub const SGID: Self = Self(0o2000);
    pub const SVTX: Self = Self(0o1000);

    pub const RWXU: Self = Self(0o0700);
    pub const RUSR: Self = Self(0o0400);
    pub const WUSR: Self = Self(0o0200);
    pub const XUSR: Self = Self(0o0100);

    pub const RWXG: Self = Self(0o0070);
    pub const RGRP: Self = Self(0o0040);
    pub const WGRP: Self = Self(0o0020);
    pub const XGRP: Self = Self(0o0010);

    pub const RWXO: Self = Self(0o0007);
    pub const ROTH: Self = Self(0o0004);
    pub const WOTH: Self = Self(0o0002);
    pub const XOTH: Self = Self(0o0001);
}

pub fn open(path: &CStr, flags: OpenFlags, mode: FileMode) -> Result<Fd, Errno> {
    let path = path.to_bytes_with_nul();
    let path_ptr = path.as_ptr();
    unsafe {
        syscalls::syscall4(
            nc::SYS_OPENAT, // Use SYS_OPENAT instead of SYS_OPEN because not all architectures have the latter.
            nc::AT_FDCWD as usize,
            path_ptr as usize,
            flags.0,
            mode.0,
        )
    }
    .map(|fd| fd.into())
}

pub fn dup2(from: Fd, to: Fd) -> Result<Fd, Errno> {
    unsafe {
        // Use SYS_DUP3 instead of SYS_DUP2 because not all architectures have the latter.
        syscalls::syscall3(nc::SYS_DUP3, from.0, to.0, 0)
    }
    .map(|fd| fd.into())
}

#[derive(Clone, Copy)]
pub struct SocketDomain(usize);

impl SocketDomain {
    pub const NETLINK: Self = Self(nc::AF_NETLINK as usize);
}

#[derive(BitOr, Clone, Copy)]
pub struct SocketType(usize);

impl SocketType {
    pub const RAW: Self = Self(nc::SOCK_RAW as usize);
    pub const CLOEXEC: Self = Self(nc::SOCK_CLOEXEC as usize);
}

#[derive(Clone, Copy)]
pub struct SocketProtocol(usize);

impl SocketProtocol {
    pub const NETLINK_ROUTE: Self = Self(0);
}

pub fn socket(
    domain: SocketDomain,
    type_: SocketType,
    protocol: SocketProtocol,
) -> Result<Fd, Errno> {
    unsafe { syscalls::syscall3(nc::SYS_SOCKET, domain.0, type_.0, protocol.0) }.map(|fd| fd.into())
}

pub fn bind_netlink(fd: Fd, sockaddr: &NetlinkSocketAddr) -> Result<(), Errno> {
    let sockaddr_ptr = sockaddr as *const NetlinkSocketAddr;
    let sockaddr_len = mem::size_of::<NetlinkSocketAddr>();
    unsafe { syscalls::syscall3(nc::SYS_BIND, fd.0, sockaddr_ptr as usize, sockaddr_len) }.map(drop)
}

pub fn read(fd: Fd, buf: &mut [u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_mut_ptr();
    let buf_len = buf.len();
    unsafe { syscalls::syscall3(nc::SYS_READ, fd.0, buf_ptr as usize, buf_len) }
}

pub fn write(fd: Fd, buf: &[u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_ptr();
    let buf_len = buf.len();
    unsafe { syscalls::syscall3(nc::SYS_WRITE, fd.0, buf_ptr as usize, buf_len) }
}

#[derive(Clone, Copy, Default)]
pub struct CloseRangeFlags(usize);

impl CloseRangeFlags {
    pub const CLOEXEC: Self = Self(nc::CLOSE_RANGE_CLOEXEC as usize);
}

pub fn close_range(first: Fd, last: Fd, flags: CloseRangeFlags) -> Result<(), Errno> {
    unsafe { syscalls::syscall3(nc::SYS_CLOSE_RANGE, first.0, last.0, flags.0) }.map(drop)
}

pub fn setsid() -> Result<(), Errno> {
    unsafe { syscalls::syscall0(nc::SYS_SETSID) }.map(drop)
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct MountFlags(usize);

impl MountFlags {
    pub const BIND: Self = Self(nc::MS_BIND);
    pub const REMOUNT: Self = Self(nc::MS_REMOUNT);
    pub const RDONLY: Self = Self(nc::MS_RDONLY);
    pub const NOSUID: Self = Self(nc::MS_NOSUID);
    pub const NOEXEC: Self = Self(nc::MS_NOEXEC);
    pub const NODEV: Self = Self(nc::MS_NODEV);
}

pub fn mount(
    source: Option<&CStr>,
    target: &CStr,
    fstype: Option<&CStr>,
    flags: MountFlags,
    data: Option<&[u8]>,
) -> Result<(), Errno> {
    let source_ptr = source
        .map(|r| r.to_bytes_with_nul().as_ptr())
        .unwrap_or(ptr::null());
    let target_ptr = target.to_bytes_with_nul().as_ptr();
    let fstype_ptr = fstype.map(|r| r.as_ptr()).unwrap_or(ptr::null());
    let data_ptr = data.map(|r| r.as_ptr()).unwrap_or(ptr::null());
    unsafe {
        syscalls::syscall5(
            nc::SYS_MOUNT,
            source_ptr as usize,
            target_ptr as usize,
            fstype_ptr as usize,
            flags.0,
            data_ptr as usize,
        )
    }
    .map(drop)
}

pub fn chdir(path: &CStr) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    unsafe { syscalls::syscall1(nc::SYS_CHDIR, path_ptr as usize) }.map(drop)
}

pub fn mkdir(path: &CStr, mode: FileMode) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    unsafe {
        syscalls::syscall3(
            nc::SYS_MKDIRAT, // Use SYS_MKDIRAT instead of SYS_MKDIR because not all architectures have the latter.
            nc::AT_FDCWD as usize,
            path_ptr as usize,
            mode.0,
        )
    }
    .map(drop)
}

pub fn pivot_root(new_root: &CStr, put_old: &CStr) -> Result<(), Errno> {
    let new_root_ptr = new_root.to_bytes_with_nul().as_ptr();
    let put_old_ptr = put_old.to_bytes_with_nul().as_ptr();
    unsafe {
        syscalls::syscall2(
            nc::SYS_PIVOT_ROOT,
            new_root_ptr as usize,
            put_old_ptr as usize,
        )
    }
    .map(drop)
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct UmountFlags(usize);

impl UmountFlags {
    pub const DETACH: Self = Self(2);
}

pub fn umount2(path: &CStr, flags: UmountFlags) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    unsafe { syscalls::syscall2(nc::SYS_UMOUNT2, path_ptr as usize, flags.0) }.map(drop)
}

pub fn execve(path: &CStr, argv: &[Option<&u8>], envp: &[Option<&u8>]) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    let argv_ptr = argv.as_ptr();
    let envp_ptr = envp.as_ptr();
    unsafe {
        syscalls::syscall3(
            nc::SYS_EXECVE,
            path_ptr as usize,
            argv_ptr as usize,
            envp_ptr as usize,
        )
    }
    .map(drop)
}

pub fn exit(status: usize) -> ! {
    let _ = unsafe { syscalls::syscall1(nc::SYS_EXIT, status) };
    unreachable!();
}
