//! Function wrappers for Linux syscalls.
#![no_std]

use core::{ffi::CStr, mem, ptr};
use nc::syscalls;

pub type Errno = nc::Errno;

#[allow(non_camel_case_types)]
pub type mode_t = nc::mode_t;

pub const NETLINK_ROUTE: i32 = 0;
pub const AF_NETLINK: i32 = nc::AF_NETLINK;
pub const SOCK_RAW: i32 = nc::SOCK_RAW;
pub const SOCK_CLOEXEC: i32 = nc::SOCK_CLOEXEC;

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct sockaddr_nl_t {
    pub sin_family: nc::sa_family_t,
    pub nl_pad: u16,
    pub nl_pid: u32,
    pub nl_groups: u32,
}

pub fn open(path: &CStr, flags: i32, mode: nc::mode_t) -> Result<u32, Errno> {
    let path = path.to_bytes_with_nul();
    let path_ptr = path.as_ptr();
    unsafe {
        syscalls::syscall4(
            nc::SYS_OPENAT, // Use SYS_OPENAT instead of SYS_OPEN because not all architectures have the latter.
            nc::AT_FDCWD as usize,
            path_ptr as usize,
            flags as usize,
            mode as usize,
        )
    }
    .map(|fd| fd as u32)
}

pub fn dup2(from: u32, to: u32) -> Result<u32, Errno> {
    unsafe {
        // Use SYS_DUP3 instead of SYS_DUP2 because not all architectures have the latter.
        syscalls::syscall3(nc::SYS_DUP3, from as usize, to as usize, 0)
    }
    .map(|fd| fd as u32)
}

pub fn socket(domain: i32, sock_type: i32, protocol: i32) -> Result<u32, Errno> {
    unsafe {
        syscalls::syscall3(
            nc::SYS_SOCKET,
            domain as usize,
            sock_type as usize,
            protocol as usize,
        )
    }
    .map(|fd| fd as u32)
}

pub fn bind_netlink(fd: u32, sockaddr: &sockaddr_nl_t) -> Result<(), Errno> {
    let sockaddr_ptr = sockaddr as *const sockaddr_nl_t;
    let sockaddr_len = mem::size_of::<sockaddr_nl_t>();
    unsafe {
        syscalls::syscall3(
            nc::SYS_BIND,
            fd as usize,
            sockaddr_ptr as usize,
            sockaddr_len,
        )
    }
    .map(drop)
}

pub fn read(fd: u32, buf: &mut [u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_mut_ptr();
    let buf_len = buf.len();
    unsafe { syscalls::syscall3(nc::SYS_READ, fd as usize, buf_ptr as usize, buf_len) }
}

pub fn write(fd: u32, buf: &[u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_ptr();
    let buf_len = buf.len();
    unsafe { syscalls::syscall3(nc::SYS_WRITE, fd as usize, buf_ptr as usize, buf_len) }
}

pub fn close_range(first: u32, last: u32, flags: u32) -> Result<(), Errno> {
    unsafe {
        syscalls::syscall3(
            nc::SYS_CLOSE_RANGE,
            first as usize,
            last as usize,
            flags as usize,
        )
    }
    .map(drop)
}

pub fn setsid() -> Result<(), Errno> {
    unsafe { syscalls::syscall0(nc::SYS_SETSID) }.map(drop)
}

pub fn mount(
    source: Option<&CStr>,
    target: &CStr,
    fstype: Option<&CStr>,
    flags: usize,
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
            flags,
            data_ptr as usize,
        )
    }
    .map(drop)
}

pub fn chdir(path: &CStr) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    unsafe { syscalls::syscall1(nc::SYS_CHDIR, path_ptr as usize) }.map(drop)
}

pub fn mkdir(path: &CStr, mode: mode_t) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    unsafe {
        syscalls::syscall3(
            nc::SYS_MKDIRAT, // Use SYS_MKDIRAT instead of SYS_MKDIR because not all architectures have the latter.
            nc::AT_FDCWD as usize,
            path_ptr as usize,
            mode as usize,
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
