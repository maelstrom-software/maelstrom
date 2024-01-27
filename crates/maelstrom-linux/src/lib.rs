//! Function wrappers for Linux syscalls.
#![no_std]

use core::{ffi::CStr, mem};
use nc::syscalls;

pub type Errno = nc::Errno;

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

pub fn open(path: &CStr, flags: i32, mode: nc::mode_t) -> Result<u32, Errno> {
    let path = path.to_bytes_with_nul();
    let path_ptr = path.as_ptr();
    unsafe {
        syscalls::syscall4(
            nc::SYS_OPENAT, // Use SYS_OPENAT instead of SYS_OPEN because not all architectures have SYS_OPEN.
            nc::AT_FDCWD as usize,
            path_ptr as usize,
            flags as usize,
            mode as usize,
        )
    }
    .map(|fd| fd as u32)
}
