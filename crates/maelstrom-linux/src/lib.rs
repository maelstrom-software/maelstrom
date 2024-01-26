//! Function wrappers for Linux syscalls.
#![no_std]

use core::mem;
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
    .map(|ret| ret as u32)
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
