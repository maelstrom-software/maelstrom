//! Helper library for maelstrom-worker.
//!
//! This code is run in the child process after the call to `clone`. In this environment, since the
//! cloning process is multi-threaded, there is very little that we can do safely. In particular,
//! we can't allocate from the heap. This library is separate so we can make it `no_std` and manage
//! its dependencies carefully.
#![no_std]

use core::{
    ffi::{c_int, CStr},
    ptr, result,
};
use maelstrom_linux::{self as linux, sockaddr_nl_t};
use nc::{
    mode_t,
    syscalls::{self, Errno},
};

/// A syscall to call. This should be part of slice, which we refer to as a script. Some variants
/// deal with a value. This is a `usize` local variable that can be written to and read from.
pub enum Syscall<'a> {
    SocketAndSaveFd(i32, i32, i32),
    BindNetlinkUsingSavedFd(&'a sockaddr_nl_t),
    ReadUsingSavedFd(&'a mut [u8]),
    OpenAndSaveFd(&'a CStr, i32, mode_t),
    WriteUsingSavedFd(&'a [u8]),
    SetSid,
    Dup2(c_int, c_int),
    CloseRange(u32, u32, u32),
    Mount(
        Option<&'a CStr>,
        &'a CStr,
        Option<&'a CStr>,
        usize,
        Option<&'a [u8]>,
    ),
    Chdir(&'a CStr),
    Mkdir(&'a CStr, mode_t),
    PivotRoot(&'a CStr, &'a CStr),
    Umount2(&'a CStr, usize),
    Execve(&'a CStr, &'a [Option<&'a u8>], &'a [Option<&'a u8>]),
}

impl<'a> Syscall<'a> {
    unsafe fn call(&mut self, saved_fd: &mut u32) -> result::Result<(), Errno> {
        match self {
            Syscall::SocketAndSaveFd(domain, sock_type, protocol) => {
                linux::socket(*domain, *sock_type, *protocol).map(|fd| {
                    *saved_fd = fd;
                })
            }
            Syscall::BindNetlinkUsingSavedFd(sockaddr) => linux::bind_netlink(*saved_fd, sockaddr),
            Syscall::ReadUsingSavedFd(buf) => linux::read(*saved_fd, buf).map(drop),
            Syscall::OpenAndSaveFd(filename, flags, mode) => linux::open(filename, *flags, *mode)
                .map(|fd| {
                    *saved_fd = fd;
                }),
            Syscall::WriteUsingSavedFd(buf) => linux::write(*saved_fd, buf).map(drop),
            Syscall::SetSid => syscalls::syscall0(nc::SYS_SETSID).map(drop),
            Syscall::Dup2(from, to) => {
                syscalls::syscall3(nc::SYS_DUP3, *from as usize, *to as usize, 0).map(drop)
            }
            Syscall::CloseRange(first, last, flags) => syscalls::syscall3(
                nc::SYS_CLOSE_RANGE,
                *first as usize,
                *last as usize,
                *flags as usize,
            )
            .map(drop),
            Syscall::Mount(source, target, fstype, flags, data) => syscalls::syscall5(
                nc::SYS_MOUNT,
                source
                    .map(|r| r.to_bytes_with_nul().as_ptr())
                    .unwrap_or(ptr::null()) as usize,
                target.to_bytes_with_nul().as_ptr() as usize,
                fstype.map(|r| r.as_ptr()).unwrap_or(ptr::null()) as usize,
                *flags,
                data.map(|r| r.as_ptr()).unwrap_or(ptr::null()) as usize,
            )
            .map(drop),
            Syscall::Chdir(path) => {
                syscalls::syscall1(nc::SYS_CHDIR, path.to_bytes_with_nul().as_ptr() as usize)
                    .map(drop)
            }
            Syscall::Mkdir(path, mode) => syscalls::syscall3(
                nc::SYS_MKDIRAT,
                nc::AT_FDCWD as usize,
                path.to_bytes_with_nul().as_ptr() as usize,
                *mode as usize,
            )
            .map(drop),
            Syscall::PivotRoot(new_root, put_old) => syscalls::syscall2(
                nc::SYS_PIVOT_ROOT,
                new_root.to_bytes_with_nul().as_ptr() as usize,
                put_old.to_bytes_with_nul().as_ptr() as usize,
            )
            .map(drop),
            Syscall::Umount2(path, flags) => syscalls::syscall2(
                nc::SYS_UMOUNT2,
                path.to_bytes_with_nul().as_ptr() as usize,
                *flags,
            )
            .map(drop),
            Syscall::Execve(program, arguments, environment) => syscalls::syscall3(
                nc::SYS_EXECVE,
                program.to_bytes_with_nul().as_ptr() as usize,
                arguments.as_ptr() as usize,
                environment.as_ptr() as usize,
            )
            .map(drop),
        }
    }
}

/// The guts of the child code. This function shouldn't return on success, because in that case,
/// the last syscall should be an execve. If this function returns, than an error was encountered.
/// In that case, the script item index and the errno will be returned.
fn start_and_exec_in_child_inner(syscalls: &mut [Syscall]) -> (usize, nc::Errno) {
    let mut saved_fd = 0;
    for (index, syscall) in syscalls.iter_mut().enumerate() {
        if let Err(errno) = unsafe { syscall.call(&mut saved_fd) } {
            return (index, errno);
        }
    }
    panic!("should not reach here");
}

/// Run the provided syscall script in `syscalls`.
///
/// It is assumed that the last syscall won't return (i.e. will be `execve`). If there is an error,
/// write an 8-byte value to `exec_result_write_fd` describing the error in little-endian format.
/// The upper 32 bits will be the index in the script of the syscall that errored, and the lower 32
/// bits will be the errno value.
///
/// The caller should ensure that `exec_result_write_fd` is marked close-on-exec. This way, upon
/// normal completion, no bytes will be written to the file descriptor and the worker can
/// distinguish between an error and no error.
pub fn start_and_exec_in_child(exec_result_write_fd: c_int, syscalls: &mut [Syscall]) -> ! {
    let (index, errno) = start_and_exec_in_child_inner(syscalls);
    let result = (index as u64) << 32 | (errno as u64);
    // There's not really much to do if this write fails. Therefore, we just ignore the result.
    // However, it's hard to imagine any case where this could fail and we'd actually care.
    let _ = unsafe {
        nc::write(
            exec_result_write_fd,
            result.to_le_bytes().as_ptr() as usize,
            8,
        )
    };
    unsafe { nc::exit(1) };
}
