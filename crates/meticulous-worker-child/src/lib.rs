#![no_std]

use anyhow::Result;
use core::{
    ffi::{c_char, c_int},
    fmt::{self, Arguments, Error, Write},
    result,
};
use nix::errno::Errno;

// These might not work for all linux architectures. We can fix them as we add more architectures.
#[allow(non_camel_case_types)]
pub type uid_t = u32;
#[allow(non_camel_case_types)]
pub type gid_t = u32;

struct Buf<const N: usize> {
    buf: [u8; N],
    used: usize,
}

impl<const N: usize> Default for Buf<N> {
    fn default() -> Self {
        Buf {
            buf: [0u8; N],
            used: 0,
        }
    }
}

impl<const N: usize> Buf<N> {
    unsafe fn write_to_fd(&self, fd: i32) -> result::Result<(), nc::Errno> {
        nc::write(fd, self.buf.as_ptr() as usize, self.used).map(|_| ())
    }
}

impl<const N: usize> Write for Buf<N> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        if self.used + s.len() > N {
            Err(Error)
        } else {
            self.buf[self.used..self.used + s.len()].copy_from_slice(s.as_bytes());
            self.used += s.len();
            Ok(())
        }
    }
}

fn write_file<const N: usize>(path: &str, args: Arguments) -> Result<()> {
    let fd = unsafe { nc::open(path, nc::O_WRONLY | nc::O_TRUNC, 0) }.map_err(Errno::from_i32)?;
    let mut buf: Buf<N> = Buf::default();
    buf.write_fmt(args)?;
    unsafe { buf.write_to_fd(fd) }.map_err(Errno::from_i32)?;
    unsafe { nc::close(fd) }.map_err(Errno::from_i32)?;
    Ok(())
}

/// The guts of the child code. This function can return a [`Result`].
fn start_and_exec_in_child_inner(
    program: *const c_char,
    argv: *const *const c_char,
    env: *const *const c_char,
    stdout_write_fd: c_int,
    stderr_write_fd: c_int,
    parent_uid: uid_t,
    parent_gid: gid_t,
) -> Result<()> {
    write_file::<5>("/proc/self/setgroups", format_args!("deny\n"))?;
    //            '0' ' '  {}  ' ' '1' '\n'
    write_file::<{ 1 + 1 + 10 + 1 + 1 + 1 }>(
        //write_file::<{ 1 + 1 + 10 + 1 + 1 + 1 }>(
        "/proc/self/uid_map",
        format_args!("0 {} 1\n", parent_uid),
    )?;
    //            '0' ' '  {}  ' ' '1' '\n'
    write_file::<{ 1 + 1 + 10 + 1 + 1 + 1 }>(
        "/proc/self/gid_map",
        format_args!("0 {} 1\n", parent_gid),
    )?;
    unsafe { nc::setsid() }.map_err(Errno::from_i32)?;
    unsafe { nc::dup2(stdout_write_fd, 1) }.map_err(Errno::from_i32)?;
    unsafe { nc::dup2(stderr_write_fd, 2) }.map_err(Errno::from_i32)?;
    unsafe { nc::close_range(3, !0u32, nc::CLOSE_RANGE_CLOEXEC) }.map_err(Errno::from_i32)?;
    unsafe {
        nc::syscalls::syscall3(
            nc::SYS_EXECVE,
            program as usize,
            argv as usize,
            env as usize,
        )
    }
    .map_err(Errno::from_i32)?;
    unreachable!();
}

/// Try to exec the job and write the error message to the pipe on failure.
#[allow(clippy::too_many_arguments)]
pub fn start_and_exec_in_child(
    program: *const c_char,
    argv: *const *const c_char,
    env: *const *const c_char,
    _stdout_read_fd: c_int,
    stdout_write_fd: c_int,
    _stderr_read_fd: c_int,
    stderr_write_fd: c_int,
    _exec_result_read_fd: c_int,
    exec_result_write_fd: c_int,
    parent_uid: uid_t,
    parent_gid: gid_t,
) -> ! {
    // TODO: https://github.com/meticulous-software/meticulous/issues/47
    //
    // We assume any error we encounter in the child is an execution error. While highly unlikely,
    // we could theoretically encounter a system error.
    let Err(err) = start_and_exec_in_child_inner(
        program,
        argv,
        env,
        stdout_write_fd,
        stderr_write_fd,
        parent_uid,
        parent_gid,
    ) else {
        unreachable!();
    };
    let mut buf: Buf<1024> = Buf::default();
    buf.write_fmt(format_args!("{:1024}", err)).unwrap();
    unsafe { buf.write_to_fd(exec_result_write_fd) }.unwrap();
    unsafe { nc::exit(1) };
}
