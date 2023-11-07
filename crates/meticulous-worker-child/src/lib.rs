#![no_std]

use core::{
    ffi::{c_char, c_int},
    fmt::{self, Write as _},
    result,
};

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
    unsafe fn write_to_fd(&self, fd: i32) -> Result<()> {
        nc::write(fd, self.buf.as_ptr() as usize, self.used)?;
        Ok(())
    }

    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        if self.used + bytes.len() > N {
            Err(Error::BufferTooSmall)
        } else {
            self.buf[self.used..self.used + bytes.len()].copy_from_slice(bytes);
            self.used += bytes.len();
            Ok(())
        }
    }
}

impl<const N: usize> fmt::Write for Buf<N> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.append(s.as_bytes()).map_err(|_| fmt::Error)
    }
}

#[derive(Debug)]
pub enum Error {
    Errno(i32),
    BufferTooSmall,
}

impl From<nc::Errno> for Error {
    fn from(val: nc::Errno) -> Self {
        Error::Errno(val)
    }
}

impl From<fmt::Error> for Error {
    fn from(_: fmt::Error) -> Self {
        Error::BufferTooSmall
    }
}

impl<const N: usize> TryFrom<Error> for Buf<N> {
    type Error = Error;
    fn try_from(err: Error) -> Result<Buf<N>> {
        let mut buf = Buf::<N>::default();
        match err {
            Error::Errno(errno) => {
                buf.append([0u8; 1].as_slice())?;
                buf.append(errno.to_ne_bytes().as_slice())?;
            }
            Error::BufferTooSmall => {
                buf.append([1u8; 1].as_slice())?;
            }
        }
        Ok(buf)
    }
}

impl TryFrom<&[u8]> for Error {
    type Error = &'static str;
    fn try_from(buf: &[u8]) -> result::Result<Error, &'static str> {
        let mut cursor = 0;
        if cursor == buf.len() {
            return Err("no type byte");
        }
        let t = buf[cursor];
        cursor += 1;
        let error = if t == 0 {
            if cursor + 4 > buf.len() {
                return Err("not enough bytes for errno");
            }
            let errno = i32::from_ne_bytes(buf[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;
            Error::Errno(errno)
        } else if t == 1 {
            Error::BufferTooSmall
        } else {
            return Err("bad type byte");
        };
        if cursor != buf.len() {
            Err("unexpected trailing bytes")
        } else {
            Ok(error)
        }
    }
}

type Result<T> = result::Result<T, Error>;

// path must be NUL-terminated
unsafe fn open(path: &[u8], flags: i32, mode: nc::mode_t) -> result::Result<i32, nc::Errno> {
    assert!(path.len() > 0 && path[path.len() - 1] == 0u8);
    let path = path.as_ptr() as usize;
    let flags = flags as usize;
    let mode = mode as usize;
    nc::syscalls::syscall3(nc::SYS_OPEN, path, flags, mode).map(|ret| ret as i32)
}

// path must be NUL-terminated
unsafe fn write_file<const N: usize>(path: &[u8], args: fmt::Arguments) -> Result<()> {
    let fd = open(path, nc::O_WRONLY | nc::O_TRUNC, 0)?;
    let mut buf: Buf<N> = Buf::default();
    buf.write_fmt(args)?;
    buf.write_to_fd(fd)?;
    nc::close(fd)?;
    Ok(())
}

/// The guts of the child code. This function can return a [`Result`].
unsafe fn start_and_exec_in_child_inner(
    program: *const c_char,
    argv: *const *const c_char,
    env: *const *const c_char,
    stdout_write_fd: c_int,
    stderr_write_fd: c_int,
    parent_uid: uid_t,
    parent_gid: gid_t,
) -> Result<()> {
    write_file::<5>(b"/proc/self/setgroups\0", format_args!("deny\n"))?;
    //            '0' ' '  {}  ' ' '1' '\n'
    write_file::<{ 1 + 1 + 10 + 1 + 1 + 1 }>(
        b"/proc/self/uid_map\0",
        format_args!("0 {} 1\n", parent_uid),
    )?;
    //            '0' ' '  {}  ' ' '1' '\n'
    write_file::<{ 1 + 1 + 10 + 1 + 1 + 1 }>(
        b"/proc/self/gid_map\0",
        format_args!("0 {} 1\n", parent_gid),
    )?;
    nc::setsid()?;
    nc::dup2(stdout_write_fd, 1)?;
    nc::dup2(stderr_write_fd, 2)?;
    nc::close_range(3, !0u32, nc::CLOSE_RANGE_CLOEXEC)?;
    nc::syscalls::syscall3(
        nc::SYS_EXECVE,
        program as usize,
        argv as usize,
        env as usize,
    )?;
    unreachable!();
}

/// Try to exec the job and write the error message to the pipe on failure.
#[allow(clippy::too_many_arguments)]
pub unsafe fn start_and_exec_in_child(
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
    let buf = Buf::<1024>::try_from(err).unwrap();
    buf.write_to_fd(exec_result_write_fd).unwrap();
    nc::exit(1);
}
