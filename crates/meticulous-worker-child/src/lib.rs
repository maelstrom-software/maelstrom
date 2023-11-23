#![no_std]

pub mod rtnetlink;

use core::{
    ffi::{c_int, CStr},
    fmt::{self, Write as _},
    mem, result, slice, str,
};
use nc::syscalls::{self, Errno, Sysno};

pub enum Syscall {
    One(Sysno, usize),
    Two(Sysno, usize, usize),
    Three(Sysno, usize, usize, usize),
    Five(Sysno, usize, usize, usize, usize, usize),
}

impl Syscall {
    unsafe fn call(&self) -> result::Result<usize, Errno> {
        match self {
            Syscall::One(n, a1) => syscalls::syscall1(*n, *a1),
            Syscall::Two(n, a1, a2) => syscalls::syscall2(*n, *a1, *a2),
            Syscall::Three(n, a1, a2, a3) => syscalls::syscall3(*n, *a1, *a2, *a3),
            Syscall::Five(n, a1, a2, a3, a4, a5) => syscalls::syscall5(*n, *a1, *a2, *a3, *a4, *a5),
        }
    }
}

pub enum Layers<'a> {
    One {
        path: &'a CStr,
    },
    Many {
        overlayfs_options: &'a CStr,
        mount_dir: &'a CStr,
    },
}

pub struct JobDetails<'a> {
    pub program: &'a CStr,
    pub arguments: &'a [Option<&'a u8>],
    pub environment: &'a [Option<&'a u8>],
    pub layers: Layers<'a>,
}

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
        nc::write(fd, self.buf.as_ptr() as usize, self.used).map_system_errno("write")?;
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
    ExecutionErrno(i32, &'static str),
    SystemErrno(i32, &'static str),
    BufferTooSmall,
}

impl From<fmt::Error> for Error {
    fn from(_: fmt::Error) -> Self {
        Error::BufferTooSmall
    }
}

fn encode_errno_error<const N: usize>(
    code: u8,
    errno: i32,
    context: &'static str,
    buf: &mut Buf<N>,
) -> Result<()> {
    buf.append(slice::from_ref(&code))?;
    buf.append(errno.to_ne_bytes().as_slice())?;
    buf.append((context.as_ptr() as usize).to_ne_bytes().as_slice())?;
    buf.append(context.len().to_ne_bytes().as_slice())?;
    Ok(())
}

impl<const N: usize> TryFrom<Error> for Buf<N> {
    type Error = Error;
    fn try_from(err: Error) -> Result<Buf<N>> {
        let mut buf = Buf::<N>::default();
        match err {
            Error::ExecutionErrno(errno, context) => {
                encode_errno_error(0, errno, context, &mut buf)?;
            }
            Error::SystemErrno(errno, context) => {
                encode_errno_error(1, errno, context, &mut buf)?;
            }
            Error::BufferTooSmall => {
                buf.append(slice::from_ref(&2u8))?;
            }
        }
        Ok(buf)
    }
}

fn decode_errno_error(
    constructor: impl Fn(i32, &'static str) -> Error,
    buf: &[u8],
    cursor: &mut usize,
) -> result::Result<Error, &'static str> {
    if *cursor + 4 > buf.len() {
        return Err("not enough bytes for errno");
    }
    let errno = i32::from_ne_bytes(buf[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;

    let usize_size = mem::size_of::<usize>();
    if *cursor + usize_size > buf.len() {
        return Err("not enough bytes for context pointer");
    }
    let context_ptr =
        usize::from_ne_bytes(buf[*cursor..*cursor + usize_size].try_into().unwrap()) as *const u8;
    *cursor += usize_size;

    if *cursor + usize_size > buf.len() {
        return Err("not enough bytes for context length");
    }
    let context_len = usize::from_ne_bytes(buf[*cursor..*cursor + usize_size].try_into().unwrap());
    *cursor += usize_size;

    let context = unsafe {
        str::from_utf8_unchecked(slice::from_raw_parts::<'static>(context_ptr, context_len))
    };

    Ok(constructor(errno, context))
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
        let error = match t {
            0 => decode_errno_error(Error::ExecutionErrno, buf, &mut cursor)?,
            1 => decode_errno_error(Error::SystemErrno, buf, &mut cursor)?,
            2 => Error::BufferTooSmall,
            _ => {
                return Err("bad type byte");
            }
        };
        if cursor != buf.len() {
            Err("unexpected trailing bytes")
        } else {
            Ok(error)
        }
    }
}

type Result<T> = result::Result<T, Error>;

trait ErrnoExt<T> {
    fn map_execution_errno(self, context: &'static str) -> Result<T>;
    fn map_system_errno(self, context: &'static str) -> Result<T>;
}

impl<T> ErrnoExt<T> for result::Result<T, nc::Errno> {
    fn map_execution_errno(self, context: &'static str) -> Result<T> {
        match self {
            Ok(val) => Ok(val),
            Err(errno) => Err(Error::ExecutionErrno(errno, context)),
        }
    }

    fn map_system_errno(self, context: &'static str) -> Result<T> {
        match self {
            Ok(val) => Ok(val),
            Err(errno) => Err(Error::SystemErrno(errno, context)),
        }
    }
}

// path must be NUL-terminated
unsafe fn open(path: &[u8], flags: i32, mode: nc::mode_t) -> result::Result<i32, nc::Errno> {
    assert!(!path.is_empty() && path[path.len() - 1] == 0u8);
    let path = path.as_ptr() as usize;
    let flags = flags as usize;
    let mode = mode as usize;
    nc::syscalls::syscall3(nc::SYS_OPEN, path, flags, mode).map(|ret| ret as i32)
}

// path must be NUL-terminated
unsafe fn write_file<const N: usize>(path: &[u8], args: fmt::Arguments) -> Result<()> {
    let fd = open(path, nc::O_WRONLY | nc::O_TRUNC, 0).map_system_errno("opening file to write")?;
    let mut buf: Buf<N> = Buf::default();
    buf.write_fmt(args)?;
    buf.write_to_fd(fd)?;
    nc::close(fd).map_system_errno("closing file file descriptor")?;
    Ok(())
}

/// The guts of the child code. This function can return a [`Result`].
unsafe fn start_and_exec_in_child_inner(
    details: JobDetails,
    stdout_write_fd: c_int,
    stderr_write_fd: c_int,
    parent_uid: uid_t,
    parent_gid: gid_t,
    syscalls: &[Syscall],
) -> Result<()> {
    // in a new network namespace we need to ifup loopback ourselves
    rtnetlink::ifup_loopback()?;

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
    nc::setsid().map_system_errno("setsid")?;
    nc::dup2(stdout_write_fd, 1).map_system_errno("dup2 to stdout")?;
    nc::dup2(stderr_write_fd, 2).map_system_errno("dup2 to stderr")?;
    nc::close_range(3, !0u32, nc::CLOSE_RANGE_CLOEXEC).map_system_errno("close_range")?;
    const SLASH: *const u8 = b"/\0".as_ptr();
    const OVERLAY: *const u8 = b"overlay\0".as_ptr();
    nc::syscalls::syscall5(
        nc::SYS_MOUNT,
        0,
        SLASH as usize,
        0,
        nc::MS_REC | nc::MS_PRIVATE,
        0,
    )
    .map_system_errno("mount of / to set private and rec")?;
    let new_root_path = match details.layers {
        Layers::One { path } => {
            let path = &path.to_bytes_with_nul()[0] as *const u8;

            // Bind mount the directory onto our mount dir. This ensures it's a mount point so we can
            // pivot_root to it later.
            nc::syscalls::syscall5(
                nc::SYS_MOUNT,
                path as usize,
                path as usize,
                0,
                nc::MS_BIND,
                0,
            )
            .map_system_errno("bind mount")?;

            // We want that mount to be read-only!
            nc::syscalls::syscall5(
                nc::SYS_MOUNT,
                0,
                path as usize,
                0,
                nc::MS_REMOUNT | nc::MS_BIND | nc::MS_RDONLY,
                0,
            )
            .map_system_errno("remount of bind mount")?;

            path
        }
        Layers::Many {
            overlayfs_options,
            mount_dir,
        } => {
            let overlayfs_options = &overlayfs_options.to_bytes_with_nul()[0] as *const u8;
            let mount_dir = &mount_dir.to_bytes_with_nul()[0] as *const u8;

            nc::syscalls::syscall5(
                nc::SYS_MOUNT,
                OVERLAY as usize,
                mount_dir as usize,
                OVERLAY as usize,
                0,
                overlayfs_options as usize,
            )
            .map_system_errno("mount")?;

            mount_dir
        }
    };

    // Chdir to what will be the new root.
    nc::syscalls::syscall1(nc::SYS_CHDIR, new_root_path as usize).map_system_errno("chdir")?;

    for syscall in syscalls {
        unsafe { syscall.call() }.map_system_errno("unknown")?;
    }

    nc::syscalls::syscall3(
        nc::SYS_EXECVE,
        &details.program.to_bytes_with_nul()[0] as *const u8 as usize,
        details.arguments.as_ptr() as usize,
        details.environment.as_ptr() as usize,
    )
    .map_execution_errno("execve")?;

    panic!("should not reach here");
}

/// Try to exec the job and write the error message to the pipe on failure.
///
/// # Safety
///
/// The provided `program` variable must be a NUL-terminated C-string. The `argv` and `env`
/// variables must be NULL-terminated arrays of pointers to NUL-terminated C-strings. The provided
/// file descriptors must be valid, open file descriptors.
pub unsafe fn start_and_exec_in_child(
    details: JobDetails,
    stdout_write_fd: c_int,
    stderr_write_fd: c_int,
    exec_result_write_fd: c_int,
    parent_uid: uid_t,
    parent_gid: gid_t,
    syscalls: &[Syscall],
) -> ! {
    // TODO: https://github.com/meticulous-software/meticulous/issues/47
    //
    // We assume any error we encounter in the child is an execution error. While highly unlikely,
    // we could theoretically encounter a system error.
    let Err(err) = start_and_exec_in_child_inner(
        details,
        stdout_write_fd,
        stderr_write_fd,
        parent_uid,
        parent_gid,
        syscalls,
    ) else {
        unreachable!();
    };
    let buf = Buf::<21>::try_from(err).unwrap();
    buf.write_to_fd(exec_result_write_fd).unwrap();
    nc::exit(1);
}
