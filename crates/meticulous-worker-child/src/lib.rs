#![no_std]

pub mod rtnetlink;

use core::{
    ffi::{c_int, CStr},
    fmt::{self},
    mem, result, slice, str,
};
use nc::syscalls::{self, Errno, Sysno};
use netlink_packet_core::{NetlinkMessage, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST};
use netlink_packet_route::{rtnl::constants::RTM_SETLINK, LinkMessage, RtnlMessage, IFF_UP};
use rtnetlink::sockaddr_nl_t;

pub enum Syscall {
    Zero(Sysno),
    One(Sysno, usize),
    OneFromSaved(Sysno),
    Two(Sysno, usize, usize),
    Three(Sysno, usize, usize, usize),
    ThreeAndSave(Sysno, usize, usize, usize),
    ThreeFromSaved(Sysno, usize, usize),
    Five(Sysno, usize, usize, usize, usize, usize),
}

impl Syscall {
    unsafe fn call(&self, saved: &mut usize) -> result::Result<usize, Errno> {
        match self {
            Syscall::Zero(n) => syscalls::syscall0(*n),
            Syscall::One(n, a1) => syscalls::syscall1(*n, *a1),
            Syscall::OneFromSaved(n) => syscalls::syscall1(*n, *saved),
            Syscall::Two(n, a1, a2) => syscalls::syscall2(*n, *a1, *a2),
            Syscall::Three(n, a1, a2, a3) => syscalls::syscall3(*n, *a1, *a2, *a3),
            Syscall::ThreeAndSave(n, a1, a2, a3) => {
                syscalls::syscall3(*n, *a1, *a2, *a3).map(|v| {
                    *saved = v;
                    v
                })
            }
            Syscall::ThreeFromSaved(n, a2, a3) => syscalls::syscall3(*n, *saved, *a2, *a3),
            Syscall::Five(n, a1, a2, a3, a4, a5) => syscalls::syscall5(*n, *a1, *a2, *a3, *a4, *a5),
        }
    }
}

pub struct JobDetails<'a> {
    pub program: &'a CStr,
    pub arguments: &'a [Option<&'a u8>],
    pub environment: &'a [Option<&'a u8>],
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

const NETLINK_ROUTE: usize = 0;

/// The guts of the child code. This function can return a [`Result`].
fn start_and_exec_in_child_inner(details: JobDetails, syscalls: &[Syscall]) -> Result<()> {
    // in a new network namespace we need to ifup loopback ourselves
    let socket_fd = unsafe {
        nc::syscalls::syscall3(
            nc::SYS_SOCKET,
            nc::AF_NETLINK as usize,
            (nc::SOCK_RAW | nc::SOCK_CLOEXEC) as usize,
            NETLINK_ROUTE,
        )
    }
    .map_system_errno("failed to create netlink socket")?;
    let addr = sockaddr_nl_t {
        sin_family: nc::AF_NETLINK as nc::sa_family_t,
        nl_pad: 0,
        nl_pid: 0, // the kernel
        nl_groups: 0,
    };
    unsafe {
        nc::syscalls::syscall3(
            nc::SYS_BIND,
            socket_fd,
            &addr as *const sockaddr_nl_t as usize,
            mem::size_of::<sockaddr_nl_t>() as usize,
        )
    }
    .map_system_errno("failed to bind netlink socket")?;

    let mut message = LinkMessage::default();
    message.header.index = 1;
    message.header.flags |= IFF_UP;
    message.header.change_mask |= IFF_UP;

    let mut req = NetlinkMessage::from(RtnlMessage::SetLink(message));
    req.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_EXCL | NLM_F_CREATE;
    req.header.length = req.buffer_len() as u32;
    req.header.message_type = RTM_SETLINK;

    let mut buffer = [0; 1024];
    req.serialize(&mut buffer);

    let mut addr = nc::sockaddr_in_t::default();
    unsafe {
        nc::syscalls::syscall6(
            nc::SYS_SENDTO,
            socket_fd,
            buffer.as_ptr() as usize,
            req.buffer_len(),
            0,
            &addr as *const nc::sockaddr_in_t as usize,
            0,
        )
    }
    .map_system_errno("sendto on netlink socket failed")?;

    let mut addr_len = 0u32;
    unsafe {
        nc::syscalls::syscall6(
            nc::SYS_RECVFROM,
            socket_fd,
            buffer.as_mut_ptr() as usize,
            buffer.len(),
            0,
            &mut addr as *mut nc::sockaddr_in_t as usize,
            &mut addr_len as *mut nc::socklen_t as usize,
        )
    }
    .map_system_errno("recvfrom on netlink socket failed")?;

    // we can't deserialize since that allocates memory, so we just hope that it worked

    let mut saved = 0;
    for syscall in syscalls {
        unsafe { syscall.call(&mut saved) }.map_system_errno("unknown")?;
    }

    unsafe {
        nc::syscalls::syscall3(
            nc::SYS_EXECVE,
            &details.program.to_bytes_with_nul()[0] as *const u8 as usize,
            details.arguments.as_ptr() as usize,
            details.environment.as_ptr() as usize,
        )
    }
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
    exec_result_write_fd: c_int,
    syscalls: &[Syscall],
) -> ! {
    // TODO: https://github.com/meticulous-software/meticulous/issues/47
    //
    // We assume any error we encounter in the child is an execution error. While highly unlikely,
    // we could theoretically encounter a system error.
    let Err(err) = start_and_exec_in_child_inner(details, syscalls) else {
        unreachable!();
    };
    let buf = Buf::<21>::try_from(err).unwrap();
    buf.write_to_fd(exec_result_write_fd).unwrap();
    nc::exit(1);
}
