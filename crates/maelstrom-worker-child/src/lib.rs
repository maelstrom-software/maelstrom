//! Helper library for maelstrom-worker.
//!
//! This code is run in the child process after the call to `clone`. In this environment, since the
//! cloning process is multi-threaded, there is very little that we can do safely. In particular,
//! we can't allocate from the heap. This library is separate so we can make it `no_std` and manage
//! its dependencies carefully.
#![no_std]

use core::{ffi::CStr, fmt::Write as _, result};
use maelstrom_linux::{
    self as linux, CloseRangeFirst, CloseRangeFlags, CloseRangeLast, Errno, Fd, FileMode,
    MountFlags, NetlinkSocketAddr, OpenFlags, SocketDomain, SocketProtocol, SocketType,
    UmountFlags,
};

struct SliceFmt<'a> {
    slice: &'a mut [u8],
    offset: usize,
}

impl<'a> SliceFmt<'a> {
    fn new(slice: &'a mut [u8]) -> Self {
        Self { slice, offset: 0 }
    }
}

impl<'a> core::fmt::Write for SliceFmt<'a> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let bytes = s.as_bytes();
        if self.slice.len() - self.offset < bytes.len() {
            return Err(core::fmt::Error);
        }

        self.slice[self.offset..(self.offset + bytes.len())].copy_from_slice(bytes);
        self.offset += bytes.len();

        Ok(())
    }
}

/// A syscall to call. This should be part of slice, which we refer to as a script. Some variants
/// deal with a value. This is a `usize` local variable that can be written to and read from.
pub enum Syscall<'a> {
    OpenAndSaveFd(&'a CStr, OpenFlags, FileMode),
    SocketAndSaveFd(SocketDomain, SocketType, SocketProtocol),
    BindNetlinkUsingSavedFd(&'a NetlinkSocketAddr),
    ReadUsingSavedFd(&'a mut [u8]),
    WriteUsingSavedFd(&'a [u8]),
    SetSid,
    Dup2(Fd, Fd),
    CloseRange(CloseRangeFirst, CloseRangeLast, CloseRangeFlags),
    Mount(
        Option<&'a CStr>,
        &'a CStr,
        Option<&'a CStr>,
        MountFlags,
        Option<&'a [u8]>,
    ),
    Chdir(&'a CStr),
    Mkdir(&'a CStr, FileMode),
    PivotRoot(&'a CStr, &'a CStr),
    Umount2(&'a CStr, UmountFlags),
    Execve(&'a CStr, &'a [Option<&'a u8>], &'a [Option<&'a u8>]),
    FuseMountUsingSavedFd(&'a CStr, &'a CStr, MountFlags, u32, linux::Uid, linux::Gid),
    SendMsgSavedFd(&'a [u8]),
}

impl<'a> Syscall<'a> {
    fn call(
        &mut self,
        write_sock: &linux::UnixStream,
        saved_fd: &mut Fd,
    ) -> result::Result<(), Errno> {
        match self {
            Syscall::SocketAndSaveFd(domain, sock_type, protocol) => {
                linux::socket(*domain, *sock_type, *protocol).map(|fd| {
                    *saved_fd = fd.into_fd();
                })
            }
            Syscall::BindNetlinkUsingSavedFd(sockaddr) => linux::bind_netlink(*saved_fd, sockaddr),
            Syscall::ReadUsingSavedFd(buf) => linux::read(*saved_fd, buf).map(drop),
            Syscall::OpenAndSaveFd(filename, flags, mode) => linux::open(filename, *flags, *mode)
                .map(|fd| {
                    *saved_fd = fd.into_fd();
                }),
            Syscall::WriteUsingSavedFd(buf) => linux::write(*saved_fd, buf).map(drop),
            Syscall::SetSid => linux::setsid(),
            Syscall::Dup2(from, to) => linux::dup2(*from, *to).map(drop),
            Syscall::CloseRange(first, last, flags) => linux::close_range(*first, *last, *flags),
            Syscall::Mount(source, target, fstype, flags, data) => {
                linux::mount(*source, target, *fstype, *flags, *data)
            }
            Syscall::Chdir(path) => linux::chdir(path),
            Syscall::Mkdir(path, mode) => linux::mkdir(path, *mode),
            Syscall::PivotRoot(new_root, put_old) => linux::pivot_root(new_root, put_old),
            Syscall::Umount2(path, flags) => linux::umount2(path, *flags),
            Syscall::Execve(program, arguments, environment) => {
                linux::execve(program, arguments, environment)
            }
            Syscall::FuseMountUsingSavedFd(source, target, flags, root_mode, uid, gid) => {
                let mut options = [0; 100];
                write!(
                    SliceFmt::new(&mut options),
                    "fd={},rootmode={:o},user_id={},group_id={}\0",
                    saved_fd.as_c_int(),
                    root_mode,
                    uid.as_u32(),
                    gid.as_u32()
                )
                .unwrap();
                let source = Some(*source);
                let fstype = Some(unsafe { CStr::from_ptr("fuse\0".as_ptr() as *const i8) });
                linux::mount(source, target, fstype, *flags, Some(options.as_slice()))
            }
            Syscall::SendMsgSavedFd(buffer) => {
                let count = write_sock.send_with_fd(buffer, *saved_fd)?;
                assert_eq!(count, buffer.len());
                Ok(())
            }
        }
    }
}

/// The guts of the child code. This function shouldn't return on success, because in that case,
/// the last syscall should be an execve. If this function returns, than an error was encountered.
/// In that case, the script item index and the errno will be returned.
fn start_and_exec_in_child_inner(
    write_sock: &linux::UnixStream,
    syscalls: &mut [Syscall],
) -> (usize, Errno) {
    let mut saved_fd = Fd::STDIN; // STDIN is arbitrary.
    for (index, syscall) in syscalls.iter_mut().enumerate() {
        if let Err(errno) = syscall.call(write_sock, &mut saved_fd) {
            return (index, errno);
        }
    }
    panic!("should not reach here");
}

/// Run the provided syscall script in `syscalls`.
///
/// It is assumed that the last syscall won't return (i.e. will be `execve`). If there is an error,
/// write an 8-byte value to `write_sock` describing the error in little-endian format.
/// The upper 32 bits will be the index in the script of the syscall that errored, and the lower 32
/// bits will be the errno value.
///
/// The caller should ensure that `write_sock` is marked close-on-exec. This way, upon
/// normal completion, no bytes will be written to the file descriptor and the worker can
/// distinguish between an error and no error.
pub fn start_and_exec_in_child(write_sock: linux::UnixStream, syscalls: &mut [Syscall]) -> ! {
    let (index, errno) = start_and_exec_in_child_inner(&write_sock, syscalls);
    let result = (index as u64) << 32 | errno.as_u64();
    // There's not really much to do if this write fails. Therefore, we just ignore the result.
    // However, it's hard to imagine any case where this could fail and we'd actually care.
    let _ = write_sock.send(result.to_ne_bytes().as_slice());
    linux::_exit(linux::ExitCode::from_u8(1));
}
