//! Helper library for maelstrom-worker.
//!
//! This code is run in the child process after the call to `clone`. In this environment, since the
//! cloning process is multi-threaded, there is very little that we can do safely. In particular,
//! we can't allocate from the heap. This library is separate so we can make it `no_std` and manage
//! its dependencies carefully.
#![no_std]

use core::{cell::UnsafeCell, ffi::CStr, fmt::Write as _, result};
use maelstrom_linux::{
    self as linux, AccessMode, CloseRangeFirst, CloseRangeFlags, CloseRangeLast, Errno, Fd,
    FileMode, FsconfigCommand, FsmountFlags, FsopenFlags, Gid, MountAttrs, MountFlags,
    MoveMountFlags, OpenFlags, OpenTreeFlags, OwnedFd, Sockaddr, SocketDomain, SocketProtocol,
    SocketType, Uid, UmountFlags,
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

impl core::fmt::Write for SliceFmt<'_> {
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

#[derive(Clone, Copy)]
pub struct FdSlot<'a>(&'a UnsafeCell<Fd>);

impl<'a> FdSlot<'a> {
    pub fn new(slot: &'a UnsafeCell<Fd>) -> Self {
        Self(slot)
    }

    pub fn set(&self, fd: Fd) {
        let fd_ptr = self.0.get();
        unsafe { *fd_ptr = fd };
    }

    pub fn get(&self) -> Fd {
        let fd_ptr = self.0.get();
        unsafe { *fd_ptr }
    }
}

impl linux::AsFd for FdSlot<'_> {
    fn fd(&self) -> Fd {
        self.get()
    }
}

/// A syscall to call. This should be part of slice, which we refer to as a script. Some variants
/// deal with a value. This is a `usize` local variable that can be written to and read from.
pub enum Syscall<'a> {
    Bind {
        fd: FdSlot<'a>,
        addr: &'a Sockaddr,
    },
    Chdir {
        path: &'a CStr,
    },
    CloseRange {
        first: CloseRangeFirst,
        last: CloseRangeLast,
        flags: CloseRangeFlags,
    },
    Dup2 {
        from: Fd,
        to: Fd,
    },
    Execve {
        path: &'a CStr,
        argv: &'a [Option<&'a u8>],
        envp: &'a [Option<&'a u8>],
    },
    ExecveList {
        paths: &'a [&'a CStr],
        fallback: &'a CStr,
        argv: &'a [Option<&'a u8>],
        envp: &'a [Option<&'a u8>],
    },
    Fsconfig {
        fd: FdSlot<'a>,
        command: FsconfigCommand,
        key: Option<&'a CStr>,
        value: Option<&'a u8>,
        aux: Option<i32>,
    },
    Fsmount {
        fd: FdSlot<'a>,
        flags: FsmountFlags,
        mount_attrs: MountAttrs,
        out: FdSlot<'a>,
    },
    Fsopen {
        fsname: &'a CStr,
        flags: FsopenFlags,
        out: FdSlot<'a>,
    },
    FuseMount {
        source: &'a CStr,
        target: &'a CStr,
        flags: MountFlags,
        root_mode: u32,
        uid: Uid,
        gid: Gid,
        fuse_fd: FdSlot<'a>,
    },
    IoctlTiocsctty {
        fd: Fd,
        arg: i32,
    },
    Mkdir {
        path: &'a CStr,
        mode: FileMode,
    },
    Mount {
        source: Option<&'a CStr>,
        target: &'a CStr,
        fstype: Option<&'a CStr>,
        flags: MountFlags,
        data: Option<&'a [u8]>,
    },
    MoveMount {
        from_dirfd: FdSlot<'a>,
        from_path: &'a CStr,
        to_dirfd: Fd,
        to_path: &'a CStr,
        flags: MoveMountFlags,
    },
    Open {
        path: &'a CStr,
        flags: OpenFlags,
        mode: FileMode,
        out: FdSlot<'a>,
    },
    OpenTree {
        dirfd: Fd,
        path: &'a CStr,
        flags: OpenTreeFlags,
        out: FdSlot<'a>,
    },
    PivotRoot {
        new_root: &'a CStr,
        put_old: &'a CStr,
    },
    Read {
        fd: FdSlot<'a>,
        buf: &'a mut [u8],
    },
    SendMsg {
        buf: &'a [u8],
        fd_to_send: FdSlot<'a>,
    },
    SetSid,
    Socket {
        domain: SocketDomain,
        type_: SocketType,
        protocol: SocketProtocol,
        out: FdSlot<'a>,
    },
    Umount2 {
        path: &'a CStr,
        flags: UmountFlags,
    },
    Write {
        fd: FdSlot<'a>,
        buf: &'a [u8],
    },
}

impl Syscall<'_> {
    fn call(&mut self, write_sock: &linux::UnixStream) -> result::Result<(), Errno> {
        match self {
            Syscall::Bind { fd, addr } => linux::bind(fd, addr),
            Syscall::Chdir { path } => linux::chdir(path),
            Syscall::CloseRange { first, last, flags } => linux::close_range(*first, *last, *flags),
            Syscall::Dup2 { from, to } => linux::dup2(&*from, &*to).map(drop),
            Syscall::Execve { path, argv, envp } => linux::execve(path, argv, envp),
            Syscall::ExecveList {
                paths,
                fallback,
                argv,
                envp,
            } => {
                for path in paths.iter() {
                    if linux::access(path, AccessMode::X).is_ok() {
                        return linux::execve(path, argv, envp);
                    }
                }
                linux::execve(fallback, argv, envp)
            }
            Syscall::Fsmount {
                fd,
                flags,
                mount_attrs,
                out,
            } => {
                out.set(linux::fsmount(fd, *flags, *mount_attrs).map(OwnedFd::into_fd)?);
                Ok(())
            }
            Syscall::Fsopen { fsname, flags, out } => {
                out.set(linux::fsopen(fsname, *flags).map(OwnedFd::into_fd)?);
                Ok(())
            }
            Syscall::Fsconfig {
                fd,
                command,
                key,
                value,
                aux,
            } => linux::fsconfig(fd, *command, *key, *value, *aux),
            Syscall::FuseMount {
                source,
                target,
                flags,
                root_mode,
                uid,
                gid,
                fuse_fd,
            } => {
                let mut options = [0; 100];
                write!(
                    SliceFmt::new(&mut options),
                    "fd={},rootmode={:o},user_id={},group_id={}\0",
                    fuse_fd.get().as_c_int(),
                    root_mode,
                    uid.as_u32(),
                    gid.as_u32()
                )
                .unwrap();
                let source = Some(*source);
                let fstype = Some(c"fuse");
                linux::mount(source, target, fstype, *flags, Some(options.as_slice()))
            }
            Syscall::IoctlTiocsctty { fd, arg } => linux::ioctl_tiocsctty(fd, *arg),
            Syscall::Mkdir { path, mode } => linux::mkdir(path, *mode),
            Syscall::Mount {
                source,
                target,
                fstype,
                flags,
                data,
            } => linux::mount(*source, target, *fstype, *flags, *data),
            Syscall::MoveMount {
                from_dirfd,
                from_path,
                to_dirfd,
                to_path,
                flags,
            } => linux::move_mount(from_dirfd, from_path, to_dirfd, to_path, *flags),
            Syscall::Open {
                path,
                flags,
                mode,
                out,
            } => {
                out.set(linux::open(path, *flags, *mode).map(OwnedFd::into_fd)?);
                Ok(())
            }
            Syscall::OpenTree {
                dirfd,
                path,
                flags,
                out,
            } => {
                out.set(linux::open_tree(dirfd, path, *flags).map(OwnedFd::into_fd)?);
                Ok(())
            }
            Syscall::PivotRoot { new_root, put_old } => linux::pivot_root(new_root, put_old),
            Syscall::Read { fd, buf } => linux::read(fd, buf).map(drop),
            Syscall::SendMsg { buf, fd_to_send } => {
                let count = write_sock.send_with_fd(buf, fd_to_send.get())?;
                assert_eq!(count, buf.len());
                Ok(())
            }
            Syscall::SetSid => linux::setsid(),
            Syscall::Socket {
                domain,
                type_,
                protocol,
                out,
            } => {
                out.set(linux::socket(*domain, *type_, *protocol).map(OwnedFd::into_fd)?);
                Ok(())
            }
            Syscall::Umount2 { path, flags } => linux::umount2(path, *flags),
            Syscall::Write { fd, buf } => linux::write(fd, buf).map(drop),
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
    for (index, syscall) in syscalls.iter_mut().enumerate() {
        if let Err(errno) = syscall.call(write_sock) {
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
    let result = ((index as u64) << 32) | errno.as_u64();
    // There's not really much to do if this write fails. Therefore, we just ignore the result.
    // However, it's hard to imagine any case where this could fail and we'd actually care.
    let _ = write_sock.send(result.to_ne_bytes().as_slice());
    linux::_exit(linux::ExitCode::from_u8(1));
}

pub struct ChildArgs<'a, 'b> {
    pub write_sock: linux::Fd,
    pub syscalls: &'a mut [Syscall<'b>],
}

pub extern "C" fn start_and_exec_in_child_trampoline(arg: *mut core::ffi::c_void) -> i32 {
    let args = unsafe { &mut *(arg as *mut ChildArgs<'_, '_>) };
    start_and_exec_in_child(
        linux::OwnedFd::from_fd(args.write_sock).into(),
        args.syscalls,
    )
}
