//! Function wrappers for Linux syscalls.
#![no_std]

use core::{
    ffi::{c_int, c_long, CStr},
    mem, ptr,
    time::Duration,
};
use derive_more::{BitOr, Display, From, Into};
use nc::syscalls;

pub type Errno = nix::errno::Errno;

#[derive(Clone, Copy, Default, From)]
pub struct Fd(usize);

impl Fd {
    pub const STDIN: Self = Self(0);
    pub const STDOUT: Self = Self(1);
    pub const STDERR: Self = Self(2);
    pub const FIRST_NON_SPECIAL: Self = Self(3);
    pub const LAST: Self = Self(!0);

    pub fn to_raw_fd(self) -> i32 {
        self.0 as i32
    }
}

impl From<c_int> for Fd {
    fn from(fd: c_int) -> Fd {
        Fd(fd as usize)
    }
}

impl From<libc::c_long> for Fd {
    fn from(fd: c_long) -> Fd {
        Fd(fd as usize)
    }
}

#[repr(C)]
pub struct NetlinkSocketAddr {
    sin_family: nc::sa_family_t,
    nl_pad: u16,
    nl_pid: u32,
    nl_groups: u32,
}

impl Default for NetlinkSocketAddr {
    fn default() -> Self {
        NetlinkSocketAddr {
            sin_family: nc::AF_NETLINK as nc::sa_family_t,
            nl_pad: 0,
            nl_pid: 0, // the kernel
            nl_groups: 0,
        }
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct OpenFlags(usize);

impl OpenFlags {
    pub const WRONLY: Self = Self(nc::O_WRONLY as usize);
    pub const TRUNC: Self = Self(nc::O_TRUNC as usize);
    pub const NONBLOCK: Self = Self(nc::O_NONBLOCK as usize);
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct FileMode(usize);

impl FileMode {
    pub const SUID: Self = Self(0o4000);
    pub const SGID: Self = Self(0o2000);
    pub const SVTX: Self = Self(0o1000);

    pub const RWXU: Self = Self(0o0700);
    pub const RUSR: Self = Self(0o0400);
    pub const WUSR: Self = Self(0o0200);
    pub const XUSR: Self = Self(0o0100);

    pub const RWXG: Self = Self(0o0070);
    pub const RGRP: Self = Self(0o0040);
    pub const WGRP: Self = Self(0o0020);
    pub const XGRP: Self = Self(0o0010);

    pub const RWXO: Self = Self(0o0007);
    pub const ROTH: Self = Self(0o0004);
    pub const WOTH: Self = Self(0o0002);
    pub const XOTH: Self = Self(0o0001);
}

pub fn open(path: &CStr, flags: OpenFlags, mode: FileMode) -> Result<Fd, Errno> {
    let path = path.to_bytes_with_nul();
    let path_ptr = path.as_ptr() as *const libc::c_char;
    Errno::result(unsafe { libc::open(path_ptr, flags.0 as c_int, mode.0 as libc::mode_t) })
        .map(|fd| fd.into())
}

pub fn dup2(from: Fd, to: Fd) -> Result<Fd, Errno> {
    Errno::result(unsafe { libc::dup2(from.0 as c_int, to.0 as c_int) }).map(|fd| fd.into())
}

#[derive(Clone, Copy)]
pub struct SocketDomain(usize);

impl SocketDomain {
    pub const NETLINK: Self = Self(nc::AF_NETLINK as usize);
}

#[derive(BitOr, Clone, Copy)]
pub struct SocketType(usize);

impl SocketType {
    pub const RAW: Self = Self(nc::SOCK_RAW as usize);
    pub const CLOEXEC: Self = Self(nc::SOCK_CLOEXEC as usize);
}

#[derive(Clone, Copy)]
pub struct SocketProtocol(usize);

impl SocketProtocol {
    pub const NETLINK_ROUTE: Self = Self(0);
}

pub fn socket(
    domain: SocketDomain,
    type_: SocketType,
    protocol: SocketProtocol,
) -> Result<Fd, Errno> {
    Errno::result(unsafe { libc::socket(domain.0 as i32, type_.0 as i32, protocol.0 as i32) })
        .map(|fd| fd.into())
}

pub fn bind_netlink(fd: Fd, sockaddr: &NetlinkSocketAddr) -> Result<(), Errno> {
    let sockaddr_ptr = sockaddr as *const NetlinkSocketAddr as *const libc::sockaddr;
    let sockaddr_len = mem::size_of::<NetlinkSocketAddr>();
    Errno::result(unsafe {
        libc::bind(fd.0 as c_int, sockaddr_ptr, sockaddr_len as libc::socklen_t)
    })
    .map(drop)
}

pub fn read(fd: Fd, buf: &mut [u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_mut_ptr() as *mut libc::c_void;
    let buf_len = buf.len();
    Errno::result(unsafe { libc::read(fd.0 as libc::c_int, buf_ptr, buf_len) })
        .map(|ret| ret as usize)
}

pub fn write(fd: Fd, buf: &[u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_ptr() as *const libc::c_void;
    let buf_len = buf.len();
    Errno::result(unsafe { libc::write(fd.0 as libc::c_int, buf_ptr, buf_len) })
        .map(|ret| ret as usize)
}

#[derive(Clone, Copy, Default)]
pub struct CloseRangeFlags(usize);

impl CloseRangeFlags {
    pub const CLOEXEC: Self = Self(nc::CLOSE_RANGE_CLOEXEC as usize);
}

pub fn close_range(first: Fd, last: Fd, flags: CloseRangeFlags) -> Result<(), Errno> {
    Errno::result(unsafe {
        libc::close_range(
            first.0 as libc::c_uint,
            last.0 as libc::c_uint,
            flags.0 as libc::c_int,
        )
    })
    .map(drop)
}

pub fn setsid() -> Result<(), Errno> {
    Errno::result(unsafe { libc::setsid() }).map(drop)
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct MountFlags(usize);

impl MountFlags {
    pub const BIND: Self = Self(nc::MS_BIND);
    pub const REMOUNT: Self = Self(nc::MS_REMOUNT);
    pub const RDONLY: Self = Self(nc::MS_RDONLY);
    pub const NOSUID: Self = Self(nc::MS_NOSUID);
    pub const NOEXEC: Self = Self(nc::MS_NOEXEC);
    pub const NODEV: Self = Self(nc::MS_NODEV);
}

pub fn mount(
    source: Option<&CStr>,
    target: &CStr,
    fstype: Option<&CStr>,
    flags: MountFlags,
    data: Option<&[u8]>,
) -> Result<(), Errno> {
    let source_ptr = source
        .map(|r| r.to_bytes_with_nul().as_ptr())
        .unwrap_or(ptr::null());
    let target_ptr = target.to_bytes_with_nul().as_ptr();
    let fstype_ptr = fstype.map(|r| r.as_ptr()).unwrap_or(ptr::null());
    let data_ptr = data.map(|r| r.as_ptr()).unwrap_or(ptr::null());
    Errno::result(unsafe {
        libc::mount(
            source_ptr as *const libc::c_char,
            target_ptr as *const libc::c_char,
            fstype_ptr as *const libc::c_char,
            flags.0 as libc::c_ulong,
            data_ptr as *const libc::c_void,
        )
    })
    .map(drop)
}

pub fn chdir(path: &CStr) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    Errno::result(unsafe { libc::chdir(path_ptr as *const libc::c_char) }).map(drop)
}

pub fn mkdir(path: &CStr, mode: FileMode) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    Errno::result(unsafe { libc::mkdir(path_ptr as *const libc::c_char, mode.0 as libc::mode_t) })
        .map(drop)
}

pub fn pivot_root(new_root: &CStr, put_old: &CStr) -> Result<(), Errno> {
    let new_root_ptr = new_root.to_bytes_with_nul().as_ptr();
    let put_old_ptr = put_old.to_bytes_with_nul().as_ptr();
    Errno::result(unsafe { libc::syscall(libc::SYS_pivot_root, new_root_ptr, put_old_ptr) })
        .map(drop)
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct UmountFlags(usize);

impl UmountFlags {
    pub const DETACH: Self = Self(2);
}

pub fn umount2(path: &CStr, flags: UmountFlags) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    Errno::result(unsafe { libc::umount2(path_ptr as *const libc::c_char, flags.0 as libc::c_int) })
        .map(drop)
}

pub fn execve(path: &CStr, argv: &[Option<&u8>], envp: &[Option<&u8>]) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr() as *const libc::c_char;
    let argv_ptr = argv.as_ptr() as *const *const libc::c_char;
    let envp_ptr = envp.as_ptr() as *const *const libc::c_char;
    Errno::result(unsafe { libc::execve(path_ptr, argv_ptr, envp_ptr) }).map(drop)
}

pub fn _exit(status: usize) -> ! {
    unsafe { libc::_exit(status as libc::c_int) };
}

#[derive(Clone, Copy, Default, Display, Into)]
pub struct Signal(u8);

impl Signal {
    pub const CHLD: Self = Self(nc::SIGCHLD as u8);
    pub const KILL: Self = Self(nc::SIGKILL as u8);
}

impl From<Signal> for i32 {
    fn from(signo: Signal) -> i32 {
        signo.0.into()
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct CloneFlags(u64);

impl CloneFlags {
    pub const CLEAR_SIGHAND: Self = Self(nc::CLONE_CLEAR_SIGHAND);
    pub const FILES: Self = Self(nc::CLONE_FILES as u64);
    pub const FS: Self = Self(nc::CLONE_FS as u64);
    pub const NEWCGROUP: Self = Self(nc::CLONE_NEWCGROUP as u64);
    pub const NEWIPC: Self = Self(nc::CLONE_NEWIPC as u64);
    pub const NEWNET: Self = Self(nc::CLONE_NEWNET as u64);
    pub const NEWNS: Self = Self(nc::CLONE_NEWNS as u64);
    pub const NEWPID: Self = Self(nc::CLONE_NEWPID as u64);
    pub const NEWUSER: Self = Self(nc::CLONE_NEWUSER as u64);
}

#[derive(Clone, Default)]
pub struct CloneArgs(nc::clone_args_t);

impl CloneArgs {
    pub fn flags(self, flags: CloneFlags) -> Self {
        Self(nc::clone_args_t {
            flags: flags.0,
            ..self.0
        })
    }

    pub fn exit_signal(self, signal: Signal) -> Self {
        Self(nc::clone_args_t {
            exit_signal: signal.0 as u64,
            ..self.0
        })
    }
}

pub type Pid = nc::pid_t;

pub fn clone3(args: &mut CloneArgs) -> Result<Option<Pid>, Errno> {
    let args_ptr = args as *mut CloneArgs as *mut libc::c_void;
    let size = mem::size_of::<CloneArgs>() as libc::size_t;
    Errno::result(unsafe { libc::syscall(libc::SYS_clone3, args_ptr, size) }).map(|ret| {
        if ret == 0 {
            None
        } else {
            Some(ret as Pid)
        }
    })
}

pub fn pidfd_open(pid: Pid) -> Result<Fd, Errno> {
    Errno::result(unsafe {
        libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0 as libc::c_uint)
    })
    .map(|fd| fd.into())
}

pub fn close(fd: Fd) -> Result<(), Errno> {
    unsafe { syscalls::syscall1(nc::SYS_CLOSE, fd.0) }
        .map(drop)
        .map_err(Errno::from_i32)
}

pub fn prctl_set_pdeathsig(signal: Signal) -> Result<(), Errno> {
    unsafe {
        syscalls::syscall5(
            nc::SYS_PRCTL,
            nc::PR_SET_PDEATHSIG as usize,
            signal.0 as usize,
            0,
            0,
            0,
        )
    }
    .map(drop)
    .map_err(Errno::from_i32)
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct Timespec {
    sec: nc::time_t,
    nsec: c_long, // This doesn't work for x86_64 with a target_pointer_width of 32.
}

impl From<Duration> for Timespec {
    fn from(duration: Duration) -> Self {
        Timespec {
            sec: duration.as_secs() as nc::time_t,
            nsec: duration.subsec_nanos() as c_long,
        }
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct PollEvents(i16);

impl PollEvents {
    pub const IN: Self = Self(nc::POLLIN);
}

#[repr(C)]
pub struct PollFd(nc::pollfd_t);

impl PollFd {
    pub fn new(fd: Fd, events: PollEvents) -> Self {
        PollFd(nc::pollfd_t {
            fd: fd.0 as i32,
            events: events.0,
            revents: 0,
        })
    }
}

pub fn poll(fds: &mut [PollFd], timeout: Duration) -> Result<usize, Errno> {
    let fds_ptr = fds.as_mut_ptr();
    let nfds = fds.len();
    let timeout: Timespec = timeout.into();
    let inner = |timeout: &Timespec| {
        let timeout_ptr = timeout as *const Timespec;
        let sigmask_ptr = 0;
        let sigsetsize = 0;
        unsafe {
            syscalls::syscall5(
                nc::SYS_PPOLL,
                fds_ptr as usize,
                nfds,
                timeout_ptr as usize,
                sigmask_ptr,
                sigsetsize,
            )
        }
        .map_err(Errno::from_i32)
    };
    inner(&timeout)
}

#[derive(Clone, Copy, Into)]
pub struct ExitCode(u8);

impl From<ExitCode> for i32 {
    fn from(code: ExitCode) -> i32 {
        code.0.into()
    }
}

#[derive(Clone, Copy)]
pub enum WaitStatus {
    Exited(ExitCode),
    Signaled(Signal),
}

#[derive(Clone, Copy)]
pub struct WaitResult {
    pub pid: Pid,
    pub status: WaitStatus,
}

fn extract_wait_status(status: c_int) -> WaitStatus {
    if libc::WIFEXITED(status) {
        WaitStatus::Exited(ExitCode(libc::WEXITSTATUS(status).try_into().unwrap()))
    } else if libc::WIFSIGNALED(status) {
        WaitStatus::Signaled(Signal(libc::WTERMSIG(status).try_into().unwrap()))
    } else {
        panic!(
            "neither WIFEXITED nor WIFSIGNALED true on wait status {}",
            status
        );
    }
}

pub fn wait() -> Result<WaitResult, Errno> {
    let inner = |status: &mut c_int| {
        let status_ptr = status as *mut c_int;
        unsafe { libc::wait(status_ptr) }
    };
    let mut status = 0;
    Errno::result(inner(&mut status)).map(|pid| WaitResult {
        pid,
        status: extract_wait_status(status),
    })
}

#[derive(Clone, Copy, Default)]
pub struct WaitpidFlags(c_int);

pub fn waitpid(pid: Pid, flags: WaitpidFlags) -> Result<WaitStatus, Errno> {
    let inner = |status: &mut c_int| {
        let status_ptr = status as *mut c_int;
        unsafe { libc::waitpid(pid, status_ptr, flags.0) }
    };
    let mut status = 0;
    Errno::result(inner(&mut status)).map(|_| extract_wait_status(status))
}

pub fn raise(signal: Signal) -> Result<(), Errno> {
    Errno::result(unsafe { libc::raise(signal.0 as c_int) }).map(drop)
}

pub fn kill(pid: Pid, signal: Signal) -> Result<(), Errno> {
    Errno::result(unsafe { libc::kill(pid, signal.0 as c_int) }).map(drop)
}

pub fn pause() {
    unsafe { libc::pause() };
}

pub fn getpid() -> Pid {
    unsafe { libc::getpid() }
}

pub type Uid = libc::uid_t;

pub fn getuid() -> Uid {
    unsafe { libc::getuid() }
}

pub type Gid = libc::gid_t;

pub fn getgid() -> Gid {
    unsafe { libc::getgid() }
}

pub fn pipe() -> Result<(Fd, Fd), Errno> {
    let mut fds: [c_int; 2] = [0; 2];
    let fds_ptr = fds.as_mut_ptr() as *mut c_int;
    Errno::result(unsafe { libc::pipe(fds_ptr) }).map(|_| (fds[0].into(), fds[1].into()))
}

pub fn fcntl_setfl(fd: Fd, flags: OpenFlags) -> Result<(), Errno> {
    unsafe { syscalls::syscall3(nc::SYS_FCNTL, fd.0, nc::F_SETFL as usize, flags.0) }
        .map(drop)
        .map_err(Errno::from_i32)
}
