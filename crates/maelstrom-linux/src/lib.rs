//! Function wrappers for Linux syscalls.
#![no_std]

#[cfg(feature = "std")]
extern crate std;

use core::{ffi::CStr, mem, ptr, time::Duration};
use derive_more::{BitOr, Display, Into};
use libc::{
    c_char, c_int, c_long, c_uint, c_ulong, c_void, gid_t, mode_t, nfds_t, pid_t, pollfd,
    sa_family_t, size_t, sockaddr, socklen_t, time_t, uid_t,
};

pub type Errno = nix::errno::Errno;

/// This should be the same as [`std::os::fd::RawFd`]. We don't have access to that type since
/// we're in `no_std`. This type is used when converting from our [`Fd`] to system file descriptors
/// in `std` code.
pub type RawFd = c_int;

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Fd(RawFd);

impl Fd {
    pub const STDIN: Self = Self(libc::STDIN_FILENO);
    pub const STDOUT: Self = Self(libc::STDOUT_FILENO);
    pub const STDERR: Self = Self(libc::STDERR_FILENO);

    pub fn from_raw_fd(fd: RawFd) -> Self {
        Self(fd)
    }

    fn from_c_long(fd: c_long) -> Self {
        Self::from_raw_fd(fd.try_into().unwrap())
    }

    fn as_raw_fd(self) -> RawFd {
        self.0
    }

    fn as_c_uint(self) -> c_uint {
        self.0.try_into().unwrap()
    }
}

pub struct OwnedFd(Fd);

impl OwnedFd {
    pub fn from_fd(fd: Fd) -> Self {
        Self(fd)
    }

    pub fn as_fd(&self) -> Fd {
        self.0
    }

    #[cfg(feature = "std")]
    pub fn into_file(self) -> std::fs::File {
        let raw_fd = self.0 .0;
        mem::forget(self);
        unsafe { std::os::fd::FromRawFd::from_raw_fd(raw_fd) }
    }
}

impl Drop for OwnedFd {
    fn drop(&mut self) {
        let _ = close(self.0);
    }
}

#[repr(C)]
pub struct NetlinkSocketAddr {
    sin_family: sa_family_t,
    nl_pad: u16,
    nl_pid: u32,
    nl_groups: u32,
}

impl Default for NetlinkSocketAddr {
    fn default() -> Self {
        NetlinkSocketAddr {
            sin_family: libc::AF_NETLINK as sa_family_t,
            nl_pad: 0,
            nl_pid: 0, // the kernel
            nl_groups: 0,
        }
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct OpenFlags(usize);

impl OpenFlags {
    pub const WRONLY: Self = Self(libc::O_WRONLY as usize);
    pub const TRUNC: Self = Self(libc::O_TRUNC as usize);
    pub const NONBLOCK: Self = Self(libc::O_NONBLOCK as usize);
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
    let path_ptr = path.as_ptr() as *const c_char;
    Errno::result(unsafe { libc::open(path_ptr, flags.0 as c_int, mode.0 as mode_t) })
        .map(Fd::from_raw_fd)
}

pub fn dup2(from: Fd, to: Fd) -> Result<Fd, Errno> {
    Errno::result(unsafe { libc::dup2(from.as_raw_fd(), to.as_raw_fd()) }).map(Fd::from_raw_fd)
}

#[derive(Clone, Copy)]
pub struct SocketDomain(usize);

impl SocketDomain {
    pub const NETLINK: Self = Self(libc::AF_NETLINK as usize);
}

#[derive(BitOr, Clone, Copy)]
pub struct SocketType(usize);

impl SocketType {
    pub const RAW: Self = Self(libc::SOCK_RAW as usize);
    pub const CLOEXEC: Self = Self(libc::SOCK_CLOEXEC as usize);
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
        .map(Fd::from_raw_fd)
}

pub fn bind_netlink(fd: Fd, sockaddr: &NetlinkSocketAddr) -> Result<(), Errno> {
    let sockaddr_ptr = sockaddr as *const NetlinkSocketAddr as *const sockaddr;
    let sockaddr_len = mem::size_of::<NetlinkSocketAddr>();
    Errno::result(unsafe { libc::bind(fd.as_raw_fd(), sockaddr_ptr, sockaddr_len as socklen_t) })
        .map(drop)
}

pub fn read(fd: Fd, buf: &mut [u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_mut_ptr() as *mut c_void;
    let buf_len = buf.len();
    Errno::result(unsafe { libc::read(fd.as_raw_fd(), buf_ptr, buf_len) }).map(|ret| ret as usize)
}

pub fn write(fd: Fd, buf: &[u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_ptr() as *const c_void;
    let buf_len = buf.len();
    Errno::result(unsafe { libc::write(fd.as_raw_fd(), buf_ptr, buf_len) }).map(|ret| ret as usize)
}

#[derive(Clone, Copy, Default)]
pub struct CloseRangeFlags(usize);

impl CloseRangeFlags {
    pub const CLOEXEC: Self = Self(libc::CLOSE_RANGE_CLOEXEC as usize);
}

#[derive(Clone, Copy)]
pub enum CloseRangeFirst {
    AfterStderr,
    Fd(Fd),
}

#[derive(Clone, Copy)]
pub enum CloseRangeLast {
    Max,
    Fd(Fd),
}

pub fn close_range(
    first: CloseRangeFirst,
    last: CloseRangeLast,
    flags: CloseRangeFlags,
) -> Result<(), Errno> {
    let first = match first {
        CloseRangeFirst::AfterStderr => (libc::STDERR_FILENO + 1) as c_uint,
        CloseRangeFirst::Fd(fd) => fd.as_c_uint(),
    };
    let last = match last {
        CloseRangeLast::Max => c_uint::MAX,
        CloseRangeLast::Fd(fd) => fd.as_c_uint(),
    };
    Errno::result(unsafe { libc::close_range(first, last, flags.0 as c_int) }).map(drop)
}

pub fn setsid() -> Result<(), Errno> {
    Errno::result(unsafe { libc::setsid() }).map(drop)
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct MountFlags(usize);

impl MountFlags {
    pub const BIND: Self = Self(libc::MS_BIND as usize);
    pub const REMOUNT: Self = Self(libc::MS_REMOUNT as usize);
    pub const RDONLY: Self = Self(libc::MS_RDONLY as usize);
    pub const NOSUID: Self = Self(libc::MS_NOSUID as usize);
    pub const NOEXEC: Self = Self(libc::MS_NOEXEC as usize);
    pub const NODEV: Self = Self(libc::MS_NODEV as usize);
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
            source_ptr as *const c_char,
            target_ptr as *const c_char,
            fstype_ptr as *const c_char,
            flags.0 as c_ulong,
            data_ptr as *const c_void,
        )
    })
    .map(drop)
}

pub fn chdir(path: &CStr) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    Errno::result(unsafe { libc::chdir(path_ptr as *const c_char) }).map(drop)
}

pub fn mkdir(path: &CStr, mode: FileMode) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    Errno::result(unsafe { libc::mkdir(path_ptr as *const c_char, mode.0 as mode_t) }).map(drop)
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
    pub const DETACH: Self = Self(libc::MNT_DETACH as usize);
}

pub fn umount2(path: &CStr, flags: UmountFlags) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    Errno::result(unsafe { libc::umount2(path_ptr as *const c_char, flags.0 as c_int) }).map(drop)
}

pub fn execve(path: &CStr, argv: &[Option<&u8>], envp: &[Option<&u8>]) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr() as *const c_char;
    let argv_ptr = argv.as_ptr() as *const *const c_char;
    let envp_ptr = envp.as_ptr() as *const *const c_char;
    Errno::result(unsafe { libc::execve(path_ptr, argv_ptr, envp_ptr) }).map(drop)
}

pub fn _exit(status: usize) -> ! {
    unsafe { libc::_exit(status as c_int) };
}

#[derive(Clone, Copy, Default, Display, Into)]
pub struct Signal(u8);

impl Signal {
    pub const CHLD: Self = Self(libc::SIGCHLD as u8);
    pub const KILL: Self = Self(libc::SIGKILL as u8);
}

impl From<Signal> for i32 {
    fn from(signo: Signal) -> i32 {
        signo.0.into()
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct CloneFlags(u64);

impl CloneFlags {
    pub const CLEAR_SIGHAND: Self = Self(libc::CLONE_CLEAR_SIGHAND as u64);
    pub const FILES: Self = Self(libc::CLONE_FILES as u64);
    pub const FS: Self = Self(libc::CLONE_FS as u64);
    pub const NEWCGROUP: Self = Self(libc::CLONE_NEWCGROUP as u64);
    pub const NEWIPC: Self = Self(libc::CLONE_NEWIPC as u64);
    pub const NEWNET: Self = Self(libc::CLONE_NEWNET as u64);
    pub const NEWNS: Self = Self(libc::CLONE_NEWNS as u64);
    pub const NEWPID: Self = Self(libc::CLONE_NEWPID as u64);
    pub const NEWUSER: Self = Self(libc::CLONE_NEWUSER as u64);
}

#[derive(Clone)]
pub struct CloneArgs(libc::clone_args);

impl Default for CloneArgs {
    fn default() -> Self {
        unsafe { mem::zeroed() }
    }
}

impl CloneArgs {
    pub fn flags(self, flags: CloneFlags) -> Self {
        Self(libc::clone_args {
            flags: flags.0,
            ..self.0
        })
    }

    pub fn exit_signal(self, signal: Signal) -> Self {
        Self(libc::clone_args {
            exit_signal: signal.0 as u64,
            ..self.0
        })
    }
}

pub type Pid = pid_t;

pub fn clone3(args: &mut CloneArgs) -> Result<Option<Pid>, Errno> {
    let args_ptr = args as *mut CloneArgs as *mut c_void;
    let size = mem::size_of::<CloneArgs>() as size_t;
    Errno::result(unsafe { libc::syscall(libc::SYS_clone3, args_ptr, size) }).map(|ret| {
        if ret == 0 {
            None
        } else {
            Some(ret as Pid)
        }
    })
}

pub fn pidfd_open(pid: Pid) -> Result<Fd, Errno> {
    Errno::result(unsafe { libc::syscall(libc::SYS_pidfd_open, pid as pid_t, 0 as c_uint) })
        .map(Fd::from_c_long)
}

pub fn close(fd: Fd) -> Result<(), Errno> {
    Errno::result(unsafe { libc::close(fd.as_raw_fd()) }).map(drop)
}

pub fn prctl_set_pdeathsig(signal: Signal) -> Result<(), Errno> {
    Errno::result(unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, signal.0 as c_ulong) }).map(drop)
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct Timespec {
    sec: time_t,
    nsec: c_long, // This doesn't work for x86_64 with a target_pointer_width of 32.
}

impl From<Duration> for Timespec {
    fn from(duration: Duration) -> Self {
        Timespec {
            sec: duration.as_secs() as time_t,
            nsec: duration.subsec_nanos() as c_long,
        }
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct PollEvents(i16);

impl PollEvents {
    pub const IN: Self = Self(libc::POLLIN);
}

#[repr(C)]
pub struct PollFd(pollfd);

impl PollFd {
    pub fn new(fd: Fd, events: PollEvents) -> Self {
        PollFd(pollfd {
            fd: fd.as_raw_fd(),
            events: events.0,
            revents: 0,
        })
    }
}

pub fn poll(fds: &mut [PollFd], timeout: Duration) -> Result<usize, Errno> {
    let fds_ptr = fds.as_mut_ptr();
    let nfds = fds.len();
    let timeout = timeout.as_millis();
    Errno::result(unsafe { libc::poll(fds_ptr as *mut pollfd, nfds as nfds_t, timeout as c_int) })
        .map(|ret| ret as usize)
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

pub type Uid = uid_t;

pub fn getuid() -> Uid {
    unsafe { libc::getuid() }
}

pub type Gid = gid_t;

pub fn getgid() -> Gid {
    unsafe { libc::getgid() }
}

pub fn pipe() -> Result<(Fd, Fd), Errno> {
    let mut fds: [c_int; 2] = [0; 2];
    let fds_ptr = fds.as_mut_ptr() as *mut c_int;
    Errno::result(unsafe { libc::pipe(fds_ptr) })
        .map(|_| (Fd::from_raw_fd(fds[0]), Fd::from_raw_fd(fds[1])))
}

pub fn fcntl_setfl(fd: Fd, flags: OpenFlags) -> Result<(), Errno> {
    Errno::result(unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL as c_int, flags.0 as c_int) })
        .map(drop)
}
