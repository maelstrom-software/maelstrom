//! Function wrappers for Linux syscalls.
#![no_std]

#[cfg(any(test, feature = "std"))]
extern crate std;

use core::{ffi::CStr, fmt, mem, ptr, time::Duration};
use derive_more::{BitOr, Display, Into};
use libc::{
    c_char, c_int, c_long, c_short, c_uint, c_ulong, c_void, gid_t, id_t, idtype_t, mode_t, nfds_t,
    pid_t, pollfd, sa_family_t, siginfo_t, size_t, sockaddr, socklen_t, uid_t,
};

#[cfg(any(test, feature = "std"))]
use std::os::fd;

extern "C" {
    fn sigabbrev_np(sig: c_int) -> *const c_char;
    fn strerrorname_np(errnum: c_int) -> *const c_char;
    fn strerrordesc_np(errnum: c_int) -> *const c_char;
}

#[derive(Clone)]
#[repr(transparent)]
pub struct CloneArgs(libc::clone_args);

impl Default for CloneArgs {
    fn default() -> Self {
        unsafe { mem::zeroed() }
    }
}

impl CloneArgs {
    pub fn flags(mut self, flags: CloneFlags) -> Self {
        self.0.flags = flags.as_u64();
        self
    }

    pub fn exit_signal(mut self, signal: Signal) -> Self {
        self.0.exit_signal = signal.as_u64();
        self
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct CloneFlags(c_int);

impl CloneFlags {
    pub const CLEAR_SIGHAND: Self = Self(libc::CLONE_CLEAR_SIGHAND);
    pub const FILES: Self = Self(libc::CLONE_FILES);
    pub const FS: Self = Self(libc::CLONE_FS);
    pub const NEWCGROUP: Self = Self(libc::CLONE_NEWCGROUP);
    pub const NEWIPC: Self = Self(libc::CLONE_NEWIPC);
    pub const NEWNET: Self = Self(libc::CLONE_NEWNET);
    pub const NEWNS: Self = Self(libc::CLONE_NEWNS);
    pub const NEWPID: Self = Self(libc::CLONE_NEWPID);
    pub const NEWUSER: Self = Self(libc::CLONE_NEWUSER);

    fn as_u64(&self) -> u64 {
        self.0.try_into().unwrap()
    }
}

#[derive(Clone, Copy, Default)]
pub struct CloseRangeFlags(c_uint);

impl CloseRangeFlags {
    pub const CLOEXEC: Self = Self(libc::CLOSE_RANGE_CLOEXEC);

    // The documentation for close_range(2) says it takes an unsigned int flags parameter. The
    // flags are defined as unsigned ints as well. However, the close_range wrapper we get from the
    // libc crate expects a signed int for the flags parameter.
    fn as_c_int(&self) -> c_int {
        self.0.try_into().unwrap()
    }
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

pub struct Errno(c_int);

impl Errno {
    pub fn from_u64(errno: u64) -> Self {
        Errno(errno.try_into().unwrap())
    }

    pub fn as_u64(&self) -> u64 {
        self.0.try_into().unwrap()
    }

    pub fn as_i32(&self) -> i32 {
        self.0
    }

    pub fn name(&self) -> Option<&'static str> {
        let errno = unsafe { strerrorname_np(self.0) };
        (!errno.is_null()).then(|| unsafe { CStr::from_ptr(errno) }.to_str().unwrap())
    }

    pub fn desc(&self) -> Option<&'static str> {
        let errno = unsafe { strerrordesc_np(self.0) };
        (!errno.is_null()).then(|| unsafe { CStr::from_ptr(errno) }.to_str().unwrap())
    }

    /// Returns `Ok(value)` if it does not contain the sentinel value. This
    /// should not be used when `-1` is not the errno sentinel value.
    fn result<S: ErrnoSentinel + PartialEq<S>>(value: S) -> Result<S, Errno> {
        if value == S::sentinel() {
            Err(Errno(unsafe { *libc::__errno_location() }))
        } else {
            Ok(value)
        }
    }

    pub const ENOSYS: Self = Self(libc::ENOSYS);
    pub const EPERM: Self = Self(libc::EPERM);
    pub const ENOENT: Self = Self(libc::ENOENT);
    pub const ESRCH: Self = Self(libc::ESRCH);
    pub const EINTR: Self = Self(libc::EINTR);
    pub const EIO: Self = Self(libc::EIO);
    pub const ENXIO: Self = Self(libc::ENXIO);
    pub const E2BIG: Self = Self(libc::E2BIG);
    pub const ENOEXEC: Self = Self(libc::ENOEXEC);
    pub const EBADF: Self = Self(libc::EBADF);
    pub const ECHILD: Self = Self(libc::ECHILD);
    pub const EAGAIN: Self = Self(libc::EAGAIN);
    pub const ENOMEM: Self = Self(libc::ENOMEM);
    pub const EACCES: Self = Self(libc::EACCES);
    pub const EFAULT: Self = Self(libc::EFAULT);
    pub const ENOTBLK: Self = Self(libc::ENOTBLK);
    pub const EBUSY: Self = Self(libc::EBUSY);
    pub const EEXIST: Self = Self(libc::EEXIST);
    pub const EXDEV: Self = Self(libc::EXDEV);
    pub const ENODEV: Self = Self(libc::ENODEV);
    pub const ENOTDIR: Self = Self(libc::ENOTDIR);
    pub const EISDIR: Self = Self(libc::EISDIR);
    pub const EINVAL: Self = Self(libc::EINVAL);
    pub const ENFILE: Self = Self(libc::ENFILE);
    pub const EMFILE: Self = Self(libc::EMFILE);
    pub const ENOTTY: Self = Self(libc::ENOTTY);
    pub const ETXTBSY: Self = Self(libc::ETXTBSY);
    pub const EFBIG: Self = Self(libc::EFBIG);
    pub const ENOSPC: Self = Self(libc::ENOSPC);
    pub const ESPIPE: Self = Self(libc::ESPIPE);
    pub const EROFS: Self = Self(libc::EROFS);
    pub const EMLINK: Self = Self(libc::EMLINK);
    pub const EPIPE: Self = Self(libc::EPIPE);
    pub const EDOM: Self = Self(libc::EDOM);
    pub const ERANGE: Self = Self(libc::ERANGE);
    pub const EWOULDBLOCK: Self = Self::EAGAIN;
}

impl fmt::Debug for Errno {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => write!(f, "{name}"),
            None => write!(f, "UNKNOWN({})", self.0),
        }
    }
}

impl fmt::Display for Errno {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.name(), self.desc()) {
            (Some(name), Some(desc)) => {
                write!(f, "{name}: {desc}")
            }
            _ => {
                write!(f, "{}: Unknown error", self.0)
            }
        }
    }
}

#[cfg(any(test, feature = "std"))]
impl std::error::Error for Errno {}

/// The sentinel value indicates that a function failed and more detailed
/// information about the error can be found in `errno`
pub trait ErrnoSentinel: Sized {
    fn sentinel() -> Self;
}

impl ErrnoSentinel for isize {
    fn sentinel() -> Self {
        -1
    }
}

impl ErrnoSentinel for i32 {
    fn sentinel() -> Self {
        -1
    }
}

impl ErrnoSentinel for i64 {
    fn sentinel() -> Self {
        -1
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ExitCode(c_int);

impl ExitCode {
    // Only 8 bits of exit code is actually stored by the kernel.
    pub fn from_u8(code: u8) -> Self {
        Self(code.into())
    }

    pub fn as_u8(&self) -> u8 {
        self.0 as u8
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Fd(c_int);

impl Fd {
    pub const STDIN: Self = Self(libc::STDIN_FILENO);
    pub const STDOUT: Self = Self(libc::STDOUT_FILENO);
    pub const STDERR: Self = Self(libc::STDERR_FILENO);

    fn from_c_long(fd: c_long) -> Self {
        Self(fd.try_into().unwrap())
    }

    fn as_c_uint(self) -> c_uint {
        self.0.try_into().unwrap()
    }

    pub fn from_raw(raw: c_int) -> Self {
        Self(raw)
    }

    pub fn as_c_int(self) -> c_int {
        self.0
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct FileMode(mode_t);

impl FileMode {
    pub const SUID: Self = Self(libc::S_ISUID);
    pub const SGID: Self = Self(libc::S_ISGID);
    pub const SVTX: Self = Self(libc::S_ISVTX);

    pub const RWXU: Self = Self(libc::S_IRWXU);
    pub const RUSR: Self = Self(libc::S_IRUSR);
    pub const WUSR: Self = Self(libc::S_IWUSR);
    pub const XUSR: Self = Self(libc::S_IXUSR);

    pub const RWXG: Self = Self(libc::S_IRWXG);
    pub const RGRP: Self = Self(libc::S_IRGRP);
    pub const WGRP: Self = Self(libc::S_IWGRP);
    pub const XGRP: Self = Self(libc::S_IXGRP);

    pub const RWXO: Self = Self(libc::S_IRWXO);
    pub const ROTH: Self = Self(libc::S_IROTH);
    pub const WOTH: Self = Self(libc::S_IWOTH);
    pub const XOTH: Self = Self(libc::S_IXOTH);
}

#[derive(Clone, Copy, Display)]
pub struct Gid(gid_t);

impl Gid {
    pub fn as_u32(&self) -> u32 {
        self.0
    }

    pub fn from_u32(v: u32) -> Self {
        Self(v)
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct MountFlags(c_ulong);

impl MountFlags {
    pub const BIND: Self = Self(libc::MS_BIND);
    pub const REMOUNT: Self = Self(libc::MS_REMOUNT);
    pub const RDONLY: Self = Self(libc::MS_RDONLY);
    pub const NOSUID: Self = Self(libc::MS_NOSUID);
    pub const NOEXEC: Self = Self(libc::MS_NOEXEC);
    pub const NODEV: Self = Self(libc::MS_NODEV);
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
pub struct OpenFlags(c_int);

impl OpenFlags {
    pub const RDWR: Self = Self(libc::O_RDWR);
    pub const WRONLY: Self = Self(libc::O_WRONLY);
    pub const TRUNC: Self = Self(libc::O_TRUNC);
    pub const NONBLOCK: Self = Self(libc::O_NONBLOCK);
}

pub struct OwnedFd(Fd);

impl OwnedFd {
    pub fn from_fd(fd: Fd) -> Self {
        Self(fd)
    }

    pub fn as_fd(&self) -> Fd {
        self.0
    }

    pub fn into_fd(self) -> Fd {
        let raw_fd = self.0;
        mem::forget(self);
        raw_fd
    }
}

#[cfg(any(test, feature = "std"))]
impl fd::AsRawFd for OwnedFd {
    fn as_raw_fd(&self) -> fd::RawFd {
        self.0 .0
    }
}

#[cfg(any(test, feature = "std"))]
impl From<OwnedFd> for fd::OwnedFd {
    fn from(owned_fd: OwnedFd) -> Self {
        let raw_fd = owned_fd.into_fd();
        unsafe { <Self as fd::FromRawFd>::from_raw_fd(raw_fd.0) }
    }
}

#[cfg(any(test, feature = "std"))]
impl From<fd::OwnedFd> for OwnedFd {
    fn from(owned_fd: fd::OwnedFd) -> Self {
        let raw_fd = fd::IntoRawFd::into_raw_fd(owned_fd);
        Self(Fd(raw_fd))
    }
}

impl Drop for OwnedFd {
    fn drop(&mut self) {
        // Just ignore the return value from close.
        unsafe { libc::close(self.0 .0) };
    }
}

#[derive(Clone, Copy, Debug, Display, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Pid(pid_t);

impl Pid {
    fn from_c_long(pid: c_long) -> Self {
        Self(pid.try_into().unwrap())
    }

    #[cfg(feature = "test")]
    pub fn new_for_test(pid: pid_t) -> Self {
        Self(pid)
    }

    pub fn as_i32(self) -> i32 {
        self.0
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct PollEvents(c_short);

impl PollEvents {
    pub const IN: Self = Self(libc::POLLIN);
}

#[repr(transparent)]
pub struct PollFd(pollfd);

impl PollFd {
    pub fn new(fd: Fd, events: PollEvents) -> Self {
        PollFd(pollfd {
            fd: fd.0,
            events: events.0,
            revents: 0,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Into)]
pub struct Signal(c_int);

impl Signal {
    pub const CHLD: Self = Self(libc::SIGCHLD);
    pub const KILL: Self = Self(libc::SIGKILL);

    pub fn as_u8(&self) -> u8 {
        self.0.try_into().unwrap()
    }

    fn as_c_ulong(&self) -> c_ulong {
        self.0.try_into().unwrap()
    }

    fn as_u64(&self) -> u64 {
        self.0.try_into().unwrap()
    }
}

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let abbrev = unsafe { sigabbrev_np(self.0) };
        if abbrev.is_null() {
            write!(f, "Invalid Signal {}", self.0)
        } else {
            let abbrev = unsafe { CStr::from_ptr(abbrev) }.to_str().unwrap();
            write!(f, "SIG{abbrev}")
        }
    }
}

#[derive(Clone, Copy)]
pub struct SocketDomain(c_int);

impl SocketDomain {
    pub const NETLINK: Self = Self(libc::PF_NETLINK);
}

#[derive(Clone, Copy)]
pub struct SocketProtocol(c_int);

impl SocketProtocol {
    pub const NETLINK_ROUTE: Self = Self(0);
}

#[derive(BitOr, Clone, Copy)]
pub struct SocketType(c_int);

impl SocketType {
    pub const RAW: Self = Self(libc::SOCK_RAW);
}

#[derive(Clone, Copy, Display)]
pub struct Uid(uid_t);

impl Uid {
    pub fn as_u32(&self) -> u32 {
        self.0
    }

    pub fn from_u32(v: u32) -> Self {
        Self(v)
    }
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct UmountFlags(c_int);

impl UmountFlags {
    pub const DETACH: Self = Self(libc::MNT_DETACH);
}

#[derive(Clone, Copy)]
pub struct WaitResult {
    pub pid: Pid,
    pub status: WaitStatus,
}

#[derive(Clone, Copy, Debug)]
pub enum WaitStatus {
    Exited(ExitCode),
    Signaled(Signal),
}

pub fn bind_netlink(fd: Fd, sockaddr: &NetlinkSocketAddr) -> Result<(), Errno> {
    let sockaddr_ptr = sockaddr as *const NetlinkSocketAddr as *const sockaddr;
    let sockaddr_len = mem::size_of::<NetlinkSocketAddr>() as socklen_t;
    Errno::result(unsafe { libc::bind(fd.0, sockaddr_ptr, sockaddr_len) }).map(drop)
}

pub fn chdir(path: &CStr) -> Result<(), Errno> {
    let path_ptr = path.as_ptr();
    Errno::result(unsafe { libc::chdir(path_ptr) }).map(drop)
}

pub fn clone3(args: &mut CloneArgs) -> Result<Option<Pid>, Errno> {
    let args_ptr = args as *mut CloneArgs;
    let size = mem::size_of::<CloneArgs>() as size_t;
    let ret = Errno::result(unsafe { libc::syscall(libc::SYS_clone3, args_ptr, size) })?;
    Ok(if ret == 0 {
        None
    } else {
        Some(Pid::from_c_long(ret))
    })
}

pub fn clone3_with_child_pidfd(args: &mut CloneArgs) -> Result<Option<(Pid, OwnedFd)>, Errno> {
    let inner = |args: &mut CloneArgs, child_pidfd: &mut Fd| {
        assert_eq!(args.0.flags & (libc::CLONE_PIDFD as c_ulong), 0);
        args.0.flags |= libc::CLONE_PIDFD as c_ulong;
        args.0.pidfd = child_pidfd as *mut Fd as u64;
        let args_ptr = args as *mut CloneArgs;
        let size = mem::size_of::<CloneArgs>() as size_t;
        unsafe { libc::syscall(libc::SYS_clone3, args_ptr, size) }
    };
    let mut child_pidfd = Fd(0);
    let ret = Errno::result(inner(args, &mut child_pidfd))?;
    Ok(if ret == 0 {
        None
    } else {
        Some((Pid::from_c_long(ret), OwnedFd(child_pidfd)))
    })
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
    let flags = flags.as_c_int();
    Errno::result(unsafe { libc::close_range(first, last, flags) }).map(drop)
}

pub fn dup2(from: Fd, to: Fd) -> Result<Fd, Errno> {
    Errno::result(unsafe { libc::dup2(from.0, to.0) }).map(Fd)
}

pub fn execve(path: &CStr, argv: &[Option<&u8>], envp: &[Option<&u8>]) -> Result<(), Errno> {
    let path_ptr = path.as_ptr();
    let argv_ptr = argv.as_ptr() as *const *const c_char;
    let envp_ptr = envp.as_ptr() as *const *const c_char;
    Errno::result(unsafe { libc::execve(path_ptr, argv_ptr, envp_ptr) }).map(drop)
}

pub fn fcntl_setfl(fd: Fd, flags: OpenFlags) -> Result<(), Errno> {
    Errno::result(unsafe { libc::fcntl(fd.0, libc::F_SETFL, flags.0) }).map(drop)
}

pub fn getgid() -> Gid {
    Gid(unsafe { libc::getgid() })
}

pub fn getpid() -> Pid {
    Pid(unsafe { libc::getpid() })
}

pub fn getuid() -> Uid {
    Uid(unsafe { libc::getuid() })
}

pub fn kill(pid: Pid, signal: Signal) -> Result<(), Errno> {
    Errno::result(unsafe { libc::kill(pid.0, signal.0) }).map(drop)
}

pub fn mkdir(path: &CStr, mode: FileMode) -> Result<(), Errno> {
    let path_ptr = path.as_ptr();
    Errno::result(unsafe { libc::mkdir(path_ptr, mode.0) }).map(drop)
}

pub fn mount(
    source: Option<&CStr>,
    target: &CStr,
    fstype: Option<&CStr>,
    flags: MountFlags,
    data: Option<&[u8]>,
) -> Result<(), Errno> {
    let source_ptr = source.map(CStr::as_ptr).unwrap_or(ptr::null());
    let target_ptr = target.as_ptr();
    let fstype_ptr = fstype.map(CStr::as_ptr).unwrap_or(ptr::null());
    let data_ptr = data.map(|r| r.as_ptr()).unwrap_or(ptr::null()) as *const c_void;
    Errno::result(unsafe { libc::mount(source_ptr, target_ptr, fstype_ptr, flags.0, data_ptr) })
        .map(drop)
}

pub fn open(path: &CStr, flags: OpenFlags, mode: FileMode) -> Result<OwnedFd, Errno> {
    let path_ptr = path.as_ptr();
    let fd = Errno::result(unsafe { libc::open(path_ptr, flags.0, mode.0) })
        .map(Fd)
        .map(OwnedFd)?;
    Ok(fd)
}

pub fn pause() {
    unsafe { libc::pause() };
}

pub fn pidfd_open(pid: Pid) -> Result<OwnedFd, Errno> {
    let flags = 0 as c_uint;
    Errno::result(unsafe { libc::syscall(libc::SYS_pidfd_open, pid.0, flags) })
        .map(Fd::from_c_long)
        .map(OwnedFd)
}

pub fn pidfd_send_signal(pidfd: Fd, signal: Signal) -> Result<(), Errno> {
    let info: *const siginfo_t = ptr::null();
    let flags = 0 as c_uint;
    Errno::result(unsafe {
        libc::syscall(libc::SYS_pidfd_send_signal, pidfd.0, signal.0, info, flags)
    })
    .map(drop)
}

pub fn pipe() -> Result<(OwnedFd, OwnedFd), Errno> {
    let mut fds: [c_int; 2] = [0; 2];
    let fds_ptr = fds.as_mut_ptr() as *mut c_int;
    Errno::result(unsafe { libc::pipe(fds_ptr) })
        .map(|_| (OwnedFd(Fd(fds[0])), OwnedFd(Fd(fds[1]))))
}

pub fn pivot_root(new_root: &CStr, put_old: &CStr) -> Result<(), Errno> {
    let new_root_ptr = new_root.as_ptr();
    let put_old_ptr = put_old.as_ptr();
    Errno::result(unsafe { libc::syscall(libc::SYS_pivot_root, new_root_ptr, put_old_ptr) })
        .map(drop)
}

pub fn poll(fds: &mut [PollFd], timeout: Duration) -> Result<usize, Errno> {
    let fds_ptr = fds.as_mut_ptr() as *mut pollfd;
    let nfds = fds.len() as nfds_t;
    let timeout = c_int::try_from(timeout.as_millis()).unwrap();
    Errno::result(unsafe { libc::poll(fds_ptr, nfds, timeout) }).map(|ret| ret as usize)
}

pub fn prctl_set_pdeathsig(signal: Signal) -> Result<(), Errno> {
    let signal = signal.as_c_ulong();
    Errno::result(unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, signal) }).map(drop)
}

pub fn raise(signal: Signal) -> Result<(), Errno> {
    Errno::result(unsafe { libc::raise(signal.0) }).map(drop)
}

pub fn read(fd: Fd, buf: &mut [u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_mut_ptr() as *mut c_void;
    let buf_len = buf.len();
    Errno::result(unsafe { libc::read(fd.0, buf_ptr, buf_len) }).map(|ret| ret as usize)
}

pub fn setsid() -> Result<(), Errno> {
    Errno::result(unsafe { libc::setsid() }).map(drop)
}

pub fn socket(
    domain: SocketDomain,
    type_: SocketType,
    protocol: SocketProtocol,
) -> Result<OwnedFd, Errno> {
    Errno::result(unsafe { libc::socket(domain.0, type_.0, protocol.0) })
        .map(Fd)
        .map(OwnedFd)
}

pub fn umount2(path: &CStr, flags: UmountFlags) -> Result<(), Errno> {
    let path_ptr = path.as_ptr();
    Errno::result(unsafe { libc::umount2(path_ptr, flags.0) }).map(drop)
}

pub fn _exit(status: ExitCode) -> ! {
    unsafe { libc::_exit(status.0) };
}

fn extract_wait_status(status: c_int) -> WaitStatus {
    if libc::WIFEXITED(status) {
        WaitStatus::Exited(ExitCode(libc::WEXITSTATUS(status)))
    } else if libc::WIFSIGNALED(status) {
        WaitStatus::Signaled(Signal(libc::WTERMSIG(status)))
    } else {
        panic!("neither WIFEXITED nor WIFSIGNALED true on wait status {status}");
    }
}

fn extract_wait_status_from_siginfo(siginfo: siginfo_t) -> WaitStatus {
    let status = unsafe { siginfo.si_status() };
    match siginfo.si_code {
        libc::CLD_EXITED => WaitStatus::Exited(ExitCode(status)),
        libc::CLD_KILLED | libc::CLD_DUMPED => WaitStatus::Signaled(Signal(status)),
        code => panic!("siginfo's si_code was {code} instead of CLD_EXITED or CLD_KILLED"),
    }
}

pub fn fork() -> Result<Option<Pid>, Errno> {
    Errno::result(unsafe { libc::fork() }).map(|p| (p != 0).then_some(Pid(p)))
}

pub fn wait() -> Result<WaitResult, Errno> {
    let inner = |status: &mut c_int| {
        let status_ptr = status as *mut c_int;
        unsafe { libc::wait(status_ptr) }
    };
    let mut status = 0;
    Errno::result(inner(&mut status)).map(|pid| WaitResult {
        pid: Pid(pid),
        status: extract_wait_status(status),
    })
}

pub fn waitpid(pid: Pid) -> Result<WaitStatus, Errno> {
    let inner = |status: &mut c_int| {
        let status_ptr = status as *mut c_int;
        let flags = 0 as c_int;
        unsafe { libc::waitpid(pid.0, status_ptr, flags) }
    };
    let mut status = 0;
    Errno::result(inner(&mut status)).map(|_| extract_wait_status(status))
}

pub fn waitid(pidfd: Fd) -> Result<WaitStatus, Errno> {
    let inner = |siginfo: &mut siginfo_t| {
        let idtype = libc::P_PIDFD as idtype_t;
        let id = pidfd.0 as id_t;
        let siginfo_ptr = siginfo as *mut siginfo_t;
        let options = libc::WEXITED;
        unsafe { libc::waitid(idtype, id, siginfo_ptr, options) }
    };
    let mut siginfo = unsafe { mem::zeroed() };
    Errno::result(inner(&mut siginfo)).map(|_| extract_wait_status_from_siginfo(siginfo))
}

pub fn write(fd: Fd, buf: &[u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_ptr() as *const c_void;
    let buf_len = buf.len();
    Errno::result(unsafe { libc::write(fd.0, buf_ptr, buf_len) }).map(|ret| ret as usize)
}

fn with_iovec<RetT>(buffer: &[u8], body: impl FnOnce(&libc::iovec) -> RetT) -> RetT {
    let io_vec = libc::iovec {
        iov_base: buffer.as_ptr() as *mut libc::c_void,
        iov_len: buffer.len(),
    };
    body(&io_vec)
}

fn with_iovec_mut<RetT>(buffer: &mut [u8], body: impl FnOnce(&mut libc::iovec) -> RetT) -> RetT {
    let mut io_vec = libc::iovec {
        iov_base: buffer.as_mut_ptr() as *mut libc::c_void,
        iov_len: buffer.len(),
    };
    body(&mut io_vec)
}

// prepare message with enough cmsg space for a file descriptor
const CMSG_BUFFER_LEN: c_uint = unsafe { libc::CMSG_SPACE(mem::size_of::<c_int>() as c_uint) };

fn with_msghdr<RetT>(buffer: &[u8], body: impl FnOnce(&mut libc::msghdr) -> RetT) -> RetT {
    let mut cmsg_buffer = [0u8; CMSG_BUFFER_LEN as usize];

    with_iovec(buffer, |io_vec| {
        let mut message = libc::msghdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: io_vec as *const _ as *mut _,
            msg_iovlen: 1,
            msg_control: cmsg_buffer.as_mut_ptr() as *mut libc::c_void,
            msg_controllen: cmsg_buffer.len(),
            msg_flags: 0,
        };
        body(&mut message)
    })
}

fn with_msghdr_mut<RetT>(buffer: &mut [u8], body: impl FnOnce(&mut libc::msghdr) -> RetT) -> RetT {
    let mut cmsg_buffer = [0u8; CMSG_BUFFER_LEN as usize];

    with_iovec_mut(buffer, |io_vec| {
        let mut message = libc::msghdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: io_vec,
            msg_iovlen: 1,
            msg_control: cmsg_buffer.as_mut_ptr() as *mut libc::c_void,
            msg_controllen: cmsg_buffer.len(),
            msg_flags: 0,
        };
        body(&mut message)
    })
}

pub struct UnixStream {
    fd: OwnedFd,
}

#[cfg(any(test, feature = "std"))]
impl fd::AsRawFd for UnixStream {
    fn as_raw_fd(&self) -> fd::RawFd {
        self.fd.as_raw_fd()
    }
}

impl From<OwnedFd> for UnixStream {
    fn from(fd: OwnedFd) -> Self {
        Self { fd }
    }
}

impl UnixStream {
    pub fn pair() -> Result<(Self, Self), Errno> {
        let mut fds = [0, 0];

        Errno::result(unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
                0,
                fds.as_mut_ptr(),
            )
        })?;

        Ok((
            OwnedFd::from_fd(Fd(fds[0])).into(),
            OwnedFd::from_fd(Fd(fds[1])).into(),
        ))
    }

    pub fn as_fd(&self) -> Fd {
        self.fd.as_fd()
    }

    pub fn recv_with_fd(&self, buffer: &mut [u8]) -> Result<(usize, Option<OwnedFd>), Errno> {
        with_msghdr_mut(buffer, |message| {
            let count = Errno::result(unsafe {
                libc::recvmsg(self.fd.as_fd().0, message, 0 /* flags */)
            })? as usize;

            assert_eq!(message.msg_flags & libc::MSG_TRUNC, 0);
            assert_eq!(message.msg_flags & libc::MSG_CTRUNC, 0);

            // See if we got a file descriptor, only checking the first message
            let mut fd_out = None;
            let control_msg = unsafe { libc::CMSG_FIRSTHDR(message) };
            if !control_msg.is_null() && unsafe { (*control_msg).cmsg_type == libc::SCM_RIGHTS } {
                let fd_data = unsafe { libc::CMSG_DATA(control_msg) };

                let fd = unsafe { *(fd_data as *const c_int) };
                fd_out = Some(OwnedFd(Fd(fd)));
            }

            Ok((count, fd_out))
        })
    }

    pub fn send(&self, buffer: &[u8]) -> Result<usize, Errno> {
        Ok(Errno::result(unsafe {
            libc::send(
                self.fd.as_fd().0,
                buffer.as_ptr() as *const c_void,
                buffer.len(),
                0, /* flags */
            )
        })? as usize)
    }

    pub fn send_with_fd(&self, buffer: &[u8], fd: Fd) -> Result<usize, Errno> {
        with_msghdr(buffer, |message| {
            let control_msg = unsafe { &mut *libc::CMSG_FIRSTHDR(message) };
            control_msg.cmsg_level = libc::SOL_SOCKET;
            control_msg.cmsg_type = libc::SCM_RIGHTS;
            control_msg.cmsg_len =
                unsafe { libc::CMSG_LEN(mem::size_of::<c_int>() as libc::c_uint) } as libc::size_t;

            let fd_data = unsafe { &mut *(libc::CMSG_DATA(control_msg) as *mut c_int) };
            *fd_data = fd.0;

            Ok(Errno::result(unsafe { libc::sendmsg(self.fd.as_fd().0, message, 0) })? as usize)
        })
    }

    pub fn shutdown(&self) -> Result<(), Errno> {
        Errno::result(unsafe { libc::shutdown(self.fd.as_fd().0, libc::SHUT_RDWR) }).map(drop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_display() {
        assert_eq!(std::format!("{}", Signal::CHLD).as_str(), "SIGCHLD",);
    }

    #[test]
    fn invalid_signal_display() {
        assert_eq!(
            std::format!("{}", Signal(1234)).as_str(),
            "Invalid Signal 1234",
        );
    }

    #[test]
    fn pid_display() {
        assert_eq!(std::format!("{}", Pid(1234)).as_str(), "1234");
    }

    #[test]
    fn uid_display() {
        assert_eq!(std::format!("{}", Uid(1234)).as_str(), "1234");
    }

    #[test]
    fn gid_display() {
        assert_eq!(std::format!("{}", Gid(1234)).as_str(), "1234");
    }

    #[test]
    fn errno_display() {
        assert_eq!(
            std::format!("{}", Errno(libc::EPERM)).as_str(),
            "EPERM: Operation not permitted"
        );
    }

    #[test]
    fn invalid_errno_display() {
        assert_eq!(
            std::format!("{}", Errno(1234)).as_str(),
            "1234: Unknown error"
        );
    }

    #[test]
    fn errno_debug() {
        assert_eq!(std::format!("{:?}", Errno(libc::EPERM)).as_str(), "EPERM");
    }

    #[test]
    fn invalid_errno_debug() {
        assert_eq!(std::format!("{:?}", Errno(1234)).as_str(), "UNKNOWN(1234)");
    }

    #[test]
    fn unix_stream_send_recv() {
        let (a, b) = UnixStream::pair().unwrap();

        let count = a.send(b"abcdefg").unwrap();
        assert_eq!(count, 7);

        let mut buf = [0; 7];
        let (count, fd) = b.recv_with_fd(&mut buf).unwrap();
        assert_eq!(count, 7);
        assert_eq!(&buf, b"abcdefg");
        assert!(fd.is_none());
    }

    #[test]
    fn unix_stream_send_recv_with_fd() {
        let (a1, a2) = UnixStream::pair().unwrap();

        if clone3(&mut CloneArgs::default()).unwrap().is_none() {
            let (b1, b2) = UnixStream::pair().unwrap();
            a2.send_with_fd(b"boop", b1.as_fd()).unwrap();
            b2.send(b"bop").unwrap();
            return;
        }

        let mut buf = [0; 4];
        let (count, fd) = a1.recv_with_fd(&mut buf).unwrap();
        assert_eq!(count, 4);
        assert_eq!(&buf, b"boop");

        let b1 = UnixStream::from(fd.unwrap());
        let mut buf = [0; 3];
        let (count, fd) = b1.recv_with_fd(&mut buf).unwrap();
        assert_eq!(count, 3);
        assert_eq!(&buf, b"bop");
        assert!(fd.is_none());
    }

    #[test]
    fn unix_stream_send_message_without_then_with_fd() {
        let (a1, a2) = UnixStream::pair().unwrap();

        if clone3(&mut CloneArgs::default()).unwrap().is_none() {
            let (b1, b2) = UnixStream::pair().unwrap();
            a2.send(b"boop").unwrap();
            a2.send_with_fd(b"bing", b1.as_fd()).unwrap();
            b2.send(b"bop").unwrap();
            return;
        }

        let mut buf = [0; 4];
        let (count, fd) = a1.recv_with_fd(&mut buf).unwrap();
        assert_eq!(count, 4);
        assert_eq!(&buf, b"boop");
        assert!(fd.is_none());

        let mut buf = [0; 4];
        let (count, fd) = a1.recv_with_fd(&mut buf).unwrap();
        assert_eq!(count, 4);
        assert_eq!(&buf, b"bing");

        let b1 = UnixStream::from(fd.unwrap());
        let mut buf = [0; 3];
        let (count, fd) = b1.recv_with_fd(&mut buf).unwrap();
        assert_eq!(count, 3);
        assert_eq!(&buf, b"bop");
        assert!(fd.is_none());
    }
}
