//! Function wrappers for Linux syscalls.
#![no_std]

#[cfg(feature = "std")]
extern crate std;

use core::{ffi::CStr, fmt, mem, ptr, time::Duration};
use derive_more::{BitOr, Display, Into};
use libc::{
    c_char, c_int, c_long, c_short, c_uint, c_ulong, c_void, gid_t, mode_t, nfds_t, pid_t, pollfd,
    sa_family_t, size_t, sockaddr, socklen_t, uid_t,
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
pub struct OpenFlags(c_int);

impl OpenFlags {
    pub const WRONLY: Self = Self(libc::O_WRONLY);
    pub const TRUNC: Self = Self(libc::O_TRUNC);
    pub const NONBLOCK: Self = Self(libc::O_NONBLOCK);
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

pub fn open(path: &CStr, flags: OpenFlags, mode: FileMode) -> Result<Fd, Errno> {
    let path = path.to_bytes_with_nul();
    let path_ptr = path.as_ptr() as *const c_char;
    Errno::result(unsafe { libc::open(path_ptr, flags.0, mode.0) }).map(Fd::from_raw_fd)
}

pub fn dup2(from: Fd, to: Fd) -> Result<Fd, Errno> {
    Errno::result(unsafe { libc::dup2(from.0, to.0) }).map(Fd::from_raw_fd)
}

#[derive(Clone, Copy)]
pub struct SocketDomain(c_int);

impl SocketDomain {
    pub const NETLINK: Self = Self(libc::PF_NETLINK);
}

#[derive(BitOr, Clone, Copy)]
pub struct SocketType(c_int);

impl SocketType {
    pub const RAW: Self = Self(libc::SOCK_RAW);
    pub const CLOEXEC: Self = Self(libc::SOCK_CLOEXEC);
}

#[derive(Clone, Copy)]
pub struct SocketProtocol(c_int);

impl SocketProtocol {
    pub const NETLINK_ROUTE: Self = Self(0);
}

pub fn socket(
    domain: SocketDomain,
    type_: SocketType,
    protocol: SocketProtocol,
) -> Result<Fd, Errno> {
    Errno::result(unsafe { libc::socket(domain.0, type_.0, protocol.0) }).map(Fd::from_raw_fd)
}

pub fn bind_netlink(fd: Fd, sockaddr: &NetlinkSocketAddr) -> Result<(), Errno> {
    let sockaddr_ptr = sockaddr as *const NetlinkSocketAddr as *const sockaddr;
    let sockaddr_len = mem::size_of::<NetlinkSocketAddr>();
    Errno::result(unsafe { libc::bind(fd.0, sockaddr_ptr, sockaddr_len as socklen_t) }).map(drop)
}

pub fn read(fd: Fd, buf: &mut [u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_mut_ptr() as *mut c_void;
    let buf_len = buf.len();
    Errno::result(unsafe { libc::read(fd.0, buf_ptr, buf_len) }).map(|ret| ret as usize)
}

pub fn write(fd: Fd, buf: &[u8]) -> Result<usize, Errno> {
    let buf_ptr = buf.as_ptr() as *const c_void;
    let buf_len = buf.len();
    Errno::result(unsafe { libc::write(fd.0, buf_ptr, buf_len) }).map(|ret| ret as usize)
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
    Errno::result(unsafe { libc::close_range(first, last, flags.as_c_int()) }).map(drop)
}

pub fn setsid() -> Result<(), Errno> {
    Errno::result(unsafe { libc::setsid() }).map(drop)
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
            flags.0,
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
    Errno::result(unsafe { libc::mkdir(path_ptr as *const c_char, mode.0) }).map(drop)
}

pub fn pivot_root(new_root: &CStr, put_old: &CStr) -> Result<(), Errno> {
    let new_root_ptr = new_root.to_bytes_with_nul().as_ptr();
    let put_old_ptr = put_old.to_bytes_with_nul().as_ptr();
    Errno::result(unsafe { libc::syscall(libc::SYS_pivot_root, new_root_ptr, put_old_ptr) })
        .map(drop)
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct UmountFlags(c_int);

impl UmountFlags {
    pub const DETACH: Self = Self(libc::MNT_DETACH);
}

pub fn umount2(path: &CStr, flags: UmountFlags) -> Result<(), Errno> {
    let path_ptr = path.to_bytes_with_nul().as_ptr();
    Errno::result(unsafe { libc::umount2(path_ptr as *const c_char, flags.0) }).map(drop)
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

#[derive(Clone, Copy, Default, Into)]
pub struct Signal(c_int);

impl Signal {
    pub const CHLD: Self = Self(libc::SIGCHLD);
    pub const KILL: Self = Self(libc::SIGKILL);

    pub fn as_u8(&self) -> u8 {
        self.0.try_into().unwrap()
    }
}

extern "C" {
    fn sigabbrev_np(sig: c_int) -> *const c_char;
}

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let abbrev = unsafe { sigabbrev_np(self.0) };
        if abbrev.is_null() {
            write!(f, "Invalid Signal {}", self.0)
        } else {
            let abbrev = unsafe { CStr::from_ptr(abbrev) }.to_str().unwrap();
            write!(f, "SIG{}", abbrev)
        }
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
            flags: flags.as_u64(),
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

#[derive(Clone, Copy, Debug, Display, Eq, Hash, PartialEq)]
pub struct Pid(pid_t);

impl Pid {
    fn from_pid_t(pid: pid_t) -> Self {
        Self(pid)
    }

    fn from_c_long(pid: c_long) -> Self {
        Self::from_pid_t(pid.try_into().unwrap())
    }

    #[cfg(any(test, feature = "test"))]
    pub fn new_for_test(pid: pid_t) -> Self {
        Self(pid)
    }
}

pub fn clone3(args: &mut CloneArgs) -> Result<Option<Pid>, Errno> {
    let args_ptr = args as *mut CloneArgs as *mut c_void;
    let size = mem::size_of::<CloneArgs>() as size_t;
    Errno::result(unsafe { libc::syscall(libc::SYS_clone3, args_ptr, size) }).map(|ret| {
        if ret == 0 {
            None
        } else {
            Some(Pid::from_c_long(ret))
        }
    })
}

pub fn pidfd_open(pid: Pid) -> Result<Fd, Errno> {
    Errno::result(unsafe { libc::syscall(libc::SYS_pidfd_open, pid.0, 0 as c_uint) })
        .map(Fd::from_c_long)
}

pub fn close(fd: Fd) -> Result<(), Errno> {
    Errno::result(unsafe { libc::close(fd.0) }).map(drop)
}

pub fn prctl_set_pdeathsig(signal: Signal) -> Result<(), Errno> {
    Errno::result(unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, signal.0 as c_ulong) }).map(drop)
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct PollEvents(c_short);

impl PollEvents {
    pub const IN: Self = Self(libc::POLLIN);
}

#[repr(C)]
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

pub fn poll(fds: &mut [PollFd], timeout: Duration) -> Result<usize, Errno> {
    let fds_ptr = fds.as_mut_ptr();
    let nfds = fds.len();
    let timeout = timeout.as_millis();
    Errno::result(unsafe { libc::poll(fds_ptr as *mut pollfd, nfds as nfds_t, timeout as c_int) })
        .map(|ret| ret as usize)
}

#[derive(Clone, Copy)]
pub struct ExitCode(c_int);

impl ExitCode {
    pub fn as_u8(&self) -> u8 {
        self.0 as u8
    }

    pub fn as_i32(&self) -> i32 {
        self.0
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
        WaitStatus::Exited(ExitCode(libc::WEXITSTATUS(status)))
    } else if libc::WIFSIGNALED(status) {
        WaitStatus::Signaled(Signal(libc::WTERMSIG(status)))
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
        pid: Pid::from_pid_t(pid),
        status: extract_wait_status(status),
    })
}

#[derive(Clone, Copy, Default)]
pub struct WaitpidFlags(c_int);

pub fn waitpid(pid: Pid, flags: WaitpidFlags) -> Result<WaitStatus, Errno> {
    let inner = |status: &mut c_int| {
        let status_ptr = status as *mut c_int;
        unsafe { libc::waitpid(pid.0, status_ptr, flags.0) }
    };
    let mut status = 0;
    Errno::result(inner(&mut status)).map(|_| extract_wait_status(status))
}

pub fn raise(signal: Signal) -> Result<(), Errno> {
    Errno::result(unsafe { libc::raise(signal.0 as c_int) }).map(drop)
}

pub fn kill(pid: Pid, signal: Signal) -> Result<(), Errno> {
    Errno::result(unsafe { libc::kill(pid.0, signal.0 as c_int) }).map(drop)
}

pub fn pause() {
    unsafe { libc::pause() };
}

pub fn getpid() -> Pid {
    Pid::from_pid_t(unsafe { libc::getpid() })
}

#[derive(Clone, Copy, Display)]
pub struct Uid(uid_t);

impl Uid {
    fn from_uid_t(uid: uid_t) -> Uid {
        Self(uid)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

pub fn getuid() -> Uid {
    Uid::from_uid_t(unsafe { libc::getuid() })
}

#[derive(Clone, Copy, Display)]
pub struct Gid(gid_t);

impl Gid {
    fn from_gid_t(gid: gid_t) -> Gid {
        Self(gid)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

pub fn getgid() -> Gid {
    Gid::from_gid_t(unsafe { libc::getgid() })
}

pub fn pipe() -> Result<(Fd, Fd), Errno> {
    let mut fds: [c_int; 2] = [0; 2];
    let fds_ptr = fds.as_mut_ptr() as *mut c_int;
    Errno::result(unsafe { libc::pipe(fds_ptr) })
        .map(|_| (Fd::from_raw_fd(fds[0]), Fd::from_raw_fd(fds[1])))
}

pub fn fcntl_setfl(fd: Fd, flags: OpenFlags) -> Result<(), Errno> {
    Errno::result(unsafe { libc::fcntl(fd.0, libc::F_SETFL as c_int, flags.0) }).map(drop)
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
        assert_eq!(std::format!("{}", Pid(1234)).as_str(), "1234",);
    }

    #[test]
    fn uid_display() {
        assert_eq!(std::format!("{}", Uid(1234)).as_str(), "1234",);
    }

    #[test]
    fn gid_display() {
        assert_eq!(std::format!("{}", Gid(1234)).as_str(), "1234",);
    }
}
