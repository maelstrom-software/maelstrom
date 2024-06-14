//! Function wrappers for Linux syscalls.
#![no_std]

#[cfg(any(test, feature = "std"))]
extern crate std;

use core::{
    ffi::CStr,
    fmt,
    mem::{self, MaybeUninit},
    ops::Deref,
    ptr,
    time::Duration,
};
use derive_more::{BitOr, BitOrAssign, Display, Into};
use libc::{
    c_char, c_int, c_long, c_short, c_uint, c_ulong, c_void, gid_t, id_t, idtype_t, mode_t, nfds_t,
    pid_t, pollfd, sa_family_t, siginfo_t, sigset_t, size_t, sockaddr, sockaddr_storage,
    sockaddr_un, socklen_t, uid_t,
};

#[cfg(any(test, feature = "std"))]
use std::os::fd;

extern "C" {
    fn sigabbrev_np(sig: c_int) -> *const c_char;
    fn strerrorname_np(errnum: c_int) -> *const c_char;
    fn strerrordesc_np(errnum: c_int) -> *const c_char;
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct AccessMode(c_int);

impl AccessMode {
    pub const F: Self = Self(libc::F_OK);
    pub const R: Self = Self(libc::R_OK);
    pub const W: Self = Self(libc::W_OK);
    pub const X: Self = Self(libc::X_OK);
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct AcceptFlags(c_int);

impl AcceptFlags {
    pub const CLOEXEC: Self = Self(libc::SOCK_CLOEXEC);
    pub const NONBLOCK: Self = Self(libc::SOCK_NONBLOCK);
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

#[derive(BitOr, BitOrAssign, Clone, Copy, Default)]
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
    pub const VM: Self = Self(libc::CLONE_VM);

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

#[derive(PartialEq, Eq)]
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

#[cfg(any(test, feature = "std"))]
impl From<Errno> for std::io::Error {
    fn from(e: Errno) -> Self {
        std::io::Error::from_raw_os_error(e.0)
    }
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fd(c_int);

impl Fd {
    pub const STDIN: Self = Self(libc::STDIN_FILENO);
    pub const STDOUT: Self = Self(libc::STDOUT_FILENO);
    pub const STDERR: Self = Self(libc::STDERR_FILENO);
    pub const AT_FDCWD: Self = Self(libc::AT_FDCWD);

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

#[derive(BitOr, Clone, Copy, Default)]
pub struct FsconfigCommand(c_uint);

impl FsconfigCommand {
    pub const SET_FLAG: Self = Self(0);
    pub const SET_STRING: Self = Self(1);
    pub const SET_BINARY: Self = Self(2);
    pub const SET_PATH: Self = Self(3);
    pub const SET_PATH_EMPTY: Self = Self(4);
    pub const SET_FD: Self = Self(5);
    pub const CMD_CREATE: Self = Self(6);
    pub const CMD_RECONFIGURE: Self = Self(7);
    pub const CMD_CREATE_EXCL: Self = Self(8);
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct FsmountFlags(c_ulong);

impl FsmountFlags {
    pub const CLOEXEC: Self = Self(1);
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct FsopenFlags(c_uint);

impl FsopenFlags {
    pub const CLOEXEC: Self = Self(1);
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
pub struct MountAttrs(c_uint);

impl MountAttrs {
    pub const RDONLY: Self = Self(0x00000001);
    pub const NOSUID: Self = Self(0x00000002);
    pub const NODEV: Self = Self(0x00000004);
    pub const NOEXEC: Self = Self(0x00000008);
    pub const _ATIME: Self = Self(0x00000070);
    pub const RELATIME: Self = Self(0x00000000);
    pub const NOATIME: Self = Self(0x00000010);
    pub const STRICTATIME: Self = Self(0x00000020);
    pub const NODIRATIME: Self = Self(0x00000080);
    pub const IDMAP: Self = Self(0x00100000);
    pub const NOSYMFOLLOW: Self = Self(0x00200000);
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

#[derive(BitOr, Clone, Copy, Default)]
pub struct MoveMountFlags(c_uint);

impl MoveMountFlags {
    pub const F_EMPTY_PATH: Self = Self(libc::MOVE_MOUNT_F_EMPTY_PATH);
    pub const T_EMPTY_PATH: Self = Self(libc::MOVE_MOUNT_T_EMPTY_PATH);
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct OpenFlags(c_int);

impl OpenFlags {
    pub const RDWR: Self = Self(libc::O_RDWR);
    pub const WRONLY: Self = Self(libc::O_WRONLY);
    pub const TRUNC: Self = Self(libc::O_TRUNC);
    pub const NONBLOCK: Self = Self(libc::O_NONBLOCK);
    pub const NOCTTY: Self = Self(libc::O_NOCTTY);
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct OpenTreeFlags(c_uint);

impl OpenTreeFlags {
    pub const CLOEXEC: Self = Self(libc::OPEN_TREE_CLOEXEC);
    pub const CLONE: Self = Self(libc::OPEN_TREE_CLONE);
    pub const RECURSIVE: Self = Self(libc::AT_RECURSIVE as c_uint);
    pub const EMPTY_PATH: Self = Self(libc::AT_EMPTY_PATH as c_uint);
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

#[cfg(any(test, feature = "std"))]
impl From<OwnedFd> for std::os::unix::net::UnixStream {
    fn from(owned_fd: OwnedFd) -> std::os::unix::net::UnixStream {
        fd::OwnedFd::from(owned_fd).into()
    }
}

#[cfg(feature = "tokio")]
impl TryFrom<OwnedFd> for tokio::net::UnixStream {
    type Error = std::io::Error;
    fn try_from(owned_fd: OwnedFd) -> Result<tokio::net::UnixStream, Self::Error> {
        tokio::net::UnixStream::from_std(fd::OwnedFd::from(owned_fd).into())
    }
}

#[cfg(any(test, feature = "std"))]
impl From<OwnedFd> for std::os::unix::net::UnixListener {
    fn from(owned_fd: OwnedFd) -> std::os::unix::net::UnixListener {
        fd::OwnedFd::from(owned_fd).into()
    }
}

#[cfg(feature = "tokio")]
impl TryFrom<OwnedFd> for tokio::net::UnixListener {
    type Error = std::io::Error;
    fn try_from(owned_fd: OwnedFd) -> Result<tokio::net::UnixListener, Self::Error> {
        tokio::net::UnixListener::from_std(fd::OwnedFd::from(owned_fd).into())
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

#[cfg(any(test, feature = "std"))]
impl From<std::process::Child> for Pid {
    fn from(p: std::process::Child) -> Self {
        Self(p.id().try_into().unwrap())
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

#[derive(Clone, Copy, Debug, Default, Into, PartialEq, Eq, PartialOrd, Ord)]
pub struct Signal(c_int);

impl Signal {
    pub const ALRM: Self = Self(libc::SIGALRM);
    pub const CHLD: Self = Self(libc::SIGCHLD);
    pub const HUP: Self = Self(libc::SIGHUP);
    pub const INT: Self = Self(libc::SIGINT);
    pub const IO: Self = Self(libc::SIGIO);
    pub const KILL: Self = Self(libc::SIGKILL);
    pub const LOST: Self = Self(29);
    pub const PIPE: Self = Self(libc::SIGPIPE);
    pub const POLL: Self = Self(libc::SIGPOLL);
    pub const PROF: Self = Self(libc::SIGPROF);
    pub const PWR: Self = Self(libc::SIGPWR);
    pub const QUIT: Self = Self(libc::SIGQUIT);
    pub const TERM: Self = Self(libc::SIGTERM);
    pub const TSTP: Self = Self(libc::SIGTSTP);
    pub const TTIN: Self = Self(libc::SIGTTIN);
    pub const TTOU: Self = Self(libc::SIGTTOU);
    pub const USR1: Self = Self(libc::SIGUSR1);
    pub const USR2: Self = Self(libc::SIGUSR2);
    pub const VTALRM: Self = Self(libc::SIGVTALRM);
    pub const WINCH: Self = Self(libc::SIGWINCH);

    pub fn as_u8(&self) -> u8 {
        self.0.try_into().unwrap()
    }

    pub fn as_c_int(&self) -> c_int {
        self.0
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

impl From<u8> for Signal {
    fn from(signo: u8) -> Self {
        Self(signo.into())
    }
}

#[repr(transparent)]
pub struct SignalSet(sigset_t);

impl SignalSet {
    pub fn empty() -> Self {
        let mut inner: MaybeUninit<sigset_t> = MaybeUninit::uninit();
        let inner_ptr = inner.as_mut_ptr();
        Errno::result(unsafe { libc::sigemptyset(inner_ptr) }).unwrap();
        Self(unsafe { inner.assume_init() })
    }

    pub fn full() -> Self {
        let mut inner: MaybeUninit<sigset_t> = MaybeUninit::uninit();
        let inner_ptr = inner.as_mut_ptr();
        Errno::result(unsafe { libc::sigfillset(inner_ptr) }).unwrap();
        Self(unsafe { inner.assume_init() })
    }

    pub fn insert(&mut self, signal: Signal) {
        Errno::result(unsafe { libc::sigaddset(&mut self.0, signal.0) }).unwrap();
    }

    pub fn remove(&mut self, signal: Signal) {
        Errno::result(unsafe { libc::sigdelset(&mut self.0, signal.0) }).unwrap();
    }

    pub fn contains(&mut self, signal: Signal) -> bool {
        Errno::result(unsafe { libc::sigismember(&self.0, signal.0) })
            .map(|res| res != 0)
            .unwrap()
    }
}

#[derive(Clone, Copy)]
pub struct SigprocmaskHow(c_int);

impl SigprocmaskHow {
    pub const BLOCK: Self = Self(libc::SIG_BLOCK);
    pub const UNBLOCK: Self = Self(libc::SIG_UNBLOCK);
    pub const SETMASK: Self = Self(libc::SIG_SETMASK);
}

#[repr(C)]
pub struct Sockaddr {
    family: sa_family_t,
    data: [u8],
}

impl Sockaddr {
    pub fn family(&self) -> sa_family_t {
        self.family
    }

    /// Return the size, in bytes, of the pointed-to `Sockaddr`. This byte count includes the
    /// leading two bytes used to store the address family.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        let res = mem::size_of_val(self);
        assert_eq!(res, self.data.len() + mem::size_of::<sa_family_t>());
        res
    }

    /// Return a reference to the underlying `sockaddr` and the length. This is useful for calling
    /// underlying libc functions that take a constant sockaddr. For libc functions that take
    /// mutable sockaddrs, see [`SockaddrStorage::as_mut_parts`].
    pub fn as_parts(&self) -> (&sockaddr, socklen_t) {
        unsafe {
            (
                &*(&self.family as *const sa_family_t as *const sockaddr),
                self.len() as socklen_t,
            )
        }
    }

    /// Create a `Sockaddr` reference from a `sockaddr` pointer and a size.
    ///
    /// # Safety
    ///
    /// The `sockaddr` pointer must point to a valid `sockaddr` of some sort, and `len` must be
    /// less than or equal to the size of the underlying `sockaddr` object.
    pub unsafe fn from_raw_parts<'a>(addr: *const sockaddr, len: usize) -> &'a Self {
        let res =
            &*(ptr::slice_from_raw_parts(addr as *const u8, len - mem::size_of::<sa_family_t>())
                as *const Self);
        assert_eq!(res.len(), len);
        res
    }

    pub fn as_sockaddr_un(&self) -> Option<&SockaddrUn> {
        (self.family == libc::AF_UNIX as sa_family_t).then(|| unsafe { mem::transmute(self) })
    }
}

pub struct SockaddrStorage {
    inner: sockaddr_storage,
    len: socklen_t,
}

impl SockaddrStorage {
    /// Return raw mutable references into the `SockaddrStorage` so that a function can be used to
    /// initialize the sockaddr.
    ///
    /// # Safety
    ///
    /// The referenced length cannot be adjusted to a larger size.
    pub unsafe fn as_mut_parts(&mut self) -> (&mut sockaddr, &mut socklen_t) {
        (
            mem::transmute::<&mut libc::sockaddr_storage, &mut libc::sockaddr>(&mut self.inner),
            &mut self.len,
        )
    }
}

impl Default for SockaddrStorage {
    fn default() -> Self {
        Self {
            inner: unsafe { mem::zeroed() },
            len: mem::size_of::<sockaddr_storage>() as socklen_t,
        }
    }
}

impl Deref for SockaddrStorage {
    type Target = Sockaddr;
    fn deref(&self) -> &Self::Target {
        unsafe {
            Sockaddr::from_raw_parts(
                &self.inner as *const sockaddr_storage as *const sockaddr,
                self.len as usize,
            )
        }
    }
}

#[repr(C)]
pub struct SockaddrNetlink {
    sin_family: sa_family_t,
    nl_pad: u16,
    nl_pid: u32,
    nl_groups: u32,
}

impl Default for SockaddrNetlink {
    fn default() -> Self {
        Self {
            sin_family: libc::AF_NETLINK as sa_family_t,
            nl_pad: 0,
            nl_pid: 0, // the kernel
            nl_groups: 0,
        }
    }
}

impl Deref for SockaddrNetlink {
    type Target = Sockaddr;
    fn deref(&self) -> &Self::Target {
        unsafe {
            Sockaddr::from_raw_parts(
                self as *const SockaddrNetlink as *const sockaddr,
                mem::size_of_val(self),
            )
        }
    }
}

#[repr(C)]
pub struct SockaddrUn {
    family: sa_family_t,
    path: [u8],
}

impl SockaddrUn {
    pub fn path(&self) -> &[u8] {
        &self.path
    }

    /// Create a `SockaddrUn` reference from a `sockaddr_un` pointer and a size.
    ///
    /// # Safety
    ///
    /// The `sockaddr_un` pointer must point to a valid `sockaddr_un`, and `len` must be less than
    /// or equal to the size of the underlying `sockaddr` object.
    pub unsafe fn from_raw_parts<'a>(addr: *const sockaddr, len: usize) -> &'a Self {
        let res =
            &*(ptr::slice_from_raw_parts(addr as *const u8, len - mem::size_of::<sa_family_t>())
                as *const Self);
        assert_eq!(res.len(), len);
        assert_eq!(res.family, libc::AF_UNIX as sa_family_t);
        res
    }
}

impl Deref for SockaddrUn {
    type Target = Sockaddr;
    fn deref(&self) -> &Self::Target {
        unsafe { mem::transmute(self) }
    }
}

pub struct SockaddrUnStorage {
    inner: sockaddr_un,
    len: socklen_t,
}

impl SockaddrUnStorage {
    pub fn new(path: &[u8]) -> Result<Self, SockaddrUnStoragePathTooLongError> {
        let mut inner = sockaddr_un {
            sun_family: libc::AF_UNIX as sa_family_t,
            sun_path: unsafe { mem::zeroed() },
        };
        if mem::size_of_val(&inner.sun_path) < path.len() {
            Err(SockaddrUnStoragePathTooLongError {
                maximum_accepted: mem::size_of_val(&inner.sun_path),
                actual: path.len(),
            })
        } else {
            unsafe {
                ptr::copy_nonoverlapping(
                    path.as_ptr(),
                    inner.sun_path.as_mut_ptr() as *mut u8,
                    path.len(),
                )
            };
            Ok(Self {
                inner,
                len: (path.len() + mem::size_of::<sa_family_t>()) as socklen_t,
            })
        }
    }

    pub fn new_autobind() -> Self {
        Self::new(b"").unwrap()
    }
}

impl Deref for SockaddrUnStorage {
    type Target = SockaddrUn;
    fn deref(&self) -> &Self::Target {
        unsafe {
            SockaddrUn::from_raw_parts(
                &self.inner as *const sockaddr_un as *const sockaddr,
                self.len as usize,
            )
        }
    }
}

#[derive(Debug)]
pub struct SockaddrUnStoragePathTooLongError {
    pub maximum_accepted: usize,
    pub actual: usize,
}

impl fmt::Display for SockaddrUnStoragePathTooLongError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "sockaddr_un path too long; maximum_accepted: {maximum_accepted}, actual: {actual}",
            maximum_accepted = self.maximum_accepted,
            actual = self.actual
        )
    }
}

#[cfg(any(test, feature = "std"))]
impl std::error::Error for SockaddrUnStoragePathTooLongError {}

#[derive(Clone, Copy)]
pub struct SocketDomain(c_int);

impl SocketDomain {
    pub const NETLINK: Self = Self(libc::PF_NETLINK);
    pub const UNIX: Self = Self(libc::PF_UNIX);
}

#[derive(Clone, Copy, Default)]
pub struct SocketProtocol(c_int);

impl SocketProtocol {
    pub const NETLINK_ROUTE: Self = Self(0);
}

#[derive(BitOr, Clone, Copy, Default)]
pub struct SocketType(c_int);

impl SocketType {
    pub const RAW: Self = Self(libc::SOCK_RAW);
    pub const STREAM: Self = Self(libc::SOCK_STREAM);
    pub const NONBLOCK: Self = Self(libc::SOCK_NONBLOCK);
    pub const CLOEXEC: Self = Self(libc::SOCK_CLOEXEC);
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitStatus {
    Exited(ExitCode),
    Signaled(Signal),
}

pub fn access(path: &CStr, mode: AccessMode) -> Result<(), Errno> {
    let path_ptr = path.as_ptr();
    Errno::result(unsafe { libc::access(path_ptr, mode.0) }).map(drop)
}

pub fn accept(fd: Fd, flags: AcceptFlags) -> Result<(OwnedFd, SockaddrStorage), Errno> {
    let mut sockaddr = SockaddrStorage::default();
    let (addr_ptr, len_ptr) = unsafe { sockaddr.as_mut_parts() };
    let fd = Errno::result(unsafe { libc::accept4(fd.0, addr_ptr, len_ptr, flags.0) })
        .map(Fd)
        .map(OwnedFd)?;
    Ok((fd, sockaddr))
}

pub fn autobound_unix_listener(
    socket_flags: SocketType,
    backlog: u32,
) -> Result<(OwnedFd, [u8; 6]), Errno> {
    let sock = socket(
        SocketDomain::UNIX,
        SocketType::STREAM | socket_flags,
        Default::default(),
    )?;
    bind(sock.as_fd(), &SockaddrUnStorage::new_autobind())?;
    listen(sock.as_fd(), backlog)?;
    let addr = getsockname(sock.as_fd())?
        .as_sockaddr_un()
        .unwrap()
        .path()
        .try_into()
        .unwrap();
    Ok((sock, addr))
}

pub fn bind(fd: Fd, sockaddr: &Sockaddr) -> Result<(), Errno> {
    let (addr, len) = sockaddr.as_parts();
    Errno::result(unsafe { libc::bind(fd.0, addr, len) }).map(drop)
}

pub fn chdir(path: &CStr) -> Result<(), Errno> {
    let path_ptr = path.as_ptr();
    Errno::result(unsafe { libc::chdir(path_ptr) }).map(drop)
}

pub fn clone3(args: &mut CloneArgs) -> Result<Option<Pid>, Errno> {
    assert_eq!(args.0.flags & libc::CLONE_VM as c_ulong, 0);
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
    assert_eq!(args.0.flags & libc::CLONE_VM as c_ulong, 0);
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

/// # Safety
///
/// stack and arg pointers must point to memory the child can access while it is around
pub unsafe fn clone_with_child_pidfd(
    func: extern "C" fn(*mut c_void) -> i32,
    stack: *mut c_void,
    arg: *mut c_void,
    args: &mut CloneArgs,
) -> Result<(Pid, OwnedFd), Errno> {
    let inner = |args: &mut CloneArgs, child_pidfd: &mut Fd| {
        assert_eq!(args.0.flags & (libc::CLONE_PIDFD as c_ulong), 0);
        args.0.flags |= libc::CLONE_PIDFD as c_ulong;
        args.0.pidfd = child_pidfd as *mut Fd as u64;

        let flags = args.0.flags as i32 | (args.0.exit_signal & 0xFF) as i32;
        unsafe { libc::clone(func, stack, flags, arg, args.0.pidfd) }
    };
    let mut child_pidfd = Fd(0);
    let ret = Errno::result(inner(args, &mut child_pidfd))?;
    Ok((Pid(ret), OwnedFd(child_pidfd)))
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

pub fn connect(fd: Fd, sockaddr: &Sockaddr) -> Result<(), Errno> {
    let (addr, len) = sockaddr.as_parts();
    Errno::result(unsafe { libc::connect(fd.0, addr, len) }).map(drop)
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

pub fn _exit(status: ExitCode) -> ! {
    unsafe { libc::_exit(status.0) };
}

pub fn fcntl_setfl(fd: Fd, flags: OpenFlags) -> Result<(), Errno> {
    Errno::result(unsafe { libc::fcntl(fd.0, libc::F_SETFL, flags.0) }).map(drop)
}

pub fn fork() -> Result<Option<Pid>, Errno> {
    Errno::result(unsafe { libc::fork() }).map(|p| (p != 0).then_some(Pid(p)))
}

pub fn fsconfig(
    fd: Fd,
    command: FsconfigCommand,
    key: Option<&CStr>,
    value: Option<&u8>,
    aux: Option<i32>,
) -> Result<(), Errno> {
    let key_ptr = key.map(|key| key.as_ptr()).unwrap_or(ptr::null());
    let value_ptr = value.map(|value| value as *const u8).unwrap_or(ptr::null());
    let aux = aux.unwrap_or(0);
    Errno::result(unsafe {
        libc::syscall(libc::SYS_fsconfig, fd.0, command.0, key_ptr, value_ptr, aux)
    })
    .map(drop)
}

pub fn fsmount(fsfd: Fd, flags: FsmountFlags, mount_attrs: MountAttrs) -> Result<OwnedFd, Errno> {
    Errno::result(unsafe { libc::syscall(libc::SYS_fsmount, fsfd.0, flags.0, mount_attrs.0) })
        .map(Fd::from_c_long)
        .map(OwnedFd)
}

pub fn fsopen(fsname: &CStr, flags: FsopenFlags) -> Result<OwnedFd, Errno> {
    let fsname_ptr = fsname.as_ptr();
    Errno::result(unsafe { libc::syscall(libc::SYS_fsopen, fsname_ptr, flags.0) })
        .map(Fd::from_c_long)
        .map(OwnedFd)
}

pub fn getgid() -> Gid {
    Gid(unsafe { libc::getgid() })
}

pub fn getpid() -> Pid {
    Pid(unsafe { libc::getpid() })
}

pub fn getsockname(fd: Fd) -> Result<SockaddrStorage, Errno> {
    let mut sockaddr = SockaddrStorage::default();
    let (addr_ptr, len_ptr) = unsafe { sockaddr.as_mut_parts() };
    Errno::result(unsafe { libc::getsockname(fd.0, addr_ptr, len_ptr) })?;
    Ok(sockaddr)
}

pub fn getuid() -> Uid {
    Uid(unsafe { libc::getuid() })
}

pub fn grantpt(fd: Fd) -> Result<(), Errno> {
    Errno::result(unsafe { libc::grantpt(fd.0) }).map(drop)
}

pub fn ioctl_tiocsctty(fd: Fd, arg: i32) -> Result<(), Errno> {
    Errno::result(unsafe { libc::ioctl(fd.0, libc::TIOCSCTTY, arg as c_int) }).map(drop)
}

pub fn ioctl_tiocgwinsz(fd: Fd) -> Result<(u16, u16), Errno> {
    let mut winsize: libc::winsize = unsafe { mem::zeroed() };
    Errno::result(unsafe { libc::ioctl(fd.0, libc::TIOCGWINSZ, &mut winsize) }).map(drop)?;
    Ok((winsize.ws_row, winsize.ws_col))
}

pub fn ioctl_tiocswinsz(fd: Fd, rows: u16, columns: u16) -> Result<(), Errno> {
    let winsize = libc::winsize {
        ws_row: rows,
        ws_col: columns,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    Errno::result(unsafe { libc::ioctl(fd.0, libc::TIOCSWINSZ, &winsize) }).map(drop)
}

pub fn kill(pid: Pid, signal: Signal) -> Result<(), Errno> {
    Errno::result(unsafe { libc::kill(pid.0, signal.0) }).map(drop)
}

pub fn listen(fd: Fd, backlog: u32) -> Result<(), Errno> {
    Errno::result(unsafe { libc::listen(fd.0, backlog as c_int) }).map(drop)
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

pub fn move_mount(
    from_dirfd: Fd,
    from_path: &CStr,
    to_dirfd: Fd,
    to_path: &CStr,
    flags: MoveMountFlags,
) -> Result<(), Errno> {
    let from_path_ptr = from_path.as_ptr();
    let to_path_ptr = to_path.as_ptr();
    Errno::result(unsafe {
        libc::syscall(
            libc::SYS_move_mount,
            from_dirfd.0,
            from_path_ptr,
            to_dirfd.0,
            to_path_ptr,
            flags.0,
        )
    })
    .map(drop)
}

pub fn open(path: &CStr, flags: OpenFlags, mode: FileMode) -> Result<OwnedFd, Errno> {
    let path_ptr = path.as_ptr();
    let fd = Errno::result(unsafe { libc::open(path_ptr, flags.0, mode.0) })
        .map(Fd)
        .map(OwnedFd)?;
    Ok(fd)
}

pub fn open_tree(dirfd: Fd, path: &CStr, flags: OpenTreeFlags) -> Result<OwnedFd, Errno> {
    let path_ptr = path.as_ptr();
    Errno::result(unsafe { libc::syscall(libc::SYS_open_tree, dirfd.0, path_ptr, flags.0) })
        .map(Fd::from_c_long)
        .map(OwnedFd)
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

pub fn posix_openpt(flags: OpenFlags) -> Result<OwnedFd, Errno> {
    let fd = Errno::result(unsafe { libc::posix_openpt(flags.0) })
        .map(Fd)
        .map(OwnedFd)?;
    Ok(fd)
}

pub fn prctl_set_pdeathsig(signal: Signal) -> Result<(), Errno> {
    let signal = signal.as_c_ulong();
    Errno::result(unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, signal) }).map(drop)
}

pub fn pthread_sigmask(how: SigprocmaskHow, set: Option<&SignalSet>) -> Result<SignalSet, Errno> {
    let set: *const sigset_t = set.map(|s| &s.0 as *const sigset_t).unwrap_or(ptr::null());
    let mut oldset: MaybeUninit<sigset_t> = MaybeUninit::uninit();
    Errno::result(unsafe { libc::pthread_sigmask(how.0, set, oldset.as_mut_ptr()) })?;
    Ok(SignalSet(unsafe { oldset.assume_init() }))
}

pub fn ptsname(fd: Fd, name: &mut [u8]) -> Result<(), Errno> {
    let buf_ptr = name.as_mut_ptr() as *mut c_char;
    let buf_len = name.len();
    Errno::result(unsafe { libc::ptsname_r(fd.0, buf_ptr, buf_len) }).map(drop)
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

pub fn sigprocmask(how: SigprocmaskHow, set: Option<&SignalSet>) -> Result<SignalSet, Errno> {
    let set: *const sigset_t = set.map(|s| &s.0 as *const sigset_t).unwrap_or(ptr::null());
    let mut oldset: MaybeUninit<sigset_t> = MaybeUninit::uninit();
    Errno::result(unsafe { libc::sigprocmask(how.0, set, oldset.as_mut_ptr()) })?;
    Ok(SignalSet(unsafe { oldset.assume_init() }))
}

pub fn sigwait(blocked_signals: &SignalSet) -> Result<Signal, Errno> {
    let mut signal = Signal::default();
    Errno::result(unsafe { libc::sigwait(&blocked_signals.0, &mut signal.0) })?;
    Ok(signal)
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

pub fn unlockpt(fd: Fd) -> Result<(), Errno> {
    Errno::result(unsafe { libc::unlockpt(fd.0) }).map(drop)
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

            // I have deduced that when we have too many files open, this is what happens
            if message.msg_flags & libc::MSG_CTRUNC != 0 {
                return Err(Errno::EMFILE);
            }

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

pub fn splice(
    fd_in: Fd,
    off_in: Option<u64>,
    fd_out: Fd,
    off_out: Option<u64>,
    length: usize,
) -> Result<usize, Errno> {
    let off_in = off_in.map(|v| i64::try_from(v).unwrap());
    let off_out = off_out.map(|v| i64::try_from(v).unwrap());

    let inner = |off_in: &Option<i64>, off_out: &Option<i64>| {
        let off_in = off_in
            .as_ref()
            .map(|v| v as *const i64 as *mut _)
            .unwrap_or(core::ptr::null_mut());
        let off_out = off_out
            .as_ref()
            .map(|v| v as *const i64 as *mut _)
            .unwrap_or(core::ptr::null_mut());
        Ok(
            Errno::result(unsafe { libc::splice(fd_in.0, off_in, fd_out.0, off_out, length, 0) })?
                as usize,
        )
    };
    inner(&off_in, &off_out)
}

pub fn set_pipe_size(fd: Fd, size: usize) -> Result<(), Errno> {
    Errno::result(unsafe { libc::fcntl(fd.0, libc::F_SETPIPE_SZ, i32::try_from(size).unwrap()) })
        .map(drop)
}

pub fn get_pipe_size(fd: Fd) -> Result<usize, Errno> {
    Errno::result(unsafe { libc::fcntl(fd.0, libc::F_GETPIPE_SZ) }).map(|sz| sz as usize)
}

pub enum Whence {
    SeekSet,
    SeekCur,
    SeekEnd,
}

impl Whence {
    fn as_i32(&self) -> i32 {
        match self {
            Self::SeekSet => libc::SEEK_SET,
            Self::SeekCur => libc::SEEK_CUR,
            Self::SeekEnd => libc::SEEK_END,
        }
    }
}

pub fn lseek(fd: Fd, offset: i64, whence: Whence) -> Result<i64, Errno> {
    Errno::result(unsafe { libc::lseek(fd.0, offset, whence.as_i32()) })
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

    #[test]
    fn sockaddr_len() {
        let mut sa: sockaddr = unsafe { mem::zeroed() };
        sa.sa_family = libc::AF_UNIX as sa_family_t;
        let sa2 = unsafe { Sockaddr::from_raw_parts(&sa, mem::size_of::<sockaddr>()) };
        assert_eq!(mem::size_of::<sockaddr>(), 16);
        assert_eq!(sa2.data.len(), 14);
        assert_eq!(sa2.family(), libc::AF_UNIX as sa_family_t);
        assert_eq!(sa2.len(), mem::size_of::<sockaddr>());
    }
}

pub fn abort() -> ! {
    unsafe { libc::abort() }
}
