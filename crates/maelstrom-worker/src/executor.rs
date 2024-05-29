//! Easily start and stop processes.

use anyhow::{anyhow, Error, Result};
use bumpalo::{
    collections::{String as BumpString, Vec as BumpVec},
    Bump,
};
use futures::ready;
use maelstrom_base::{
    EnumSet, GroupId, JobCompleted, JobDevice, JobEffects, JobError, JobMount, JobNetwork,
    JobOutputResult, JobResult, JobStatus, Timeout, UserId, Utf8PathBuf,
};
use maelstrom_linux::{
    self as linux, CloneArgs, CloneFlags, CloseRangeFirst, CloseRangeFlags, CloseRangeLast, Errno,
    Fd, FileMode, FsconfigCommand, FsmountFlags, FsopenFlags, Gid, MountAttrs, MountFlags,
    MoveMountFlags, NetlinkSocketAddr, OpenFlags, OpenTreeFlags, OwnedFd, Signal, SocketDomain,
    SocketProtocol, SocketType, Uid, UmountFlags, WaitStatus,
};
use maelstrom_util::{
    config::common::InlineLimit,
    root::RootBuf,
    sync::EventReceiver,
    time::{Clock, ClockInstant as _},
};
use maelstrom_worker_child::{FdSlot, Syscall};
use netlink_packet_core::{NetlinkMessage, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST};
use netlink_packet_route::{rtnl::constants::RTM_SETLINK, LinkMessage, RtnlMessage, IFF_UP};
use std::{
    cell::UnsafeCell,
    ffi::{CStr, CString},
    fmt::Write as _,
    fs::File,
    io::Read as _,
    mem,
    os::{
        fd,
        unix::{ffi::OsStrExt as _, fs::MetadataExt},
    },
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{self, unix::AsyncFd, AsyncRead, AsyncReadExt as _, Interest, ReadBuf},
    runtime, select,
    sync::oneshot,
    task::JoinSet,
};

/*              _     _ _
 *  _ __  _   _| |__ | (_) ___
 * | '_ \| | | | '_ \| | |/ __|
 * | |_) | |_| | |_) | | | (__
 * | .__/ \__,_|_.__/|_|_|\___|
 * |_|
 *  FIGLET: public
 */

/// All necessary information for the worker to execute a job.
pub struct JobSpec {
    pub program: Utf8PathBuf,
    pub arguments: Vec<String>,
    pub environment: Vec<String>,
    pub devices: EnumSet<JobDevice>,
    pub mounts: Vec<JobMount>,
    pub network: JobNetwork,
    pub enable_writable_file_system: bool,
    pub working_directory: Utf8PathBuf,
    pub user: UserId,
    pub group: GroupId,
    pub timeout: Option<Timeout>,
}

impl JobSpec {
    pub fn from_spec(spec: maelstrom_base::JobSpec) -> Self {
        let maelstrom_base::JobSpec {
            program,
            arguments,
            environment,
            layers: _,
            devices,
            mounts,
            network,
            enable_writable_file_system,
            working_directory,
            user,
            group,
            timeout,
            estimated_duration: _,
        } = spec;
        JobSpec {
            program,
            arguments,
            environment,
            devices,
            mounts,
            network,
            enable_writable_file_system,
            working_directory,
            user,
            group,
            timeout,
        }
    }
}

pub struct MountDir;
pub struct TmpfsDir;

pub struct Executor<'clock, ClockT> {
    user: UserId,
    group: GroupId,
    mount_dir: CString,
    tmpfs_dir: CString,
    upper_dir: CString,
    work_dir: CString,
    root_mode: u32,
    netlink_socket_addr: NetlinkSocketAddr,
    netlink_message: Box<[u8]>,
    clock: &'clock ClockT,
}

impl<'clock, ClockT> Executor<'clock, ClockT> {
    pub fn new(
        mount_dir: RootBuf<MountDir>,
        tmpfs_dir: RootBuf<TmpfsDir>,
        clock: &'clock ClockT,
    ) -> Result<Self> {
        // Set up stdin to be a file that will always return EOF. We could do something similar
        // by opening /dev/null but then we would depend on /dev being mounted. The fewer
        // dependencies, the better.
        let (stdin_read_fd, stdin_write_fd) = linux::pipe()?;
        if stdin_read_fd.as_fd() == Fd::STDIN {
            // On the off chance that stdin was already closed, we may have already opened our read
            // fd onto stdin.
            mem::forget(stdin_read_fd);
        } else if stdin_write_fd.as_fd() == Fd::STDIN {
            // This would be a really weird scenario. Somehow we got the read end of the pipe to
            // not be fd 0, but the write end is. We can just dup the read end onto fd 0 and be
            // done.
            linux::dup2(stdin_read_fd.as_fd(), Fd::STDIN)?;
            mem::forget(stdin_read_fd);
            mem::forget(stdin_write_fd);
        } else {
            // This is the normal case where neither fd is fd 0.
            linux::dup2(stdin_read_fd.as_fd(), Fd::STDIN)?;
        }

        struct OverlayFsUpperDir;
        struct OverlayFsWorkDir;

        let user = UserId::from(linux::getuid().as_u32());
        let group = GroupId::from(linux::getgid().as_u32());
        let root_mode = std::fs::metadata(&mount_dir)?.mode();
        let mount_dir = CString::new(mount_dir.as_os_str().as_bytes())?;
        let upper_dir = tmpfs_dir.join::<OverlayFsUpperDir>("upper");
        let work_dir = tmpfs_dir.join::<OverlayFsWorkDir>("work");
        let tmpfs_dir = CString::new(tmpfs_dir.as_os_str().as_bytes())?;
        let upper_dir = CString::new(upper_dir.as_os_str().as_bytes())?;
        let work_dir = CString::new(work_dir.as_os_str().as_bytes())?;
        let netlink_socket_addr = NetlinkSocketAddr::default();
        let mut netlink_message = LinkMessage::default();
        netlink_message.header.index = 1;
        netlink_message.header.flags |= IFF_UP;
        netlink_message.header.change_mask |= IFF_UP;
        let mut netlink_message = NetlinkMessage::from(RtnlMessage::SetLink(netlink_message));
        netlink_message.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_EXCL | NLM_F_CREATE;
        netlink_message.header.length = netlink_message.buffer_len() as u32;
        netlink_message.header.message_type = RTM_SETLINK;
        let mut buffer = vec![0; netlink_message.buffer_len()].into_boxed_slice();
        netlink_message.serialize(&mut buffer[..]);

        Ok(Executor {
            user,
            group,
            mount_dir,
            tmpfs_dir,
            upper_dir,
            work_dir,
            root_mode,
            netlink_socket_addr,
            netlink_message: buffer,
            clock,
        })
    }
}

impl<'clock, ClockT: Clock> Executor<'clock, ClockT> {
    /// Run a process (i.e. job).
    ///
    /// On success, this function returns when the process has completed, with a [`JobCompleted`].
    /// This includes the exit status, stdout, and stderr.
    ///
    /// On failure, this function will return immediately. If a child process was started, it will
    /// be waited for in the background and the zombie process will be reaped.
    ///
    /// This function expects a Tokio runtime, which it uses to start a few tasks.
    ///
    /// The `kill_event_receiver` is used to kill the child process. If the attached sender is ever
    /// closed, the child will be immediately killed with a SIGTERM.
    ///
    /// This function should be run in a `spawn_blocking` context. Ideally, this function would be
    /// async, but that doesn't work because we rely on [`bumpalo::Bump`] as a fast arena
    /// allocator, and it's not `Sync`.
    pub fn run_job(
        &self,
        spec: &JobSpec,
        inline_limit: InlineLimit,
        kill_event_receiver: EventReceiver,
        fuse_spawn: impl FnOnce(OwnedFd),
        runtime: runtime::Handle,
    ) -> JobResult<JobCompleted, Error> {
        self.run_job_inner(spec, inline_limit, kill_event_receiver, fuse_spawn, runtime)
    }
}

/*             _            _
 *  _ __  _ __(_)_   ____ _| |_ ___
 * | '_ \| '__| \ \ / / _` | __/ _ \
 * | |_) | |  | |\ V / (_| | ||  __/
 * | .__/|_|  |_| \_/ \__,_|\__\___|
 * |_|
 *  FIGLET: private
 */

async fn wait_for_child(
    child_pidfd: OwnedFd,
    mut kill_event_receiver: EventReceiver,
) -> Result<JobStatus> {
    let async_fd = AsyncFd::with_interest(child_pidfd, Interest::READABLE)?;
    let mut kill_event_received = false;
    loop {
        select! {
            _ = &mut kill_event_receiver, if !kill_event_received => {
                linux::pidfd_send_signal(async_fd.get_ref().as_fd(), Signal::KILL)?;
                kill_event_received = true;
            },
            res = async_fd.readable() => {
                let _ = res?;
                break;
            },
        }
    }
    Ok(match linux::waitid(async_fd.into_inner().as_fd())? {
        WaitStatus::Exited(code) => JobStatus::Exited(code.as_u8()),
        WaitStatus::Signaled(signo) => JobStatus::Signaled(signo.as_u8()),
    })
}

/// A wrapper for a raw, non-blocking fd that allows it to be read from async code.
struct AsyncFile(AsyncFd<File>);

impl AsyncRead for AsyncFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let mut guard = ready!(self.0.poll_read_ready(cx))?;

            let unfilled = buf.initialize_unfilled();
            match guard.try_io(|inner| inner.get_ref().read(unfilled)) {
                Ok(Ok(len)) => {
                    buf.advance(len);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(err)) => return Poll::Ready(Err(err)),
                Err(_would_block) => continue,
            }
        }
    }
}

/// Read all of the contents of `stream` and return the appropriate [`JobOutputResult`].
async fn output_reader(fd: OwnedFd, inline_limit: InlineLimit) -> Result<JobOutputResult> {
    let mut buf = Vec::<u8>::new();
    // Make the read side of the pipe non-blocking so that we can use it with Tokio.
    linux::fcntl_setfl(fd.as_fd(), OpenFlags::NONBLOCK).map_err(Error::from)?;
    let stream = AsyncFile(AsyncFd::new(File::from(fd::OwnedFd::from(fd))).map_err(Error::from)?);
    let mut take = stream.take(inline_limit.as_bytes());
    take.read_to_end(&mut buf).await?;
    let buf = buf.into_boxed_slice();
    let truncated = io::copy(&mut take.into_inner(), &mut io::sink()).await?;
    match truncated {
        0 if buf.is_empty() => Ok(JobOutputResult::None),
        0 => Ok(JobOutputResult::Inline(buf)),
        _ => Ok(JobOutputResult::Truncated {
            first: buf,
            truncated,
        }),
    }
}

/// Task main for the output reader: Read the output and then call the callback.
async fn output_reader_task_main(
    fd: OwnedFd,
    inline_limit: InlineLimit,
    sender: oneshot::Sender<Result<JobOutputResult>>,
) {
    let _ = sender.send(output_reader(fd, inline_limit).await);
}

struct ScriptBuilder<'a> {
    syscalls: BumpVec<'a, Syscall<'a>>,
    error_transformers: BumpVec<'a, &'a dyn Fn(&'static str) -> JobError<Error>>,
}

impl<'a> ScriptBuilder<'a> {
    fn new(bump: &'a Bump) -> Self {
        ScriptBuilder {
            syscalls: BumpVec::new_in(bump),
            error_transformers: BumpVec::new_in(bump),
        }
    }

    fn push(
        &mut self,
        syscall: Syscall<'a>,
        error_transformer: &'a dyn Fn(&'static str) -> JobError<Error>,
    ) {
        self.syscalls.push(syscall);
        self.error_transformers.push(error_transformer);
    }
}

fn bump_c_str<'bump>(bump: &'bump Bump, bytes: &str) -> Result<&'bump CStr> {
    bump_c_str_from_bytes(bump, bytes.as_bytes())
}

fn bump_c_str_from_bytes<'bump>(bump: &'bump Bump, bytes: &[u8]) -> Result<&'bump CStr> {
    let mut vec = BumpVec::with_capacity_in(bytes.len().checked_add(1).unwrap(), bump);
    vec.extend_from_slice(bytes);
    vec.push(0);
    CStr::from_bytes_with_nul(vec.into_bump_slice()).map_err(Error::new)
}

impl<'clock, ClockT: Clock> Executor<'clock, ClockT> {
    fn run_job_inner(
        &self,
        spec: &JobSpec,
        inline_limit: InlineLimit,
        kill_event_receiver: EventReceiver,
        fuse_spawn: impl FnOnce(OwnedFd),
        runtime: runtime::Handle,
    ) -> JobResult<JobCompleted, Error> {
        let mut fuse_spawn = Some(fuse_spawn);

        fn syserr<E>(err: E) -> JobError<Error>
        where
            Error: From<E>,
        {
            JobError::System(Error::from(err))
        }

        // We're going to need three pipes: one for stdout, one for stderr, and one to convey back any
        // error that occurs in the child before it execs. It's easiest to create the pipes in the
        // parent before cloning and then closing the unnecessary ends in the parent and child.
        let (stdout_read_fd, stdout_write_fd) = linux::pipe().map_err(syserr)?;
        let (stderr_read_fd, stderr_write_fd) = linux::pipe().map_err(syserr)?;
        let (read_sock, write_sock) = linux::UnixStream::pair().map_err(syserr)?;

        // Set up the script. This will be run in the child where we have to follow some very
        // stringent rules to avoid deadlocking. This comes about because we're going to clone in a
        // multi-threaded program. The child program will only have one thread: this one. The other
        // threads will just not exist in the child process. If any of those threads held a lock at
        // the time of the clone, those locks will be locked forever in the child. Any attempt to
        // acquire those locks in the child would deadlock.
        //
        // The most burdensome result of this is that we can't allocate memory using the global
        // allocator in the child.
        //
        // Instead, we create a script of things we're going to execute in the child, but we do
        // that before the clone. After the clone, the child will just run through the script
        // directly. If it encounters an error, it will write the script index back to the parent
        // via the exec_result pipe. The parent will then map that to a closure for generating the
        // error.
        //
        // A few things to keep in mind in the code below:
        //
        // We don't have to worry about closing file descriptors in the child,
        // or even explicitly marking them close-on-exec, because one of the last things we do is
        // mark all file descriptors -- except for stdin, stdout, and stderr -- as close-on-exec.
        //
        // We have to be careful about the move closures and bump.alloc. The drop method won't be
        // run on the closure, which means drop won't be run on any captured-and-moved variables.
        // As long as we only pass references in those variables, we're okay.

        let bump = Bump::new();
        let mut builder = ScriptBuilder::new(&bump);

        fn new_fd_slot(bump: &Bump) -> FdSlot<'_> {
            FdSlot::new(bump.alloc(UnsafeCell::new(Fd::from_raw(-1))))
        }

        let fd = new_fd_slot(&bump);

        // First we set up the network namespace. It's possible that we won't even create a new
        // network namespace, if JobNetwork::Local is specified.

        let newnet;
        match spec.network {
            JobNetwork::Disabled => {
                newnet = true;
            }
            JobNetwork::Local => {
                newnet = false;
            }
            JobNetwork::Loopback => {
                newnet = true;

                // In order to have a loopback network interface, we need to create a netlink
                // socket and configure things with the kernel.

                // Create the socket.
                builder.push(
                    Syscall::Socket {
                        domain: SocketDomain::NETLINK,
                        type_: SocketType::RAW,
                        protocol: SocketProtocol::NETLINK_ROUTE,
                        out: fd,
                    },
                    &|err| syserr(anyhow!("opening rtnetlink socket: {err}")),
                );

                // Bind the socket.
                builder.push(
                    Syscall::BindNetlink {
                        fd,
                        addr: &self.netlink_socket_addr,
                    },
                    &|err| syserr(anyhow!("binding rtnetlink socket: {err}")),
                );

                // Send the message to the kernel.
                builder.push(
                    Syscall::Write {
                        fd,
                        buf: self.netlink_message.as_ref(),
                    },
                    &|err| syserr(anyhow!("writing rtnetlink message: {err}")),
                );

                // Receive the reply from the kernel.
                // TODO: actually parse the reply to validate that we set up the loopback
                // interface.
                let rtnetlink_response = bump.alloc_slice_fill_default(1024);
                builder.push(
                    Syscall::Read {
                        fd,
                        buf: rtnetlink_response,
                    },
                    &|err| syserr(anyhow!("receiving rtnetlink message: {err}")),
                );
            }
        }

        // Set up the user namespace.

        // This first set of syscalls sets up the uid mapping.
        let mut uid_map_contents = BumpString::with_capacity_in(24, &bump);
        writeln!(uid_map_contents, "{} {} 1", spec.user, self.user).map_err(syserr)?;
        builder.push(
            Syscall::Open {
                path: c"/proc/self/uid_map",
                flags: OpenFlags::WRONLY | OpenFlags::TRUNC,
                mode: FileMode::default(),
                out: fd,
            },
            &|err| syserr(anyhow!("opening /proc/self/uid_map for writing: {err}")),
        );
        builder.push(
            Syscall::Write {
                fd,
                buf: uid_map_contents.into_bump_str().as_bytes(),
            },
            &|err| syserr(anyhow!("writing to /proc/self/uid_map: {err}")),
        );

        // This set of syscalls disables setgroups, which is required for setting up the gid
        // mapping.
        builder.push(
            Syscall::Open {
                path: c"/proc/self/setgroups",
                flags: OpenFlags::WRONLY | OpenFlags::TRUNC,
                mode: FileMode::default(),
                out: fd,
            },
            &|err| syserr(anyhow!("opening /proc/self/setgroups for writing: {err}")),
        );
        builder.push(Syscall::Write { fd, buf: b"deny\n" }, &|err| {
            syserr(anyhow!("writing to /proc/self/setgroups: {err}"))
        });

        // Finally, we set up the gid mapping.
        let mut gid_map_contents = BumpString::with_capacity_in(24, &bump);
        writeln!(gid_map_contents, "{} {} 1", spec.group, self.group).map_err(syserr)?;
        builder.push(
            Syscall::Open {
                path: c"/proc/self/gid_map",
                flags: OpenFlags::WRONLY | OpenFlags::TRUNC,
                mode: FileMode::default(),
                out: fd,
            },
            &|err| syserr(anyhow!("opening /proc/self/gid_map for writing: {err}")),
        );
        builder.push(
            Syscall::Write {
                fd,
                buf: gid_map_contents.into_bump_str().as_bytes(),
            },
            &|err| syserr(anyhow!("writing to /proc/self/gid_map: {err}")),
        );

        // Make the child process the leader of a new session and process group. If we didn't do
        // this, then the process would be a member of a process group and session headed by a
        // process outside of the pid namespace, which would be confusing.
        builder.push(Syscall::SetSid, &|err| syserr(anyhow!("setsid: {err}")));

        // Dup2 the pipe file descriptors to be stdout and stderr. This will close the old stdout
        // and stderr. We don't have to worry about closing the old fds because they will be marked
        // close-on-exec below.
        builder.push(
            Syscall::Dup2 {
                from: stdout_write_fd.as_fd(),
                to: Fd::STDOUT,
            },
            &|err| syserr(anyhow!("dup2-ing to stdout: {err}")),
        );
        builder.push(
            Syscall::Dup2 {
                from: stderr_write_fd.as_fd(),
                to: Fd::STDERR,
            },
            &|err| syserr(anyhow!("dup2-ing to stderr: {err}")),
        );

        let new_root_path = self.mount_dir.as_c_str();

        // Create the fuse mount. We need to do this in the child's namespace, then pass the file
        // descriptor back to the parent.
        builder.push(
            Syscall::Open {
                path: c"/dev/fuse",
                flags: OpenFlags::RDWR | OpenFlags::NONBLOCK,
                mode: FileMode::default(),
                out: fd,
            },
            &|err| JobError::System(anyhow!("open /dev/fuse: {err}")),
        );
        builder.push(
            Syscall::FuseMount {
                source: c"Maelstrom LayerFS",
                target: new_root_path,
                flags: MountFlags::NODEV | MountFlags::NOSUID | MountFlags::RDONLY,
                root_mode: self.root_mode,
                uid: Uid::from_u32(spec.user.as_u32()),
                gid: Gid::from_u32(spec.group.as_u32()),
                fuse_fd: fd,
            },
            &|err| JobError::System(anyhow!("fuse mount: {err}")),
        );

        // Send the fuse mount file descriptor from the child to the parent.
        builder.push(
            Syscall::SendMsg {
                buf: &[0xFF; 8],
                fd_to_send: fd,
            },
            &|err| JobError::System(anyhow!("sendmsg: {err}")),
        );

        if spec.enable_writable_file_system {
            // We need to create an upperdir and workdir. Create a temporary file system to contain
            // both of them.
            builder.push(
                Syscall::Mount {
                    source: None,
                    target: self.tmpfs_dir.as_c_str(),
                    fstype: Some(c"tmpfs"),
                    flags: MountFlags::default(),
                    data: None,
                },
                &|err| {
                    syserr(anyhow!(
                        "mounting tmpfs file system for overlayfs's upperdir and workdir: {err}"
                    ))
                },
            );
            builder.push(
                Syscall::Mkdir {
                    path: self.upper_dir.as_c_str(),
                    mode: FileMode::RWXU,
                },
                &|err| syserr(anyhow!("making uppderdir for overlayfs: {err}")),
            );
            builder.push(
                Syscall::Mkdir {
                    path: self.work_dir.as_c_str(),
                    mode: FileMode::RWXU,
                },
                &|err| syserr(anyhow!("making workdir for overlayfs: {err}")),
            );

            // Use overlayfs.
            let fsfd = new_fd_slot(&bump);
            builder.push(
                Syscall::Fsopen {
                    fsname: c"overlay",
                    flags: FsopenFlags::default(),
                    out: fsfd,
                },
                &|err| syserr(anyhow!("fsopen of overlayfs: {err}")),
            );
            builder.push(
                Syscall::Fsconfig {
                    fd: fsfd,
                    command: FsconfigCommand::SET_STRING,
                    key: Some(c"lowerdir"),
                    value: Some(&new_root_path.to_bytes_with_nul()[0]),
                    aux: None,
                },
                &|err| syserr(anyhow!("fsconfig of lowerdir for overlayfs: {err}")),
            );
            builder.push(
                Syscall::Fsconfig {
                    fd: fsfd,
                    command: FsconfigCommand::SET_STRING,
                    key: Some(c"upperdir"),
                    value: Some(&self.upper_dir.to_bytes_with_nul()[0]),
                    aux: None,
                },
                &|err| syserr(anyhow!("fsconfig of upperdir for overlayfs: {err}")),
            );
            builder.push(
                Syscall::Fsconfig {
                    fd: fsfd,
                    command: FsconfigCommand::SET_STRING,
                    key: Some(c"workdir"),
                    value: Some(&self.work_dir.to_bytes_with_nul()[0]),
                    aux: None,
                },
                &|err| syserr(anyhow!("fsconfig of workdir for overlayfs: {err}")),
            );
            builder.push(
                Syscall::Fsconfig {
                    fd: fsfd,
                    command: FsconfigCommand::CMD_CREATE,
                    key: None,
                    value: None,
                    aux: None,
                },
                &|err| syserr(anyhow!("fsconfig of CMD_CREATE for overlayfs: {err}")),
            );
            builder.push(
                Syscall::Fsmount {
                    fd: fsfd,
                    flags: FsmountFlags::default(),
                    mount_attrs: MountAttrs::default(),
                    out: fsfd,
                },
                &|err| syserr(anyhow!("fsmount for overlayfs: {err}")),
            );
            builder.push(
                Syscall::MoveMount {
                    from_dirfd: fsfd,
                    from_path: c"",
                    to_dirfd: Fd::AT_FDCWD,
                    to_path: new_root_path,
                    flags: MoveMountFlags::F_EMPTY_PATH,
                },
                &|err| syserr(anyhow!("move_mount for overlayfs: {err}")),
            );
        }

        struct Device {
            cstr: &'static CStr,
            str: &'static str,
        }

        impl Device {
            fn new(device: JobDevice) -> Self {
                let (cstr, str) = match device {
                    JobDevice::Full => (c"/dev/full", "/dev/full"),
                    JobDevice::Fuse => (c"/dev/fuse", "/dev/fuse"),
                    JobDevice::Null => (c"/dev/null", "/dev/null"),
                    JobDevice::Ptmx => (c"/dev/ptmx", "/dev/ptmx"),
                    JobDevice::Random => (c"/dev/random", "/dev/random"),
                    JobDevice::Shm => (c"/dev/shm", "/dev/shm"),
                    JobDevice::Tty => (c"/dev/tty", "/dev/tty"),
                    JobDevice::Urandom => (c"/dev/urandom", "/dev/urandom"),
                    JobDevice::Zero => (c"/dev/zero", "/dev/zero"),
                };
                Self { cstr, str }
            }
        }

        let mut local_path_fds = BumpVec::new_in(&bump);

        // Open all of the source paths for devices before we chdir or pivot_root. We create
        // devices in the container my bind mounting them from the host instead of creating
        // devices. We do this because we don't assume we're running as root, and as such, we can't
        // create device files.
        for device in spec.devices.iter() {
            let Device { cstr, str } = Device::new(device);
            let dirfd = new_fd_slot(&bump);
            local_path_fds.push(dirfd);
            builder.push(
                Syscall::OpenTree {
                    dirfd: Fd::AT_FDCWD,
                    path: cstr,
                    // We don't pass recursive because we assume we're just cloning a file.
                    flags: OpenTreeFlags::CLONE,
                    out: dirfd,
                },
                bump.alloc(move |err| {
                    JobError::Execution(anyhow!(
                        "error opening local path for bind mount of device {str}: {err}",
                    ))
                }),
            );
        }

        // Resolve all of the source paths for bind mounts before we chdir or pivot_root. Each path
        // gets opened as a fsfd and pushed on the `local_path_fds` vec.
        for mount in &spec.mounts {
            match mount {
                JobMount::Bind { local_path, .. } => {
                    let dirfd = new_fd_slot(&bump);
                    local_path_fds.push(dirfd);
                    builder.push(
                        Syscall::OpenTree {
                            dirfd: Fd::AT_FDCWD,
                            path: bump_c_str(&bump, local_path.as_str()).map_err(syserr)?,
                            // We always pass recursive here because non-recursive bind mounts don't make a
                            // lot of sense in this context. The reason is that, when a new mount namespace
                            // is created in Linux, all pre-existing mounts become locked. These locked
                            // mounts can't be unmounted or have their mount flags adjusted. In this
                            // context, we're always in a new mount namespace, hence all mounts on the
                            // system are locked.
                            //
                            // When you try to call open_tree without AT_RECURSIVE on a tree that has
                            // mounts under it, you get an error. The reason is that allowing the binding
                            // of this subtree would have the effect of revealing what was underneath those
                            // locked mounts -- in effect, unmounting them.
                            //
                            // If we allowed the user to specify the recursive flag, it would just mean
                            // that their mount would fail if there happened to be any mount at that point
                            // or lower in the tree. That's probably not what they want.
                            flags: OpenTreeFlags::CLONE | OpenTreeFlags::RECURSIVE,
                            out: dirfd,
                        },
                        bump.alloc(move |err| {
                            JobError::Execution(anyhow!(
                                "error opening local path {local_path} for bind mount: {err}",
                            ))
                        }),
                    );
                }
                JobMount::Devices { devices, .. } => {
                    // Open all of the source paths for devices before we chdir or pivot_root. We
                    // create devices in the container my bind mounting them from the host instead
                    // of creating devices. We do this because we don't assume we're running as
                    // root, and as such, we can't create device files.
                    for device in devices.iter() {
                        let Device { cstr, str } = Device::new(device);
                        let dirfd = new_fd_slot(&bump);
                        local_path_fds.push(dirfd);
                        builder.push(
                            Syscall::OpenTree {
                                dirfd: Fd::AT_FDCWD,
                                path: cstr,
                                // We don't pass recursive because we assume we're just cloning a
                                // file.
                                flags: OpenTreeFlags::CLONE,
                                out: dirfd,
                            },
                            bump.alloc(move |err| {
                                JobError::Execution(anyhow!(
                                    "error opening local path for bind mount of device {str}: {err}",
                                ))
                            }),
                        );
                    }
                }
                _ => {}
            }
        }

        // Chdir to what will be the new root.
        builder.push(
            Syscall::Chdir {
                path: new_root_path,
            },
            &|err| syserr(anyhow!("chdir to target root directory: {err}")),
        );

        // Pivot root to be the new root. See man 2 pivot_root.
        builder.push(
            Syscall::PivotRoot {
                new_root: c".",
                put_old: c".",
            },
            &|err| syserr(anyhow!("pivot_root: {err}")),
        );

        let mut local_path_fds = local_path_fds.into_iter();

        // Complete the bind mounting of devices.
        for device in spec.devices.iter() {
            let Device { cstr, str } = Device::new(device);
            let mount_local_path_fd = local_path_fds.next().unwrap();
            builder.push(
                Syscall::MoveMount {
                    from_dirfd: mount_local_path_fd,
                    from_path: c"",
                    to_dirfd: Fd::AT_FDCWD,
                    to_path: cstr,
                    flags: MoveMountFlags::F_EMPTY_PATH,
                },
                bump.alloc(move |err| {
                    JobError::Execution(anyhow!(
                        "error doing move_mount for bind mount of device {str}: {err}",
                    ))
                }),
            );
        }

        // Set up the mounts after we've called pivot_root so the absolute paths specified stay
        // within the container.
        //
        // N.B. It seems like it's a security feature of Linux that sysfs and proc can't be mounted
        // unless they are already mounted. So we have to do this before we unmount the old root.
        // If we do the unmount first, then we'll get permission errors mounting those fs types.
        for mount in &spec.mounts {
            fn normal_mount<'a>(
                bump: &'a Bump,
                builder: &mut ScriptBuilder<'a>,
                mount_point: &'a Utf8PathBuf,
                cfstype: &'static CStr,
                fstype: &'static str,
            ) -> JobResult<(), Error> {
                builder.push(
                    Syscall::Mount {
                        source: None,
                        target: bump_c_str(bump, mount_point.as_str()).map_err(syserr)?,
                        fstype: Some(cfstype),
                        flags: MountFlags::default(),
                        data: None,
                    },
                    bump.alloc(move |err| {
                        JobError::Execution(anyhow!(
                            "mount of {fstype} file system to {mount_point}: {err}",
                        ))
                    }),
                );
                Ok(())
            }
            match mount {
                JobMount::Bind {
                    mount_point,
                    local_path,
                    read_only,
                } => {
                    let mount_local_path_fd = local_path_fds.next().unwrap();
                    let mount_point_cstr =
                        bump_c_str(&bump, mount_point.as_str()).map_err(syserr)?;
                    builder.push(
                        Syscall::MoveMount {
                            from_dirfd: mount_local_path_fd,
                            from_path: c"",
                            to_dirfd: Fd::AT_FDCWD,
                            to_path: mount_point_cstr,
                            flags: MoveMountFlags::F_EMPTY_PATH,
                        },
                        bump.alloc(move |err| {
                            JobError::Execution(anyhow!(
                                "bind mount of {local_path} to {mount_point}: {err}",
                            ))
                        }),
                    );
                    if *read_only {
                        builder.push(
                            Syscall::Mount {
                                source: None,
                                target: mount_point_cstr,
                                fstype: None,
                                flags: MountFlags::BIND | MountFlags::REMOUNT | MountFlags::RDONLY,
                                data: None,
                            },
                            bump.alloc(move |err| {
                                JobError::Execution(anyhow!(
                                    "remounting bind mount of {mount_point} as read-only: {err}",
                                ))
                            }),
                        );
                    }
                }
                JobMount::Devpts { mount_point } => {
                    normal_mount(&bump, &mut builder, mount_point, c"devpts", "devpts")?;
                }
                JobMount::Devices { devices } => {
                    for device in devices.iter() {
                        let Device { cstr, str } = Device::new(device);
                        let mount_local_path_fd = local_path_fds.next().unwrap();
                        builder.push(
                            Syscall::MoveMount {
                                from_dirfd: mount_local_path_fd,
                                from_path: c"",
                                to_dirfd: Fd::AT_FDCWD,
                                to_path: cstr,
                                flags: MoveMountFlags::F_EMPTY_PATH,
                            },
                            bump.alloc(move |err| {
                                JobError::Execution(anyhow!(
                                    "error doing move_mount for bind mount of device {str}: {err}",
                                ))
                            }),
                        );
                    }
                }
                JobMount::Mqueue { mount_point } => {
                    normal_mount(&bump, &mut builder, mount_point, c"mqueue", "mqueue")?;
                }
                JobMount::Proc { mount_point } => {
                    normal_mount(&bump, &mut builder, mount_point, c"proc", "proc")?;
                }
                JobMount::Sys { mount_point } => {
                    normal_mount(&bump, &mut builder, mount_point, c"sysfs", "sysfs")?;
                }
                JobMount::Tmp { mount_point } => {
                    normal_mount(&bump, &mut builder, mount_point, c"tmpfs", "tmpfs")?;
                }
            };
        }

        // Unmount the old root. See man 2 pivot_root.
        builder.push(
            Syscall::Umount2 {
                path: c".",
                flags: UmountFlags::DETACH,
            },
            &|err| syserr(anyhow!("umount of old root: {err}")),
        );

        // Change to the working directory, if it's not "/".
        if spec.working_directory != Path::new("/") {
            let working_directory =
                bump_c_str_from_bytes(&bump, spec.working_directory.as_os_str().as_bytes())
                    .map_err(syserr)?;
            builder.push(
                Syscall::Chdir {
                    path: working_directory,
                },
                &|err| JobError::Execution(anyhow!("chdir: {err}")),
            );
        }

        // Set close-on-exec for all file descriptors except stdin, stdout, and stderr. We do this
        // last thing, right before the exec, so that we catch any file descriptors opened above.
        builder.push(
            Syscall::CloseRange {
                first: CloseRangeFirst::AfterStderr,
                last: CloseRangeLast::Max,
                flags: CloseRangeFlags::CLOEXEC,
            },
            &|err| {
                syserr(anyhow!(
                    "setting CLOEXEC on range of open file descriptors: {err}"
                ))
            },
        );

        // Finally, do the exec.
        let program = bump_c_str(&bump, spec.program.as_str()).map_err(syserr)?;
        let mut arguments =
            BumpVec::with_capacity_in(spec.arguments.len().checked_add(2).unwrap(), &bump);
        arguments.push(Some(&program.to_bytes_with_nul()[0]));
        for argument in &spec.arguments {
            let argument_cstr = bump_c_str(&bump, argument.as_str()).map_err(syserr)?;
            arguments.push(Some(&argument_cstr.to_bytes_with_nul()[0]));
        }
        arguments.push(None);
        let mut environment =
            BumpVec::with_capacity_in(spec.environment.len().checked_add(1).unwrap(), &bump);
        for var in &spec.environment {
            let var_cstr = bump_c_str(&bump, var.as_str()).map_err(syserr)?;
            environment.push(Some(&var_cstr.to_bytes_with_nul()[0]));
        }
        environment.push(None);
        builder.push(
            Syscall::Execve {
                path: program,
                argv: arguments.into_bump_slice(),
                envp: environment.into_bump_slice(),
            },
            &|err| JobError::Execution(anyhow!("execvc: {err}")),
        );

        // We're finally ready to actually clone the child.
        let mut clone_flags = CloneFlags::NEWCGROUP
            | CloneFlags::NEWIPC
            | CloneFlags::NEWNS
            | CloneFlags::NEWPID
            | CloneFlags::NEWUSER
            | CloneFlags::VM;
        if newnet {
            clone_flags |= CloneFlags::NEWNET;
        }
        let mut clone_args = CloneArgs::default()
            .flags(clone_flags)
            .exit_signal(Signal::CHLD);
        let mut args = maelstrom_worker_child::ChildArgs {
            write_sock: write_sock.as_fd(),
            syscalls: builder.syscalls.as_mut_slice(),
        };
        const CHILD_STACK_SIZE: usize = 1024;
        let mut stack = bumpalo::vec![in &bump; 0; CHILD_STACK_SIZE];
        let clone_res = unsafe {
            linux::clone_with_child_pidfd(
                maelstrom_worker_child::start_and_exec_in_child_trampoline,
                stack.as_mut_ptr().wrapping_add(CHILD_STACK_SIZE) as *mut _,
                &mut args as *mut _ as *mut _,
                &mut clone_args,
            )
        };
        let child_pidfd = match clone_res {
            Ok((_, child_pidfd)) => child_pidfd,
            Err(err) => {
                return Err(syserr(err));
            }
        };

        let (stdout_sender, stdout_receiver) = oneshot::channel();
        let (stderr_sender, stderr_receiver) = oneshot::channel();

        // Spawn a waiter task to wait on child to terminate. We do this immediately after the
        // clone so that even if this functions returns early, we will make sure that we wait on
        // the child.
        //
        // It's not clear what to do if we get an error waiting, which, in theory, should never
        // happen. What we do is return the error so that the client can get back a system error.
        // An alternative would be to panic and send a plain JobStatus back.
        let (status_sender, status_receiver) = oneshot::channel();
        runtime.spawn(async move {
            let _ = status_sender.send(wait_for_child(child_pidfd, kill_event_receiver).await);
        });

        // Read (in a blocking manner) from the exec result pipe. The child will write to the pipe if
        // it has an error exec-ing. The child will mark the write side of the pipe exec-on-close, so
        // we'll read an immediate EOF if the exec is successful.
        //
        // It's important to drop our copy of the write side of the pipe first. Otherwise, we'd
        // deadlock on ourselves!
        drop(write_sock);

        let mut exec_result_buf = [0; mem::size_of::<u64>()];
        loop {
            let (count, fd) = read_sock
                .recv_with_fd(&mut exec_result_buf)
                .map_err(syserr)?;
            if count == 0 {
                // If we get EOF, the child successfully exec'd
                break;
            }

            // We don't expect to get a truncated message in this case
            if count != exec_result_buf.len() {
                return Err(syserr(anyhow!(
                    "couldn't parse exec result pipe's contents: {:?}",
                    &exec_result_buf[..count]
                )));
            }

            // If we get an file-descriptor, we pass it to the FUSE callback
            if let Some(fd) = fd {
                let fuse_spawn = fuse_spawn
                    .take()
                    .ok_or(syserr(anyhow!("multiple FUSE fds")))?;
                fuse_spawn(fd);
                continue;
            }

            // Otherwise it should be an error we got back from exec
            let result = u64::from_ne_bytes(exec_result_buf);
            let index = (result >> 32) as usize;
            let errno = result & 0xffffffff;
            return Err(builder.error_transformers[index](
                Errno::from_u64(errno).desc().unwrap_or("Unknown error"),
            ));
        }

        let start = self.clock.now();

        // Spawn independent tasks to consume stdout and stderr. We want to do this in parallel so
        // that we don't cause a deadlock on one while we're reading the other one.
        //
        // We put these in a JoinSet so that they will be canceled if we return early.
        //
        // We have to drop our own copies of the write side of the pipe. If we didn't, these tasks
        // would never complete.
        let mut joinset = JoinSet::new();
        joinset.spawn_on(
            output_reader_task_main(stdout_read_fd, inline_limit, stdout_sender),
            &runtime,
        );
        joinset.spawn_on(
            output_reader_task_main(stderr_read_fd, inline_limit, stderr_sender),
            &runtime,
        );
        drop(stdout_write_fd);
        drop(stderr_write_fd);

        // Wait for everything and return the result.
        fn read_from_receiver<T>(receiver: oneshot::Receiver<Result<T>>) -> JobResult<T, Error> {
            receiver
                .blocking_recv()
                .unwrap_or_else(|e| {
                    Err(anyhow!(
                        "unexpected error receiving from oneshot receiver: {e}"
                    ))
                })
                .map_err(syserr)
        }
        Ok(JobCompleted {
            status: read_from_receiver(status_receiver)?,
            effects: JobEffects {
                stdout: read_from_receiver(stdout_receiver)?,
                stderr: read_from_receiver(stderr_receiver)?,
                duration: start.elapsed(),
            },
        })
    }
}

/*  _            _
 * | |_ ___  ___| |_ ___
 * | __/ _ \/ __| __/ __|
 * | ||  __/\__ \ |_\__ \
 *  \__\___||___/\__|___/
 *  FIGLET: tests
 */

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::*;
    use bytesize::ByteSize;
    use indoc::indoc;
    use maelstrom_base::{enum_set, nonempty, ArtifactType, JobStatus, Utf8Path};
    use maelstrom_layer_fs::{BlobDir, BottomLayerBuilder, LayerFs, ReaderCache};
    use maelstrom_test::{boxed_u8, digest, utf8_path_buf};
    use maelstrom_util::{async_fs, log::test_logger, sync, time::TickingClock};
    use std::{collections::HashSet, env, fs, path::PathBuf, str, sync::Arc};
    use tempfile::{NamedTempFile, TempDir};
    use tokio::{io::AsyncWriteExt as _, net::TcpListener, sync::oneshot, sync::Mutex, task};

    const ARBITRARY_TIME: maelstrom_base::manifest::UnixTimestamp =
        maelstrom_base::manifest::UnixTimestamp(1705000271);

    struct TarMount {
        _temp_dir: TempDir,
        data_path: PathBuf,
        blob_dir: RootBuf<BlobDir>,
        cache: Arc<Mutex<ReaderCache>>,
    }

    impl TarMount {
        async fn new() -> Self {
            let fs = async_fs::Fs::new();
            let temp_dir = TempDir::new().unwrap();
            let data_path = temp_dir.path().join("data");
            fs.create_dir(&data_path).await.unwrap();
            let blob_dir = RootBuf::<BlobDir>::new(temp_dir.path().join("cache"));
            fs.create_dir(&blob_dir).await.unwrap();
            let tar_bytes = include_bytes!("executor-test-deps.tar");
            struct BlobFile;
            let tar_path = blob_dir.join::<BlobFile>(format!("{}", digest!(42)));
            fs.write(&tar_path, &tar_bytes).await.unwrap();
            let mut builder =
                BottomLayerBuilder::new(test_logger(), &fs, &data_path, &blob_dir, ARBITRARY_TIME)
                    .await
                    .unwrap();
            builder
                .add_from_tar(digest!(42), fs.open_file(&tar_path).await.unwrap())
                .await
                .unwrap();
            let _ = builder.finish().await.unwrap();
            let cache = Arc::new(Mutex::new(ReaderCache::new()));
            Self {
                _temp_dir: temp_dir,
                cache,
                data_path,
                blob_dir,
            }
        }

        fn spawn(&self, fd: linux::OwnedFd) {
            let layer_fs = LayerFs::from_path(&self.data_path, &self.blob_dir).unwrap();
            let cache = self.cache.clone();
            task::spawn(async move { layer_fs.run_fuse(test_logger(), cache, fd).await.unwrap() });
        }
    }

    async fn run(
        spec: maelstrom_base::JobSpec,
        inline_limit: InlineLimit,
    ) -> JobResult<JobCompleted, Error> {
        let clock = TickingClock::new();
        let mount = TarMount::new().await;
        let spec = JobSpec::from_spec(spec);
        let (_kill_event_sender, kill_event_receiver) = sync::event();
        task::spawn_blocking(move || {
            Executor::new(
                RootBuf::new(tempfile::tempdir().unwrap().into_path()),
                RootBuf::new(tempfile::tempdir().unwrap().into_path()),
                &clock,
            )
            .unwrap()
            .run_job(
                &spec,
                inline_limit,
                kill_event_receiver,
                |fd| mount.spawn(fd),
                runtime::Handle::current(),
            )
        })
        .await
        .unwrap()
    }

    struct Test {
        spec: maelstrom_base::JobSpec,
        inline_limit: InlineLimit,
        expected_status: JobStatus,
        expected_stdout: JobOutputResult,
        expected_stderr: JobOutputResult,
        expected_duration: std::time::Duration,
    }

    impl Test {
        fn new(spec: maelstrom_base::JobSpec) -> Self {
            Test {
                spec,
                inline_limit: InlineLimit::from(ByteSize::b(1000)),
                expected_status: JobStatus::Exited(0),
                expected_stdout: JobOutputResult::None,
                expected_stderr: JobOutputResult::None,
                expected_duration: std::time::Duration::from_secs(1),
            }
        }

        fn inline_limit(mut self, inline_limit: impl Into<InlineLimit>) -> Self {
            self.inline_limit = inline_limit.into();
            self
        }

        fn expected_status(mut self, expected_status: JobStatus) -> Self {
            self.expected_status = expected_status;
            self
        }

        fn expected_stdout(mut self, expected_stdout: JobOutputResult) -> Self {
            self.expected_stdout = expected_stdout;
            self
        }

        fn expected_stderr(mut self, expected_stderr: JobOutputResult) -> Self {
            self.expected_stderr = expected_stderr;
            self
        }

        async fn run(self) {
            let JobCompleted {
                status,
                effects:
                    JobEffects {
                        stdout,
                        stderr,
                        duration,
                    },
            } = run(self.spec, self.inline_limit).await.unwrap();

            assert_eq!(stderr, self.expected_stderr);
            assert_eq!(status, self.expected_status);
            assert_eq!(stdout, self.expected_stdout);
            assert_eq!(duration, self.expected_duration);
        }
    }

    fn test_spec(program: &str) -> maelstrom_base::JobSpec {
        maelstrom_base::JobSpec::new(program, nonempty![(digest![0], ArtifactType::Tar)])
    }

    fn bash_spec(script: &str) -> maelstrom_base::JobSpec {
        test_spec("/usr/bin/bash").arguments(["-c", script])
    }

    fn python_spec(script: &str) -> maelstrom_base::JobSpec {
        test_spec("/usr/bin/python3").arguments(["-c", script])
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn exited_0() {
        Test::new(bash_spec("exit 0")).run().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn exited_1() {
        Test::new(bash_spec("exit 1"))
            .expected_status(JobStatus::Exited(1))
            .run()
            .await;
    }

    // $$ returns the pid of outer-most bash. This doesn't do what we expect it to do when using
    // our executor. We should probably rewrite these tests to run python or something, and take
    // input from stdin.
    #[tokio::test(flavor = "multi_thread")]
    async fn signaled_11() {
        Test::new(python_spec(indoc! {r#"
            import os
            import sys
            print('a')
            sys.stdout.flush()
            print('b', file=sys.stderr)
            sys.stderr.flush()
            os.abort()
        "#}))
        .expected_status(JobStatus::Signaled(11))
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"a\n")))
        .expected_stderr(JobOutputResult::Inline(boxed_u8!(b"b\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stdout_inline_limit_0() {
        Test::new(bash_spec("echo a"))
            .inline_limit(ByteSize::b(0))
            .expected_stdout(JobOutputResult::Truncated {
                first: boxed_u8!(b""),
                truncated: 2,
            })
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stdout_inline_limit_1() {
        Test::new(bash_spec("echo a"))
            .inline_limit(ByteSize::b(1))
            .expected_stdout(JobOutputResult::Truncated {
                first: boxed_u8!(b"a"),
                truncated: 1,
            })
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stdout_inline_limit_2() {
        Test::new(bash_spec("echo a"))
            .inline_limit(ByteSize::b(2))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stdout_inline_limit_3() {
        Test::new(bash_spec("echo a"))
            .inline_limit(ByteSize::b(3))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stderr_inline_limit_0() {
        Test::new(bash_spec("echo a >&2"))
            .inline_limit(ByteSize::b(0))
            .expected_stderr(JobOutputResult::Truncated {
                first: boxed_u8!(b""),
                truncated: 2,
            })
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stderr_inline_limit_1() {
        Test::new(bash_spec("echo a >&2"))
            .inline_limit(ByteSize::b(1))
            .expected_stderr(JobOutputResult::Truncated {
                first: boxed_u8!(b"a"),
                truncated: 1,
            })
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stderr_inline_limit_2() {
        Test::new(bash_spec("echo a >&2"))
            .inline_limit(ByteSize::b(2))
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stderr_inline_limit_3() {
        Test::new(bash_spec("echo a >&2"))
            .inline_limit(ByteSize::b(3))
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn environment() {
        Test::new(bash_spec("echo -n $FOO - $BAR").environment(["FOO=3", "BAR=4"]))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"3 - 4")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stdin_empty() {
        Test::new(test_spec("/bin/cat")).run().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pid_ppid_pgid_and_sid() {
        // We should be pid 1, that is, init for our namespace).
        // We should have ppid 0, indicating that our parent isn't accessible in our namespace.
        // We should have pgid 1, indicating that we're the group leader.
        // We should have sid 1, indicating that we're the session leader.
        Test::new(python_spec(indoc! {r#"
            import os
            print('pid:', os.getpid())
            print('ppid:', os.getppid())
            print('pgid:', os.getpgid(0))
            print('sid:', os.getsid(0))
        "#}))
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(indoc! {b"
            pid: 1
            ppid: 0
            pgid: 1
            sid: 1
        "})))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_loopback() {
        Test::new(
            test_spec("/bin/cat")
                .arguments(["/sys/class/net/lo/carrier"])
                .mounts([JobMount::Sys {
                    mount_point: utf8_path_buf!("/sys"),
                }]),
        )
        .expected_status(JobStatus::Exited(1))
        .expected_stderr(JobOutputResult::Inline(boxed_u8!(
            b"cat: read error: Invalid argument\n"
        )))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn loopback() {
        Test::new(
            test_spec("/bin/cat")
                .arguments(["/sys/class/net/lo/carrier"])
                .mounts([JobMount::Sys {
                    mount_point: utf8_path_buf!("/sys"),
                }])
                .network(JobNetwork::Loopback),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_network() {
        let (port_sender, port_receiver) = oneshot::channel();
        let listening_task = task::spawn(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            port_sender
                .send(listener.local_addr().unwrap().port())
                .unwrap();
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut contents = String::new();
            socket.read_to_string(&mut contents).await.unwrap();
            assert_eq!(contents, "hello");
            socket.write_all(b"goodbye").await.unwrap();
        });
        let port = port_receiver.await.unwrap();
        Test::new(
            python_spec(&format!(
                indoc! {r#"
                    import socket
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect(("127.0.0.1", {port}))
                        s.sendall(b"hello")
                        s.shutdown(socket.SHUT_WR)
                        print(s.recv(1024).decode(), end="")
                "#},
                port = port
            ))
            .network(JobNetwork::Local),
        )
        .expected_status(JobStatus::Exited(0))
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"goodbye")))
        .run()
        .await;
        listening_task.await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn user_and_group_0() {
        Test::new(python_spec(indoc! {r#"
            import os
            print('uid:', os.getuid())
            print('gid:', os.getgid())
        "#}))
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(indoc! {b"
            uid: 0
            gid: 0
        "})))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn user_and_group_nonzero() {
        Test::new(
            python_spec(indoc! {r#"
                import os
                print('uid:', os.getuid())
                print('gid:', os.getgid())
            "#})
            .user(UserId::from(43))
            .group(GroupId::from(100)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(indoc! {b"
            uid: 43
            gid: 100
        "})))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn close_range() {
        // Throw the kitchen sink in the spec: we want an example of anything that opens a file
        // descriptor.
        let spec = test_spec("/bin/ls")
            .arguments(["/proc/self/fd"])
            .network(JobNetwork::Loopback)
            .mounts([
                JobMount::Proc {
                    mount_point: utf8_path_buf!("/proc"),
                },
                JobMount::Bind {
                    mount_point: utf8_path_buf!("/mnt"),
                    local_path: utf8_path_buf!("/"),
                    read_only: false,
                },
            ])
            .devices([JobDevice::Null]);
        let JobCompleted {
            status,
            effects: JobEffects { stdout, stderr, .. },
        } = run(spec, "100".parse().unwrap()).await.unwrap();
        assert_eq!(stderr, JobOutputResult::None);
        assert_eq!(status, JobStatus::Exited(0));
        let JobOutputResult::Inline(contents) = stdout else {
            panic!("expected JobOutputResult::Inline, got {stdout:?}");
        };
        let fds: HashSet<u32> = str::from_utf8(&contents)
            .unwrap()
            .split_whitespace()
            .map(|fd| fd.parse().unwrap())
            .collect();
        assert!(fds.is_superset(&HashSet::from([0, 1, 2])));
        assert_eq!(
            fds.len(),
            4,
            "expected fds to just be stdin, stdout, stderr, and one more, got {fds:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_layer_is_read_only() {
        Test::new(test_spec("/bin/touch").arguments(["/foo"]))
            .expected_status(JobStatus::Exited(1))
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(
                b"touch: /foo: Read-only file system\n"
            )))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_layer_with_writable_file_system_is_writable() {
        Test::new(bash_spec("echo bar > /foo && cat /foo").enable_writable_file_system(true))
            .expected_status(JobStatus::Exited(0))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"bar\n")))
            .run()
            .await;

        // Run another job to ensure that the file doesn't persist.
        Test::new(bash_spec("test -e /foo"))
            .expected_status(JobStatus::Exited(1))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multiple_layers_with_writable_file_system_is_writable() {
        let spec = bash_spec("echo bar > /foo && cat /foo").enable_writable_file_system(true);
        Test::new(spec)
            .expected_status(JobStatus::Exited(0))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"bar\n")))
            .run()
            .await;

        // Run another job to ensure that the file doesn't persist.
        let spec = bash_spec("test -e /foo").enable_writable_file_system(true);
        Test::new(spec)
            .expected_status(JobStatus::Exited(1))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_mount_dev_full() {
        Test::new(bash_spec("/bin/ls -l /dev/full | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mount_dev_full() {
        Test::new(
            bash_spec("/bin/ls -l /dev/full | awk '{print $5, $6}'").mounts([JobMount::Devices {
                devices: EnumSet::only(JobDevice::Full),
            }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 7\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_mount_dev_fuse() {
        Test::new(bash_spec("/bin/ls -l /dev/fuse | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mount_dev_fuse() {
        Test::new(
            bash_spec("/bin/ls -l /dev/fuse | awk '{print $5, $6}'").mounts([JobMount::Devices {
                devices: EnumSet::only(JobDevice::Fuse),
            }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"10, 229\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_mount_dev_null() {
        Test::new(bash_spec("/bin/ls -l /dev/null | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mount_dev_null() {
        Test::new(
            bash_spec("/bin/ls -l /dev/null | awk '{print $5, $6}'").mounts([JobMount::Devices {
                devices: EnumSet::only(JobDevice::Null),
            }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 3\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mount_dev_null_write() {
        Test::new(
            bash_spec("echo foo > /dev/null && cat /dev/null").mounts([JobMount::Devices {
                devices: EnumSet::only(JobDevice::Null),
            }]),
        )
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_mount_dev_ptmx() {
        Test::new(bash_spec("/bin/ls -l /dev/ptmx | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mount_dev_ptmx() {
        Test::new(
            bash_spec("/bin/ls -l /dev/ptmx | awk '{print $5, $6}'").mounts([JobMount::Devices {
                devices: EnumSet::only(JobDevice::Ptmx),
            }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"5, 2\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_mount_dev_random() {
        Test::new(bash_spec("/bin/ls -l /dev/random | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mount_dev_random() {
        Test::new(
            bash_spec("/bin/ls -l /dev/random | awk '{print $5, $6}'").mounts([
                JobMount::Devices {
                    devices: EnumSet::only(JobDevice::Random),
                },
            ]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 8\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_mount_dev_tty() {
        Test::new(bash_spec("/bin/ls -l /dev/tty | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mount_dev_tty() {
        Test::new(
            bash_spec("/bin/ls -l /dev/tty | awk '{print $5, $6}'").mounts([JobMount::Devices {
                devices: EnumSet::only(JobDevice::Tty),
            }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"5, 0\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_mount_dev_urandom() {
        Test::new(bash_spec("/bin/ls -l /dev/urandom | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mount_dev_urandom() {
        Test::new(
            bash_spec("/bin/ls -l /dev/urandom | awk '{print $5, $6}'").mounts([
                JobMount::Devices {
                    devices: EnumSet::only(JobDevice::Urandom),
                },
            ]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 9\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_mount_dev_zero() {
        Test::new(bash_spec("/bin/ls -l /dev/zero | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mount_dev_zero() {
        Test::new(
            bash_spec("/bin/ls -l /dev/zero | awk '{print $5, $6}'").mounts([JobMount::Devices {
                devices: EnumSet::only(JobDevice::Zero),
            }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 5\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_full() {
        Test::new(bash_spec("/bin/ls -l /dev/full | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_full() {
        Test::new(
            bash_spec("/bin/ls -l /dev/full | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Full)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 7\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_null() {
        Test::new(bash_spec("/bin/ls -l /dev/null | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_null() {
        Test::new(
            bash_spec("/bin/ls -l /dev/null | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Null)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 3\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_null_write() {
        Test::new(
            bash_spec("echo foo > /dev/null && cat /dev/null")
                .devices(EnumSet::only(JobDevice::Null)),
        )
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_ptmx() {
        Test::new(bash_spec("/bin/ls -l /dev/ptmx | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_ptmx() {
        Test::new(
            bash_spec("/bin/ls -l /dev/ptmx | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Ptmx)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"5, 2\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_random() {
        Test::new(bash_spec("/bin/ls -l /dev/random | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_random() {
        Test::new(
            bash_spec("/bin/ls -l /dev/random | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Random)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 8\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_tty() {
        Test::new(bash_spec("/bin/ls -l /dev/tty | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_tty() {
        Test::new(
            bash_spec("/bin/ls -l /dev/tty | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Tty)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"5, 0\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_urandom() {
        Test::new(bash_spec("/bin/ls -l /dev/urandom | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_urandom() {
        Test::new(
            bash_spec("/bin/ls -l /dev/urandom | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Urandom)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 9\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_zero() {
        Test::new(bash_spec("/bin/ls -l /dev/zero | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_zero() {
        Test::new(
            bash_spec("/bin/ls -l /dev/zero | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Zero)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 5\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_tmpfs() {
        Test::new(
            test_spec("/bin/grep")
                .arguments(["^tmpfs /tmp", "/proc/self/mounts"])
                .mounts([JobMount::Proc {
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_status(JobStatus::Exited(1))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tmpfs() {
        Test::new(
            test_spec("/bin/awk")
                .arguments([r#"/^none \/tmp/ { print $1, $2, $3 }"#, "/proc/self/mounts"])
                .mounts([
                    JobMount::Proc {
                        mount_point: utf8_path_buf!("/proc"),
                    },
                    JobMount::Tmp {
                        mount_point: utf8_path_buf!("/tmp"),
                    },
                ]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"none /tmp tmpfs\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_sysfs() {
        Test::new(
            test_spec("/bin/grep")
                .arguments(["^sysfs /sys", "/proc/self/mounts"])
                .mounts([JobMount::Proc {
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_status(JobStatus::Exited(1))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sysfs() {
        Test::new(
            test_spec("/bin/awk")
                .arguments([r#"/^none \/sys/ { print $1, $2, $3 }"#, "/proc/self/mounts"])
                .mounts([
                    JobMount::Proc {
                        mount_point: utf8_path_buf!("/proc"),
                    },
                    JobMount::Sys {
                        mount_point: utf8_path_buf!("/sys"),
                    },
                ]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"none /sys sysfs\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_procfs() {
        Test::new(test_spec("/bin/ls").arguments(["/proc"]))
            .run()
            .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn procfs() {
        Test::new(
            test_spec("/bin/grep")
                .arguments(["proc", "/proc/self/mounts"])
                .mounts([JobMount::Proc {
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(
            b"none /proc proc rw,relatime 0 0\n"
        )))
        .run()
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_devpty() {
        Test::new(
            test_spec("/bin/grep")
                .arguments(["^devpty /dev/pty", "/proc/self/mounts"])
                .mounts([JobMount::Proc {
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_status(JobStatus::Exited(1))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn devpty() {
        Test::new(
            test_spec("/bin/awk")
                .arguments([
                    r#"/^none \/dev\/pts/ { print $1, $2, $3 }"#,
                    "/proc/self/mounts",
                ])
                .mounts([
                    JobMount::Proc {
                        mount_point: utf8_path_buf!("/proc"),
                    },
                    JobMount::Devpts {
                        mount_point: utf8_path_buf!("/dev/pts"),
                    },
                ]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(
            b"none /dev/pts devpts\n"
        )))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_mqueue() {
        Test::new(
            test_spec("/bin/grep")
                .arguments(["^mqueue /dev/mqueue", "/proc/self/mounts"])
                .mounts([JobMount::Proc {
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_status(JobStatus::Exited(1))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mqueue() {
        Test::new(
            test_spec("/bin/awk")
                .arguments([
                    r#"/^none \/dev\/mqueue/ { print $1, $2, $3 }"#,
                    "/proc/self/mounts",
                ])
                .mounts([
                    JobMount::Proc {
                        mount_point: utf8_path_buf!("/proc"),
                    },
                    JobMount::Mqueue {
                        mount_point: utf8_path_buf!("/dev/mqueue"),
                    },
                ]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(
            b"none /dev/mqueue mqueue\n"
        )))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bind_mount_writable() {
        let temp_file = NamedTempFile::new().unwrap();
        Test::new(
            bash_spec(&format!(
                "echo hello > /mnt/{}",
                temp_file.path().file_name().unwrap().to_str().unwrap()
            ))
            .mounts([JobMount::Bind {
                mount_point: utf8_path_buf!("/mnt"),
                local_path: <&Utf8Path>::try_from(temp_file.path().parent().unwrap())
                    .unwrap()
                    .to_owned(),
                read_only: false,
            }]),
        )
        .run()
        .await;
        let contents = fs::read_to_string(temp_file).unwrap();
        assert_eq!(contents, "hello\n");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bind_mount_read_only() {
        let temp_file = NamedTempFile::new().unwrap();
        let fs = async_fs::Fs::new();
        fs.write(temp_file.path(), b"hello\n").await.unwrap();
        Test::new(
            bash_spec(&format!(
                "(echo goodbye > /mnt/{}) 2>/dev/null",
                temp_file.path().file_name().unwrap().to_str().unwrap()
            ))
            .devices(enum_set!(JobDevice::Null))
            .mounts([JobMount::Bind {
                mount_point: utf8_path_buf!("/mnt"),
                local_path: <&Utf8Path>::try_from(temp_file.path().parent().unwrap())
                    .unwrap()
                    .to_owned(),
                read_only: true,
            }]),
        )
        .expected_status(JobStatus::Exited(1))
        .run()
        .await;
        let contents = fs::read_to_string(temp_file).unwrap();
        assert_eq!(contents, "hello\n");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bind_mount_path_is_relative_to_pwd() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_dir_path = temp_file.path().parent().unwrap();
        let pwd = env::current_dir().unwrap();
        let temp_dir_relative_path =
            Utf8PathBuf::from_path_buf(pathdiff::diff_paths(&temp_dir_path, pwd).unwrap()).unwrap();
        let fs = async_fs::Fs::new();
        fs.write(temp_file.path(), b"hello\n").await.unwrap();
        Test::new(
            test_spec("/bin/cat")
                .arguments([format!(
                    "/mnt/{}",
                    temp_file.path().file_name().unwrap().to_str().unwrap()
                )])
                .mounts([JobMount::Bind {
                    mount_point: utf8_path_buf!("/mnt"),
                    local_path: temp_dir_relative_path,
                    read_only: true,
                }]),
        )
        .expected_status(JobStatus::Exited(0))
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"hello\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn old_mounts_are_unmounted() {
        Test::new(
            test_spec("/bin/wc")
                .arguments(["-l", "/proc/self/mounts"])
                .mounts([JobMount::Proc {
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"2 /proc/self/mounts\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn working_directory_root() {
        Test::new(bash_spec("pwd"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"/\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn working_directory_not_root() {
        Test::new(bash_spec("pwd").working_directory("/usr/bin"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"/usr/bin\n")))
            .run()
            .await;
    }

    async fn assert_execution_error(spec: maelstrom_base::JobSpec) {
        assert_matches!(
            run(spec, "0".parse().unwrap()).await,
            Err(JobError::Execution(_))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execution_error() {
        assert_execution_error(test_spec("a_program_that_does_not_exist")).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bad_working_directory_is_an_execution_error() {
        assert_execution_error(test_spec("/bin/cat").working_directory("/dev/null")).await;
    }
}
