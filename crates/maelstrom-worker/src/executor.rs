//! Easily start and stop processes.

use anyhow::{anyhow, Context as _, Error, Result};
use bumpalo::{
    collections::{CollectIn as _, String as BumpString, Vec as BumpVec},
    Bump,
};
use maelstrom_base::{
    tty::{self, DecodeInputChunk, DecodeInputRemainder},
    GroupId, JobCompleted, JobDevice, JobEffects, JobError, JobMount, JobNetwork, JobOutputResult,
    JobResult, JobRootOverlay, JobStatus, JobTty, UserId, Utf8PathBuf, WindowSize,
};
use maelstrom_linux::{
    self as linux, CloneArgs, CloneFlags, CloseRangeFirst, CloseRangeFlags, CloseRangeLast, Errno,
    Fd, FileMode, FsconfigCommand, FsmountFlags, FsopenFlags, Gid, MountAttrs, MountFlags,
    MoveMountFlags, OpenFlags, OpenTreeFlags, OwnedFd, Signal, SockaddrNetlink, SockaddrUnStorage,
    SocketDomain, SocketProtocol, SocketType, Uid, UmountFlags, WaitStatus,
};
use maelstrom_util::{
    config::common::InlineLimit,
    io::AsyncFile,
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
    mem,
    os::unix::{ffi::OsStrExt as _, fs::MetadataExt},
    path::Path,
    result,
};
use tokio::{
    io::{self, unix::AsyncFd, AsyncReadExt as _, AsyncWriteExt as _, Interest},
    net::UnixStream,
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
    pub mounts: Vec<JobMount>,
    pub network: JobNetwork,
    pub root_overlay: JobRootOverlay,
    pub working_directory: Utf8PathBuf,
    pub user: UserId,
    pub group: GroupId,
    pub allocate_tty: Option<JobTty>,
}

impl JobSpec {
    pub fn from_spec(spec: maelstrom_base::JobSpec) -> Self {
        let maelstrom_base::JobSpec {
            program,
            arguments,
            environment,
            layers: _,
            mounts,
            network,
            root_overlay,
            working_directory,
            user,
            group,
            estimated_duration: _,
            allocate_tty,
            ..
        } = spec;
        JobSpec {
            program,
            arguments,
            environment,
            mounts,
            network,
            root_overlay,
            working_directory,
            user,
            group,
            allocate_tty,
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
    netlink_socket_addr: SockaddrNetlink,
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
            linux::dup2(&stdin_read_fd, &Fd::STDIN)?;
            mem::forget(stdin_read_fd);
            mem::forget(stdin_write_fd);
        } else {
            // This is the normal case where neither fd is fd 0.
            linux::dup2(&stdin_read_fd, &Fd::STDIN)?;
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
        let netlink_socket_addr = SockaddrNetlink::default();
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
                linux::pidfd_send_signal(async_fd.get_ref(), Signal::KILL)?;
                kill_event_received = true;
            },
            res = async_fd.readable() => {
                let _ = res?;
                break;
            },
        }
    }
    Ok(match linux::waitid(&async_fd.into_inner())? {
        WaitStatus::Exited(code) => JobStatus::Exited(code.as_u8()),
        WaitStatus::Signaled(signo) => JobStatus::Signaled(signo.as_u8()),
    })
}

/// Read all of the contents of `stream` and return the appropriate [`JobOutputResult`].
async fn output_reader(fd: OwnedFd, inline_limit: InlineLimit) -> Result<JobOutputResult> {
    let mut buf = Vec::<u8>::new();
    // Make the read side of the pipe non-blocking so that we can use it with Tokio.
    linux::fcntl_setfl(&fd, OpenFlags::NONBLOCK).map_err(Error::from)?;
    let stream = AsyncFile::new(fd)?;
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

fn syserr<E>(err: E) -> JobError<Error>
where
    Error: From<E>,
{
    JobError::System(Error::from(err))
}

fn execerr<E>(err: E) -> JobError<Error>
where
    Error: From<E>,
{
    JobError::Execution(Error::from(err))
}

fn new_fd_slot(bump: &Bump) -> FdSlot<'_> {
    FdSlot::new(bump.alloc(UnsafeCell::new(Fd::from_raw(-1))))
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
            JobDevice::Random => (c"/dev/random", "/dev/random"),
            JobDevice::Shm => (c"/dev/shm", "/dev/shm"),
            JobDevice::Tty => (c"/dev/tty", "/dev/tty"),
            JobDevice::Urandom => (c"/dev/urandom", "/dev/urandom"),
            JobDevice::Zero => (c"/dev/zero", "/dev/zero"),
        };
        Self { cstr, str }
    }
}

struct ChildProcess<'bump> {
    child_pidfd: Option<OwnedFd>,
    _stack: &'bump mut [u8],
}

impl<'bump> ChildProcess<'bump> {
    fn new(
        bump: &'bump Bump,
        clone_flags: CloneFlags,
        func: extern "C" fn(*mut core::ffi::c_void) -> i32,
        arg: *mut core::ffi::c_void,
    ) -> Result<Self> {
        let mut clone_args = CloneArgs::default()
            .flags(clone_flags)
            .exit_signal(Signal::CHLD);
        const CHILD_STACK_SIZE: usize = 1024;
        let stack = bump.alloc_slice_fill_default(CHILD_STACK_SIZE);
        let stack_ptr: *mut u8 = stack.as_mut_ptr();
        let (_, child_pidfd) = unsafe {
            linux::clone_with_child_pidfd(
                func,
                stack_ptr.wrapping_add(CHILD_STACK_SIZE) as *mut _,
                arg,
                &mut clone_args,
            )
        }?;
        Ok(Self {
            child_pidfd: Some(child_pidfd),
            _stack: stack,
        })
    }

    fn into_child_pidfd(mut self) -> OwnedFd {
        self.child_pidfd.take().unwrap()
    }
}

impl<'bump> Drop for ChildProcess<'bump> {
    fn drop(&mut self) {
        if let Some(child_pidfd) = &self.child_pidfd {
            // The pidfd_send_signal really shouldn't ever give us an error. But even if it does
            // for some reason, we're need to wait for the child.
            let _ = linux::pidfd_send_signal(child_pidfd, Signal::KILL);

            // This should never fail, but if it does, it means that the process doesn't exist so
            // we're free to continue and drop the stack.
            let _ = linux::waitid(child_pidfd);
        }
    }
}

enum Stdio {
    Pipes {
        stdout_read: OwnedFd,
        stdout_write: OwnedFd,
        stderr_read: OwnedFd,
        stderr_write: OwnedFd,
    },
    Pty {
        master: OwnedFd,
        slave: OwnedFd,
        socket: OwnedFd,
    },
}

impl<'clock, ClockT: Clock> Executor<'clock, ClockT> {
    // Set up the network namespace. It's possible that we won't even create a new network
    // namespace, if JobNetwork::Local is specified.
    //
    // Return true if we should create a new network namespace, false otherwise.
    fn set_up_network<'bump>(
        &'bump self,
        spec: &JobSpec,
        bump: &'bump Bump,
        builder: &mut ScriptBuilder<'bump>,
    ) -> bool {
        match spec.network {
            JobNetwork::Disabled => true,
            JobNetwork::Local => false,
            JobNetwork::Loopback => {
                // In order to have a loopback network interface, we need to create a netlink
                // socket and configure things with the kernel.

                let fd = new_fd_slot(bump);

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
                    Syscall::Bind {
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

                true
            }
        }
    }

    fn set_up_session<'bump>(&'bump self, builder: &mut ScriptBuilder<'bump>) {
        // Make the child process the leader of a new session and process group. If we didn't do
        // this, then the process would be a member of a process group and session headed by a
        // process outside of the pid namespace, which would be confusing.
        //
        // This needs to be called before set_up_stdio because if we're allocating a PTY, the child
        // needs to be in its own session before it can make the PTY slave its controlling
        // terminal.
        builder.push(Syscall::SetSid, &|err| syserr(anyhow!("setsid: {err}")));
    }

    fn set_up_stdio<'bump>(&'bump self, stdio: &Stdio, builder: &mut ScriptBuilder<'bump>) {
        match stdio {
            Stdio::Pipes {
                stdout_write,
                stderr_write,
                ..
            } => {
                // Dup2 the pipe file descriptors to be stdout and stderr. This will close the old
                // stdout and stderr. We don't have to worry about closing the old fds because they
                // will be marked close-on-exec below.
                builder.push(
                    Syscall::Dup2 {
                        from: stdout_write.as_fd(),
                        to: Fd::STDOUT,
                    },
                    &|err| syserr(anyhow!("dup2-ing to stdout: {err}")),
                );
                builder.push(
                    Syscall::Dup2 {
                        from: stderr_write.as_fd(),
                        to: Fd::STDERR,
                    },
                    &|err| syserr(anyhow!("dup2-ing to stderr: {err}")),
                );
            }
            Stdio::Pty { slave, .. } => {
                // Dup2 the pipe file descriptor to be stdin, stdout, and stderr. This will close
                // the old stdin, stdout, and stderr. We don't have to worry about closing the old
                // fd because it will be marked close-on-exec below.
                builder.push(
                    Syscall::Dup2 {
                        from: slave.as_fd(),
                        to: Fd::STDIN,
                    },
                    &|err| syserr(anyhow!("dup2-ing to stdin: {err}")),
                );
                builder.push(
                    Syscall::Dup2 {
                        from: slave.as_fd(),
                        to: Fd::STDOUT,
                    },
                    &|err| syserr(anyhow!("dup2-ing to stdout: {err}")),
                );
                builder.push(
                    Syscall::Dup2 {
                        from: slave.as_fd(),
                        to: Fd::STDERR,
                    },
                    &|err| syserr(anyhow!("dup2-ing to stderr: {err}")),
                );

                // We now have to make the slave PTY be our controlling terminal.
                builder.push(
                    Syscall::IoctlTiocsctty {
                        fd: Fd::STDIN,
                        arg: 0,
                    },
                    &|err| syserr(anyhow!("setting TIOCSCTTY on stdin: {err}")),
                );
            }
        }
    }

    fn set_up_user_namespace<'bump>(
        &'bump self,
        spec: &JobSpec,
        bump: &'bump Bump,
        builder: &mut ScriptBuilder<'bump>,
    ) -> JobResult<(), Error> {
        let fd = new_fd_slot(bump);

        // This first set of syscalls sets up the uid mapping.
        let mut uid_map_contents = BumpString::with_capacity_in(24, bump);
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
        let mut gid_map_contents = BumpString::with_capacity_in(24, bump);
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

        Ok(())
    }

    fn set_up_fuse_root<'bump>(
        &'bump self,
        spec: &JobSpec,
        new_root_path: &'bump CStr,
        bump: &'bump Bump,
        builder: &mut ScriptBuilder<'bump>,
    ) {
        let fd = new_fd_slot(bump);

        // Create the fuse mount. We need to do this in the child's namespace, then pass the file
        // descriptor back to the parent.
        builder.push(
            Syscall::Open {
                path: c"/dev/fuse",
                flags: OpenFlags::RDWR | OpenFlags::NONBLOCK,
                mode: FileMode::default(),
                out: fd,
            },
            &|err| syserr(anyhow!("open /dev/fuse: {err}")),
        );

        // Mount the fuse file system.
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
            &|err| syserr(anyhow!("fuse mount: {err}")),
        );

        // Send the fuse mount file descriptor from the child to the parent.
        builder.push(
            Syscall::SendMsg {
                buf: &[0xFF; 8],
                fd_to_send: fd,
            },
            &|err| syserr(anyhow!("sendmsg: {err}")),
        );
    }

    /// Set up the root overlay file system. This needs to happen after the root file system has
    /// been mounted, but before the `pivot_root` has occurred, since we need to be able to access
    /// local paths. Also, we need to do this before we mount other file systems, so we don't hide
    /// other file systems.
    fn set_up_root_overlay<'bump>(
        &'bump self,
        spec: &JobSpec,
        new_root_path: &'bump CStr,
        bump: &'bump Bump,
        builder: &mut ScriptBuilder<'bump>,
    ) -> JobResult<(), Error> {
        let (upper, work) = match spec.root_overlay {
            JobRootOverlay::None => {
                // There is nothing to do. We're just going to have a read-only root without an
                // overlay on top of it.
                return Ok(());
            }

            JobRootOverlay::Tmp => {
                // The overlay is going to write to a tmpfs that will be discarded when the job finishes.
                // We need two directories on the same mount.

                let upper = self.upper_dir.as_c_str();
                let work = self.work_dir.as_c_str();

                // Mount a new tmpfs that's local to this mount namespace.
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

                // Create the two directories in the newly-created tmpfs.
                builder.push(
                    Syscall::Mkdir {
                        path: upper,
                        mode: FileMode::RWXU,
                    },
                    &|err| syserr(anyhow!("making uppderdir for overlayfs: {err}")),
                );
                builder.push(
                    Syscall::Mkdir {
                        path: work,
                        mode: FileMode::RWXU,
                    },
                    &|err| syserr(anyhow!("making workdir for overlayfs: {err}")),
                );

                (upper, work)
            }

            JobRootOverlay::Local {
                ref upper,
                ref work,
            } => {
                // We're going to use the upper and work directories provided to us. We assume the
                // directories have been created and are on the same file system.
                (
                    bump_c_str(bump, upper.as_str()).map_err(syserr)?,
                    bump_c_str(bump, work.as_str()).map_err(syserr)?,
                )
            }
        };

        let fd = new_fd_slot(bump);

        // Open an fs context for an overlay file system.
        builder.push(
            Syscall::Fsopen {
                fsname: c"overlay",
                flags: FsopenFlags::default(),
                out: fd,
            },
            &|err| syserr(anyhow!("fsopen of overlayfs: {err}")),
        );

        // Set all of the configuration parameters.
        builder.push(
            Syscall::Fsconfig {
                fd,
                command: FsconfigCommand::SET_STRING,
                key: Some(c"lowerdir"),
                value: Some(&new_root_path.to_bytes_with_nul()[0]),
                aux: None,
            },
            &|err| syserr(anyhow!("fsconfig of lowerdir for overlayfs: {err}")),
        );
        builder.push(
            Syscall::Fsconfig {
                fd,
                command: FsconfigCommand::SET_STRING,
                key: Some(c"upperdir"),
                value: Some(&upper.to_bytes_with_nul()[0]),
                aux: None,
            },
            &|err| syserr(anyhow!("fsconfig of upperdir for overlayfs: {err}")),
        );
        builder.push(
            Syscall::Fsconfig {
                fd,
                command: FsconfigCommand::SET_STRING,
                key: Some(c"workdir"),
                value: Some(&work.to_bytes_with_nul()[0]),
                aux: None,
            },
            &|err| syserr(anyhow!("fsconfig of workdir for overlayfs: {err}")),
        );

        // Effect the configuration. This preps the file descriptor for the fsmount next.
        builder.push(
            Syscall::Fsconfig {
                fd,
                command: FsconfigCommand::CMD_CREATE,
                key: None,
                value: None,
                aux: None,
            },
            &|err| syserr(anyhow!("fsconfig of CMD_CREATE for overlayfs: {err}")),
        );

        // Create a mount fd from the fs context. This will open a new file descriptor. We capture
        // the new file descriptor and throw away the old one. In theory, we could re-use the old
        // one. The mount is detached at this point.
        builder.push(
            Syscall::Fsmount {
                fd,
                flags: FsmountFlags::default(),
                mount_attrs: MountAttrs::default(),
                out: fd,
            },
            &|err| syserr(anyhow!("fsmount for overlayfs: {err}")),
        );

        // Attach the mount to the file tree.
        builder.push(
            Syscall::MoveMount {
                from_dirfd: fd,
                from_path: c"",
                to_dirfd: Fd::AT_FDCWD,
                to_path: new_root_path,
                flags: MoveMountFlags::F_EMPTY_PATH,
            },
            &|err| syserr(anyhow!("move_mount for overlayfs: {err}")),
        );

        Ok(())
    }

    fn open_mount_fds_for_mounts_pre_pivot_root<'bump>(
        &'bump self,
        spec: &'bump JobSpec,
        bump: &'bump Bump,
        builder: &mut ScriptBuilder<'bump>,
        mount_fds: &mut BumpVec<'bump, FdSlot<'bump>>,
    ) -> JobResult<(), Error> {
        for mount in &spec.mounts {
            fn normal_mount<'a>(
                bump: &'a Bump,
                builder: &mut ScriptBuilder<'a>,
                mount_fds: &mut BumpVec<'a, FdSlot<'a>>,
                cfstype: &'static CStr,
                fstype: &'static str,
            ) -> JobResult<(), Error> {
                let fd = new_fd_slot(bump);

                // Open an fs context for the file system.
                builder.push(
                    Syscall::Fsopen {
                        fsname: cfstype,
                        flags: FsopenFlags::default(),
                        out: fd,
                    },
                    bump.alloc(move |err| syserr(anyhow!("fsopen for mount of {fstype}: {err}"))),
                );

                // Effect the configuration. This preps the file descriptor for the fsmount next.
                builder.push(
                    Syscall::Fsconfig {
                        fd,
                        command: FsconfigCommand::CMD_CREATE,
                        key: None,
                        value: None,
                        aux: None,
                    },
                    bump.alloc(move |err| {
                        syserr(anyhow!("fsconfig(CMD_CREATE) for mount of {fstype}: {err}"))
                    }),
                );

                // Create a mount fd from the fs context. This will open a new file descriptor. We
                // capture the new file descriptor and throw away the old one. In theory, we could
                // re-use the old one. The mount is detached at this point.
                builder.push(
                    Syscall::Fsmount {
                        fd,
                        flags: FsmountFlags::default(),
                        mount_attrs: MountAttrs::default(),
                        out: fd,
                    },
                    bump.alloc(move |err| syserr(anyhow!("fsmount for mount of {fstype}: {err}"))),
                );

                mount_fds.push(fd);
                Ok(())
            }

            match mount {
                JobMount::Bind { local_path, .. } => {
                    let mount_fd = new_fd_slot(bump);
                    mount_fds.push(mount_fd);
                    builder.push(
                        Syscall::OpenTree {
                            dirfd: Fd::AT_FDCWD,
                            path: bump_c_str(bump, local_path.as_str()).map_err(syserr)?,
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
                            out: mount_fd,
                        },
                        bump.alloc(move |err| {
                            execerr(anyhow!(
                                "opening local path {local_path} for bind mount: {err}",
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
                        let mount_fd = new_fd_slot(bump);
                        mount_fds.push(mount_fd);
                        builder.push(
                            Syscall::OpenTree {
                                dirfd: Fd::AT_FDCWD,
                                path: cstr,
                                // We don't pass recursive because we assume we're just cloning a
                                // file.
                                flags: OpenTreeFlags::CLONE,
                                out: mount_fd,
                            },
                            bump.alloc(move |err| {
                                syserr(anyhow!(
                                    "opening local path for bind mount of device {str}: {err}",
                                ))
                            }),
                        );
                    }
                }
                JobMount::Devpts { .. } => {
                    // The devpts file system doesn't seem to accept new-style configuration
                    // parameters via the new mount API. As a result, we mount it using the
                    // old-style mount syscall later.
                }
                JobMount::Mqueue { .. } => {
                    normal_mount(bump, builder, mount_fds, c"mqueue", "mqueue")?;
                }
                JobMount::Proc { .. } => {
                    normal_mount(bump, builder, mount_fds, c"proc", "proc")?;
                }
                JobMount::Sys { .. } => {
                    normal_mount(bump, builder, mount_fds, c"sysfs", "sysfs")?;
                }
                JobMount::Tmp { .. } => {
                    normal_mount(bump, builder, mount_fds, c"tmpfs", "tmpfs")?;
                }
            }
        }
        Ok(())
    }

    fn complete_mounts_post_pivot_root<'bump>(
        &'bump self,
        spec: &'bump JobSpec,
        bump: &'bump Bump,
        builder: &mut ScriptBuilder<'bump>,
        mount_fds: &mut impl Iterator<Item = FdSlot<'bump>>,
    ) -> JobResult<(), Error> {
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
                fstype: &'static str,
                mount_fd: FdSlot<'a>,
                mount_point: &'a Utf8PathBuf,
            ) -> JobResult<(), Error> {
                builder.push(
                    Syscall::MoveMount {
                        from_dirfd: mount_fd,
                        from_path: c"",
                        to_dirfd: Fd::AT_FDCWD,
                        to_path: bump_c_str(bump, mount_point.as_str()).map_err(syserr)?,
                        flags: MoveMountFlags::F_EMPTY_PATH,
                    },
                    bump.alloc(move |err| {
                        syserr(anyhow!(
                            "move_mount for mount of {fstype} to {mount_point}: {err}"
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
                    let mount_local_path_fd = mount_fds.next().unwrap();
                    let mount_point_cstr =
                        bump_c_str(bump, mount_point.as_str()).map_err(syserr)?;
                    builder.push(
                        Syscall::MoveMount {
                            from_dirfd: mount_local_path_fd,
                            from_path: c"",
                            to_dirfd: Fd::AT_FDCWD,
                            to_path: mount_point_cstr,
                            flags: MoveMountFlags::F_EMPTY_PATH,
                        },
                        bump.alloc(move |err| {
                            execerr(anyhow!(
                                "move_mount for bind mount of {local_path} to {mount_point}: {err}",
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
                                syserr(anyhow!(
                                    "remounting bind mount of {mount_point} as read-only: {err}",
                                ))
                            }),
                        );
                    }
                }
                JobMount::Devices { devices } => {
                    for device in devices.iter() {
                        let Device { cstr, str } = Device::new(device);
                        let mount_local_path_fd = mount_fds.next().unwrap();
                        builder.push(
                            Syscall::MoveMount {
                                from_dirfd: mount_local_path_fd,
                                from_path: c"",
                                to_dirfd: Fd::AT_FDCWD,
                                to_path: cstr,
                                flags: MoveMountFlags::F_EMPTY_PATH,
                            },
                            bump.alloc(move |err| {
                                execerr(
                                    anyhow!("move_mount for bind mount of device {str}: {err}",),
                                )
                            }),
                        );
                    }
                }
                JobMount::Devpts { mount_point } => {
                    let mount_point_cstr =
                        bump_c_str(bump, mount_point.as_str()).map_err(syserr)?;
                    builder.push(
                        Syscall::Mount {
                            source: None,
                            target: mount_point_cstr,
                            fstype: Some(c"devpts"),
                            flags: Default::default(),
                            data: Some(c"ptmxmode=0666".to_bytes_with_nul()),
                        },
                        bump.alloc(move |err| {
                            syserr(anyhow!("mount of devpts to {mount_point}: {err}"))
                        }),
                    )
                }
                JobMount::Mqueue { mount_point } => normal_mount(
                    bump,
                    builder,
                    "mqueue",
                    mount_fds.next().unwrap(),
                    mount_point,
                )?,
                JobMount::Proc { mount_point } => normal_mount(
                    bump,
                    builder,
                    "proc",
                    mount_fds.next().unwrap(),
                    mount_point,
                )?,
                JobMount::Sys { mount_point } => normal_mount(
                    bump,
                    builder,
                    "sysfs",
                    mount_fds.next().unwrap(),
                    mount_point,
                )?,
                JobMount::Tmp { mount_point } => normal_mount(
                    bump,
                    builder,
                    "tmpfs",
                    mount_fds.next().unwrap(),
                    mount_point,
                )?,
            };
        }
        Ok(())
    }

    fn do_pivot_root<'bump>(
        &'bump self,
        new_root_path: &'bump CStr,
        builder: &mut ScriptBuilder<'bump>,
    ) {
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

        // Unmount the old root. See man 2 pivot_root.
        builder.push(
            Syscall::Umount2 {
                path: c".",
                flags: UmountFlags::DETACH,
            },
            &|err| syserr(anyhow!("umount of old root: {err}")),
        );
    }

    fn do_chdir<'bump>(
        &'bump self,
        spec: &'bump JobSpec,
        bump: &'bump Bump,
        builder: &mut ScriptBuilder<'bump>,
    ) -> JobResult<(), Error> {
        // Change to the working directory, if it's not "/".
        if spec.working_directory != Path::new("/") {
            let working_directory =
                bump_c_str_from_bytes(bump, spec.working_directory.as_os_str().as_bytes())
                    .map_err(syserr)?;
            builder.push(
                Syscall::Chdir {
                    path: working_directory,
                },
                &|err| execerr(anyhow!("chdir: {err}")),
            );
        }
        Ok(())
    }

    fn do_close_range<'bump>(&'bump self, builder: &mut ScriptBuilder<'bump>) {
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
    }

    fn do_exec<'bump>(
        &'bump self,
        spec: &'bump JobSpec,
        bump: &'bump Bump,
        builder: &mut ScriptBuilder<'bump>,
    ) -> JobResult<(), Error> {
        let program = bump_c_str(bump, spec.program.as_str()).map_err(syserr)?;
        let mut arguments =
            BumpVec::with_capacity_in(spec.arguments.len().checked_add(2).unwrap(), bump);
        arguments.push(Some(&program.to_bytes_with_nul()[0]));
        for argument in &spec.arguments {
            let argument_cstr = bump_c_str(bump, argument.as_str()).map_err(syserr)?;
            arguments.push(Some(&argument_cstr.to_bytes_with_nul()[0]));
        }
        arguments.push(None);
        let mut environment =
            BumpVec::with_capacity_in(spec.environment.len().checked_add(1).unwrap(), bump);
        for var in &spec.environment {
            let var_cstr = bump_c_str(bump, var.as_str()).map_err(syserr)?;
            environment.push(Some(&var_cstr.to_bytes_with_nul()[0]));
        }
        environment.push(None);
        builder.push(
            match (
                spec.environment.iter().find(|var| var.starts_with("PATH=")),
                spec.program.as_str(),
            ) {
                (Some(path), program_str) if !program_str.contains('/') => Syscall::ExecveList {
                    paths: path
                        .strip_prefix("PATH=")
                        .unwrap()
                        .split(|c| c == ':')
                        .filter(|dir| !dir.is_empty())
                        .map(|dir| {
                            let mut bump_string = BumpString::from_str_in(dir, bump);
                            bump_string.push('/');
                            bump_string.push_str(program_str);
                            bump_c_str(bump, bump_string.as_str())
                        })
                        .collect_in::<result::Result<BumpVec<_>, _>>(bump)
                        .map_err(syserr)?
                        .into_bump_slice(),
                    fallback: program,
                    argv: arguments.into_bump_slice(),
                    envp: environment.into_bump_slice(),
                },
                _ => Syscall::Execve {
                    path: program,
                    argv: arguments.into_bump_slice(),
                    envp: environment.into_bump_slice(),
                },
            },
            &|err| execerr(anyhow!("execvc: {err}")),
        );
        Ok(())
    }

    fn run_job_inner(
        &self,
        spec: &JobSpec,
        inline_limit: InlineLimit,
        kill_event_receiver: EventReceiver,
        fuse_spawn: impl FnOnce(OwnedFd),
        runtime: runtime::Handle,
    ) -> JobResult<JobCompleted, Error> {
        // We're going to need three channels between the parent and child: one for stdout, one for
        // stderr, and one to tranfer back the fuse file descriptor from the child and to convey
        // back any error that occurs in the child before it execs. The first two can be regular
        // pipes, but the last one needs to be a unix-domain socket.
        //
        // It's easiest to create the pipes in the parent before cloning. We then close the
        // unnecessary ends in the parent and child. Alternatively, we could creates the pipes in
        // the child and then send the read ends back over the socket, but that seems unnecessarily
        // complex.
        let stdio = match spec.allocate_tty {
            None => {
                let (stdout_read, stdout_write) = linux::pipe().map_err(syserr)?;
                let (stderr_read, stderr_write) = linux::pipe().map_err(syserr)?;
                Stdio::Pipes {
                    stdout_read,
                    stdout_write,
                    stderr_read,
                    stderr_write,
                }
            }
            Some(JobTty {
                socket_address,
                window_size,
            }) => {
                // Open and connect the socket.
                let socket = linux::socket(
                    SocketDomain::UNIX,
                    SocketType::STREAM | SocketType::NONBLOCK,
                    Default::default(),
                )
                .map_err(syserr)?;
                let sockaddr = SockaddrUnStorage::new(socket_address.as_slice()).map_err(syserr)?;
                linux::connect(&socket, &sockaddr).map_err(syserr)?;

                // Open the master.
                let master =
                    linux::posix_openpt(OpenFlags::RDWR | OpenFlags::NOCTTY | OpenFlags::NONBLOCK)
                        .context("posix_openpt")
                        .map_err(syserr)?;
                linux::ioctl_tiocswinsz(&master, window_size.rows, window_size.columns)
                    .map_err(syserr)?;
                linux::grantpt(&master).map_err(syserr)?;
                linux::unlockpt(&master).map_err(syserr)?;

                // Open the slave.
                let mut slave_name = [0u8; 100];
                linux::ptsname(&master, slave_name.as_mut_slice()).map_err(syserr)?;
                let slave_name =
                    CStr::from_bytes_until_nul(slave_name.as_slice()).map_err(syserr)?;
                let slave = linux::open(
                    slave_name,
                    OpenFlags::RDWR | OpenFlags::NOCTTY,
                    Default::default(),
                )
                .map_err(syserr)?;

                Stdio::Pty {
                    master,
                    slave,
                    socket,
                }
            }
        };

        let (read_sock, write_sock) = linux::UnixStream::pair().map_err(syserr)?;

        // At a high level, the approach we're going to take is to build a "script" here in the
        // parent, and then pass the script to the child for execution. The script will consist of
        // operations chosen from a specific set of safe operations. The reason for taking this
        // approach is that in the child we have to follow some very stringent rules to avoid
        // deadlocking. This comes about because we're going to clone in a multi-threaded program.
        // The child program will only have one thread: this one. The other threads will just not
        // exist in the child process. If any of those threads held a lock at the time of the
        // clone, those locks will be locked forever in the child. Any attempt to acquire those
        // locks in the child would deadlock.
        //
        // The most burdensome result of this is that we can't allocate memory using the global
        // allocator in the child. But generally, we can only do "async signal safe" operations:
        // https://man7.org/linux/man-pages/man7/signal-safety.7.html
        //
        // So, we create a script of things we're going to execute in the child, but we do that
        // before the clone. After the clone, the child will just run through the script directly.
        // If it encounters an error, it will write the script index back to the parent via the
        // exec_result socket. The parent will then map that to a closure for generating the error.
        //
        // The parent and the child will share memory until the exec happens. In fact, the child
        // will basically be another thread, except that it's not a "real" thread from libc's point
        // of view.
        //
        // A few things to keep in mind in the script-building code:
        //
        // We don't have to worry about closing file descriptors in the child, or even explicitly
        // marking them close-on-exec, because one of the last things we do is mark all file
        // descriptors -- except for stdin, stdout, and stderr -- as close-on-exec.
        //
        // We have to be careful about the move closures and bump.alloc. The drop method won't be
        // run on the closure, which means drop won't be run on any captured-and-moved variables.
        // As long as we only pass references in those variables, we're okay.

        let bump = Bump::new();
        let mut builder = ScriptBuilder::new(&bump);

        // Put the child in its own session (and process group). This will make it the session and
        // group leader, and detach it from the parent's controlling terminal.
        self.set_up_session(&mut builder);

        // Set up stdio to either be pipes or a PTY. This has to happen after setting up the
        // session if we're allocating a PTY.
        self.set_up_stdio(&stdio, &mut builder);

        // Set up the network namespace, returning true iff we should actually create a new network
        // namespace. If `newnet` is false, we should share the parent's network namespace.
        let newnet = self.set_up_network(spec, &bump, &mut builder);
        self.set_up_user_namespace(spec, &bump, &mut builder)?;

        // Set up the fuse mount and send back the open fuse fd.
        let new_root_path = self.mount_dir.as_c_str();
        self.set_up_fuse_root(spec, new_root_path, &bump, &mut builder);

        // We need to resolve any local paths before we pivot_root, and we want to do the
        // move_mount before we complete the move_mounts below. We could split this up into two
        // functions like we do before, but there's no need to do so, since we don't need to
        // evaluate a client-provided mount point path. So, we do the whole thing, move_mount
        // included, here before the pivot_root.
        self.set_up_root_overlay(spec, new_root_path, &bump, &mut builder)?;

        // Prepare all of the mounts before we pivot_root. This way we can evaluate local
        // paths for bind  mounts before we go into the container's root. Also, Linux won't let us
        // mount (or sysfs?) if we don't already have one open in our namespace. By doing this
        // here, we don't have to worry about the existing proc going away when we pivot_root.
        let mut mount_fds = BumpVec::new_in(&bump);
        self.open_mount_fds_for_mounts_pre_pivot_root(spec, &bump, &mut builder, &mut mount_fds)?;

        // Pivot root and unmount the old root. This places us in the new /.
        self.do_pivot_root(new_root_path, &mut builder);

        // At this point, we've pivoted root and are in /. Do all of the move_mounts to complete
        // the mounting of file systems.
        let mut mount_fds = mount_fds.into_iter();
        self.complete_mounts_post_pivot_root(spec, &bump, &mut builder, &mut mount_fds)?;

        // We don't want to chdir until we've completed mounting, since we want clients to be able
        // to specify relative paths, and have them be relative to /.
        self.do_chdir(spec, &bump, &mut builder)?;

        // This needs to happen last, right before the exec, so we don't leak any file descriptors.
        self.do_close_range(&mut builder);

        // This has to come last.
        self.do_exec(spec, &bump, &mut builder)?;

        // Start timing the job now.
        let start = self.clock.now();

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
        let mut args = maelstrom_worker_child::ChildArgs {
            write_sock: write_sock.as_fd(),
            syscalls: builder.syscalls.as_mut_slice(),
        };
        let child_process = ChildProcess::new(
            &bump,
            clone_flags,
            maelstrom_worker_child::start_and_exec_in_child_trampoline,
            &mut args as *mut _ as *mut _,
        )
        .map_err(syserr)?;

        // Read (in a blocking manner) from the exec result socket. The child will write to the
        // socket if it has an error exec-ing. The child will mark the write side of the socket
        // exec-on-close, so we'll read an immediate EOF if the exec is successful.
        //
        // It's important to drop our copy of the write side of the socket first. Otherwise, we'd
        // deadlock on ourselves!
        //
        // If we encounter an error here, we will run Drop on the ChildProcess. This will guarantee
        // that our child is dead before we return from this function and destroy bump.
        drop(write_sock);
        let mut fuse_spawn = Some(fuse_spawn);
        let mut exec_result_buf = [0; mem::size_of::<u64>()];
        loop {
            let (count, fd) = read_sock
                .recv_with_fd(&mut exec_result_buf)
                .map_err(syserr)?;
            if count == 0 {
                // If we get EOF, the child successfully exec-ed.
                break;
            }

            // We don't expect to get a truncated message in this case.
            if count != exec_result_buf.len() {
                return Err(syserr(anyhow!(
                    "couldn't parse exec result socket's contents: {:?}",
                    &exec_result_buf[..count]
                )));
            }

            // If we get a file descriptor, we pass it to the FUSE callback.
            if let Some(fd) = fd {
                let fuse_spawn = fuse_spawn
                    .take()
                    .ok_or(syserr(anyhow!("multiple FUSE fds")))?;
                fuse_spawn(fd);
                continue;
            }

            // Otherwise it should be an error we got back from the child.
            let result = u64::from_ne_bytes(exec_result_buf);
            let index = (result >> 32) as usize;
            let errno = result & 0xffffffff;
            return Err(builder.error_transformers[index](
                Errno::from_u64(errno).desc().unwrap_or("Unknown error"),
            ));
        }

        // At this point it's safe to destructure the ChildProcess, since we know it has exec-ed,
        // and therefore isn't sharing our virtual memory anymore (so we can safely return without
        // waiting for the process).
        //
        // However, we want to make sure that we always wait on the child somehow, even if there is
        // an error, so that we don't end up accumlating zombie children. That's why we don't put
        // the following task into the JoinSet: we want it to run eve if we ignore its results.
        let child_pidfd = child_process.into_child_pidfd();
        let (status_sender, status_receiver) = oneshot::channel();
        runtime.spawn(async move {
            // It's not clear what to do if we get an error waiting, which, in theory, should never
            // happen. What we do is return the error so that the client can get back a system
            // error. An alternative would be to panic and send a plain JobStatus back.
            let _ = status_sender.send(wait_for_child(child_pidfd, kill_event_receiver).await);
        });

        let (stdout_sender, stdout_receiver) = oneshot::channel();
        let (stderr_sender, stderr_receiver) = oneshot::channel();

        let mut joinset = JoinSet::new();
        match stdio {
            Stdio::Pipes {
                stdout_read,
                stdout_write,
                stderr_read,
                stderr_write,
            } => {
                // Spawn independent tasks to consume stdout and stderr. We want to do this in parallel so
                // that we don't cause a deadlock on one while we're reading the other one.
                //
                // We put these in a JoinSet so that they will be canceled if we return early.
                //
                // We have to drop our own copies of the write side of the pipe. If we didn't, these tasks
                // would never complete.
                drop(stdout_write);
                drop(stderr_write);
                joinset.spawn_on(
                    output_reader_task_main(stdout_read, inline_limit, stdout_sender),
                    &runtime,
                );
                joinset.spawn_on(
                    output_reader_task_main(stderr_read, inline_limit, stderr_sender),
                    &runtime,
                );
            }
            Stdio::Pty {
                master,
                slave,
                socket,
            } => {
                // There is no output sent back in the JobResult if there is a pty allocated.
                let _ = stdout_sender.send(Ok(JobOutputResult::None));
                let _ = stderr_sender.send(Ok(JobOutputResult::None));

                // We don't use the PTY slave in the parent.
                drop(slave);

                // Turn the fds into things that tokio can use. Both of these were previously
                // opened non-blocking.
                let master_fd = master.as_fd();
                let (mut master_read, mut master_write) =
                    AsyncFile::new(master).map_err(syserr)?.into_split();
                let (mut socket_read, mut socket_write) =
                    UnixStream::try_from(socket).map_err(syserr)?.into_split();

                // Spawn two tasks to proxy between the master and the socket.
                let master_to_socket_handle = joinset.spawn_on(
                    async move {
                        let _ = io::copy(&mut master_read, &mut socket_write).await;
                    },
                    &runtime,
                );
                joinset.spawn_on(
                    async move {
                        let mut buf = [0u8; 1024];
                        let mut remainder = DecodeInputRemainder::default();
                        'outer: loop {
                            let offset = remainder.move_to_slice(&mut buf[..]);
                            let Ok(n) = socket_read.read(&mut buf[offset..]).await else {
                                break;
                            };
                            if n == 0 {
                                break;
                            }
                            for chunk in tty::decode_input(&buf[..offset + n]) {
                                match chunk {
                                    DecodeInputChunk::Input(buf) => {
                                        if master_write.write_all(buf).await.is_err() {
                                            break 'outer;
                                        }
                                    }
                                    DecodeInputChunk::WindowSizeChange(WindowSize {
                                        rows,
                                        columns,
                                    }) => {
                                        let _ = linux::ioctl_tiocswinsz(&master_fd, rows, columns);
                                    }
                                    DecodeInputChunk::Remainder(new_remainder) => {
                                        remainder = new_remainder;
                                    }
                                }
                            }
                        }

                        // If the socket shuts down, we want to close the master. To do so, we
                        // need to cancel the other which is reading from the master. Once that
                        // task and this task exit, the read and write sides of the AsyncFile will
                        // be dropped, which will drop the AsyncFd, which will close the file
                        // descriptor for the master. This will cause the child process to get a
                        // SIGHUP.
                        master_to_socket_handle.abort();
                    },
                    &runtime,
                );
            }
        }

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

        // Wait for the job to terminate.
        let status = read_from_receiver(status_receiver)?;

        // Stop timing the job now.
        let duration = start.elapsed();

        Ok(JobCompleted {
            status,
            effects: JobEffects {
                stdout: read_from_receiver(stdout_receiver)?,
                stderr: read_from_receiver(stderr_receiver)?,
                duration,
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
    use bytes::BytesMut;
    use bytesize::ByteSize;
    use indoc::indoc;
    use maelstrom_base::{
        enum_set, nonempty, ArtifactType, EnumSet, JobStatus, Utf8Path, WindowSize,
    };
    use maelstrom_layer_fs::{BlobDir, BottomLayerBuilder, LayerFs, ReaderCache};
    use maelstrom_test::{boxed_u8, digest, utf8_path_buf};
    use maelstrom_util::{async_fs, log::test_logger, sync, time::TickingClock};
    use std::{
        ascii, collections::HashSet, env, fs, path::PathBuf, str, sync::Arc, time::Duration,
    };
    use tempfile::{NamedTempFile, TempDir};
    use tokio::{
        io::AsyncRead,
        net::{TcpListener, UnixListener, UnixStream},
        sync::{oneshot, Mutex},
        task::{self, JoinHandle},
        time,
    };

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

        fn spawn(&self, fd: OwnedFd) {
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
        expected_duration: Duration,
    }

    impl Test {
        fn new(spec: maelstrom_base::JobSpec) -> Self {
            Test {
                spec,
                inline_limit: InlineLimit::from(ByteSize::b(1000)),
                expected_status: JobStatus::Exited(0),
                expected_stdout: JobOutputResult::None,
                expected_stderr: JobOutputResult::None,
                expected_duration: Duration::from_secs(1),
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

    #[tokio::test]
    async fn exited_0() {
        Test::new(bash_spec("exit 0")).run().await;
    }

    #[tokio::test]
    async fn exited_1() {
        Test::new(bash_spec("exit 1"))
            .expected_status(JobStatus::Exited(1))
            .run()
            .await;
    }

    // $$ returns the pid of outer-most bash. This doesn't do what we expect it to do when using
    // our executor. We should probably rewrite these tests to run python or something, and take
    // input from stdin.
    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn stdout_inline_limit_2() {
        Test::new(bash_spec("echo a"))
            .inline_limit(ByteSize::b(2))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test]
    async fn stdout_inline_limit_3() {
        Test::new(bash_spec("echo a"))
            .inline_limit(ByteSize::b(3))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn stderr_inline_limit_2() {
        Test::new(bash_spec("echo a >&2"))
            .inline_limit(ByteSize::b(2))
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test]
    async fn stderr_inline_limit_3() {
        Test::new(bash_spec("echo a >&2"))
            .inline_limit(ByteSize::b(3))
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test]
    async fn environment() {
        Test::new(bash_spec("echo -n $FOO - $BAR").environment(["FOO=3", "BAR=4"]))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"3 - 4")))
            .run()
            .await;
    }

    #[tokio::test]
    async fn stdin_empty() {
        Test::new(test_spec("/bin/cat")).run().await;
    }

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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
                JobMount::Devices {
                    devices: enum_set!(JobDevice::Null),
                },
            ]);
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

    #[tokio::test]
    async fn one_layer_is_read_only() {
        Test::new(test_spec("/bin/touch").arguments(["/foo"]))
            .expected_status(JobStatus::Exited(1))
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(
                b"touch: /foo: Read-only file system\n"
            )))
            .run()
            .await;
    }

    #[tokio::test]
    async fn one_layer_with_tmp_root_overlay_is_writable() {
        Test::new(bash_spec("echo bar > /foo && cat /foo").root_overlay(JobRootOverlay::Tmp))
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

    #[tokio::test]
    async fn multiple_layers_with_tmp_root_overlay_is_writable() {
        let spec = bash_spec("echo bar > /foo && cat /foo").root_overlay(JobRootOverlay::Tmp);
        Test::new(spec)
            .expected_status(JobStatus::Exited(0))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"bar\n")))
            .run()
            .await;

        // Run another job to ensure that the file doesn't persist.
        let spec = bash_spec("test -e /foo").root_overlay(JobRootOverlay::Tmp);
        Test::new(spec)
            .expected_status(JobStatus::Exited(1))
            .run()
            .await;
    }

    #[tokio::test]
    async fn local_root_overlay_is_writable_and_output_is_captured() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();
        let upper_path = temp_path.join("upper");
        let work_path = temp_path.join("work");
        let fs = async_fs::Fs::new();
        fs.create_dir(&upper_path).await.unwrap();
        fs.create_dir(&work_path).await.unwrap();

        Test::new(
            bash_spec("echo bar > /foo && cat /foo").root_overlay(JobRootOverlay::Local {
                upper: upper_path.clone().try_into().unwrap(),
                work: work_path.clone().try_into().unwrap(),
            }),
        )
        .expected_status(JobStatus::Exited(0))
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"bar\n")))
        .run()
        .await;

        // Run another job to ensure that the file persisted.
        Test::new(
            bash_spec("test -e /foo").root_overlay(JobRootOverlay::Local {
                upper: upper_path.clone().try_into().unwrap(),
                work: work_path.clone().try_into().unwrap(),
            }),
        )
        .expected_status(JobStatus::Exited(0))
        .run()
        .await;

        // Read from the directory to make sure it's there.
        assert_eq!(
            fs.read_to_string(upper_path.join("foo")).await.unwrap(),
            "bar\n"
        );
    }

    #[tokio::test]
    async fn no_mount_dev_full() {
        Test::new(bash_spec("/bin/ls -l /dev/full | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn no_mount_dev_fuse() {
        Test::new(bash_spec("/bin/ls -l /dev/fuse | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn no_mount_dev_null() {
        Test::new(bash_spec("/bin/ls -l /dev/null | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn mount_dev_null_write() {
        Test::new(
            bash_spec("echo foo > /dev/null && cat /dev/null").mounts([JobMount::Devices {
                devices: EnumSet::only(JobDevice::Null),
            }]),
        )
        .run()
        .await;
    }

    #[tokio::test]
    async fn no_mount_dev_random() {
        Test::new(bash_spec("/bin/ls -l /dev/random | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn no_mount_dev_tty() {
        Test::new(bash_spec("/bin/ls -l /dev/tty | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn no_mount_dev_urandom() {
        Test::new(bash_spec("/bin/ls -l /dev/urandom | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn no_mount_dev_zero() {
        Test::new(bash_spec("/bin/ls -l /dev/zero | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn no_dev_full() {
        Test::new(bash_spec("/bin/ls -l /dev/full | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    async fn dev_full() {
        Test::new(
            bash_spec("/bin/ls -l /dev/full | awk '{print $5, $6}'").mounts([JobMount::Devices {
                devices: enum_set!(JobDevice::Full),
            }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 7\n")))
        .run()
        .await;
    }

    #[tokio::test]
    async fn no_dev_null() {
        Test::new(bash_spec("/bin/ls -l /dev/null | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    async fn dev_null() {
        Test::new(
            bash_spec("/bin/ls -l /dev/null | awk '{print $5, $6}'").mounts([JobMount::Devices {
                devices: enum_set!(JobDevice::Null),
            }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 3\n")))
        .run()
        .await;
    }

    #[tokio::test]
    async fn dev_null_write() {
        Test::new(
            bash_spec("echo foo > /dev/null && cat /dev/null").mounts([JobMount::Devices {
                devices: enum_set!(JobDevice::Null),
            }]),
        )
        .run()
        .await;
    }

    #[tokio::test]
    async fn no_dev_random() {
        Test::new(bash_spec("/bin/ls -l /dev/random | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    async fn dev_random() {
        Test::new(
            bash_spec("/bin/ls -l /dev/random | awk '{print $5, $6}'").mounts([
                JobMount::Devices {
                    devices: enum_set!(JobDevice::Random),
                },
            ]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 8\n")))
        .run()
        .await;
    }

    #[tokio::test]
    async fn no_dev_tty() {
        Test::new(bash_spec("/bin/ls -l /dev/tty | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    async fn dev_tty() {
        Test::new(
            bash_spec("/bin/ls -l /dev/tty | awk '{print $5, $6}'").mounts([JobMount::Devices {
                devices: enum_set!(JobDevice::Tty),
            }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"5, 0\n")))
        .run()
        .await;
    }

    #[tokio::test]
    async fn no_dev_urandom() {
        Test::new(bash_spec("/bin/ls -l /dev/urandom | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    async fn dev_urandom() {
        Test::new(
            bash_spec("/bin/ls -l /dev/urandom | awk '{print $5, $6}'").mounts([
                JobMount::Devices {
                    devices: enum_set!(JobDevice::Urandom),
                },
            ]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 9\n")))
        .run()
        .await;
    }

    #[tokio::test]
    async fn no_dev_zero() {
        Test::new(bash_spec("/bin/ls -l /dev/zero | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    async fn dev_zero() {
        Test::new(
            bash_spec("/bin/ls -l /dev/zero | awk '{print $5, $6}'").mounts([JobMount::Devices {
                devices: enum_set!(JobDevice::Zero),
            }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 5\n")))
        .run()
        .await;
    }

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn no_procfs() {
        Test::new(test_spec("/bin/ls").arguments(["/proc"]))
            .run()
            .await
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn no_devpts() {
        Test::new(
            test_spec("/bin/grep")
                .arguments(["^devpts /dev/pty", "/proc/self/mounts"])
                .mounts([JobMount::Proc {
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_status(JobStatus::Exited(1))
        .run()
        .await;
    }

    #[tokio::test]
    async fn devpts() {
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

    #[tokio::test]
    async fn devpts_ptmx_mode() {
        Test::new(
            bash_spec("/bin/ls -l /dev/pts/ptmx | awk '{ print $1, $5, $6 }'").mounts([
                JobMount::Devpts {
                    mount_point: utf8_path_buf!("/dev/pts"),
                },
            ]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"crw-rw-rw- 5, 2\n")))
        .run()
        .await;
    }

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn bind_mount_read_only() {
        let temp_file = NamedTempFile::new().unwrap();
        let fs = async_fs::Fs::new();
        fs.write(temp_file.path(), b"hello\n").await.unwrap();
        Test::new(
            bash_spec(&format!(
                "(echo goodbye > /mnt/{}) 2>/dev/null",
                temp_file.path().file_name().unwrap().to_str().unwrap()
            ))
            .mounts([
                JobMount::Bind {
                    mount_point: utf8_path_buf!("/mnt"),
                    local_path: <&Utf8Path>::try_from(temp_file.path().parent().unwrap())
                        .unwrap()
                        .to_owned(),
                    read_only: true,
                },
                JobMount::Devices {
                    devices: enum_set!(JobDevice::Null),
                },
            ]),
        )
        .expected_status(JobStatus::Exited(1))
        .run()
        .await;
        let contents = fs::read_to_string(temp_file).unwrap();
        assert_eq!(contents, "hello\n");
    }

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn working_directory_root() {
        Test::new(bash_spec("pwd"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"/\n")))
            .run()
            .await;
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn execution_error() {
        assert_execution_error(test_spec("a_program_that_does_not_exist")).await;
    }

    #[tokio::test]
    async fn bad_working_directory_is_an_execution_error() {
        assert_execution_error(test_spec("/bin/cat").working_directory("/dev/null")).await;
    }

    async fn expect(mut socket: impl AsyncRead + Unpin, expected: &[u8]) {
        fn escaped_string(bytes: &[u8]) -> String {
            bytes
                .into_iter()
                .copied()
                .flat_map(ascii::escape_default)
                .map(|c| char::from_u32(c.into()).unwrap())
                .collect()
        }
        let mut bytes = BytesMut::with_capacity(1000);
        loop {
            let actual = &*bytes;
            if actual.len() < expected.len() {
                let expected_prefix = &expected[..actual.len()];
                assert_eq!(
                    actual,
                    expected_prefix,
                    r#"got output that started with "{}" while expecting "{}""#,
                    &escaped_string(actual),
                    &escaped_string(expected),
                );
                time::timeout(Duration::from_secs(60), socket.read_buf(&mut bytes))
                    .await
                    .unwrap()
                    .unwrap();
            } else {
                assert_eq!(
                    actual,
                    expected,
                    r#"got output "{}" while expecting "{}""#,
                    &escaped_string(actual),
                    &escaped_string(expected),
                );
                break;
            }
        }
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn PATH_can_be_used() {
        Test::new(
            test_spec("echo")
                .arguments(["foo"])
                .environment(["PATH=/bin"]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"foo\n")))
        .run()
        .await;
    }

    fn unix_listener() -> (UnixListener, [u8; 6]) {
        let (sock, path) = linux::autobound_unix_listener(SocketType::NONBLOCK, 1).unwrap();
        (sock.try_into().unwrap(), path)
    }

    async fn start_tty_job(
        program: &str,
        window_size: WindowSize,
    ) -> (JoinHandle<JobCompleted>, UnixStream) {
        let (listener, path) = unix_listener();
        let program_string = program.to_string();
        let job_handle = task::spawn(async move {
            run(
                test_spec(&program_string).allocate_tty(Some(JobTty::new(&path, window_size))),
                InlineLimit::from_bytes(0),
            )
            .await
            .unwrap()
        });
        let socket = listener.accept().await.unwrap().0;
        (job_handle, socket)
    }

    fn assert_job_exit(job_completed: JobCompleted, code: u8) {
        let JobCompleted {
            status,
            effects: JobEffects { stdout, stderr, .. },
        } = job_completed;
        assert_eq!(stderr, JobOutputResult::None);
        assert_eq!(stdout, JobOutputResult::None);
        assert_eq!(status, JobStatus::Exited(code));
    }

    #[tokio::test]
    async fn tty_eof() {
        let (job_handle, mut socket) = start_tty_job("/bin/cat", WindowSize::new(24, 80)).await;

        socket.write_all(b"dog\n").await.unwrap();
        expect(&mut socket, b"dog\r\ndog\r\n").await;

        socket.write_all(b"cow\n").await.unwrap();
        expect(&mut socket, b"cow\r\ncow\r\n").await;

        // ^D will send EOF, which will cause cat to exit normally.
        socket.write_all(b"\x04").await.unwrap();
        assert_job_exit(job_handle.await.unwrap(), 0);
    }

    #[tokio::test]
    async fn tty_ctrl_c() {
        let (job_handle, mut socket) = start_tty_job("/exitsig", WindowSize::new(24, 80)).await;

        expect(&mut socket, b"okay\r\n").await;

        // ^C will send SIGTERM, which will be caught an turned into an exit of 2.
        socket.write_all(b"\x03").await.unwrap();
        assert_job_exit(job_handle.await.unwrap(), 2);
    }

    #[tokio::test]
    async fn tty_ctrl_z() {
        let (job_handle, mut socket) = start_tty_job("/exitsig", WindowSize::new(24, 80)).await;

        expect(&mut socket, b"okay\r\n").await;

        // ^Z will send SIGTSTP, which will be caught an turned into an exit of 20.
        socket.write_all(b"\x1a").await.unwrap();
        assert_job_exit(job_handle.await.unwrap(), 20);
    }

    #[tokio::test]
    async fn tty_ctrl_backslash() {
        let (job_handle, mut socket) = start_tty_job("/exitsig", WindowSize::new(24, 80)).await;

        expect(&mut socket, b"okay\r\n").await;

        // ^\ will send SIGQUIT, which will be caught an turned into an exit of 3.
        socket.write_all(b"\x1c").await.unwrap();
        assert_job_exit(job_handle.await.unwrap(), 3);
    }

    #[tokio::test]
    async fn tty_hup() {
        let (job_handle, mut socket) = start_tty_job("/exitsig", WindowSize::new(24, 80)).await;

        expect(&mut socket, b"okay\r\n").await;

        // Closing the socket will close the controlling terminal, which will send a SIGHUP, which
        // will be caught and turned into an exit of 1.
        drop(socket);
        assert_job_exit(job_handle.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn tty_winsize() {
        let (job_handle, mut socket) = start_tty_job("/winsize", WindowSize::new(30, 90)).await;

        expect(&mut socket, b"30 90\r\n").await;

        socket
            .write_all(tty::encode_window_size_change(WindowSize::new(40, 100)).as_slice())
            .await
            .unwrap();
        expect(&mut socket, b"40 100\r\n").await;

        socket
            .write_all(tty::encode_window_size_change(WindowSize::new(50, 120)).as_slice())
            .await
            .unwrap();
        expect(&mut socket, b"50 120\r\n").await;

        // exitsig catches the sighup and exits.
        drop(socket);
        assert_job_exit(job_handle.await.unwrap(), 0);
    }
}
