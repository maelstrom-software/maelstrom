//! Easily start and stop processes.

use crate::config::InlineLimit;
use anyhow::{anyhow, Error, Result};
use bumpalo::{
    collections::{String as BumpString, Vec as BumpVec},
    Bump,
};
use c_str_macro::c_str;
use futures::ready;
use maelstrom_base::{
    EnumSet, GroupId, JobDevice, JobError, JobMount, JobMountFsType, JobOutputResult, JobResult,
    Timeout, UserId, Utf8PathBuf,
};
use maelstrom_linux::{
    self as linux, CloneArgs, CloneFlags, CloseRangeFirst, CloseRangeFlags, CloseRangeLast, Errno,
    Fd, FileMode, MountFlags, NetlinkSocketAddr, OpenFlags, OwnedFd, Pid, Signal, SocketDomain,
    SocketProtocol, SocketType, UmountFlags, WaitStatus,
};
use maelstrom_worker_child::Syscall;
use netlink_packet_core::{NetlinkMessage, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST};
use netlink_packet_route::{rtnl::constants::RTM_SETLINK, LinkMessage, RtnlMessage, IFF_UP};
use std::{
    ffi::{CStr, CString},
    fmt::Write as _,
    fs::File,
    io::Read as _,
    iter, mem,
    os::{fd, unix::ffi::OsStrExt as _},
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{self, unix::AsyncFd, AsyncRead, AsyncReadExt as _, Interest, ReadBuf},
    task,
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
    pub path: PathBuf,
    pub devices: EnumSet<JobDevice>,
    pub mounts: Vec<JobMount>,
    pub enable_loopback: bool,
    pub enable_writable_file_system: bool,
    pub working_directory: Utf8PathBuf,
    pub user: UserId,
    pub group: GroupId,
    pub timeout: Option<Timeout>,
}

impl JobSpec {
    pub fn from_spec_and_path(spec: maelstrom_base::JobSpec, path: PathBuf) -> Self {
        let maelstrom_base::JobSpec {
            program,
            arguments,
            environment,
            layers: _,
            devices,
            mounts,
            enable_loopback,
            enable_writable_file_system,
            working_directory,
            user,
            group,
            timeout,
        } = spec;
        JobSpec {
            program,
            arguments,
            environment,
            path,
            devices,
            mounts,
            enable_loopback,
            enable_writable_file_system,
            working_directory,
            user,
            group,
            timeout,
        }
    }
}

pub struct Executor {
    user: UserId,
    group: GroupId,
    mount_dir: CString,
    tmpfs_dir: CString,
    upper_dir: CString,
    work_dir: CString,
    comma_upperdir_comma_workdir: String,
    netlink_socket_addr: NetlinkSocketAddr,
    netlink_message: Box<[u8]>,
}

impl Executor {
    pub fn new(mount_dir: PathBuf, tmpfs_dir: PathBuf) -> Result<Self> {
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

        let user = UserId::from(linux::getuid().as_u32());
        let group = GroupId::from(linux::getgid().as_u32());
        let mount_dir = CString::new(mount_dir.as_os_str().as_bytes())?;
        let upper_dir = tmpfs_dir.join("upper");
        let work_dir = tmpfs_dir.join("work");
        let tmpfs_dir = CString::new(tmpfs_dir.as_os_str().as_bytes())?;
        let comma_upperdir_comma_workdir = format!(
            ",upperdir={},workdir={}",
            upper_dir
                .as_os_str()
                .to_str()
                .ok_or_else(|| anyhow!("could not convert upper_dir path to string"))?,
            work_dir
                .as_os_str()
                .to_str()
                .ok_or_else(|| anyhow!("could not convert work_dir path to string"))?
        );
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
            comma_upperdir_comma_workdir,
            netlink_socket_addr,
            netlink_message: buffer,
        })
    }
}

impl Executor {
    /// Start a process (i.e. job).
    ///
    /// Two callbacks are provided: one for stdout and one for stderr. These will be called on a
    /// separate task (they should not block) when the job has closed its stdout/stderr. This will
    /// likely happen when the job completes.
    ///
    /// No callback is called when the process actually terminates. For that, the caller should use
    /// waitid(2) or something similar to wait on the pid returned from this function. In
    /// production, that role will be filled by [`crate::reaper::main`].
    ///
    /// This function is designed to be callable in an async context, even though it temporarily
    /// blocks the calling thread while the child is starting up.
    ///
    /// If this function returns [`JobResult::Ok`], then the child process obviously will be
    /// started and the caller will need to waitid(2) on the child eventually. However, if this
    /// function returns an error result, it's still possible that a child was spawned (and has now
    /// terminated). It is assumed that the caller will be reaping all children, not just those
    /// positively identified by this function. If that assumption proves invalid, the return
    /// values of this function should be adjusted to return optional pids in error cases.
    pub fn start(
        &self,
        spec: &JobSpec,
        inline_limit: InlineLimit,
        process_done: impl FnOnce(Pid, Result<WaitStatus>) + Send + 'static,
        stdout_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
        stderr_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
    ) -> JobResult<Pid, Error> {
        self.start_inner(spec, inline_limit, process_done, stdout_done, stderr_done)
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

async fn process_waiter(child_pidfd: OwnedFd) -> Result<WaitStatus> {
    let async_fd = AsyncFd::with_interest(child_pidfd, Interest::READABLE)?;
    let _ = async_fd.readable().await?;
    Ok(linux::waitid(async_fd.into_inner().as_fd())?)
}

async fn process_waiter_task_main(
    child_pid: Pid,
    child_pidfd: OwnedFd,
    done: impl FnOnce(Pid, Result<WaitStatus>) + Send + 'static,
) {
    let status = process_waiter(child_pidfd).await.unwrap();
    done(child_pid, Ok(status));
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
async fn output_reader(
    inline_limit: InlineLimit,
    stream: impl AsyncRead + std::marker::Unpin,
) -> Result<JobOutputResult> {
    let mut buf = Vec::<u8>::new();
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
    inline_limit: InlineLimit,
    stream: impl AsyncRead + std::marker::Unpin,
    done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
) {
    done(output_reader(inline_limit, stream).await);
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

impl Executor {
    fn start_inner(
        &self,
        spec: &JobSpec,
        inline_limit: InlineLimit,
        process_done: impl FnOnce(Pid, Result<WaitStatus>) + Send + 'static,
        stdout_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
        stderr_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
    ) -> JobResult<Pid, Error> {
        // We're going to need three pipes: one for stdout, one for stderr, and one to convey back any
        // error that occurs in the child before it execs. It's easiest to create the pipes in the
        // parent before cloning and then closing the unnecessary ends in the parent and child.
        let (stdout_read_fd, stdout_write_fd) = linux::pipe()
            .map_err(Error::from)
            .map_err(JobError::System)?;
        let (stderr_read_fd, stderr_write_fd) = linux::pipe()
            .map_err(Error::from)
            .map_err(JobError::System)?;
        let (exec_result_read_fd, exec_result_write_fd) = linux::pipe()
            .map_err(Error::from)
            .map_err(JobError::System)?;

        // Now we set up the script. This will be run in the child where we have to follow some
        // very stringent rules to avoid deadlocking. This comes about because we're going to clone
        // in a multi-threaded program. The child program will only have one thread: this one. The
        // other threads will just not exist in the child process. If any of those threads held a
        // lock at the time of the clone, those locks will be locked forever in the child. Any
        // attempt to acquire those locks in the child would deadlock.
        //
        // The most burdensome result of this is that we can't allocate memory using the global
        // allocator in the child.
        //
        // Instead, we create a script of things we're going to execute in the child, but we do
        // that before the clone. After the clone, the child will just run through the script
        // directly. If it encounters an error, it will write the script index back to the parent
        // via the exec_result pipe. The parent will then map that to a closure for generating the
        // error.

        let bump = Bump::new();
        let mut builder = ScriptBuilder::new(&bump);

        if spec.enable_loopback {
            // In order to have a loopback network interface, we need to create a netlink socket and
            // configure things with the kernel. This creates the socket.
            builder.push(
                Syscall::SocketAndSaveFd(
                    SocketDomain::NETLINK,
                    SocketType::RAW,
                    SocketProtocol::NETLINK_ROUTE,
                ),
                &|err| JobError::System(anyhow!("opening rtnetlink socket: {err}")),
            );
            // This binds the socket.
            builder.push(
                Syscall::BindNetlinkUsingSavedFd(&self.netlink_socket_addr),
                &|err| JobError::System(anyhow!("binding rtnetlink socket: {err}")),
            );
            // This sends the message to the kernel.
            builder.push(
                Syscall::WriteUsingSavedFd(self.netlink_message.as_ref()),
                &|err| JobError::System(anyhow!("writing rtnetlink message: {err}")),
            );
            // This receives the reply from the kernel.
            // TODO: actually parse the reply to validate that we set up the loopback interface.
            let rtnetlink_response = bump.alloc_slice_fill_default(1024);
            builder.push(Syscall::ReadUsingSavedFd(rtnetlink_response), &|err| {
                JobError::System(anyhow!("receiving rtnetlink message: {err}"))
            });
            // We don't need to close the socket because that will happen automatically for us when we
            // exec.
        }

        // We now need to set up the new user namespace. This first set of syscalls sets up the
        // uid mapping.
        let mut uid_map_contents = BumpString::with_capacity_in(24, &bump);
        writeln!(uid_map_contents, "{} {} 1", spec.user, self.user)
            .map_err(Error::new)
            .map_err(JobError::System)?;
        builder.push(
            Syscall::OpenAndSaveFd(
                c_str!("/proc/self/uid_map"),
                OpenFlags::WRONLY | OpenFlags::TRUNC,
                FileMode::default(),
            ),
            &|err| JobError::System(anyhow!("opening /proc/self/uid_map for writing: {err}")),
        );
        builder.push(
            Syscall::WriteUsingSavedFd(uid_map_contents.into_bump_str().as_bytes()),
            &|err| JobError::System(anyhow!("writing to /proc/self/uid_map: {err}")),
        );
        // We don't need to close the file because that will happen automatically for us when we
        // exec.

        // This set of syscalls disables setgroups, which is required for setting up the gid
        // mapping.
        builder.push(
            Syscall::OpenAndSaveFd(
                c_str!("/proc/self/setgroups"),
                OpenFlags::WRONLY | OpenFlags::TRUNC,
                FileMode::default(),
            ),
            &|err| JobError::System(anyhow!("opening /proc/self/setgroups for writing: {err}")),
        );
        builder.push(Syscall::WriteUsingSavedFd(b"deny\n"), &|err| {
            JobError::System(anyhow!("writing to /proc/self/setgroups: {err}"))
        });
        // We don't need to close the file because that will happen automatically for us when we
        // exec.

        // Finally, we set up the gid mapping.
        let mut gid_map_contents = BumpString::with_capacity_in(24, &bump);
        writeln!(gid_map_contents, "{} {} 1", spec.group, self.group)
            .map_err(Error::new)
            .map_err(JobError::System)?;
        builder.push(
            Syscall::OpenAndSaveFd(
                c_str!("/proc/self/gid_map"),
                OpenFlags::WRONLY | OpenFlags::TRUNC,
                FileMode::default(),
            ),
            &|err| JobError::System(anyhow!("opening /proc/self/gid_map for writing: {err}")),
        );
        builder.push(
            Syscall::WriteUsingSavedFd(gid_map_contents.into_bump_str().as_bytes()),
            &|err| JobError::System(anyhow!("writing to /proc/self/setgroups: {err}")),
        );
        // We don't need to close the file because that will happen automatically for us when we
        // exec.

        // Make the child process the leader of a new session and process group. If we didn't do
        // this, then the process would be a member of a process group and session headed by a
        // process outside of the pid namespace, which would be confusing.
        builder.push(Syscall::SetSid, &|err| {
            JobError::System(anyhow!("setsid: {err}"))
        });

        // Dup2 the pipe file descriptors to be stdout and stderr. This will close the old stdout
        // and stderr, and the close_range will close the open pipes.
        builder.push(Syscall::Dup2(stdout_write_fd.as_fd(), Fd::STDOUT), &|err| {
            JobError::System(anyhow!("dup2-ing to stdout: {err}"))
        });
        builder.push(Syscall::Dup2(stderr_write_fd.as_fd(), Fd::STDERR), &|err| {
            JobError::System(anyhow!("dup2-ing to stderr: {err}"))
        });

        // Set close-on-exec for all file descriptors excecpt stdin, stdout, and stederr.
        builder.push(
            Syscall::CloseRange(
                CloseRangeFirst::AfterStderr,
                CloseRangeLast::Max,
                CloseRangeFlags::CLOEXEC,
            ),
            &|err| {
                JobError::System(anyhow!(
                    "setting CLOEXEC on range of open file descriptors: {err}"
                ))
            },
        );

        let new_root_path = self.mount_dir.as_c_str();
        if !spec.enable_writable_file_system {
            let layer0_path = bump_c_str_from_bytes(&bump, spec.path.as_os_str().as_bytes())
                .map_err(Error::from)
                .map_err(JobError::System)?;

            // Bind mount the directory onto our mount dir. This ensures it's a mount point so we can
            // pivot_root to it later.
            builder.push(
                Syscall::Mount(
                    Some(layer0_path),
                    new_root_path,
                    None,
                    MountFlags::BIND,
                    None,
                ),
                &|err| JobError::System(anyhow!("bind mounting target root directory: {err}")),
            );
        } else {
            // Use overlayfs.
            let mut options = BumpString::with_capacity_in(1000, &bump);
            options.push_str("lowerdir=");
            options.push_str(
                spec.path
                    .as_os_str()
                    .to_str()
                    .ok_or_else(|| anyhow!("could not convert path to string"))
                    .map_err(JobError::System)?,
            );
            // We need to create an upperdir and workdir. Create a temporary file system to contain
            // both of them.
            builder.push(
                Syscall::Mount(
                    None,
                    self.tmpfs_dir.as_c_str(),
                    Some(c_str!("tmpfs")),
                    MountFlags::default(),
                    None,
                ),
                &|err| {
                    JobError::System(anyhow!(
                        "mounting tmpfs file system for overlayfs's upperdir and workdir: {err}"
                    ))
                },
            );
            builder.push(
                Syscall::Mkdir(self.upper_dir.as_c_str(), FileMode::RWXU),
                &|err| JobError::System(anyhow!("making uppderdir for overlayfs: {err}")),
            );
            builder.push(
                Syscall::Mkdir(self.work_dir.as_c_str(), FileMode::RWXU),
                &|err| JobError::System(anyhow!("making workdir for overlayfs: {err}")),
            );
            options.push_str(self.comma_upperdir_comma_workdir.as_str());
            options.push('\0');
            builder.push(
                Syscall::Mount(
                    None,
                    new_root_path,
                    Some(c_str!("overlay")),
                    MountFlags::default(),
                    Some(options.into_bytes().into_bump_slice()),
                ),
                &|err| JobError::System(anyhow!("mounting overlayfs: {err}")),
            );
        }

        // Chdir to what will be the new root.
        builder.push(Syscall::Chdir(new_root_path), &|err| {
            JobError::System(anyhow!("chdir to target root directory: {err}"))
        });

        // Create all of the supported devices by bind mounting them from the host's /dev
        // directory. We don't assume we're running as root, and as such, we can't create device
        // files. However, we can bind mount them.
        for device in spec.devices.iter() {
            let (source, target, device_name) = match device {
                JobDevice::Full => (c_str!("/dev/full"), c_str!("./dev/full"), "/dev/full"),
                JobDevice::Fuse => (c_str!("/dev/fuse"), c_str!("./dev/fuse"), "/dev/fuse"),
                JobDevice::Null => (c_str!("/dev/null"), c_str!("./dev/null"), "/dev/null"),
                JobDevice::Random => (c_str!("/dev/random"), c_str!("./dev/random"), "/dev/random"),
                JobDevice::Tty => (c_str!("/dev/tty"), c_str!("./dev/tty"), "/dev/tty"),
                JobDevice::Urandom => (
                    c_str!("/dev/urandom"),
                    c_str!("./dev/urandom"),
                    "/dev/urandom",
                ),
                JobDevice::Zero => (c_str!("/dev/zero"), c_str!("./dev/zero"), "/dev/zero"),
            };
            builder.push(
                Syscall::Mount(Some(source), target, None, MountFlags::BIND, None),
                // We have to be careful doing bump.alloc here with the move. The drop method is
                // not going to be run on the closure, which means drop won't be run on any
                // captured-and-moved variables. Since we're using a static string for
                // `device_name`, we're okay.
                bump.alloc(move |err| {
                    JobError::Execution(anyhow!("bind mount of device {device_name}: {err}",))
                }),
            );
        }

        // Pivot root to be the new root. See man 2 pivot_root.
        builder.push(Syscall::PivotRoot(c_str!("."), c_str!(".")), &|err| {
            JobError::System(anyhow!("pivot_root: {err}"))
        });

        // Set up the mounts after we've called pivot_root so the absolute paths specified stay
        // within the container.
        //
        // N.B. It seems like it's a security feature of Linux that sysfs and proc can't be mounted
        // unless they are already mounted. So we have to do this before we unmount the old root.
        // If we do the unmount first, then we'll get permission errors mounting those fs types.
        let child_mount_points = spec
            .mounts
            .iter()
            .map(|m| bump_c_str(&bump, m.mount_point.as_str()));
        for (mount, mount_point) in iter::zip(spec.mounts.iter(), child_mount_points) {
            let mount_point_cstr = mount_point.map_err(Error::from).map_err(JobError::System)?;

            let (fs_type, flags, type_name) = match mount.fs_type {
                JobMountFsType::Proc => (
                    c_str!("proc"),
                    MountFlags::NOSUID | MountFlags::NOEXEC | MountFlags::NODEV,
                    "proc",
                ),
                JobMountFsType::Tmp => (c_str!("tmpfs"), MountFlags::default(), "tmpfs"),
                JobMountFsType::Sys => (c_str!("sysfs"), MountFlags::default(), "sysfs"),
            };
            let mount_point = mount.mount_point.as_str();
            builder.push(
                Syscall::Mount(None, mount_point_cstr, Some(fs_type), flags, None),
                // We have to be careful doing bump.alloc here with the move. The drop method is
                // not going to be run on the closure, which means drop won't be run on any
                // captured-and-moved variables. Since we're using a static string for `type_name`,
                // and since mount_point is just a reference, we're okay.
                bump.alloc(move |err| {
                    JobError::Execution(anyhow!(
                        "mount of file system of type {type_name} to {mount_point}: {err}",
                    ))
                }),
            );
        }

        // Unmount the old root. See man 2 pivot_root.
        builder.push(Syscall::Umount2(c_str!("."), UmountFlags::DETACH), &|err| {
            JobError::System(anyhow!("umount of old root: {err}"))
        });

        // Change to the working directory, if it's not "/".
        if spec.working_directory != Path::new("/") {
            let working_directory =
                bump_c_str_from_bytes(&bump, spec.working_directory.as_os_str().as_bytes())
                    .map_err(Error::from)
                    .map_err(JobError::System)?;
            builder.push(Syscall::Chdir(working_directory), &|err| {
                JobError::Execution(anyhow!("chdir: {err}"))
            });
        }

        // Finally, do the exec.
        let program = bump_c_str(&bump, spec.program.as_str()).map_err(JobError::System)?;
        let mut arguments =
            BumpVec::with_capacity_in(spec.arguments.len().checked_add(2).unwrap(), &bump);
        arguments.push(Some(&program.to_bytes_with_nul()[0]));
        for argument in &spec.arguments {
            let argument_cstr = bump_c_str(&bump, argument.as_str()).map_err(JobError::System)?;
            arguments.push(Some(&argument_cstr.to_bytes_with_nul()[0]));
        }
        arguments.push(None);
        let mut environment =
            BumpVec::with_capacity_in(spec.environment.len().checked_add(1).unwrap(), &bump);
        for var in &spec.environment {
            let var_cstr = bump_c_str(&bump, var.as_str()).map_err(JobError::System)?;
            environment.push(Some(&var_cstr.to_bytes_with_nul()[0]));
        }
        environment.push(None);
        builder.push(
            Syscall::Execve(
                program,
                arguments.into_bump_slice(),
                environment.into_bump_slice(),
            ),
            &|err| JobError::Execution(anyhow!("execvc: {err}")),
        );

        // Do the clone.
        let mut clone_args = CloneArgs::default()
            .flags(
                CloneFlags::NEWCGROUP
                    | CloneFlags::NEWIPC
                    | CloneFlags::NEWNET
                    | CloneFlags::NEWNS
                    | CloneFlags::NEWPID
                    | CloneFlags::NEWUSER,
            )
            .exit_signal(Signal::CHLD);
        let (child_pid, child_pidfd) = match linux::clone3_with_child_pidfd(&mut clone_args) {
            Ok(Some((child_pid, child_pidfd))) => (child_pid, child_pidfd),
            Ok(None) => {
                // This is the child process.
                maelstrom_worker_child::start_and_exec_in_child(
                    exec_result_write_fd.as_fd(),
                    builder.syscalls.as_mut_slice(),
                );
            }
            Err(err) => {
                return Err(JobError::System(err.into()));
            }
        };

        // Note that any early error return between here and the call to waitid will result in the
        // child not being waited for. It's unclear what to do in such a case. We could just wait,
        // but that would block the current thread. It's probably best to accept not waiting and
        // the accumulation of zombie children. We really don't expect any of these things to fail
        // anyway.

        // Spawn waiter task to wait on child to terminate.
        task::spawn(process_waiter_task_main(
            child_pid,
            child_pidfd,
            process_done,
        ));

        // At this point, it's still okay to return early in the parent. The child will continue to
        // execute, but that's okay. If it writes to one of the pipes, it will receive a SIGPIPE.
        // Otherwise, it will continue until it's done, and then we'll reap the zombie. Since we won't
        // have any jid->pid association, we'll just ignore the result.

        // Drop the write sides of the pipes in the parent. It's important that we drop
        // exec_result_write_fd before reading from that pipe next.
        drop(stdout_write_fd);
        drop(stderr_write_fd);
        drop(exec_result_write_fd);

        // Read (in a blocking manner) from the exec result pipe. The child will write to the pipe if
        // it has an error exec-ing. The child will mark the write side of the pipe exec-on-close, so
        // we'll read an immediate EOF if the exec is successful.
        let mut exec_result_buf = vec![];
        File::from(fd::OwnedFd::from(exec_result_read_fd))
            .read_to_end(&mut exec_result_buf)
            .map_err(Error::from)
            .map_err(JobError::System)?;
        if !exec_result_buf.is_empty() {
            if exec_result_buf.len() != 8 {
                return Err(JobError::System(anyhow!(
                    "couldn't parse exec result pipe's contents: {exec_result_buf:?}"
                )));
            }
            let result = u64::from_ne_bytes(exec_result_buf.try_into().unwrap());
            let index = (result >> 32) as usize;
            let errno = result & 0xffffffff;
            return Err(builder.error_transformers[index](
                Errno::from_u64(errno).desc().unwrap_or("Unknown error"),
            ));
        }

        // Make the read side of the stdout and stderr pipes non-blocking so that we can use them with
        // Tokio.
        linux::fcntl_setfl(stdout_read_fd.as_fd(), OpenFlags::NONBLOCK)
            .map_err(Error::from)
            .map_err(JobError::System)?;
        linux::fcntl_setfl(stderr_read_fd.as_fd(), OpenFlags::NONBLOCK)
            .map_err(Error::from)
            .map_err(JobError::System)?;

        // Spawn reader tasks to consume stdout and stderr.
        task::spawn(output_reader_task_main(
            inline_limit,
            AsyncFile(
                AsyncFd::new(File::from(fd::OwnedFd::from(stdout_read_fd)))
                    .map_err(Error::from)
                    .map_err(JobError::System)?,
            ),
            stdout_done,
        ));
        task::spawn(output_reader_task_main(
            inline_limit,
            AsyncFile(
                AsyncFd::new(File::from(fd::OwnedFd::from(stderr_read_fd)))
                    .map_err(Error::from)
                    .map_err(JobError::System)?,
            ),
            stderr_done,
        ));

        Ok(child_pid)
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
    use maelstrom_base::{nonempty, ArtifactType, JobStatus};
    use maelstrom_test::{boxed_u8, digest, utf8_path_buf};
    use maelstrom_util::async_fs;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::{oneshot, Mutex};

    const ARBITRARY_TIME: maelstrom_base::manifest::UnixTimestamp =
        maelstrom_base::manifest::UnixTimestamp(1705000271);

    fn test_logger() -> slog::Logger {
        use slog::Drain as _;

        let decorator = slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, slog::o!())
    }

    struct TarMount {
        temp_dir: PathBuf,
        mount_path: PathBuf,
        handle: maelstrom_fuse::FuseHandle,
    }

    impl TarMount {
        async fn new() -> Self {
            let fs = async_fs::Fs::new();
            let temp_dir = TempDir::new().unwrap().into_path();
            let data_path = temp_dir.join("data");
            let mount_path = temp_dir.join("mount");
            let cache_path = temp_dir.join("cache");
            for p in [&data_path, &mount_path, &cache_path] {
                fs.create_dir(p).await.unwrap();
            }
            let tar_bytes = include_bytes!("executor-test-deps.tar");
            let tar_path = cache_path.join(format!("{}", digest!(42)));
            fs.write(&tar_path, &tar_bytes).await.unwrap();
            let log = test_logger();
            let mut builder = maelstrom_layer_fs::BottomLayerBuilder::new(
                log.clone(),
                &fs,
                &data_path,
                &cache_path,
                ARBITRARY_TIME,
            )
            .await
            .unwrap();
            builder
                .add_from_tar(digest!(42), fs.open_file(&tar_path).await.unwrap())
                .await
                .unwrap();
            let layer_fs = builder.finish().await.unwrap();
            let cache = Arc::new(Mutex::new(maelstrom_layer_fs::ReaderCache::new()));
            let handle = layer_fs.mount(log, cache, &mount_path).unwrap();
            Self {
                temp_dir,
                mount_path,
                handle,
            }
        }

        async fn umount_and_join(self) {
            self.handle.umount_and_join().await.unwrap();
            let fs = async_fs::Fs::new();
            fs.remove_dir_all(&self.temp_dir).await.unwrap();
        }
    }

    struct Test {
        spec: JobSpec,
        inline_limit: InlineLimit,
        expected_status: JobStatus,
        expected_stdout: JobOutputResult,
        expected_stderr: JobOutputResult,
        mount: TarMount,
    }

    impl Test {
        fn new(spec: JobSpec, mount: TarMount) -> Self {
            Test {
                spec,
                inline_limit: InlineLimit::from(ByteSize::b(1000)),
                expected_status: JobStatus::Exited(0),
                expected_stdout: JobOutputResult::None,
                expected_stderr: JobOutputResult::None,
                mount,
            }
        }

        async fn from_spec(spec: maelstrom_base::JobSpec) -> Self {
            let mount = TarMount::new().await;
            let spec = JobSpec::from_spec_and_path(spec, mount.mount_path.clone());
            Self::new(spec, mount)
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
            let (status_tx, status_rx) = oneshot::channel();
            let (stdout_tx, stdout_rx) = oneshot::channel();
            let (stderr_tx, stderr_rx) = oneshot::channel();
            let start_result = tokio::task::block_in_place(|| {
                Executor::new(
                    tempfile::tempdir().unwrap().into_path(),
                    tempfile::tempdir().unwrap().into_path(),
                )
                .unwrap()
                .start(
                    &self.spec,
                    self.inline_limit,
                    |_pid, status| {
                        status_tx
                            .send(crate::job_status_from_wait_status(status.unwrap()))
                            .unwrap()
                    },
                    |stdout| stdout_tx.send(stdout.unwrap()).unwrap(),
                    |stderr| stderr_tx.send(stderr.unwrap()).unwrap(),
                )
            });
            assert!(start_result.is_ok());
            assert_eq!(status_rx.await.unwrap(), self.expected_status);
            assert_eq!(stdout_rx.await.unwrap(), self.expected_stdout);
            assert_eq!(stderr_rx.await.unwrap(), self.expected_stderr);

            self.mount.umount_and_join().await;
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
        Test::from_spec(bash_spec("exit 0")).await.run().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn exited_1() {
        Test::from_spec(bash_spec("exit 1"))
            .await
            .expected_status(JobStatus::Exited(1))
            .run()
            .await;
    }

    // $$ returns the pid of outer-most bash. This doesn't do what we expect it to do when using
    // our executor. We should probably rewrite these tests to run python or something, and take
    // input from stdin.
    #[tokio::test(flavor = "multi_thread")]
    async fn signaled_11() {
        Test::from_spec(python_spec(concat!(
            "import os;",
            "import sys;",
            "print('a');",
            "sys.stdout.flush();",
            "print('b', file=sys.stderr);",
            "sys.stderr.flush();",
            "os.abort()",
        )))
        .await
        .expected_status(JobStatus::Signaled(11))
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"a\n")))
        .expected_stderr(JobOutputResult::Inline(boxed_u8!(b"b\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stdout_inline_limit_0() {
        Test::from_spec(bash_spec("echo a"))
            .await
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
        Test::from_spec(bash_spec("echo a"))
            .await
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
        Test::from_spec(bash_spec("echo a"))
            .await
            .inline_limit(ByteSize::b(2))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stdout_inline_limit_3() {
        Test::from_spec(bash_spec("echo a"))
            .await
            .inline_limit(ByteSize::b(3))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stderr_inline_limit_0() {
        Test::from_spec(bash_spec("echo a >&2"))
            .await
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
        Test::from_spec(bash_spec("echo a >&2"))
            .await
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
        Test::from_spec(bash_spec("echo a >&2"))
            .await
            .inline_limit(ByteSize::b(2))
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stderr_inline_limit_3() {
        Test::from_spec(bash_spec("echo a >&2"))
            .await
            .inline_limit(ByteSize::b(3))
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn environment() {
        Test::from_spec(bash_spec("echo -n $FOO - $BAR").environment(["FOO=3", "BAR=4"]))
            .await
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"3 - 4")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stdin_empty() {
        Test::from_spec(test_spec("/bin/cat")).await.run().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pid_ppid_pgid_and_sid() {
        // We should be pid 1, that is, init for our namespace).
        // We should have ppid 0, indicating that our parent isn't accessible in our namespace.
        // We should have pgid 1, indicating that we're the group leader.
        // We should have sid 1, indicating that we're the session leader.
        Test::from_spec(python_spec(concat!(
            "import os;",
            "print('pid:', os.getpid());",
            "print('ppid:', os.getppid());",
            "print('pgid:', os.getpgid(0));",
            "print('sid:', os.getsid(0));",
        )))
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(
            b"pid: 1\nppid: 0\npgid: 1\nsid: 1\n"
        )))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_loopback() {
        Test::from_spec(
            test_spec("/bin/cat")
                .arguments(["/sys/class/net/lo/carrier"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Sys,
                    mount_point: utf8_path_buf!("/sys"),
                }]),
        )
        .await
        .expected_status(JobStatus::Exited(1))
        .expected_stderr(JobOutputResult::Inline(boxed_u8!(
            b"cat: read error: Invalid argument\n"
        )))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn loopback() {
        Test::from_spec(
            test_spec("/bin/cat")
                .arguments(["/sys/class/net/lo/carrier"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Sys,
                    mount_point: utf8_path_buf!("/sys"),
                }])
                .enable_loopback(true),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn user_and_group_0() {
        Test::from_spec(python_spec(concat!(
            "import os;",
            "print('uid:', os.getuid());",
            "print('gid:', os.getgid());",
        )))
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"uid: 0\ngid: 0\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn user_and_group_nonzero() {
        Test::from_spec(
            python_spec(concat!(
                "import os;",
                "print('uid:', os.getuid());",
                "print('gid:', os.getgid());",
            ))
            .user(UserId::from(43))
            .group(GroupId::from(100)),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"uid: 43\ngid: 100\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn close_range() {
        Test::from_spec(
            test_spec("/bin/ls")
                .arguments(["/proc/self/fd"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0\n1\n2\n3\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_layer_is_read_only() {
        Test::from_spec(test_spec("/bin/touch").arguments(["/foo"]))
            .await
            .expected_status(JobStatus::Exited(1))
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(
                b"touch: /foo: Read-only file system\n"
            )))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_layer_with_writable_file_system_is_writable() {
        Test::from_spec(bash_spec("echo bar > /foo && cat /foo").enable_writable_file_system(true))
            .await
            .expected_status(JobStatus::Exited(0))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"bar\n")))
            .run()
            .await;

        // Run another job to ensure that the file doesn't persist.
        Test::from_spec(bash_spec("test -e /foo"))
            .await
            .expected_status(JobStatus::Exited(1))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multiple_layers_with_writable_file_system_is_writable() {
        let mount = TarMount::new().await;
        let spec = JobSpec::from_spec_and_path(
            bash_spec("echo bar > /foo && cat /foo").enable_writable_file_system(true),
            mount.mount_path.clone(),
        );
        Test::new(spec, mount)
            .expected_status(JobStatus::Exited(0))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"bar\n")))
            .run()
            .await;

        // Run another job to ensure that the file doesn't persist.
        let mount = TarMount::new().await;
        let spec = JobSpec::from_spec_and_path(
            bash_spec("test -e /foo").enable_writable_file_system(true),
            mount.mount_path.clone(),
        );
        Test::new(spec, mount)
            .expected_status(JobStatus::Exited(1))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_full() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/full | awk '{print $5, $6}'"))
            .await
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_full() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/full | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Full)),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 7\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_null() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/null | awk '{print $5, $6}'"))
            .await
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_null() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/null | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Null)),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 3\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_null_write() {
        Test::from_spec(
            bash_spec("echo foo > /dev/null && cat /dev/null")
                .devices(EnumSet::only(JobDevice::Null)),
        )
        .await
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_random() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/random | awk '{print $5, $6}'"))
            .await
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_random() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/random | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Random)),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 8\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_tty() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/tty | awk '{print $5, $6}'"))
            .await
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_tty() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/tty | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Tty)),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"5, 0\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_urandom() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/urandom | awk '{print $5, $6}'"))
            .await
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_urandom() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/urandom | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Urandom)),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 9\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_dev_zero() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/zero | awk '{print $5, $6}'"))
            .await
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dev_zero() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/zero | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Zero)),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 5\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_tmpfs() {
        Test::from_spec(
            test_spec("/bin/grep")
                .arguments(["^tmpfs /tmp", "/proc/self/mounts"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .await
        .expected_status(JobStatus::Exited(1))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tmpfs() {
        Test::from_spec(
            test_spec("/bin/awk")
                .arguments([r#"/^none \/tmp/ { print $1, $2, $3 }"#, "/proc/self/mounts"])
                .mounts([
                    JobMount {
                        fs_type: JobMountFsType::Proc,
                        mount_point: utf8_path_buf!("/proc"),
                    },
                    JobMount {
                        fs_type: JobMountFsType::Tmp,
                        mount_point: utf8_path_buf!("/tmp"),
                    },
                ]),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"none /tmp tmpfs\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_sysfs() {
        Test::from_spec(
            test_spec("/bin/grep")
                .arguments(["^sysfs /sys", "/proc/self/mounts"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .await
        .expected_status(JobStatus::Exited(1))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sysfs() {
        Test::from_spec(
            test_spec("/bin/awk")
                .arguments([r#"/^none \/sys/ { print $1, $2, $3 }"#, "/proc/self/mounts"])
                .mounts([
                    JobMount {
                        fs_type: JobMountFsType::Proc,
                        mount_point: utf8_path_buf!("/proc"),
                    },
                    JobMount {
                        fs_type: JobMountFsType::Sys,
                        mount_point: utf8_path_buf!("/sys"),
                    },
                ]),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"none /sys sysfs\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_procfs() {
        Test::from_spec(test_spec("/bin/ls").arguments(["/proc"]))
            .await
            .run()
            .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn procfs() {
        Test::from_spec(
            test_spec("/bin/grep")
                .arguments(["proc", "/proc/self/mounts"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(
            b"none /proc proc rw,nosuid,nodev,noexec,relatime 0 0\n"
        )))
        .run()
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn old_mounts_are_unmounted() {
        Test::from_spec(
            test_spec("/bin/wc")
                .arguments(["-l", "/proc/self/mounts"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .await
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"2 /proc/self/mounts\n")))
        .run()
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn working_directory_root() {
        Test::from_spec(bash_spec("pwd"))
            .await
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"/\n")))
            .run()
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn working_directory_not_root() {
        Test::from_spec(bash_spec("pwd").working_directory("/usr/bin"))
            .await
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"/usr/bin\n")))
            .run()
            .await;
    }

    async fn assert_execution_error(spec: maelstrom_base::JobSpec) {
        let mount = TarMount::new().await;
        let spec = JobSpec::from_spec_and_path(spec, mount.mount_path.clone());
        assert_matches!(
            Executor::new(
                tempfile::tempdir().unwrap().into_path(),
                tempfile::tempdir().unwrap().into_path()
            )
            .unwrap()
            .start(
                &spec,
                ByteSize::b(0).into(),
                |_, _| unreachable!(),
                |_| unreachable!(),
                |_| unreachable!()
            ),
            Err(JobError::Execution(_))
        );
        mount.umount_and_join().await
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
