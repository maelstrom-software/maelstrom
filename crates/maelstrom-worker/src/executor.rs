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
    NonEmpty, Sha256Digest, UserId, Utf8PathBuf,
};
use maelstrom_linux::{self as linux, sockaddr_nl_t};
use maelstrom_worker_child::Syscall;
use netlink_packet_core::{NetlinkMessage, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST};
use netlink_packet_route::{rtnl::constants::RTM_SETLINK, LinkMessage, RtnlMessage, IFF_UP};
use nix::{
    errno::Errno,
    fcntl::{self, FcntlArg, OFlag},
    unistd::{self, Pid},
};
use std::{
    ffi::{CStr, CString},
    fmt::Write as _,
    fs::File,
    io::Read as _,
    iter, mem,
    os::{
        fd::{AsRawFd as _, FromRawFd as _, IntoRawFd as _, OwnedFd},
        unix::ffi::OsStrExt as _,
    },
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{self, unix::AsyncFd, AsyncRead, AsyncReadExt as _, ReadBuf},
    task,
};
use tuple::Map as _;

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
    pub layers: NonEmpty<PathBuf>,
    pub devices: EnumSet<JobDevice>,
    pub mounts: Vec<JobMount>,
    pub enable_loopback: bool,
    pub enable_writable_file_system: bool,
    pub working_directory: Utf8PathBuf,
    pub user: UserId,
    pub group: GroupId,
}

impl JobSpec {
    pub fn from_spec_and_layers(
        spec: maelstrom_base::JobSpec,
        layers: NonEmpty<PathBuf>,
    ) -> (Self, NonEmpty<Sha256Digest>) {
        let maelstrom_base::JobSpec {
            program,
            arguments,
            environment,
            layers: digest_layers,
            devices,
            mounts,
            enable_loopback,
            enable_writable_file_system,
            working_directory,
            user,
            group,
        } = spec;
        (
            JobSpec {
                program,
                arguments,
                environment,
                layers,
                devices,
                mounts,
                enable_loopback,
                enable_writable_file_system,
                working_directory,
                user,
                group,
            },
            digest_layers.map(|(d, _)| d),
        )
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
    netlink_socket_addr: sockaddr_nl_t,
    netlink_message: Box<[u8]>,
}

impl Executor {
    pub fn new(mount_dir: PathBuf, tmpfs_dir: PathBuf) -> Result<Self> {
        // Set up stdin to be a file that will always return EOF. We could do something similar
        // by opening /dev/null but then we would depend on /dev being mounted. The fewer
        // dependencies, the better.
        let (stdin_read_fd, stdin_write_fd) =
            unistd::pipe()?.map(|raw_fd| unsafe { OwnedFd::from_raw_fd(raw_fd) });
        if stdin_read_fd.as_raw_fd() == 0 {
            // On the off chance that stdin was already closed, we may have already opened our read
            // fd onto stdin.
            mem::forget(stdin_read_fd);
        } else if stdin_write_fd.as_raw_fd() == 0 {
            // This would be a really weird scenario. Somehow we got the read end of the pipe to
            // not be fd 0, but the write end is. We can just dup the read end onto fd 0 and be
            // done.
            unsafe { nc::dup3(stdin_read_fd.as_raw_fd(), 0, 0) }.map_err(Errno::from_i32)?;
            mem::forget(stdin_read_fd);
            mem::forget(stdin_write_fd);
        } else {
            // This is the normal case where neither fd is fd 0.
            unsafe { nc::dup3(stdin_read_fd.as_raw_fd(), 0, 0) }.map_err(Errno::from_i32)?;
        }

        let user = UserId::from(unistd::getuid().as_raw());
        let group = GroupId::from(unistd::getgid().as_raw());
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
        let netlink_socket_addr = sockaddr_nl_t {
            sin_family: nc::AF_NETLINK as nc::sa_family_t,
            nl_pad: 0,
            nl_pid: 0, // the kernel
            nl_groups: 0,
        };
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
        stdout_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
        stderr_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
    ) -> JobResult<Pid, Error> {
        self.start_inner(spec, inline_limit, stdout_done, stderr_done)
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
    let mut take = stream.take(inline_limit.into_inner());
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
        stdout_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
        stderr_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
    ) -> JobResult<Pid, Error> {
        // We're going to need three pipes: one for stdout, one for stderr, and one to convey back any
        // error that occurs in the child before it execs. It's easiest to create the pipes in the
        // parent before cloning and then closing the unnecessary ends in the parent and child.
        let (stdout_read_fd, stdout_write_fd) = unistd::pipe()
            .map_err(Error::from)
            .map_err(JobError::System)?
            .map(|raw_fd| unsafe { OwnedFd::from_raw_fd(raw_fd) });
        let (stderr_read_fd, stderr_write_fd) = unistd::pipe()
            .map_err(Error::from)
            .map_err(JobError::System)?
            .map(|raw_fd| unsafe { OwnedFd::from_raw_fd(raw_fd) });
        let (exec_result_read_fd, exec_result_write_fd) = unistd::pipe()
            .map_err(Error::from)
            .map_err(JobError::System)?
            .map(|raw_fd| unsafe { OwnedFd::from_raw_fd(raw_fd) });

        const MNT_DETACH: usize = 2;

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
                    linux::AF_NETLINK,
                    linux::SOCK_RAW | linux::SOCK_CLOEXEC,
                    linux::NETLINK_ROUTE,
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
            Syscall::OpenAndSaveFd(c_str!("/proc/self/uid_map"), nc::O_WRONLY | nc::O_TRUNC, 0),
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
                nc::O_WRONLY | nc::O_TRUNC,
                0,
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
            Syscall::OpenAndSaveFd(c_str!("/proc/self/gid_map"), nc::O_WRONLY | nc::O_TRUNC, 0),
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
        builder.push(
            Syscall::Dup2(stdout_write_fd.as_raw_fd() as u32, 1),
            &|err| JobError::System(anyhow!("dup2-ing to stdout: {err}")),
        );
        builder.push(
            Syscall::Dup2(stderr_write_fd.as_raw_fd() as u32, 2),
            &|err| JobError::System(anyhow!("dup2-ing to stderr: {err}")),
        );

        // Set close-on-exec for all file descriptors excecpt stdin, stdout, and stederr.
        builder.push(
            Syscall::CloseRange(3, !0, nc::CLOSE_RANGE_CLOEXEC),
            &|err| {
                JobError::System(anyhow!(
                    "setting CLOEXEC on range of open file descriptors: {err}"
                ))
            },
        );

        // We can't use overlayfs with only a single layer. So we have to diverge based on how many
        // layers there are. If enable_writable_file_system, we're going to need at least two
        // layers, since we're going to push a writable top layer.

        let new_root_path = self.mount_dir.as_c_str();
        if spec.layers.tail.is_empty() && !spec.enable_writable_file_system {
            let layer0_path = bump_c_str_from_bytes(&bump, spec.layers[0].as_os_str().as_bytes())
                .map_err(Error::from)
                .map_err(JobError::System)?;

            // Bind mount the directory onto our mount dir. This ensures it's a mount point so we can
            // pivot_root to it later.
            builder.push(
                Syscall::Mount(Some(layer0_path), new_root_path, None, nc::MS_BIND, None),
                &|err| JobError::System(anyhow!("bind mounting target root directory: {err}")),
            );

            // We want that mount to be read-only!
            builder.push(
                Syscall::Mount(
                    None,
                    new_root_path,
                    None,
                    nc::MS_REMOUNT | nc::MS_BIND | nc::MS_RDONLY,
                    None,
                ),
                &|err| {
                    JobError::System(anyhow!(
                        "remounting target root directory as read-only: {err}"
                    ))
                },
            );
        } else {
            // Use overlayfs.
            let mut options = BumpString::with_capacity_in(1000, &bump);
            options.push_str("lowerdir=");
            for (i, layer) in spec.layers.iter().rev().enumerate() {
                if i != 0 {
                    options.push(':');
                }
                options.push_str(
                    layer
                        .as_os_str()
                        .to_str()
                        .ok_or_else(|| anyhow!("could not convert path to string"))
                        .map_err(JobError::System)?,
                );
            }
            // We need to create an upperdir and workdir. Create a temporary file system to contain
            // both of them.
            if spec.enable_writable_file_system {
                builder.push(
                    Syscall::Mount(None, self.tmpfs_dir.as_c_str(), Some(c_str!("tmpfs")), 0, None),
                    &|err| {
                        JobError::System(anyhow!(
                            "mounting tmpfs file system for overlayfs's upperdir and workdir: {err}"))
                    });
                builder.push(Syscall::Mkdir(self.upper_dir.as_c_str(), 0o700), &|err| {
                    JobError::System(anyhow!("making uppderdir for overlayfs: {err}"))
                });
                builder.push(Syscall::Mkdir(self.work_dir.as_c_str(), 0o700), &|err| {
                    JobError::System(anyhow!("making workdir for overlayfs: {err}"))
                });
                options.push_str(self.comma_upperdir_comma_workdir.as_str());
            }
            options.push('\0');
            builder.push(
                Syscall::Mount(
                    None,
                    new_root_path,
                    Some(c_str!("overlay")),
                    0,
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
                Syscall::Mount(Some(source), target, None, nc::MS_BIND, None),
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
                    nc::MS_NOSUID | nc::MS_NOEXEC | nc::MS_NODEV,
                    "proc",
                ),
                JobMountFsType::Tmp => (c_str!("tmpfs"), 0, "tmpfs"),
                JobMountFsType::Sys => (c_str!("sysfs"), 0, "sysfs"),
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
        builder.push(Syscall::Umount2(c_str!("."), MNT_DETACH), &|err| {
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
        let mut clone_args = nc::clone_args_t {
            flags: nc::CLONE_NEWCGROUP as u64
                | nc::CLONE_NEWIPC as u64
                | nc::CLONE_NEWNET as u64
                | nc::CLONE_NEWNS as u64
                | nc::CLONE_NEWPID as u64
                | nc::CLONE_NEWUSER as u64,
            exit_signal: nc::SIGCHLD as u64,
            ..Default::default()
        };
        let child_pid =
            match unsafe { nc::clone3(&mut clone_args, mem::size_of::<nc::clone_args_t>()) } {
                Ok(val) => val,
                Err(err) => {
                    return Err(JobError::System(Errno::from_i32(err).into()));
                }
            };
        if child_pid == 0 {
            // This is the child process.
            maelstrom_worker_child::start_and_exec_in_child(
                exec_result_write_fd.into_raw_fd(),
                builder.syscalls.as_mut_slice(),
            );
        }

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
        File::from(exec_result_read_fd)
            .read_to_end(&mut exec_result_buf)
            .map_err(Error::from)
            .map_err(JobError::System)?;
        if !exec_result_buf.is_empty() {
            if exec_result_buf.len() != 8 {
                return Err(JobError::System(anyhow!(
                    "couldn't parse exec result pipe's contents: {exec_result_buf:?}"
                )));
            }
            let result = u64::from_le_bytes(exec_result_buf.try_into().unwrap());
            let index = (result >> 32) as usize;
            let errno = (result & 0xffffffff) as i32;
            return Err(builder.error_transformers[index](
                nix::Error::from_i32(errno).desc(),
            ));
        }

        // Make the read side of the stdout and stderr pipes non-blocking so that we can use them with
        // Tokio.
        fcntl::fcntl(
            stdout_read_fd.as_raw_fd(),
            FcntlArg::F_SETFL(OFlag::O_NONBLOCK),
        )
        .map_err(Error::from)
        .map_err(JobError::System)?;
        fcntl::fcntl(
            stderr_read_fd.as_raw_fd(),
            FcntlArg::F_SETFL(OFlag::O_NONBLOCK),
        )
        .map_err(Error::from)
        .map_err(JobError::System)?;

        // Spawn reader tasks to consume stdout and stderr.
        task::spawn(output_reader_task_main(
            inline_limit,
            AsyncFile(
                AsyncFd::new(File::from(stdout_read_fd))
                    .map_err(Error::from)
                    .map_err(JobError::System)?,
            ),
            stdout_done,
        ));
        task::spawn(output_reader_task_main(
            inline_limit,
            AsyncFile(
                AsyncFd::new(File::from(stderr_read_fd))
                    .map_err(Error::from)
                    .map_err(JobError::System)?,
            ),
            stderr_done,
        ));

        Ok(Pid::from_raw(child_pid))
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
    use crate::reaper::{self, ReaperDeps};
    use assert_matches::*;
    use maelstrom_base::{nonempty, ArtifactType, JobStatus};
    use maelstrom_test::{boxed_u8, digest, utf8_path_buf};
    use nix::sys::signal::{self, Signal};
    use serial_test::serial;
    use std::ops::ControlFlow;
    use tar::Archive;
    use tempfile::TempDir;
    use tokio::sync::oneshot;

    struct ReaperAdapter {
        pid: Pid,
        result: Option<JobStatus>,
    }

    impl ReaperAdapter {
        fn new(pid: Pid) -> Self {
            ReaperAdapter { pid, result: None }
        }
    }

    impl ReaperDeps for &mut ReaperAdapter {
        fn on_waitid_error(&mut self, err: Errno) -> ControlFlow<()> {
            panic!("waitid error: {err}");
        }
        fn on_dummy_child_termination(&mut self) -> ControlFlow<()> {
            panic!("dummy child panicked");
        }
        fn on_unexpected_wait_code(&mut self, _pid: Pid) -> ControlFlow<()> {
            panic!("unexpected wait code");
        }
        fn on_child_termination(&mut self, pid: Pid, status: JobStatus) -> ControlFlow<()> {
            if self.pid == pid {
                self.result = Some(status);
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    fn extract_layer_tar(bytes: &'static [u8]) -> PathBuf {
        let tempdir = TempDir::new().unwrap();
        Archive::new(bytes).unpack(&tempdir).unwrap();
        tempdir.into_path()
    }

    fn extract_dependencies() -> PathBuf {
        extract_layer_tar(include_bytes!("executor-test-deps.tar").as_slice())
    }

    struct Test {
        spec: JobSpec,
        inline_limit: InlineLimit,
        expected_status: JobStatus,
        expected_stdout: JobOutputResult,
        expected_stderr: JobOutputResult,
    }

    impl Test {
        fn new(spec: JobSpec) -> Self {
            Test {
                spec,
                inline_limit: InlineLimit::from(1000),
                expected_status: JobStatus::Exited(0),
                expected_stdout: JobOutputResult::None,
                expected_stderr: JobOutputResult::None,
            }
        }

        fn from_spec(spec: maelstrom_base::JobSpec) -> Self {
            let (spec, _) =
                JobSpec::from_spec_and_layers(spec, NonEmpty::new(extract_dependencies()));
            Self::new(spec)
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

        async fn run(&self) {
            let dummy_child_pid = reaper::clone_dummy_child().unwrap();
            let (stdout_tx, stdout_rx) = oneshot::channel();
            let (stderr_tx, stderr_rx) = oneshot::channel();
            let start_result = Executor::new(
                tempfile::tempdir().unwrap().into_path(),
                tempfile::tempdir().unwrap().into_path(),
            )
            .unwrap()
            .start(
                &self.spec,
                self.inline_limit,
                |stdout| stdout_tx.send(stdout.unwrap()).unwrap(),
                |stderr| stderr_tx.send(stderr.unwrap()).unwrap(),
            );
            assert_matches!(start_result, Ok(_));
            let Ok(pid) = start_result else {
                unreachable!();
            };
            let reaper = task::spawn_blocking(move || {
                let mut adapter = ReaperAdapter::new(pid);
                reaper::main(&mut adapter, dummy_child_pid);
                let result = adapter.result.unwrap();
                signal::kill(dummy_child_pid, Signal::SIGKILL).ok();
                let mut adapter = ReaperAdapter::new(dummy_child_pid);
                reaper::main(&mut adapter, Pid::from_raw(0));
                result
            });
            assert_eq!(reaper.await.unwrap(), self.expected_status);
            assert_eq!(stdout_rx.await.unwrap(), self.expected_stdout);
            assert_eq!(stderr_rx.await.unwrap(), self.expected_stderr);
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
    #[serial]
    async fn exited_0() {
        Test::from_spec(bash_spec("exit 0")).run().await;
    }

    #[tokio::test]
    #[serial]
    async fn exited_1() {
        Test::from_spec(bash_spec("exit 1"))
            .expected_status(JobStatus::Exited(1))
            .run()
            .await;
    }

    // $$ returns the pid of outer-most bash. This doesn't do what we expect it to do when using
    // our executor. We should probably rewrite these tests to run python or something, and take
    // input from stdin.
    #[tokio::test]
    #[serial]
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
        .expected_status(JobStatus::Signaled(11))
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"a\n")))
        .expected_stderr(JobOutputResult::Inline(boxed_u8!(b"b\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn stdout_inline_limit_0() {
        Test::from_spec(bash_spec("echo a"))
            .inline_limit(0)
            .expected_stdout(JobOutputResult::Truncated {
                first: boxed_u8!(b""),
                truncated: 2,
            })
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn stdout_inline_limit_1() {
        Test::from_spec(bash_spec("echo a"))
            .inline_limit(1)
            .expected_stdout(JobOutputResult::Truncated {
                first: boxed_u8!(b"a"),
                truncated: 1,
            })
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn stdout_inline_limit_2() {
        Test::from_spec(bash_spec("echo a"))
            .inline_limit(2)
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn stdout_inline_limit_3() {
        Test::from_spec(bash_spec("echo a"))
            .inline_limit(3)
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn stderr_inline_limit_0() {
        Test::from_spec(bash_spec("echo a >&2"))
            .inline_limit(0)
            .expected_stderr(JobOutputResult::Truncated {
                first: boxed_u8!(b""),
                truncated: 2,
            })
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn stderr_inline_limit_1() {
        Test::from_spec(bash_spec("echo a >&2"))
            .inline_limit(1)
            .expected_stderr(JobOutputResult::Truncated {
                first: boxed_u8!(b"a"),
                truncated: 1,
            })
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn stderr_inline_limit_2() {
        Test::from_spec(bash_spec("echo a >&2"))
            .inline_limit(2)
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn stderr_inline_limit_3() {
        Test::from_spec(bash_spec("echo a >&2"))
            .inline_limit(3)
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(b"a\n")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn environment() {
        Test::from_spec(bash_spec("echo -n $FOO - $BAR").environment(["FOO=3", "BAR=4"]))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"3 - 4")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn stdin_empty() {
        Test::from_spec(test_spec("/bin/cat")).run().await;
    }

    #[tokio::test]
    #[serial]
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
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(
            b"pid: 1\nppid: 0\npgid: 1\nsid: 1\n"
        )))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_loopback() {
        Test::from_spec(
            test_spec("/bin/cat")
                .arguments(["/sys/class/net/lo/carrier"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Sys,
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
    #[serial]
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
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn user_and_group_0() {
        Test::from_spec(python_spec(concat!(
            "import os;",
            "print('uid:', os.getuid());",
            "print('gid:', os.getgid());",
        )))
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"uid: 0\ngid: 0\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
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
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"uid: 43\ngid: 100\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn close_range() {
        Test::from_spec(
            test_spec("/bin/ls")
                .arguments(["/proc/self/fd"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0\n1\n2\n3\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn one_layer_is_read_only() {
        Test::from_spec(test_spec("/bin/touch").arguments(["/foo"]))
            .expected_status(JobStatus::Exited(1))
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(
                b"touch: /foo: Read-only file system\n"
            )))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn one_layer_with_writable_file_system_is_writable() {
        Test::from_spec(bash_spec("echo bar > /foo && cat /foo").enable_writable_file_system(true))
            .expected_status(JobStatus::Exited(0))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"bar\n")))
            .run()
            .await;

        // Run another job to ensure that the file doesn't persist.
        Test::from_spec(bash_spec("test -e /foo"))
            .expected_status(JobStatus::Exited(1))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn multiple_layers_in_correct_order() {
        let (spec, _) = JobSpec::from_spec_and_layers(
            maelstrom_base::JobSpec::new(
                "/bin/cat",
                nonempty![
                    (digest![0], ArtifactType::Tar),
                    (digest![1], ArtifactType::Tar),
                    (digest![2], ArtifactType::Tar)
                ],
            )
            .arguments(["/root/file", "/root/bottom-file", "/root/top-file"]),
            nonempty![
                extract_dependencies(),
                extract_layer_tar(include_bytes!("bottom-layer.tar")),
                extract_layer_tar(include_bytes!("top-layer.tar"))
            ],
        );
        Test::new(spec)
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(
                b"top\nbottom file\ntop file\n"
            )))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn multiple_layers_read_only() {
        let (spec, _) = JobSpec::from_spec_and_layers(
            test_spec("/bin/touch").arguments(["/foo"]),
            nonempty![
                extract_dependencies(),
                extract_layer_tar(include_bytes!("bottom-layer.tar")),
                extract_layer_tar(include_bytes!("top-layer.tar"))
            ],
        );
        Test::new(spec)
            .expected_status(JobStatus::Exited(1))
            .expected_stderr(JobOutputResult::Inline(boxed_u8!(
                b"touch: /foo: Read-only file system\n"
            )))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn multiple_layers_with_writable_file_system_is_writable() {
        let (spec, _) = JobSpec::from_spec_and_layers(
            bash_spec("echo bar > /foo && cat /foo").enable_writable_file_system(true),
            nonempty![
                extract_dependencies(),
                extract_layer_tar(include_bytes!("bottom-layer.tar")),
                extract_layer_tar(include_bytes!("top-layer.tar"))
            ],
        );
        Test::new(spec)
            .expected_status(JobStatus::Exited(0))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"bar\n")))
            .run()
            .await;

        // Run another job to ensure that the file doesn't persist.
        let (spec, _) = JobSpec::from_spec_and_layers(
            bash_spec("test -e /foo").enable_writable_file_system(true),
            nonempty![
                extract_dependencies(),
                extract_layer_tar(include_bytes!("bottom-layer.tar")),
                extract_layer_tar(include_bytes!("top-layer.tar"))
            ],
        );
        Test::new(spec)
            .expected_status(JobStatus::Exited(1))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_full() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/full | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_full() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/full | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Full)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 7\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_null() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/null | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_null() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/null | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Null)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 3\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_null_write() {
        Test::from_spec(
            bash_spec("echo foo > /dev/null && cat /dev/null")
                .devices(EnumSet::only(JobDevice::Null)),
        )
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_random() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/random | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_random() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/random | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Random)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 8\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_tty() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/tty | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_tty() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/tty | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Tty)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"5, 0\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_urandom() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/urandom | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_urandom() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/urandom | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Urandom)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 9\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_zero() {
        Test::from_spec(bash_spec("/bin/ls -l /dev/zero | awk '{print $5, $6}'"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_zero() {
        Test::from_spec(
            bash_spec("/bin/ls -l /dev/zero | awk '{print $5, $6}'")
                .devices(EnumSet::only(JobDevice::Zero)),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"1, 5\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_tmpfs() {
        Test::from_spec(
            test_spec("/bin/grep")
                .arguments(["^tmpfs /tmp", "/proc/self/mounts"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_status(JobStatus::Exited(1))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
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
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"none /tmp tmpfs\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_sysfs() {
        Test::from_spec(
            test_spec("/bin/grep")
                .arguments(["^sysfs /sys", "/proc/self/mounts"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_status(JobStatus::Exited(1))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
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
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"none /sys sysfs\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_procfs() {
        Test::from_spec(test_spec("/bin/ls").arguments(["/proc"]))
            .run()
            .await
    }

    #[tokio::test]
    #[serial]
    async fn procfs() {
        Test::from_spec(
            test_spec("/bin/grep")
                .arguments(["proc", "/proc/self/mounts"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(
            b"none /proc proc rw,nosuid,nodev,noexec,relatime 0 0\n"
        )))
        .run()
        .await
    }

    #[tokio::test]
    #[serial]
    async fn old_mounts_are_unmounted() {
        Test::from_spec(
            test_spec("/bin/wc")
                .arguments(["-l", "/proc/self/mounts"])
                .mounts([JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: utf8_path_buf!("/proc"),
                }]),
        )
        .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"2 /proc/self/mounts\n")))
        .run()
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn working_directory_root() {
        Test::from_spec(bash_spec("pwd"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"/\n")))
            .run()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn working_directory_not_root() {
        Test::from_spec(bash_spec("pwd").working_directory("/usr/bin"))
            .expected_stdout(JobOutputResult::Inline(boxed_u8!(b"/usr/bin\n")))
            .run()
            .await;
    }

    fn assert_execution_error(spec: maelstrom_base::JobSpec) {
        let (spec, _) = JobSpec::from_spec_and_layers(spec, NonEmpty::new(extract_dependencies()));
        assert_matches!(
            Executor::new(
                tempfile::tempdir().unwrap().into_path(),
                tempfile::tempdir().unwrap().into_path()
            )
            .unwrap()
            .start(&spec, 0.into(), |_| unreachable!(), |_| unreachable!()),
            Err(JobError::Execution(_))
        );
    }

    #[test]
    #[serial]
    fn execution_error() {
        assert_execution_error(test_spec("a_program_that_does_not_exist"));
    }

    #[test]
    #[serial]
    fn bad_working_directory_is_an_execution_error() {
        assert_execution_error(test_spec("/bin/cat").working_directory("/dev/null"));
    }
}
