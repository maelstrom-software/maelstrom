//! Easily start and stop processes.

use crate::config::InlineLimit;
use anyhow::{anyhow, Error, Result};
use bumpalo::{collections::Vec as BumpVec, Bump};
use c_str_macro::c_str;
use futures::ready;
use meticulous_base::{
    EnumSet, JobDevice, JobError, JobErrorResult, JobMount, JobMountFsType, JobOutputResult,
    NonEmpty,
};
use meticulous_worker_child::{sockaddr_nl_t, Syscall};
use netlink_packet_core::{NetlinkMessage, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST};
use netlink_packet_route::{rtnl::constants::RTM_SETLINK, LinkMessage, RtnlMessage, IFF_UP};
use nix::{
    errno::Errno,
    fcntl::{self, FcntlArg, OFlag},
    unistd::{self, Pid},
};
use std::{
    ffi::CString,
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
pub struct JobSpec<'a> {
    pub program: &'a str,
    pub arguments: &'a [String],
    pub environment: &'a [String],
    pub devices: &'a EnumSet<JobDevice>,
    pub layers: &'a NonEmpty<PathBuf>,
    pub mounts: &'a [JobMount],
    pub enable_loopback: &'a bool,
    pub working_directory: &'a Path,
}

pub struct Executor {
    setgroups_contents: String,
    uid_map_contents: String,
    gid_map_contents: String,
    mount_dir: CString,
    netlink_socket_addr: sockaddr_nl_t,
    netlink_message: Box<[u8]>,
}

impl Executor {
    pub fn new(mount_dir: PathBuf) -> Result<Self> {
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
            unsafe { nc::dup2(stdin_read_fd.as_raw_fd(), 0) }.map_err(Errno::from_i32)?;
            mem::forget(stdin_read_fd);
            mem::forget(stdin_write_fd);
        } else {
            // This is the normal case where neither fd is fd 0.
            unsafe { nc::dup2(stdin_read_fd.as_raw_fd(), 0) }.map_err(Errno::from_i32)?;
        }

        let setgroups_contents = "deny\n".to_string();
        let uid_map_contents = format!("0 {} 1\n", unistd::getuid().as_raw());
        let gid_map_contents = format!("0 {} 1\n", unistd::getgid().as_raw());
        let mount_dir = CString::new(mount_dir.as_os_str().as_bytes())?;
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
            setgroups_contents,
            uid_map_contents,
            gid_map_contents,
            mount_dir,
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
    /// If this function returns [`JobErrorResult::Ok`], then the child process obviously will be
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
    ) -> JobErrorResult<Pid, Error> {
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

impl Executor {
    fn start_inner(
        &self,
        spec: &JobSpec,
        inline_limit: InlineLimit,
        stdout_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
        stderr_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
    ) -> JobErrorResult<Pid, Error> {
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
        const NETLINK_ROUTE: i32 = 0;

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

        let mut rtnetlink_response = [0; 1024];
        let new_root_path;
        let layer0_path;
        let overlayfs_options;
        let child_mount_points;
        let program = CString::new(spec.program)
            .map_err(Error::from)
            .map_err(JobError::System)?;
        let arguments = spec
            .arguments
            .iter()
            .map(String::as_str)
            .map(CString::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Error::from)
            .map_err(JobError::System)?;
        let arguments = iter::once(Some(&program.to_bytes_with_nul()[0]))
            .chain(
                arguments
                    .iter()
                    .map(|cstr| Some(&cstr.to_bytes_with_nul()[0])),
            )
            .chain(iter::once(None))
            .collect::<Vec<_>>();
        let environment = spec
            .environment
            .iter()
            .map(String::as_str)
            .map(CString::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Error::from)
            .map_err(JobError::System)?;
        let environment = environment
            .iter()
            .map(|cstr| Some(&cstr.to_bytes_with_nul()[0]))
            .chain(iter::once(None))
            .collect::<Vec<_>>();

        let mut builder = ScriptBuilder::new(&bump);

        if *spec.enable_loopback {
            // In order to have a loopback network interface, we need to create a netlink socket and
            // configure things with the kernel. This creates the socket.
            builder.push(
                Syscall::SocketAndSave(
                    nc::AF_NETLINK,
                    nc::SOCK_RAW | nc::SOCK_CLOEXEC,
                    NETLINK_ROUTE,
                ),
                &|err| JobError::System(anyhow!("opening rtnetlink socket: {err}")),
            );
            // This binds the socket.
            builder.push(Syscall::BindSaved(&self.netlink_socket_addr), &|err| {
                JobError::System(anyhow!("binding rtnetlink socket: {err}"))
            });
            // This sends the message to the kernel.
            builder.push(
                Syscall::SendToSaved(self.netlink_message.as_ref()),
                &|err| JobError::System(anyhow!("sending rtnetlink message: {err}")),
            );
            // This receives the reply from the kernel.
            // TODO: actually parse the reply to validate that we set up the loopback interface.
            builder.push(
                Syscall::RecvFromSaved(rtnetlink_response.as_mut_slice()),
                &|err| JobError::System(anyhow!("receiving rtnetlink message: {err}")),
            );
            // We don't need to close the socket because that will happen automatically for us when we
            // exec.
        }

        // We now need to set up the new user namespace. This first set of syscalls sets up the
        // uid mapping.
        builder.push(
            Syscall::OpenAndSave(c_str!("/proc/self/uid_map"), nc::O_WRONLY | nc::O_TRUNC, 0),
            &|err| JobError::System(anyhow!("opening /proc/self/uid_map for writing: {err}")),
        );
        builder.push(
            Syscall::WriteSaved(self.uid_map_contents.as_bytes()),
            &|err| JobError::System(anyhow!("writing to /proc/self/uid_map: {err}")),
        );
        // We don't need to close the file because that will happen automatically for us when we
        // exec.

        // This set of syscalls disables setgroups, which is required for setting up the gid
        // mapping.
        builder.push(
            Syscall::OpenAndSave(
                c_str!("/proc/self/setgroups"),
                nc::O_WRONLY | nc::O_TRUNC,
                0,
            ),
            &|err| JobError::System(anyhow!("opening /proc/self/setgroups for writing: {err}")),
        );
        builder.push(
            Syscall::WriteSaved(self.setgroups_contents.as_bytes()),
            &|err| JobError::System(anyhow!("writing to /proc/self/setgroups: {err}")),
        );
        // We don't need to close the file because that will happen automatically for us when we
        // exec.

        // Finally, we set up the gid mapping.
        builder.push(
            Syscall::OpenAndSave(c_str!("/proc/self/gid_map"), nc::O_WRONLY | nc::O_TRUNC, 0),
            &|err| JobError::System(anyhow!("opening /proc/self/gid_map for writing: {err}")),
        );
        builder.push(
            Syscall::WriteSaved(self.gid_map_contents.as_bytes()),
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
        builder.push(Syscall::Dup2(stdout_write_fd.as_raw_fd(), 1), &|err| {
            JobError::System(anyhow!("dup2-ing to stdout: {err}"))
        });
        builder.push(Syscall::Dup2(stderr_write_fd.as_raw_fd(), 2), &|err| {
            JobError::System(anyhow!("dup2-ing to stderr: {err}"))
        });

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
        // layers there are.

        if spec.layers.tail.is_empty() {
            layer0_path = CString::new(spec.layers[0].as_os_str().as_bytes())
                .map_err(Error::from)
                .map_err(JobError::System)?;
            new_root_path = layer0_path.as_c_str();

            // Bind mount the directory onto our mount dir. This ensures it's a mount point so we can
            // pivot_root to it later.
            builder.push(
                Syscall::Mount(Some(new_root_path), new_root_path, None, nc::MS_BIND, None),
                &|err| {
                    JobError::System(anyhow!(
                        "bind mounting target root directory onto itself: {err}"
                    ))
                },
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
            new_root_path = self.mount_dir.as_c_str();
            let mut options = "lowerdir=".to_string();
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
            overlayfs_options = CString::new(options)
                .map_err(Error::from)
                .map_err(JobError::System)?;
            builder.push(
                Syscall::Mount(
                    None,
                    new_root_path,
                    Some(c_str!("overlay")),
                    0,
                    Some(overlayfs_options.as_c_str().to_bytes_with_nul()),
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
        child_mount_points = spec
            .mounts
            .iter()
            .map(|m| CString::new(m.mount_point.as_str()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(Error::from)
            .map_err(JobError::System)?;
        for (mount, mount_point_cstr) in iter::zip(spec.mounts.iter(), child_mount_points.iter()) {
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

        // Finally, do the exec.
        builder.push(
            Syscall::Execve(
                program.as_c_str(),
                arguments.as_slice(),
                environment.as_slice(),
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
            meticulous_worker_child::start_and_exec_in_child(
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
    use meticulous_base::{nonempty, JobStatus};
    use meticulous_test::boxed_u8;
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

    async fn start_and_expect_bash(
        command: &'static str,
        expected_status: JobStatus,
        expected_stdout: JobOutputResult,
        expected_stderr: JobOutputResult,
    ) {
        start_and_expect_bash_full(
            command,
            vec![],
            &EnumSet::EMPTY,
            vec![],
            1000,
            expected_status,
            expected_stdout,
            expected_stderr,
        )
        .await;
    }

    async fn start_and_expect_bash_full(
        command: &'static str,
        environment: Vec<&'static str>,
        devices: &EnumSet<JobDevice>,
        mounts: Vec<JobMount>,
        inline_limit: u64,
        expected_status: JobStatus,
        expected_stdout: JobOutputResult,
        expected_stderr: JobOutputResult,
    ) {
        let program = "/usr/bin/bash";
        let arguments = vec!["-c".to_string(), command.to_string()];
        let environment = environment
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let layers = &NonEmpty::new(extract_dependencies());
        let spec = JobSpec {
            program,
            arguments: arguments.as_slice(),
            environment: environment.as_slice(),
            layers,
            devices,
            mounts: mounts.as_slice(),
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            inline_limit,
            expected_status,
            expected_stdout,
            expected_stderr,
        )
        .await;
    }

    async fn start_and_expect_python(
        script: &'static str,
        expected_status: JobStatus,
        expected_stdout: JobOutputResult,
        expected_stderr: JobOutputResult,
    ) {
        let program = "/usr/bin/python3";
        let arguments = vec!["-c".to_string(), script.to_string()];
        let layers = &NonEmpty::new(extract_dependencies());
        let spec = JobSpec {
            program,
            arguments: arguments.as_slice(),
            environment: &[],
            layers,
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            1000,
            expected_status,
            expected_stdout,
            expected_stderr,
        )
        .await;
    }

    async fn start_and_expect<'a>(
        spec: JobSpec<'a>,
        inline_limit: u64,
        expected_status: JobStatus,
        expected_stdout: JobOutputResult,
        expected_stderr: JobOutputResult,
    ) {
        let dummy_child_pid = reaper::clone_dummy_child().unwrap();
        let (stdout_tx, stdout_rx) = oneshot::channel();
        let (stderr_tx, stderr_rx) = oneshot::channel();
        let start_result = Executor::new(tempfile::tempdir().unwrap().into_path())
            .unwrap()
            .start(
                &spec,
                InlineLimit::from(inline_limit),
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
        assert_eq!(reaper.await.unwrap(), expected_status);
        assert_eq!(stdout_rx.await.unwrap(), expected_stdout);
        assert_eq!(stderr_rx.await.unwrap(), expected_stderr);
    }

    #[tokio::test]
    #[serial]
    async fn exited_0() {
        start_and_expect_bash(
            "exit 0",
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn exited_1() {
        start_and_expect_bash(
            "exit 1",
            JobStatus::Exited(1),
            JobOutputResult::None,
            JobOutputResult::None,
        )
        .await;
    }

    // $$ returns the pid of outer-most bash. This doesn't do what we expect it to do when using
    // our executor. We should probably rewrite these tests to run python or something, and take
    // input from stdin.
    #[tokio::test]
    #[serial]
    async fn signaled_11() {
        start_and_expect_python(
            concat!(
                "import os;",
                "import sys;",
                "print('a');",
                "sys.stdout.flush();",
                "print('b', file=sys.stderr);",
                "sys.stderr.flush();",
                "os.abort()",
            ),
            JobStatus::Signaled(11),
            JobOutputResult::Inline(boxed_u8!(b"a\n")),
            JobOutputResult::Inline(boxed_u8!(b"b\n")),
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn stdout() {
        start_and_expect_bash_full(
            "echo a",
            vec![],
            &EnumSet::EMPTY,
            vec![],
            0,
            JobStatus::Exited(0),
            JobOutputResult::Truncated {
                first: boxed_u8!(b""),
                truncated: 2,
            },
            JobOutputResult::None,
        )
        .await;
        start_and_expect_bash_full(
            "echo a",
            vec![],
            &EnumSet::EMPTY,
            vec![],
            1,
            JobStatus::Exited(0),
            JobOutputResult::Truncated {
                first: boxed_u8!(b"a"),
                truncated: 1,
            },
            JobOutputResult::None,
        )
        .await;
        start_and_expect_bash_full(
            "echo a",
            vec![],
            &EnumSet::EMPTY,
            vec![],
            2,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"a\n")),
            JobOutputResult::None,
        )
        .await;
        start_and_expect_bash_full(
            "echo a",
            vec![],
            &EnumSet::EMPTY,
            vec![],
            3,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"a\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn stderr() {
        start_and_expect_bash_full(
            "echo a >&2",
            vec![],
            &EnumSet::EMPTY,
            vec![],
            0,
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::Truncated {
                first: boxed_u8!(b""),
                truncated: 2,
            },
        )
        .await;
        start_and_expect_bash_full(
            "echo a >&2",
            vec![],
            &EnumSet::EMPTY,
            vec![],
            1,
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::Truncated {
                first: boxed_u8!(b"a"),
                truncated: 1,
            },
        )
        .await;
        start_and_expect_bash_full(
            "echo a >&2",
            vec![],
            &EnumSet::EMPTY,
            vec![],
            2,
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::Inline(boxed_u8!(b"a\n")),
        )
        .await;
        start_and_expect_bash_full(
            "echo a >&2",
            vec![],
            &EnumSet::EMPTY,
            vec![],
            3,
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::Inline(boxed_u8!(b"a\n")),
        )
        .await;
    }

    #[test]
    #[serial]
    fn execution_error() {
        let layers = &NonEmpty::new(extract_dependencies());
        let spec = JobSpec {
            program: "a_program_that_does_not_exist",
            arguments: &[],
            environment: &[],
            layers,
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        assert_matches!(
            Executor::new(tempfile::tempdir().unwrap().into_path())
                .unwrap()
                .start(&spec, 0.into(), |_| unreachable!(), |_| unreachable!()),
            Err(JobError::Execution(_))
        );
    }

    #[tokio::test]
    #[serial]
    async fn environment() {
        start_and_expect_bash_full(
            "echo -n $FOO - $BAR",
            vec!["FOO=3", "BAR=4"],
            &EnumSet::EMPTY,
            vec![],
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"3 - 4")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn stdin_empty() {
        start_and_expect_bash(
            "cat",
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn pid_ppid_pgid_and_sid() {
        // We should be pid 1, that is, init for our namespace).
        // We should have ppid 0, indicating that our parent isn't accessible in our namespace.
        // We should have pgid 1, indicating that we're the group leader.
        // We should have sid 1, indicating that we're the session leader.
        start_and_expect_python(
            concat!(
                "import os;",
                "print('pid:', os.getpid());",
                "print('ppid:', os.getppid());",
                "print('pgid:', os.getpgid(0));",
                "print('sid:', os.getsid(0));",
            ),
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"pid: 1\nppid: 0\npgid: 1\nsid: 1\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_loopback() {
        let spec = JobSpec {
            program: "/bin/cat",
            arguments: &["/sys/class/net/lo/carrier".to_string()],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[JobMount {
                fs_type: JobMountFsType::Sys,
                mount_point: "/sys".to_string(),
            }],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(1),
            JobOutputResult::None,
            JobOutputResult::Inline(boxed_u8!(b"cat: read error: Invalid argument\n")),
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn loopback() {
        let spec = JobSpec {
            program: "/bin/cat",
            arguments: &["/sys/class/net/lo/carrier".to_string()],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[JobMount {
                fs_type: JobMountFsType::Sys,
                mount_point: "/sys".to_string(),
            }],
            enable_loopback: &true,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"1\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn uid_and_gid() {
        start_and_expect_python(
            concat!(
                "import os;",
                "print('uid:', os.getuid());",
                "print('gid:', os.getgid());",
            ),
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"uid: 0\ngid: 0\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn close_range() {
        let spec = JobSpec {
            program: "/bin/ls",
            arguments: &["/proc/self/fd".to_string()],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[JobMount {
                fs_type: JobMountFsType::Proc,
                mount_point: "/proc".to_string(),
            }],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"0\n1\n2\n3\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn one_layer_is_read_only() {
        let spec = JobSpec {
            program: "/bin/touch",
            arguments: &["/foo".to_string()],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(1),
            JobOutputResult::None,
            JobOutputResult::Inline(boxed_u8!(b"touch: /foo: Read-only file system\n")),
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn multiple_layers_in_correct_order() {
        let spec = JobSpec {
            program: "/bin/cat",
            arguments: &[
                "/root/file".to_string(),
                "/root/bottom-file".to_string(),
                "/root/top-file".to_string(),
            ],
            environment: &[],
            layers: &nonempty![
                extract_dependencies(),
                extract_layer_tar(include_bytes!("bottom-layer.tar")),
                extract_layer_tar(include_bytes!("top-layer.tar"))
            ],
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"top\nbottom file\ntop file\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn multiple_layers_read_only() {
        let spec = JobSpec {
            program: "/bin/touch",
            arguments: &["/foo".to_string()],
            environment: &[],
            layers: &nonempty![
                extract_dependencies(),
                extract_layer_tar(include_bytes!("bottom-layer.tar")),
                extract_layer_tar(include_bytes!("top-layer.tar"))
            ],
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(1),
            JobOutputResult::None,
            JobOutputResult::Inline(boxed_u8!(b"touch: /foo: Read-only file system\n")),
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_full() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/full | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_full() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/full | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::only(JobDevice::Full),
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"1, 7\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_null() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/null | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_null() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/null | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::only(JobDevice::Null),
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"1, 3\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_null_write() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &["-c".to_string(), "echo foo > /dev/null".to_string()],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::only(JobDevice::Null),
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_random() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/random | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_random() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/random | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::only(JobDevice::Random),
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"1, 8\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_tty() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/tty | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_tty() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/tty | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::only(JobDevice::Tty),
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"5, 0\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_urandom() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/urandom | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_urandom() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/urandom | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::only(JobDevice::Urandom),
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"1, 9\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_dev_zero() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/zero | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"0 Nov\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn dev_zero() {
        let spec = JobSpec {
            program: "/usr/bin/bash",
            arguments: &[
                "-c".to_string(),
                "/bin/ls -l /dev/zero | awk '{print $5, $6}'".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::only(JobDevice::Zero),
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"1, 5\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_tmpfs() {
        let spec = JobSpec {
            program: "/bin/grep",
            arguments: &["^tmpfs /tmp".to_string(), "/proc/self/mounts".to_string()],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[JobMount {
                fs_type: JobMountFsType::Proc,
                mount_point: "/proc".to_string(),
            }],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(1),
            JobOutputResult::None,
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn tmpfs() {
        let spec = JobSpec {
            program: "/bin/awk",
            arguments: &[
                r#"/^none \/tmp/ { print $1, $2, $3 }"#.to_string(),
                "/proc/self/mounts".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[
                JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: "/proc".to_string(),
                },
                JobMount {
                    fs_type: JobMountFsType::Tmp,
                    mount_point: "/tmp".to_string(),
                },
            ],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"none /tmp tmpfs\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_sysfs() {
        let spec = JobSpec {
            program: "/bin/grep",
            arguments: &["^sysfs /sys".to_string(), "/proc/self/mounts".to_string()],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[JobMount {
                fs_type: JobMountFsType::Proc,
                mount_point: "/proc".to_string(),
            }],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(1),
            JobOutputResult::None,
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn sysfs() {
        let spec = JobSpec {
            program: "/bin/awk",
            arguments: &[
                r#"/^none \/sys/ { print $1, $2, $3 }"#.to_string(),
                "/proc/self/mounts".to_string(),
            ],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[
                JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: "/proc".to_string(),
                },
                JobMount {
                    fs_type: JobMountFsType::Sys,
                    mount_point: "/sys".to_string(),
                },
            ],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"none /sys sysfs\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn no_procfs() {
        let spec = JobSpec {
            program: "/bin/ls",
            arguments: &["/proc".to_string()],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            0,
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn procfs() {
        let spec = JobSpec {
            program: "/bin/grep",
            arguments: &["proc".to_string(), "/proc/self/mounts".to_string()],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[JobMount {
                fs_type: JobMountFsType::Proc,
                mount_point: "/proc".to_string(),
            }],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            100,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(
                b"none /proc proc rw,nosuid,nodev,noexec,relatime 0 0\n"
            )),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn old_mounts_are_unmounted() {
        let spec = JobSpec {
            program: "/bin/wc",
            arguments: &["-l".to_string(), "/proc/self/mounts".to_string()],
            environment: &[],
            layers: &NonEmpty::new(extract_dependencies()),
            devices: &EnumSet::EMPTY,
            mounts: &[JobMount {
                fs_type: JobMountFsType::Proc,
                mount_point: "/proc".to_string(),
            }],
            enable_loopback: &false,
            working_directory: Path::new("/"),
        };
        start_and_expect(
            spec,
            20,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"2 /proc/self/mounts\n")),
            JobOutputResult::None,
        )
        .await;
    }
}
