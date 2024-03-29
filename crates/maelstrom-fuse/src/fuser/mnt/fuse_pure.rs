//! Native FFI bindings to libfuse.
//!
//! This is a small set of bindings that are required to mount/unmount FUSE filesystems and
//! open/close a fd to the FUSE kernel driver.

#![warn(missing_debug_implementations)]
#![allow(missing_docs)]

use super::is_mounted;
use super::mount_options::{option_to_string, MountOption};
use libc::c_int;
use std::ffi::{CStr, CString, OsStr};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Error, ErrorKind, Read};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::{mem, ptr};

const FUSERMOUNT_BIN: &str = "fusermount";
const FUSERMOUNT3_BIN: &str = "fusermount3";
const FUSERMOUNT_COMM_ENV: &str = "_FUSE_COMMFD";

#[derive(Debug)]
pub struct Mount {
    mountpoint: CString,
    auto_unmount_socket: Option<UnixStream>,
    fuse_device: Arc<File>,
}
impl Mount {
    pub fn new(mountpoint: &Path, options: &[MountOption]) -> io::Result<(Arc<File>, Mount)> {
        let mountpoint = mountpoint.canonicalize()?;
        let (file, sock) = fuse_mount_pure(mountpoint.as_os_str(), options)?;
        let file = Arc::new(file);
        Ok((
            file.clone(),
            Mount {
                mountpoint: CString::new(mountpoint.as_os_str().as_bytes())?,
                auto_unmount_socket: sock,
                fuse_device: file,
            },
        ))
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        use std::io::ErrorKind::PermissionDenied;
        if !is_mounted(&self.fuse_device) {
            // If the filesystem has already been unmounted, avoid unmounting it again.
            // Unmounting it a second time could cause a race with a newly mounted filesystem
            // living at the same mountpoint
            return;
        }
        if let Some(sock) = mem::take(&mut self.auto_unmount_socket) {
            drop(sock);
            // fusermount in auto-unmount mode, no more work to do.
            return;
        }
        if let Err(err) = super::libc_umount(&self.mountpoint) {
            if err.kind() == PermissionDenied {
                // Linux always returns EPERM for non-root users.  We have to let the
                // library go through the setuid-root "fusermount -u" to unmount.
                fuse_unmount_pure(&self.mountpoint)
            }
        }
    }
}

fn fuse_mount_pure(
    mountpoint: &OsStr,
    options: &[MountOption],
) -> Result<(File, Option<UnixStream>), io::Error> {
    if options.contains(&MountOption::AutoUnmount) {
        // Auto unmount is only supported via fusermount
        return fuse_mount_fusermount(mountpoint, options);
    }

    let res = fuse_mount_sys(mountpoint, options)?;
    if let Some(file) = res {
        Ok((file, None))
    } else {
        // Retry
        fuse_mount_fusermount(mountpoint, options)
    }
}

fn fuse_unmount_pure(mountpoint: &CStr) {
    unsafe {
        let result = libc::umount2(mountpoint.as_ptr(), libc::MNT_DETACH);
        if result == 0 {
            return;
        }
    }

    let mut builder = Command::new(detect_fusermount_bin());
    builder.stdout(Stdio::piped()).stderr(Stdio::piped());
    builder
        .arg("-u")
        .arg("-q")
        .arg("-z")
        .arg("--")
        .arg(OsStr::new(&mountpoint.to_string_lossy().into_owned()));

    builder.output().ok();
}

fn detect_fusermount_bin() -> String {
    for name in [
        FUSERMOUNT3_BIN.to_string(),
        FUSERMOUNT_BIN.to_string(),
        format!("/bin/{FUSERMOUNT3_BIN}"),
        format!("/bin/{FUSERMOUNT_BIN}"),
    ]
    .iter()
    {
        if Command::new(name).arg("-h").output().is_ok() {
            return name.to_string();
        }
    }
    // Default to fusermount3
    FUSERMOUNT3_BIN.to_string()
}

fn receive_fusermount_message(socket: &UnixStream) -> Result<File, Error> {
    let mut io_vec_buf = [0u8];
    let mut io_vec = libc::iovec {
        iov_base: io_vec_buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: io_vec_buf.len(),
    };
    let cmsg_buffer_len = unsafe { libc::CMSG_SPACE(mem::size_of::<c_int>() as libc::c_uint) };
    let mut cmsg_buffer = vec![0u8; cmsg_buffer_len as usize];
    let mut message: libc::msghdr;
    message = libc::msghdr {
        msg_name: ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: &mut io_vec,
        msg_iovlen: 1,
        msg_control: cmsg_buffer.as_mut_ptr() as *mut libc::c_void,
        msg_controllen: cmsg_buffer.len(),
        msg_flags: 0,
    };
    let mut result;
    loop {
        unsafe {
            result = libc::recvmsg(socket.as_raw_fd(), &mut message, 0);
        }
        if result != -1 {
            break;
        }
        let err = Error::last_os_error();
        if err.kind() != ErrorKind::Interrupted {
            return Err(err);
        }
    }
    if result == 0 {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "Unexpected EOF reading from fusermount",
        ));
    }

    unsafe {
        let control_msg = libc::CMSG_FIRSTHDR(&message);
        if (*control_msg).cmsg_type != libc::SCM_RIGHTS {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Unknown control message from fusermount: {}",
                    (*control_msg).cmsg_type
                ),
            ));
        }
        let fd_data = libc::CMSG_DATA(control_msg);

        let fd = *(fd_data as *const c_int);
        if fd < 0 {
            Err(ErrorKind::InvalidData.into())
        } else {
            Ok(File::from_raw_fd(fd))
        }
    }
}

fn fuse_mount_fusermount(
    mountpoint: &OsStr,
    options: &[MountOption],
) -> Result<(File, Option<UnixStream>), Error> {
    let (child_socket, receive_socket) = UnixStream::pair()?;

    unsafe {
        libc::fcntl(child_socket.as_raw_fd(), libc::F_SETFD, 0);
    }

    let mut builder = Command::new(detect_fusermount_bin());
    builder.stdout(Stdio::piped()).stderr(Stdio::piped());
    if !options.is_empty() {
        builder.arg("-o");
        let options_strs: Vec<String> = options.iter().map(option_to_string).collect();
        builder.arg(options_strs.join(","));
    }
    builder
        .arg("--")
        .arg(mountpoint)
        .env(FUSERMOUNT_COMM_ENV, child_socket.as_raw_fd().to_string());

    let fusermount_child = builder.spawn()?;

    drop(child_socket); // close socket in parent

    let file = match receive_fusermount_message(&receive_socket) {
        Ok(f) => f,
        Err(_) => {
            // Drop receive socket, since fusermount has exited with an error
            drop(receive_socket);
            let output = fusermount_child.wait_with_output().unwrap();
            let stderr_string = String::from_utf8_lossy(&output.stderr).to_string();
            return if stderr_string.contains("only allowed if 'user_allow_other' is set") {
                Err(io::Error::new(ErrorKind::PermissionDenied, stderr_string))
            } else {
                Err(io::Error::new(ErrorKind::Other, stderr_string))
            };
        }
    };
    let mut receive_socket = Some(receive_socket);

    if !options.contains(&MountOption::AutoUnmount) {
        // Only close the socket, if auto unmount is not set.
        // fusermount will keep running until the socket is closed, if auto unmount is set
        drop(mem::take(&mut receive_socket));
        fusermount_child.wait_with_output()?;
    } else {
        if let Some(mut stdout) = fusermount_child.stdout {
            let stdout_fd = stdout.as_raw_fd();
            unsafe {
                let mut flags = libc::fcntl(stdout_fd, libc::F_GETFL, 0);
                flags |= libc::O_NONBLOCK;
                libc::fcntl(stdout_fd, libc::F_SETFL, flags);
            }
            let mut buf = vec![0; 64 * 1024];
            stdout.read(&mut buf).ok();
        }
        if let Some(mut stderr) = fusermount_child.stderr {
            let stderr_fd = stderr.as_raw_fd();
            unsafe {
                let mut flags = libc::fcntl(stderr_fd, libc::F_GETFL, 0);
                flags |= libc::O_NONBLOCK;
                libc::fcntl(stderr_fd, libc::F_SETFL, flags);
            }
            let mut buf = vec![0; 64 * 1024];
            stderr.read(&mut buf).ok();
        }
    }

    unsafe {
        libc::fcntl(file.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
    }

    Ok((file, receive_socket))
}

// If returned option is none. Then fusermount binary should be tried
fn fuse_mount_sys(mountpoint: &OsStr, options: &[MountOption]) -> Result<Option<File>, Error> {
    let fuse_device_name = "/dev/fuse";

    let mountpoint_mode = File::open(mountpoint)?.metadata()?.permissions().mode();

    // Auto unmount requests must be sent to fusermount binary
    assert!(!options.contains(&MountOption::AutoUnmount));

    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(fuse_device_name)
    {
        Ok(file) => file,
        Err(error) => {
            return Err(error);
        }
    };
    assert!(
        file.as_raw_fd() > 2,
        "Conflict with stdin/stdout/stderr. fd={}",
        file.as_raw_fd()
    );

    let mut mount_options = format!(
        "fd={},rootmode={:o},user_id={},group_id={}",
        file.as_raw_fd(),
        mountpoint_mode,
        unsafe { libc::getuid() },
        unsafe { libc::getgid() }
    );

    for option in options
        .iter()
        .filter(|x| option_group(x) == MountOptionGroup::KernelOption)
    {
        mount_options.push(',');
        mount_options.push_str(&option_to_string(option));
    }

    let mut flags = 0;
    if !options.contains(&MountOption::Dev) {
        // Default to nodev
        flags |= libc::MS_NODEV;
    }
    if !options.contains(&MountOption::Suid) {
        // Default to nosuid
        flags |= libc::MS_NOSUID;
    }
    for flag in options
        .iter()
        .filter(|x| option_group(x) == MountOptionGroup::KernelFlag)
    {
        flags |= option_to_flag(flag);
    }

    // Default name is "/dev/fuse", then use the subtype, and lastly prefer the name
    let mut source = fuse_device_name;
    if let Some(MountOption::Subtype(subtype)) = options
        .iter()
        .find(|x| matches!(**x, MountOption::Subtype(_)))
    {
        source = subtype;
    }
    if let Some(MountOption::FSName(name)) = options
        .iter()
        .find(|x| matches!(**x, MountOption::FSName(_)))
    {
        source = name;
    }

    let c_source = CString::new(source).unwrap();
    let c_mountpoint = CString::new(mountpoint.as_bytes()).unwrap();

    let result = unsafe {
        let c_options = CString::new(mount_options).unwrap();
        let c_type = CString::new("fuse").unwrap();
        libc::mount(
            c_source.as_ptr(),
            c_mountpoint.as_ptr(),
            c_type.as_ptr(),
            flags,
            c_options.as_ptr() as *const libc::c_void,
        )
    };
    if result == -1 {
        let err = Error::last_os_error();
        if err.kind() == ErrorKind::PermissionDenied {
            return Ok(None); // Retry with fusermount
        } else {
            return Err(Error::new(
                err.kind(),
                format!("Error calling mount() at {mountpoint:?}: {err}"),
            ));
        }
    }

    Ok(Some(file))
}

#[derive(PartialEq)]
pub enum MountOptionGroup {
    KernelOption,
    KernelFlag,
    Fusermount,
}

pub fn option_group(option: &MountOption) -> MountOptionGroup {
    match option {
        MountOption::FSName(_) => MountOptionGroup::Fusermount,
        MountOption::Subtype(_) => MountOptionGroup::Fusermount,
        MountOption::CUSTOM(_) => MountOptionGroup::KernelOption,
        MountOption::AutoUnmount => MountOptionGroup::Fusermount,
        MountOption::AllowOther => MountOptionGroup::KernelOption,
        MountOption::Dev => MountOptionGroup::KernelFlag,
        MountOption::NoDev => MountOptionGroup::KernelFlag,
        MountOption::Suid => MountOptionGroup::KernelFlag,
        MountOption::NoSuid => MountOptionGroup::KernelFlag,
        MountOption::RO => MountOptionGroup::KernelFlag,
        MountOption::RW => MountOptionGroup::KernelFlag,
        MountOption::Exec => MountOptionGroup::KernelFlag,
        MountOption::NoExec => MountOptionGroup::KernelFlag,
        MountOption::Atime => MountOptionGroup::KernelFlag,
        MountOption::NoAtime => MountOptionGroup::KernelFlag,
        MountOption::DirSync => MountOptionGroup::KernelFlag,
        MountOption::Sync => MountOptionGroup::KernelFlag,
        MountOption::Async => MountOptionGroup::KernelFlag,
        MountOption::AllowRoot => MountOptionGroup::KernelOption,
        MountOption::DefaultPermissions => MountOptionGroup::KernelOption,
    }
}

pub fn option_to_flag(option: &MountOption) -> libc::c_ulong {
    match option {
        MountOption::Dev => 0, // There is no option for dev. It's the absence of NoDev
        MountOption::NoDev => libc::MS_NODEV,
        MountOption::Suid => 0,
        MountOption::NoSuid => libc::MS_NOSUID,
        MountOption::RW => 0,
        MountOption::RO => libc::MS_RDONLY,
        MountOption::Exec => 0,
        MountOption::NoExec => libc::MS_NOEXEC,
        MountOption::Atime => 0,
        MountOption::NoAtime => libc::MS_NOATIME,
        MountOption::Async => 0,
        MountOption::Sync => libc::MS_SYNCHRONOUS,
        MountOption::DirSync => libc::MS_DIRSYNC,
        _ => unreachable!(),
    }
}
