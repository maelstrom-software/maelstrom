use anyhow::Result;
use nix::errno::Errno;
use std::{
    ffi::{c_char, c_int},
    fs::{self, File},
    io::Write as _,
    os::fd::FromRawFd as _,
};

// These might not work for all linux architectures. We can fix them as we add more architectures.
#[allow(non_camel_case_types)]
pub type uid_t = u32;
#[allow(non_camel_case_types)]
pub type gid_t = u32;

/// The guts of the child code. This function can return a [`Result`].
fn start_and_exec_in_child_inner(
    program: *const c_char,
    argv: *const *const c_char,
    env: *const *const c_char,
    stdout_write_fd: c_int,
    stderr_write_fd: c_int,
    parent_uid: uid_t,
    parent_gid: gid_t,
) -> Result<()> {
    fs::write("/proc/self/setgroups", "deny\n")?;
    fs::write("/proc/self/uid_map", format!("0 {} 1\n", parent_uid))?;
    fs::write("/proc/self/gid_map", format!("0 {} 1\n", parent_gid))?;
    unsafe { nc::setsid() }.map_err(Errno::from_i32)?;
    unsafe { nc::dup2(stdout_write_fd, 1) }.map_err(Errno::from_i32)?;
    unsafe { nc::dup2(stderr_write_fd, 2) }.map_err(Errno::from_i32)?;
    unsafe { nc::close_range(3, !0u32, nc::CLOSE_RANGE_CLOEXEC) }.map_err(Errno::from_i32)?;
    match unsafe {
        nc::syscalls::syscall3(
            nc::SYS_EXECVE,
            program as usize,
            argv as usize,
            env as usize,
        )
    } {
        Err(errno) => Err(Errno::from_i32(errno).into()),
        Ok(_) => unreachable!(),
    }
}

/// Try to exec the job and write the error message to the pipe on failure.
#[allow(clippy::too_many_arguments)]
pub fn start_and_exec_in_child(
    program: *const c_char,
    argv: *const *const c_char,
    env: *const *const c_char,
    _stdout_read_fd: c_int,
    stdout_write_fd: c_int,
    _stderr_read_fd: c_int,
    stderr_write_fd: c_int,
    _exec_result_read_fd: c_int,
    exec_result_write_fd: c_int,
    parent_uid: uid_t,
    parent_gid: gid_t,
) -> ! {
    // TODO: https://github.com/meticulous-software/meticulous/issues/47
    //
    // We assume any error we encounter in the child is an execution error. While highly unlikely,
    // we could theoretically encounter a system error.
    let Err(err) = start_and_exec_in_child_inner(
        program,
        argv,
        env,
        stdout_write_fd,
        stderr_write_fd,
        parent_uid,
        parent_gid,
    ) else {
        unreachable!();
    };
    let _ = unsafe { File::from_raw_fd(exec_result_write_fd) }.write_fmt(format_args!("{}", err));
    std::process::exit(1);
}
