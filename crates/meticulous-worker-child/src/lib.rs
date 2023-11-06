use anyhow::Result;
use meticulous_base::JobDetails;
use nix::{
    errno::Errno,
    unistd::{Gid, Uid},
};
use std::{
    ffi::{CString, c_int},
    fs::{self, File},
    io::Write as _,
    iter,
    os::fd::{FromRawFd as _},
    ptr,
};

/// The guts of the child code. This function can return a [`Result`].
fn start_and_exec_in_child_inner(
    details: &JobDetails,
    stdout_write_fd: c_int,
    stderr_write_fd: c_int,
    parent_uid: Uid,
    parent_gid: Gid,
) -> Result<()> {
    fs::write("/proc/self/setgroups", "deny\n")?;
    fs::write("/proc/self/uid_map", format!("0 {} 1\n", parent_uid))?;
    fs::write("/proc/self/gid_map", format!("0 {} 1\n", parent_gid))?;
    unsafe { nc::setsid() }.map_err(Errno::from_i32)?;
    unsafe { nc::dup2(stdout_write_fd, 1) }.map_err(Errno::from_i32)?;
    unsafe { nc::dup2(stderr_write_fd, 2) }.map_err(Errno::from_i32)?;
    unsafe { nc::close_range(3, !0u32, nc::CLOSE_RANGE_CLOEXEC) }.map_err(Errno::from_i32)?;
    let program_cstr = CString::new(details.program.as_str())?;
    let program_ptr = program_cstr.as_bytes_with_nul().as_ptr();
    let arguments = details
        .arguments
        .iter()
        .map(String::as_str)
        .map(CString::new)
        .collect::<Result<Vec<_>, _>>()?;
    let argument_ptrs = iter::once(program_ptr)
        .chain(
            arguments
                .iter()
                .map(|cstr| cstr.as_bytes_with_nul().as_ptr()),
        )
        .chain(iter::once(ptr::null()))
        .collect::<Vec<_>>();
    let env: [*const u8; 1] = [ptr::null()];
    match unsafe {
        nc::syscalls::syscall3(
            nc::SYS_EXECVE,
            program_ptr as usize,
            argument_ptrs.as_ptr() as usize,
            env.as_ptr() as usize,
        )
    } {
        Err(errno) => Err(Errno::from_i32(errno).into()),
        Ok(_) => unreachable!(),
    }
}

/// Try to exec the job and write the error message to the pipe on failure.
pub fn start_and_exec_in_child(
    details: &JobDetails,
    _stdout_read_fd: c_int,
    stdout_write_fd: c_int,
    _stderr_read_fd: c_int,
    stderr_write_fd: c_int,
    _exec_result_read_fd: c_int,
    exec_result_write_fd: c_int,
    parent_uid: Uid,
    parent_gid: Gid,
) -> ! {
    // TODO: https://github.com/meticulous-software/meticulous/issues/47
    //
    // We assume any error we encounter in the child is an execution error. While highly unlikely,
    // we could theoretically encounter a system error.
    let Err(err) = start_and_exec_in_child_inner(
        details,
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
