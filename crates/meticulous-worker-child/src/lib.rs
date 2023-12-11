#![no_std]

use core::{
    ffi::{c_int, CStr},
    mem, ptr, result,
};
use nc::{
    mode_t,
    syscalls::{self, Errno},
};

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct sockaddr_nl_t {
    pub sin_family: nc::sa_family_t,
    pub nl_pad: u16,
    pub nl_pid: u32,
    pub nl_groups: u32,
}

pub enum Syscall<'a> {
    SocketAndSave(i32, i32, i32),
    BindSaved(&'a sockaddr_nl_t),
    SendToSaved(&'a [u8]),
    RecvFromSaved(&'a mut [u8]),
    OpenAndSave(&'a CStr, i32, mode_t),
    WriteSaved(&'a [u8]),
    SetSid,
    Dup2(c_int, c_int),
    CloseRange(u32, u32, u32),
    Mount(
        Option<&'a CStr>,
        &'a CStr,
        Option<&'a CStr>,
        usize,
        Option<&'a [u8]>,
    ),
    Chdir(&'a CStr),
    Mkdir(&'a CStr, mode_t),
    PivotRoot(&'a CStr, &'a CStr),
    Umount2(&'a CStr, usize),
    Execve(&'a CStr, &'a [Option<&'a u8>], &'a [Option<&'a u8>]),
}

impl<'a> Syscall<'a> {
    unsafe fn call(&mut self, saved: &mut usize) -> result::Result<usize, Errno> {
        match self {
            Syscall::SocketAndSave(domain, sock_type, protocol) => syscalls::syscall3(
                nc::SYS_SOCKET,
                *domain as usize,
                *sock_type as usize,
                *protocol as usize,
            )
            .map(|v| {
                *saved = v;
                v
            }),
            Syscall::BindSaved(sockaddr) => {
                let sockaddr_ptr = *sockaddr as *const sockaddr_nl_t as usize;
                let sockaddr_len = mem::size_of::<sockaddr_nl_t>();
                syscalls::syscall3(nc::SYS_BIND, *saved, sockaddr_ptr, sockaddr_len)
            }
            Syscall::SendToSaved(buf) => {
                let buf_ptr = buf.as_ptr() as usize;
                let buf_len = buf.len();
                syscalls::syscall6(nc::SYS_SENDTO, *saved, buf_ptr, buf_len, 0, 0, 0)
            }
            Syscall::RecvFromSaved(buf) => {
                let buf_ptr = buf.as_mut_ptr() as usize;
                let buf_len = buf.len();
                syscalls::syscall6(nc::SYS_RECVFROM, *saved, buf_ptr, buf_len, 0, 0, 0)
            }
            Syscall::OpenAndSave(filename, flags, mode) => syscalls::syscall3(
                nc::SYS_OPEN,
                filename.to_bytes_with_nul().as_ptr() as usize,
                *flags as usize,
                *mode as usize,
            )
            .map(|v| {
                *saved = v;
                v
            }),
            Syscall::WriteSaved(buf) => {
                let buf_ptr = buf.as_ptr() as usize;
                let buf_len = buf.len();
                syscalls::syscall3(nc::SYS_WRITE, *saved, buf_ptr, buf_len)
            }
            Syscall::SetSid => syscalls::syscall0(nc::SYS_SETSID),
            Syscall::Dup2(from, to) => {
                syscalls::syscall2(nc::SYS_DUP2, *from as usize, *to as usize)
            }
            Syscall::CloseRange(first, last, flags) => syscalls::syscall3(
                nc::SYS_CLOSE_RANGE,
                *first as usize,
                *last as usize,
                *flags as usize,
            ),
            Syscall::Mount(source, target, fstype, flags, data) => syscalls::syscall5(
                nc::SYS_MOUNT,
                source
                    .map(|r| r.to_bytes_with_nul().as_ptr())
                    .unwrap_or(ptr::null()) as usize,
                target.to_bytes_with_nul().as_ptr() as usize,
                fstype.map(|r| r.as_ptr()).unwrap_or(ptr::null()) as usize,
                *flags,
                data.map(|r| r.as_ptr()).unwrap_or(ptr::null()) as usize,
            ),
            Syscall::Chdir(path) => {
                syscalls::syscall1(nc::SYS_CHDIR, path.to_bytes_with_nul().as_ptr() as usize)
            }
            Syscall::Mkdir(path, mode) => syscalls::syscall2(
                nc::SYS_MKDIR,
                path.to_bytes_with_nul().as_ptr() as usize,
                *mode as usize,
            ),
            Syscall::PivotRoot(new_root, put_old) => syscalls::syscall2(
                nc::SYS_PIVOT_ROOT,
                new_root.to_bytes_with_nul().as_ptr() as usize,
                put_old.to_bytes_with_nul().as_ptr() as usize,
            ),
            Syscall::Umount2(path, flags) => syscalls::syscall2(
                nc::SYS_UMOUNT2,
                path.to_bytes_with_nul().as_ptr() as usize,
                *flags,
            ),
            Syscall::Execve(program, arguments, environment) => syscalls::syscall3(
                nc::SYS_EXECVE,
                program.to_bytes_with_nul().as_ptr() as usize,
                arguments.as_ptr() as usize,
                environment.as_ptr() as usize,
            ),
        }
    }
}

/// The guts of the child code. This function shouldn't return on success, because in that case,
/// the last syscall should be an execve. If this function returns, than an error was encountered.
/// In that case, the script item index and the errno will be returned.
fn start_and_exec_in_child_inner(syscalls: &mut [Syscall]) -> (usize, nc::Errno) {
    let mut saved = 0;
    for (index, syscall) in syscalls.iter_mut().enumerate() {
        if let Err(errno) = unsafe { syscall.call(&mut saved) } {
            return (index, errno);
        }
    }
    panic!("should not reach here");
}

/// Try to exec the job and write the error message to the pipe on failure.
pub fn start_and_exec_in_child(exec_result_write_fd: c_int, syscalls: &mut [Syscall]) -> ! {
    let (index, errno) = start_and_exec_in_child_inner(syscalls);
    let result = (index as u64) << 32 | (errno as u64);
    // There's not really much to do if this write fails. Therefore, we just ignore the result.
    // However, it's hard to imagine any case where this could fail and we'd actually care.
    let _ = unsafe {
        nc::write(
            exec_result_write_fd,
            result.to_le_bytes().as_ptr() as usize,
            8,
        )
    };
    unsafe { nc::exit(1) };
}
