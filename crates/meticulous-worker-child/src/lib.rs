#![no_std]

use core::{ffi::c_int, result};
use nc::syscalls::{self, Errno, Sysno};

pub enum Syscall {
    Zero(Sysno),
    One(Sysno, usize),
    OneFromSaved(Sysno),
    Two(Sysno, usize, usize),
    Three(Sysno, usize, usize, usize),
    ThreeAndSave(Sysno, usize, usize, usize),
    ThreeFromSaved(Sysno, usize, usize),
    Five(Sysno, usize, usize, usize, usize, usize),
    SixFromSaved(Sysno, usize, usize, usize, usize, usize),
}

impl Syscall {
    unsafe fn call(&self, saved: &mut usize) -> result::Result<usize, Errno> {
        match self {
            Syscall::Zero(n) => syscalls::syscall0(*n),
            Syscall::One(n, a1) => syscalls::syscall1(*n, *a1),
            Syscall::OneFromSaved(n) => syscalls::syscall1(*n, *saved),
            Syscall::Two(n, a1, a2) => syscalls::syscall2(*n, *a1, *a2),
            Syscall::Three(n, a1, a2, a3) => syscalls::syscall3(*n, *a1, *a2, *a3),
            Syscall::ThreeAndSave(n, a1, a2, a3) => {
                syscalls::syscall3(*n, *a1, *a2, *a3).map(|v| {
                    *saved = v;
                    v
                })
            }
            Syscall::ThreeFromSaved(n, a2, a3) => syscalls::syscall3(*n, *saved, *a2, *a3),
            Syscall::Five(n, a1, a2, a3, a4, a5) => syscalls::syscall5(*n, *a1, *a2, *a3, *a4, *a5),
            Syscall::SixFromSaved(n, a2, a3, a4, a5, a6) => {
                syscalls::syscall6(*n, *saved, *a2, *a3, *a4, *a5, *a6)
            }
        }
    }
}

/// The guts of the child code. This function shouldn't return on success, because in that case,
/// the last syscall should be an execve. If this function returns, than an error was encountered.
/// In that case, the script item index and the errno will be returned.
fn start_and_exec_in_child_inner(syscalls: &[Syscall]) -> (usize, nc::Errno) {
    let mut saved = 0;
    for (index, syscall) in syscalls.iter().enumerate() {
        if let Err(errno) = unsafe { syscall.call(&mut saved) } {
            return (index, errno);
        }
    }
    panic!("should not reach here");
}

/// Try to exec the job and write the error message to the pipe on failure.
pub fn start_and_exec_in_child(exec_result_write_fd: c_int, syscalls: &[Syscall]) -> ! {
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
