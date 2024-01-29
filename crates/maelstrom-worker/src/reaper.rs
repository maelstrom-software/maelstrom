use anyhow::{Error, Result};
use maelstrom_base::JobStatus;
use maelstrom_linux::{self as linux, CloneArgs, CloneFlags};
use nc::types::{CLD_DUMPED, CLD_EXITED, CLD_KILLED};
use nix::{
    errno::Errno,
    unistd::{self, Pid},
};
use std::ops::ControlFlow;

fn clip_to_u8(val: i32) -> u8 {
    if val < 0 || val > u8::MAX as i32 {
        u8::MAX
    } else {
        val as u8
    }
}

pub trait ReaperDeps {
    fn on_waitid_error(&mut self, err: Errno) -> ControlFlow<()>;
    fn on_dummy_child_termination(&mut self) -> ControlFlow<()>;
    fn on_unexpected_wait_code(&mut self, pid: Pid) -> ControlFlow<()>;
    fn on_child_termination(&mut self, pid: Pid, status: JobStatus) -> ControlFlow<()>;
}

pub fn main(mut deps: impl ReaperDeps, dummy_pid: Pid) {
    let mut instruction = ControlFlow::Continue(());
    while let ControlFlow::Continue(()) = instruction {
        let mut siginfo = nc::siginfo_t::default();
        let options = nc::WEXITED;
        let mut usage = nc::rusage_t::default();
        instruction = match unsafe { nc::waitid(nc::P_ALL, -1, &mut siginfo, options, &mut usage) }
        {
            Err(err) => deps.on_waitid_error(Errno::from_i32(err)),
            Ok(_) => {
                let pid = Pid::from_raw(unsafe { siginfo.siginfo.sifields.sigchld.pid });
                if pid == dummy_pid {
                    deps.on_dummy_child_termination()
                } else {
                    let child_status = unsafe { siginfo.siginfo.sifields.sigchld.status };
                    match unsafe { siginfo.siginfo.si_code } {
                        CLD_EXITED => deps
                            .on_child_termination(pid, JobStatus::Exited(clip_to_u8(child_status))),
                        CLD_KILLED | CLD_DUMPED => deps.on_child_termination(
                            pid,
                            JobStatus::Signaled(clip_to_u8(child_status)),
                        ),
                        _ => deps.on_unexpected_wait_code(pid),
                    }
                }
            }
        };
    }
}

pub fn clone_dummy_child() -> Result<Pid> {
    // XXX: Adding CLONE_VM causes a crash. Eventually, we should fix that and put CLONE_VM back.
    let mut clone_args = CloneArgs::default()
        .flags(CloneFlags::CLEAR_SIGHAND | CloneFlags::FILES | CloneFlags::FS)
        .exit_signal(nc::SIGCHLD as u64);
    match linux::clone3(&mut clone_args) {
        Ok(Some(child_pid)) => Ok(Pid::from_raw(child_pid)),
        Ok(None) => loop {
            unistd::pause();
        },
        Err(e) => Err(Error::from(Errno::from_i32(e))),
    }
}
