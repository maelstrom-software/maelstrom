use anyhow::Result;
use maelstrom_base::JobStatus;
use maelstrom_linux::{self as linux, CloneArgs, CloneFlags, Errno, Pid, Signal, WaitStatus};
use std::ops::ControlFlow;

pub trait ReaperDeps {
    fn on_wait_error(&mut self, err: Errno) -> ControlFlow<()>;
    fn on_dummy_child_termination(&mut self) -> ControlFlow<()>;
    fn on_child_termination(&mut self, pid: Pid, status: JobStatus) -> ControlFlow<()>;
}

pub fn main(mut deps: impl ReaperDeps, dummy_pid: Pid) {
    let mut instruction = ControlFlow::Continue(());
    while let ControlFlow::Continue(()) = instruction {
        instruction = match linux::wait() {
            Err(err) => deps.on_wait_error(err),
            Ok(result) => {
                if result.pid == dummy_pid {
                    deps.on_dummy_child_termination()
                } else {
                    let status = match result.status {
                        WaitStatus::Exited(code) => JobStatus::Exited(code.into()),
                        WaitStatus::Signaled(signo) => JobStatus::Signaled(signo.into()),
                    };
                    deps.on_child_termination(result.pid, status)
                }
            }
        };
    }
}

pub fn clone_dummy_child() -> Result<Pid> {
    // XXX: Adding CLONE_VM causes a crash. Eventually, we should fix that and put CLONE_VM back.
    let mut clone_args = CloneArgs::default()
        .flags(CloneFlags::CLEAR_SIGHAND | CloneFlags::FILES | CloneFlags::FS)
        .exit_signal(Signal::CHLD);
    match linux::clone3(&mut clone_args) {
        Ok(Some(child_pid)) => Ok(child_pid),
        Ok(None) => loop {
            linux::pause();
        },
        Err(err) => Err(err.into()),
    }
}
