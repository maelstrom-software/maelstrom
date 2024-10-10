use anyhow::Result;
use clap::Parser;
use maelstrom_linux::{self as linux, ExitCode, WaitStatus};
use std::time::{Duration, Instant};

#[derive(Debug, Parser)]
pub struct CliArgs {
    #[clap(long, default_value_t = 1_000)]
    iterations: u32,
}

fn fork_execve_wait(dev_null: &impl linux::AsFd) -> Result<()> {
    if let Some(child) = linux::fork()? {
        let wait_result = linux::wait()?;
        assert_eq!(wait_result.pid, child);
        assert_eq!(wait_result.status, WaitStatus::Exited(ExitCode::from_u8(0)));
        Ok(())
    } else {
        linux::dup2(dev_null, &linux::Fd::STDOUT).unwrap();
        linux::execve(c"/bin/echo", &[None], &[None]).unwrap();
        unreachable!()
    }
}

fn posix_spawn_wait(dev_null: &impl linux::AsFd) -> Result<()> {
    let mut actions = linux::PosixSpawnFileActions::new();
    actions.add_dup2(dev_null, &linux::Fd::STDOUT)?;
    let attrs = linux::PosixSpawnAttrs::new();
    let child = linux::posix_spawn(c"/bin/echo", &actions, &attrs, &[None], &[None])?;
    let status = linux::waitpid(child)?;
    assert_eq!(status, WaitStatus::Exited(ExitCode::from_u8(0)));
    Ok(())
}

fn std_command_spawn_wait() -> Result<()> {
    let mut child = std::process::Command::new("/bin/echo")
        .stdout(std::process::Stdio::null())
        .spawn()?;

    let status = child.wait()?;
    assert!(status.success());

    Ok(())
}

struct ChildArgs {
    dev_null: linux::Fd,
}

extern "C" fn child_func(arg: *mut std::ffi::c_void) -> i32 {
    let arg: &ChildArgs = unsafe { &*(arg as *mut ChildArgs) };

    linux::dup2(&arg.dev_null, &linux::Fd::STDOUT).unwrap();
    linux::execve(c"/bin/echo", &[None], &[None]).unwrap();
    unreachable!()
}

fn clone_clone_vm_execve_wait(dev_null: &impl linux::AsFd, vfork: bool) -> Result<()> {
    const CHILD_STACK_SIZE: usize = 1024;
    let mut stack = vec![0u8; CHILD_STACK_SIZE];

    let child_args = ChildArgs {
        dev_null: dev_null.fd(),
    };
    let mut flags = linux::CloneFlags::VM;
    if vfork {
        flags |= linux::CloneFlags::VFORK;
    }
    let args = linux::CloneArgs::default()
        .flags(flags)
        .exit_signal(linux::Signal::CHLD);
    let stack_ptr: *mut u8 = stack.as_mut_ptr();
    let child = unsafe {
        linux::clone(
            child_func,
            stack_ptr.wrapping_add(CHILD_STACK_SIZE) as *mut _,
            &child_args as *const _ as *mut _,
            &args,
        )
    }?;
    let wait_result = linux::wait()?;
    assert_eq!(wait_result.pid, child);
    assert_eq!(wait_result.status, WaitStatus::Exited(ExitCode::from_u8(0)));

    Ok(())
}

fn run_benchmark(name: &str, iterations: u32, mut body: impl FnMut() -> Result<()>) -> Result<()> {
    let mut times = vec![];
    for _ in 0..iterations {
        let start = Instant::now();
        body()?;
        times.push(start.elapsed());
    }

    let total: Duration = times.iter().sum();
    let average = total / iterations;
    println!("ran {name} {iterations} times in {total:?} (avg. {average:?} / iteration)");

    Ok(())
}

pub fn main(args: CliArgs) -> Result<()> {
    let dev_null = linux::OwnedFd::from(std::os::fd::OwnedFd::from(
        std::fs::File::options().write(true).open("/dev/null")?,
    ));
    run_benchmark(
        "std::process::Command::{spawn + wait}",
        args.iterations,
        std_command_spawn_wait,
    )?;
    run_benchmark("fork + execve + wait", args.iterations, || {
        fork_execve_wait(&dev_null)
    })?;
    run_benchmark("posix_spawn + wait", args.iterations, || {
        posix_spawn_wait(&dev_null)
    })?;
    run_benchmark("clone(CLONE_VM) + execve + wait", args.iterations, || {
        clone_clone_vm_execve_wait(&dev_null, false)
    })?;
    run_benchmark(
        "clone(CLONE_VM | CLONE_VFORK) + execve + wait",
        args.iterations,
        || clone_clone_vm_execve_wait(&dev_null, true),
    )?;

    Ok(())
}
