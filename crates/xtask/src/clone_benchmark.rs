use anyhow::Result;
use clap::{Parser, Subcommand};
use maelstrom_linux::{self as linux, ExitCode, WaitStatus};
use std::time::{Duration, Instant};

#[derive(Debug, Parser)]
pub struct BenchmarkCliArgs {
    #[clap(long, default_value_t = 1_000)]
    iterations: u32,
}

#[derive(Debug, Parser)]
pub struct CliArgs {
    #[clap(subcommand)]
    command: CliCommand,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Subcommand)]
enum CliCommand {
    ComparisonBenchmark(BenchmarkCliArgs),
    CloneMappingBenchmark(BenchmarkCliArgs),
    ForkMappingBenchmark(BenchmarkCliArgs),
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

fn run_benchmark(
    name: &str,
    iterations: u32,
    print: impl FnOnce(&str, u32, Duration, Duration),
    mut body: impl FnMut() -> Result<()>,
) -> Result<()> {
    let mut times = vec![];
    for _ in 0..iterations {
        let start = Instant::now();
        body()?;
        times.push(start.elapsed());
    }

    let total: Duration = times.iter().sum();
    let average = total / iterations;
    print(name, iterations, total, average);

    Ok(())
}

fn print_result(name: &str, iterations: u32, total: Duration, average: Duration) {
    println!("ran {name} {iterations} times in {total:?} (avg. {average:?} / iteration)");
}

fn run_mapping_benchmark(
    args: BenchmarkCliArgs,
    mut body: impl FnMut() -> Result<()>,
) -> Result<()> {
    let mut total_mappings = 0;
    for _ in 0..100 {
        for _ in 0..300 {
            let m = linux::mmap(
                std::ptr::null_mut(),
                4096,
                linux::MemoryProtection::READ | linux::MemoryProtection::WRITE,
                linux::MapFlags::ANONYMOUS | linux::MapFlags::PRIVATE,
                None,
                0,
            )?;

            // Write to the page to make the mapping used.
            let m = unsafe { &mut *(m as *mut [u8; 4096]) };
            m[1024] = 123;

            total_mappings += 1;
        }

        run_benchmark(
            &format!("{total_mappings}"),
            args.iterations,
            |name, _iterations, _total, average| println!("{name} {}", average.as_micros()),
            &mut body,
        )?;
    }
    Ok(())
}

fn clone_mapping_benchmark(args: BenchmarkCliArgs) -> Result<()> {
    let dev_null = linux::OwnedFd::from(std::os::fd::OwnedFd::from(
        std::fs::File::options().write(true).open("/dev/null")?,
    ));
    run_mapping_benchmark(args, || clone_clone_vm_execve_wait(&dev_null, true))?;
    Ok(())
}

fn fork_mapping_benchmark(args: BenchmarkCliArgs) -> Result<()> {
    let dev_null = linux::OwnedFd::from(std::os::fd::OwnedFd::from(
        std::fs::File::options().write(true).open("/dev/null")?,
    ));
    run_mapping_benchmark(args, || fork_execve_wait(&dev_null))?;
    Ok(())
}

fn comparison_benchmark(args: BenchmarkCliArgs) -> Result<()> {
    let dev_null = linux::OwnedFd::from(std::os::fd::OwnedFd::from(
        std::fs::File::options().write(true).open("/dev/null")?,
    ));
    run_benchmark(
        "std::process::Command::{spawn + wait}",
        args.iterations,
        print_result,
        std_command_spawn_wait,
    )?;
    run_benchmark(
        "fork + execve + wait",
        args.iterations,
        print_result,
        || fork_execve_wait(&dev_null),
    )?;
    run_benchmark("posix_spawn + wait", args.iterations, print_result, || {
        posix_spawn_wait(&dev_null)
    })?;
    run_benchmark(
        "clone(CLONE_VM) + execve + wait",
        args.iterations,
        print_result,
        || clone_clone_vm_execve_wait(&dev_null, false),
    )?;
    run_benchmark(
        "clone(CLONE_VM | CLONE_VFORK) + execve + wait",
        args.iterations,
        print_result,
        || clone_clone_vm_execve_wait(&dev_null, true),
    )?;

    Ok(())
}

pub fn main(args: CliArgs) -> Result<()> {
    match args.command {
        CliCommand::ComparisonBenchmark(args) => comparison_benchmark(args),
        CliCommand::CloneMappingBenchmark(args) => clone_mapping_benchmark(args),
        CliCommand::ForkMappingBenchmark(args) => fork_mapping_benchmark(args),
    }
}
