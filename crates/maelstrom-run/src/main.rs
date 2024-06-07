use anyhow::{anyhow, Result};
use clap::Args;
use maelstrom_base::{
    ClientJobId, JobCompleted, JobEffects, JobError, JobOutcome, JobOutcomeResult, JobOutputResult,
    JobStatus, JobTty, WindowSize,
};
use maelstrom_client::{
    CacheDir, Client, ClientBgProcess, ContainerImageDepotDir, ProjectDir, StateDir,
};
use maelstrom_linux::{self as linux, Fd, SockaddrUnStorage, SocketDomain, SocketType};
use maelstrom_macro::Config;
use maelstrom_run::spec::job_spec_iter_from_reader;
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots},
    fs::Fs,
    log,
    process::{ExitCode, ExitCodeAccumulator},
    root::{Root, RootBuf},
};
use std::{
    env,
    io::{self, IsTerminal as _, Read, Write as _},
    mem,
    os::{fd::OwnedFd, unix::net::UnixListener},
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
    thread,
};
use xdg::BaseDirectories;

#[derive(Config, Debug)]
pub struct Config {
    /// Socket address of broker. If not provided, all jobs will be run locally.
    #[config(
        option,
        short = 'b',
        value_name = "SOCKADDR",
        default = r#""standalone mode""#
    )]
    pub broker: Option<BrokerAddr>,

    /// Minimum log level to output.
    #[config(short = 'l', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,

    /// Directory in which to put cached container images.
    #[config(
        value_name = "PATH",
        default = r#"|bd: &BaseDirectories| {
            bd.get_cache_home()
                .parent()
                .unwrap()
                .join("container/")
                .into_os_string()
                .into_string()
                .unwrap()
        }"#
    )]
    pub container_image_depot_root: RootBuf<ContainerImageDepotDir>,

    /// Directory for state that persists between runs, including the client's log file.
    #[config(
        value_name = "PATH",
        default = r#"|bd: &BaseDirectories| {
            bd.get_state_home()
                .into_os_string()
                .into_string()
                .unwrap()
        }"#
    )]
    pub state_root: RootBuf<StateDir>,

    /// Directory to use for the cache. The local worker's cache will be contained within it.
    #[config(
        value_name = "PATH",
        default = r#"|bd: &BaseDirectories| {
            bd.get_cache_home()
                .into_os_string()
                .into_string()
                .unwrap()
        }"#
    )]
    pub cache_root: RootBuf<CacheDir>,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative. SI and binary suffixes are supported.
    #[config(
        value_name = "BYTES",
        default = "CacheSize::default()",
        next_help_heading = "Local Worker Options"
    )]
    pub cache_size: CacheSize,

    /// The maximum amount of bytes to return inline for captured stdout and stderr.
    #[config(value_name = "BYTES", default = "InlineLimit::default()")]
    pub inline_limit: InlineLimit,

    /// The number of job slots available.
    #[config(value_name = "N", default = "Slots::default()")]
    pub slots: Slots,
}

#[derive(Args)]
#[command(next_help_heading = "Other Command-Line Options")]
pub struct ExtraCommandLineOptions {
    #[arg(
        long,
        short = 'f',
        value_name = "PATH",
        help = "Read the job specifications from the provided file, instead of from standard \
            input."
    )]
    pub file: Option<PathBuf>,

    #[command(flatten)]
    pub one_or_tty: OneOrTty,

    #[arg(
        num_args = 0..,
        requires = "OneOrTty",
        value_name = "PROGRAM-AND-ARGUMENTS",
        help = "Program and arguments override. Can only be used with --one or --tty. If provided \
            these will be used for the program and arguments, ignoring whatever is in the job \
            specification."
    )]
    pub args: Vec<String>,
}

#[derive(Args)]
#[group(multiple = false)]
pub struct OneOrTty {
    #[arg(
        long,
        short = '1',
        help = "Execute only one job. If multiple job specifications are provided, all but the \
            first are ignored. Optionally, positional arguments can be provided to override the \
            job's program and arguments"
    )]
    pub one: bool,

    #[arg(
        long,
        short = 't',
        help = "Execute only one job, with its standard input, output, and error assigned to \
            a TTY. The TTY in turn will be connected to this process's standard input, output. \
            This process's standard input and output must be connected to a TTY. If multiple job \
            specifications are provided, all but the first are ignored. Optionally, positional \
            arguments can be provided to override the job's program and arguments."
    )]
    pub tty: bool,
}

impl OneOrTty {
    fn any(&self) -> bool {
        self.one || self.tty
    }
}

fn print_effects(
    cjid: ClientJobId,
    JobEffects {
        stdout,
        stderr,
        duration: _,
    }: JobEffects,
) -> Result<()> {
    match stdout {
        JobOutputResult::None => {}
        JobOutputResult::Inline(bytes) => {
            io::stdout().lock().write_all(&bytes)?;
        }
        JobOutputResult::Truncated { first, truncated } => {
            io::stdout().lock().write_all(&first)?;
            io::stdout().lock().flush()?;
            eprintln!("job {cjid}: stdout truncated, {truncated} bytes lost");
        }
    }
    match stderr {
        JobOutputResult::None => {}
        JobOutputResult::Inline(bytes) => {
            io::stderr().lock().write_all(&bytes)?;
        }
        JobOutputResult::Truncated { first, truncated } => {
            io::stderr().lock().write_all(&first)?;
            eprintln!("job {cjid}: stderr truncated, {truncated} bytes lost");
        }
    }
    Ok(())
}

fn visitor(res: Result<(ClientJobId, JobOutcomeResult)>, tracker: Arc<JobTracker>) {
    let exit_code = match res {
        Ok((cjid, Ok(JobOutcome::Completed(JobCompleted { status, effects })))) => {
            print_effects(cjid, effects).ok();
            match status {
                JobStatus::Exited(0) => ExitCode::SUCCESS,
                JobStatus::Exited(code) => {
                    io::stdout().lock().flush().ok();
                    eprintln!("job {cjid}: exited with code {code}");
                    ExitCode::from(code)
                }
                JobStatus::Signaled(signum) => {
                    io::stdout().lock().flush().ok();
                    eprintln!("job {cjid}: killed by signal {signum}");
                    ExitCode::FAILURE
                }
            }
        }
        Ok((cjid, Ok(JobOutcome::TimedOut(effects)))) => {
            print_effects(cjid, effects).ok();
            io::stdout().lock().flush().ok();
            eprintln!("job {cjid}: timed out");
            ExitCode::FAILURE
        }
        Ok((cjid, Err(JobError::Execution(err)))) => {
            eprintln!("job {cjid}: execution error: {err}");
            ExitCode::FAILURE
        }
        Ok((cjid, Err(JobError::System(err)))) => {
            eprintln!("job {cjid}: system error: {err}");
            ExitCode::FAILURE
        }
        Err(err) => {
            eprintln!("remote error: {err}");
            ExitCode::FAILURE
        }
    };
    tracker.job_completed(exit_code);
}

#[derive(Default)]
struct JobTracker {
    condvar: Condvar,
    outstanding: Mutex<usize>,
    accum: ExitCodeAccumulator,
}

impl JobTracker {
    fn add_outstanding(&self) {
        let mut locked = self.outstanding.lock().unwrap();
        *locked += 1;
    }

    fn job_completed(&self, exit_code: ExitCode) {
        let mut locked = self.outstanding.lock().unwrap();
        *locked -= 1;
        self.accum.add(exit_code);
        self.condvar.notify_one();
    }

    fn wait_for_outstanding(&self) {
        let mut locked = self.outstanding.lock().unwrap();
        while *locked > 0 {
            locked = self.condvar.wait(locked).unwrap();
        }
    }
}

fn main() -> Result<ExitCode> {
    let (config, mut extra_options): (_, ExtraCommandLineOptions) =
        Config::new_with_extra_from_args("maelstrom/run", "MAELSTROM_RUN", env::args())?;
    if extra_options.one_or_tty.tty {
        if !io::stdin().is_terminal() {
            eprintln!("error: standard input is not a terminal");
            return Ok(ExitCode::FAILURE);
        }
        if !io::stdout().is_terminal() {
            eprintln!("error: standard output is not a terminal");
            return Ok(ExitCode::FAILURE);
        }
    }

    let bg_proc = ClientBgProcess::new_from_fork(config.log_level)?;

    log::run_with_logger(config.log_level, |log| {
        let fs = Fs::new();
        let reader: Box<dyn Read> = match extra_options.file {
            Some(path) => Box::new(fs.open_file(path)?),
            None => Box::new(io::stdin().lock()),
        };
        let tracker = Arc::new(JobTracker::default());
        fs.create_dir_all(&config.cache_root)?;
        fs.create_dir_all(&config.state_root)?;
        fs.create_dir_all(&config.container_image_depot_root)?;
        let client = Client::new(
            bg_proc,
            config.broker,
            Root::<ProjectDir>::new(".".as_ref()),
            config.state_root,
            config.container_image_depot_root,
            config.cache_root,
            config.cache_size,
            config.inline_limit,
            config.slots,
            log,
        )?;
        let mut job_specs = job_spec_iter_from_reader(reader, |layer| client.add_layer(layer));
        if extra_options.one_or_tty.any() {
            let tracker = tracker.clone();
            tracker.add_outstanding();
            let mut job_spec = job_specs
                .next()
                .ok_or_else(|| anyhow!("no job specification provided"))??;
            drop(job_specs);
            match &mem::take(&mut extra_options.args)[..] {
                [] => {}
                [program, arguments @ ..] => {
                    job_spec.program = program.into();
                    job_spec.arguments = arguments.to_vec();
                }
            }
            if extra_options.one_or_tty.tty {
                let (rows, columns) = linux::ioctl_tiocgwinsz(Fd::STDIN)?;
                let sock =
                    linux::socket(SocketDomain::UNIX, SocketType::STREAM, Default::default())?;
                linux::bind(sock.as_fd(), &SockaddrUnStorage::new_autobind())?;
                linux::listen(sock.as_fd(), 1)?;
                let sockaddr = linux::getsockname(sock.as_fd())?;
                let sockaddr = sockaddr
                    .as_sockaddr_un()
                    .ok_or_else(|| anyhow!("socket is not a unix domain socket"))?;
                job_spec.allocate_tty = Some(JobTty::new(
                    sockaddr.path().try_into()?,
                    WindowSize::new(rows, columns),
                ));
                client.add_job(job_spec, move |res| visitor(res, tracker))?;
                let listener = UnixListener::from(OwnedFd::from(sock));
                println!("waiting for job to start");
                thread::spawn(move || {
                    let Ok((mut sock, _)) = listener.accept() else {
                        return;
                    };
                    let Ok(mut sock_clone) = sock.try_clone() else {
                        return;
                    };
                    println!("job started, going into raw mode");
                    crossterm::terminal::enable_raw_mode().unwrap();
                    thread::spawn(move || {
                        let mut buf = [0u8; 100];
                        loop {
                            match sock_clone.read(&mut buf) {
                                Err(_) | Ok(0) => {
                                    break;
                                }
                                Ok(n) => {
                                    let _ = io::stdout().write(&buf[0..n]);
                                    let _ = io::stdout().flush();
                                }
                            }
                        }
                    });
                    let _ = io::copy(&mut io::stdin(), &mut sock);
                });
            } else {
                client.add_job(job_spec, move |res| visitor(res, tracker))?;
            }
        } else {
            for job_spec in job_specs {
                let tracker = tracker.clone();
                tracker.add_outstanding();
                client.add_job(job_spec?, move |res| visitor(res, tracker))?;
            }
        }
        tracker.wait_for_outstanding();
        crossterm::terminal::disable_raw_mode().unwrap();
        Ok(tracker.accum.get())
    })
}
