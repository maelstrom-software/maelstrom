use anyhow::{anyhow, Error, Result};
use clap::Args;
use maelstrom_base::{
    tty, ClientJobId, JobCompleted, JobEffects, JobError, JobOutcome, JobOutcomeResult,
    JobOutputResult, JobStatus, JobTty, WindowSize,
};
use maelstrom_client::{
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, Client, ClientBgProcess,
    ContainerImageDepotDir, JobSpec, ProjectDir, StateDir,
};
use maelstrom_linux::{
    self as linux, Fd, Signal, SignalSet, SigprocmaskHow, SockaddrUnStorage, SocketDomain,
    SocketType,
};
use maelstrom_macro::Config;
use maelstrom_run::{
    escape::{self, EscapeChunk, EscapeChar},
    spec,
};
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots},
    fs::Fs,
    log,
    process::{ExitCode, ExitCodeAccumulator},
    root::{Root, RootBuf},
};
use slog::Logger;
use std::{
    env,
    io::{self, IsTerminal as _, Read, Stdin, Write as _},
    mem,
    net::Shutdown,
    os::{
        fd::OwnedFd,
        unix::net::{UnixListener, UnixStream},
    },
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
        Arc, Condvar, Mutex,
    },
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

    /// The escape character to use in TTY mode. Can be specified as a single character (e.g. "~"),
    /// using caret notation (e.g. "^C"), or as a hexidecimal escape (e.g. "\x1a").
    #[config(value_name = "CHARACTER", default = "EscapeChar::default()")]
    pub escape_char: EscapeChar,

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

    /// Whether to accept invalid TLS certificates when downloading container images.
    #[config(flag, value_name = "ACCEPT_INVALID_REMOTE_CONTAINER_TLS_CERTS")]
    pub accept_invalid_remote_container_tls_certs: AcceptInvalidRemoteContainerTlsCerts,
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
    cjid: Option<ClientJobId>,
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
            if let Some(cjid) = cjid {
                eprintln!("job {cjid}: stdout truncated, {truncated} bytes lost");
            } else {
                eprintln!("stdout truncated, {truncated} bytes lost");
            }
        }
    }
    match stderr {
        JobOutputResult::None => {}
        JobOutputResult::Inline(bytes) => {
            io::stderr().lock().write_all(&bytes)?;
        }
        JobOutputResult::Truncated { first, truncated } => {
            io::stderr().lock().write_all(&first)?;
            if let Some(cjid) = cjid {
                eprintln!("job {cjid}: stderr truncated, {truncated} bytes lost");
            } else {
                eprintln!("stderr truncated, {truncated} bytes lost");
            }
        }
    }
    Ok(())
}

fn visitor(res: Result<(ClientJobId, JobOutcomeResult)>, tracker: Arc<JobTracker>) {
    let exit_code = match res {
        Ok((cjid, Ok(JobOutcome::Completed(JobCompleted { status, effects })))) => {
            print_effects(Some(cjid), effects).ok();
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
            print_effects(Some(cjid), effects).ok();
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

fn mimic_child_death(res: JobOutcomeResult) -> Result<ExitCode> {
    Ok(match res {
        Ok(JobOutcome::Completed(JobCompleted { status, effects })) => {
            print_effects(None, effects)?;
            match status {
                JobStatus::Exited(code) => code.into(),
                JobStatus::Signaled(signo) => {
                    let _ = linux::raise(signo.into());
                    let _ = linux::raise(Signal::KILL);
                    unreachable!()
                }
            }
        }
        Ok(JobOutcome::TimedOut(effects)) => {
            print_effects(None, effects)?;
            io::stdout().lock().flush()?;
            eprintln!("timed out");
            ExitCode::FAILURE
        }
        Err(JobError::Execution(err)) => {
            eprintln!("execution error: {err}");
            ExitCode::FAILURE
        }
        Err(JobError::System(err)) => {
            eprintln!("system error: {err}");
            ExitCode::FAILURE
        }
    })
}

fn one_main(client: Client, job_spec: JobSpec) -> Result<ExitCode> {
    mimic_child_death(client.run_job(job_spec)?.1)
}

#[allow(clippy::large_enum_variant)]
enum TtyMainMessage {
    Error(Error),
    JobCompleted(JobOutcomeResult),
    JobConnected(UnixStream, UnixStream),
    JobOutput([u8; 1024], usize),
    Signal(Signal),
}

enum TtyJobInputMessage {
    Eof,
    Output([u8; 100], usize),
    Sigwinch,
}

/// The signal thread just loops waiting for signals, then sends them to the main thread. For this
/// to work, it's important that the relevant signals are blocked on all threads. To accomplish
/// this, we block them on the main thread before we become multi-threaded.
///
/// If the thread ever encounters an error writing to the sender, it exits, as it means the main
/// thread has gone away.
fn tty_signal_main(blocked_signals: SignalSet, sender: SyncSender<TtyMainMessage>) -> Result<()> {
    loop {
        match linux::sigwait(&blocked_signals) {
            Err(err) => {
                sender.send(TtyMainMessage::Error(Error::new(err).context("sigwait")))?;
                break Ok(());
            }
            Ok(signal) => {
                sender.send(TtyMainMessage::Signal(signal))?;
            }
        }
    }
}

/// The job thread just waits for notification from the client that the job is done. We don't use
/// add_job and a callback because writing to the sender may block, and we can't block the task
/// that is delivering the callback.
///
/// If the thread an error writing to the sender, it ignores it, as it means the main thread has
/// gone away.
fn tty_job_main(
    client: Client,
    job_spec: JobSpec,
    sender: SyncSender<TtyMainMessage>,
) -> Result<()> {
    sender
        .send(match client.run_job(job_spec) {
            Ok((_cjid, result)) => TtyMainMessage::JobCompleted(result),
            Err(err) => TtyMainMessage::Error(err.context("client error")),
        })
        .map_err(Error::new)
}

/// The listener thread listens on the socket for a connection, then sends the result to the main
/// thread.
///
/// If the thread an error writing to the sender, it ignores it, as it means the main thread has
/// gone away.
fn tty_listener_main(sock: linux::OwnedFd, sender: SyncSender<TtyMainMessage>) -> Result<()> {
    fn inner(sock: linux::OwnedFd) -> Result<(UnixStream, UnixStream)> {
        let listener = UnixListener::from(OwnedFd::from(sock));
        let sock = listener.accept()?.0;
        let sock_clone = sock.try_clone()?;
        Ok((sock, sock_clone))
    }
    sender
        .send(match inner(sock) {
            Ok((sock1, sock2)) => TtyMainMessage::JobConnected(sock1, sock2),
            Err(err) => TtyMainMessage::Error(err.context("job connecting")),
        })
        .map_err(Error::new)
}

/// The socket reader thread reads the jobs's stdout from the socket and sends the data to the main
/// thread. We want the main thread to write to stdout since it is coordinating raw mode, signal
/// handling, etc.
///
/// If the thread encounters an error reading from the socket, it forwards that error to the main
/// thread and exits. If the thread encounters EOF, it just exits, without telling the main
/// thread.
///
/// If the thread ever encounters an error writing to the sender, it exits, as it means the main
/// thread has gone away.
fn tty_socket_reader_main(sender: SyncSender<TtyMainMessage>, mut sock: UnixStream) -> Result<()> {
    let mut bytes = [0u8; 1024];
    loop {
        match sock.read(&mut bytes) {
            Err(err) => {
                sender.send(TtyMainMessage::Error(
                    Error::new(err).context("reading from job"),
                ))?;
                break Ok(());
            }
            Ok(0) => {
                break Ok(());
            }
            Ok(n) => {
                sender.send(TtyMainMessage::JobOutput(bytes, n))?;
            }
        }
    }
}

/// The stdin reader thread reads from stdin and sends the data to the socket writer thread. The
/// main thread can send window change events to the socket writer thread too, which is why the
/// stdin reader thread and socket writer thread are separate threads.
///
/// If the thread encounters an error reading from stdin, it forwards that error to the main
/// thread and exits. If the thread encounters EOF on stdin, it tells the socket writer thread (so
/// it can shutdown the socket) and then exits.
///
/// If the thread ever encounters an error writing to either sender, it exits, as it means the
/// receiving thread had gone away.
fn tty_stdin_reader_main(
    escape_char: EscapeChar,
    job_input_sender: SyncSender<TtyJobInputMessage>,
    sender: SyncSender<TtyMainMessage>,
    mut stdin: Stdin,
) -> Result<()> {
    let mut bytes = [0u8; 100];
    let mut leading_escape = false;
    loop {
        let mut offset = 0;
        if leading_escape {
            bytes[0] = escape_char.as_byte();
            offset = 1;
            leading_escape = false;
        }
        match stdin.read(&mut bytes[offset..]) {
            Err(err) => {
                sender.send(TtyMainMessage::Error(
                    Error::new(err).context("reading from stdin"),
                ))?;
                break Ok(());
            }
            Ok(0) => {
                if offset == 1 {
                    job_input_sender.send(TtyJobInputMessage::Output(bytes, 1))?;
                }
                job_input_sender.send(TtyJobInputMessage::Eof)?;
                break Ok(());
            }
            Ok(n) => {
                let n = offset + n;
                for chunk in escape::decode_escapes(&bytes[..n], escape_char.as_byte()) {
                    match chunk {
                        EscapeChunk::Bytes(bytes) => {
                            let mut copy = [0u8; 100];
                            let n = bytes.len();
                            copy[..n].copy_from_slice(bytes);
                            job_input_sender.send(TtyJobInputMessage::Output(copy, n))?;
                        }
                        EscapeChunk::ControlC => {
                            sender.send(TtyMainMessage::Signal(Signal::INT))?;
                        }
                        EscapeChunk::ControlZ => {
                            sender.send(TtyMainMessage::Signal(Signal::TSTP))?;
                        }
                        EscapeChunk::Remainder => {
                            leading_escape = true;
                        }
                    }
                }
            }
        }
    }
}

/// The socket writer thread reads from a channel and writes to the socket. The channel is mostly
/// written to by the stdin reader thread, but when sigwinches arrive, they are written to by the
/// main thread.
///
/// The protocol for notifying this thread of sigwinches is a little tricky and deserves some
/// explanation. We want the channel to be synchronous, since we want back-pressure on the stdin
/// reader thread. One could imagine the job falling far behind the stdin reader, and we wouldn't
/// want to fill up the queue indefinitely in that scenario. However, we don't want the main thread
/// to get blocked writing to the channel when it receives a sigwinch. So, the solution chosen here
/// is to make the channel synchronous, with a non-zero queue size (we choose 1 to minimize the
/// memory footprint), and then to use an atomic bool to signal a sigwinch has occurred. Whenever
/// the socket writer thread gets a message from the channel, it must first handly any sigwinch
/// that has occurred.
///
/// When the main thread sends a sigwinch, it writes to the atomic bool first, and then sends a
/// no-op message to potentially wake up the socket writer thread. The main thread doesn't block
/// writing to the channel: it doesn't care that it's message gets enqueued, it just cares that any
/// message is enqueued. The main thread only writes to the channel to handle the case where the
/// socket writer thread is blocked.
///
/// If the thread encounters an error writing to the socket, it ignores the error and exits.
/// Importantly, it doesn't forward the error to the main thread. The rationale is that when the
/// job exits, the socket will be closed, which means this thread would get an EPIPE when writing.
/// If this thread got the error before the job's status made it to the main thread, we'd print a
/// confusing error for the very normal situation of the job completing.
///
/// However, if the thread encounters an error getting the terminal's window size, it forwards it
/// to the main thread.
///
/// Finally, if the thread gets an Eof message from the stdin reader thread, it shutdowns the
/// socket and exits (again ignoring any error it may get calling shutdown).
fn tty_socket_writer_main(
    job_input_receiver: Receiver<TtyJobInputMessage>,
    sender: SyncSender<TtyMainMessage>,
    sigwinch_pending: Arc<AtomicBool>,
    mut sock: UnixStream,
) -> Result<()> {
    // We block sigpipe so we don't kill the process if the socket is closed out from under us. In
    // that case, we should just exit the thread.
    //
    // SIGPIPE is always delivered to the thread that generated it, so we just need to mask it
    // here.
    let mut sigpipe = SignalSet::empty();
    sigpipe.insert(Signal::PIPE);
    if let Err(err) = linux::pthread_sigmask(SigprocmaskHow::BLOCK, Some(&sigpipe)) {
        sender.send(TtyMainMessage::Error(
            Error::new(err).context("blocking SIGPIPE"),
        ))?;
        return Ok(());
    }

    loop {
        let message = job_input_receiver.recv()?;

        if sigwinch_pending.swap(false, Ordering::SeqCst) {
            match linux::ioctl_tiocgwinsz(Fd::STDIN) {
                Err(err) => {
                    sender.send(TtyMainMessage::Error(
                        Error::new(err).context("ioctl(TIOCGWINSZ) on stdin"),
                    ))?;
                    break Ok(());
                }
                Ok((rows, columns)) => {
                    sock.write_all(&tty::encode_window_size_change(WindowSize::new(
                        rows, columns,
                    )))?;
                }
            }
        }

        match message {
            TtyJobInputMessage::Eof => {
                sock.shutdown(Shutdown::Write)?;
                break Ok(());
            }
            TtyJobInputMessage::Output(bytes, n) => {
                for chunk in tty::encode_input(&bytes[..n]) {
                    sock.write_all(chunk)?;
                }
            }
            TtyJobInputMessage::Sigwinch => {}
        }
    }
}

#[derive(Default)]
struct RawModeKeeper(bool);

impl RawModeKeeper {
    fn enter(&mut self) -> Result<()> {
        if !self.0 {
            crossterm::terminal::enable_raw_mode()?;
            self.0 = true;
        }
        Ok(())
    }

    fn leave(&mut self) {
        if self.0 {
            let _ = crossterm::terminal::disable_raw_mode();
            self.0 = false;
        }
    }

    fn run_without_raw_mode(&mut self, f: impl FnOnce() -> Result<()>) -> Result<()> {
        let old = self.0;
        self.leave();
        f()?;
        if old {
            self.enter()?;
        }
        Ok(())
    }
}

impl Drop for RawModeKeeper {
    fn drop(&mut self) {
        if self.0 {
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }
}

fn tty_main(
    blocked_signals: SignalSet,
    client: Client,
    escape_char: EscapeChar,
    mut job_spec: JobSpec,
) -> Result<ExitCode> {
    let sock = linux::socket(SocketDomain::UNIX, SocketType::STREAM, Default::default())?;
    linux::bind(sock.as_fd(), &SockaddrUnStorage::new_autobind())?;
    linux::listen(sock.as_fd(), 1)?;
    let sockaddr = linux::getsockname(sock.as_fd())?;
    let sockaddr = sockaddr
        .as_sockaddr_un()
        .ok_or_else(|| anyhow!("socket is not a unix domain socket"))?;
    let (rows, columns) = linux::ioctl_tiocgwinsz(Fd::STDIN)?;
    job_spec.allocate_tty = Some(JobTty::new(
        sockaddr.path().try_into()?,
        WindowSize::new(rows, columns),
    ));

    println!("waiting for job to start");

    let (sender, receiver) = mpsc::sync_channel(0);

    let sender_clone = sender.clone();
    thread::spawn(move || tty_signal_main(blocked_signals, sender_clone));

    let sender_clone = sender.clone();
    thread::spawn(move || tty_job_main(client, job_spec, sender_clone));

    let sender_clone = sender.clone();
    thread::spawn(move || tty_listener_main(sock, sender_clone));

    let mut raw_mode_keeper = RawModeKeeper::default();
    let sigwinch_pending = Arc::new(AtomicBool::new(false));
    let (job_input_sender, job_input_receiver) = mpsc::sync_channel(1);
    let mut job_input_receiver = Some(job_input_receiver);
    let result = loop {
        match receiver.recv()? {
            TtyMainMessage::Error(err) => {
                break Err(err);
            }
            TtyMainMessage::JobCompleted(result) => {
                break Ok(result);
            }
            TtyMainMessage::JobConnected(sock1, sock2) => {
                println!("job started, going into raw mode");
                raw_mode_keeper.enter()?;

                let sender_clone = sender.clone();
                thread::spawn(move || tty_socket_reader_main(sender_clone, sock1));

                let job_input_sender_clone = job_input_sender.clone();
                let sender_clone = sender.clone();
                thread::spawn(move || {
                    tty_stdin_reader_main(
                        escape_char,
                        job_input_sender_clone,
                        sender_clone,
                        io::stdin(),
                    )
                });

                let sender_clone = sender.clone();
                let sigwinch_pending_clone = sigwinch_pending.clone();
                let job_input_receiver = job_input_receiver.take().unwrap();
                thread::spawn(move || {
                    tty_socket_writer_main(
                        job_input_receiver,
                        sender_clone,
                        sigwinch_pending_clone,
                        sock2,
                    )
                });
            }
            TtyMainMessage::JobOutput(bytes, n) => {
                io::stdout().write_all(&bytes[..n])?;
                io::stdout().flush()?;
            }
            TtyMainMessage::Signal(Signal::WINCH) => {
                sigwinch_pending.store(true, Ordering::SeqCst);
                let _ = job_input_sender.try_send(TtyJobInputMessage::Sigwinch);
            }
            TtyMainMessage::Signal(signal @ Signal::TSTP) => {
                raw_mode_keeper.run_without_raw_mode(|| {
                    let mut set = SignalSet::empty();
                    set.insert(signal);
                    linux::pthread_sigmask(SigprocmaskHow::UNBLOCK, Some(&set))?;
                    linux::raise(signal)?;
                    linux::pthread_sigmask(SigprocmaskHow::BLOCK, Some(&set))?;
                    Ok(())
                })?;
            }
            TtyMainMessage::Signal(signal) => {
                raw_mode_keeper.run_without_raw_mode(|| {
                    let mut to_unblock = SignalSet::empty();
                    to_unblock.insert(signal);
                    linux::pthread_sigmask(SigprocmaskHow::UNBLOCK, Some(&to_unblock))?;
                    linux::raise(signal)?;
                    panic!("should have been killed by signal {signal:?}");
                })?;
            }
        }
    };
    raw_mode_keeper.leave();
    mimic_child_death(result?)
}

fn main_with_logger(
    blocked_signals: SignalSet,
    config: Config,
    mut extra_options: ExtraCommandLineOptions,
    bg_proc: ClientBgProcess,
    log: Logger,
) -> Result<ExitCode> {
    let fs = Fs::new();
    let reader: Box<dyn Read> = match extra_options.file {
        Some(path) => Box::new(fs.open_file(path)?),
        None => Box::new(io::stdin().lock()),
    };
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
        config.accept_invalid_remote_container_tls_certs,
        log,
    )?;
    let mut job_specs = spec::job_spec_iter_from_reader(reader, |layer| client.add_layer(layer));
    if extra_options.one_or_tty.any() {
        if extra_options.one_or_tty.tty {
            // Unblock the signals for the local thread. We'll re-block them again once we've read
            // the job spec and started up.
            linux::pthread_sigmask(SigprocmaskHow::UNBLOCK, Some(&blocked_signals))?;
        }
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
            tty_main(blocked_signals, client, config.escape_char, job_spec)
        } else {
            // Re-block the signals for the local thread.
            linux::pthread_sigmask(SigprocmaskHow::BLOCK, Some(&blocked_signals))?;
            one_main(client, job_spec)
        }
    } else {
        let tracker = Arc::new(JobTracker::default());
        for job_spec in job_specs {
            let tracker = tracker.clone();
            tracker.add_outstanding();
            client.add_job(job_spec?, move |res| visitor(res, tracker))?;
        }
        tracker.wait_for_outstanding();
        Ok(tracker.accum.get())
    }
}

fn main() -> Result<ExitCode> {
    let (config, extra_options): (_, ExtraCommandLineOptions) =
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

    // We need to block the signals before we become multi-threaded, but after we've forked a
    // background process.
    let mut blocked_signals = SignalSet::empty();
    if extra_options.one_or_tty.tty {
        blocked_signals.insert(Signal::INT);
        blocked_signals.insert(Signal::TERM);
        blocked_signals.insert(Signal::TSTP);
        blocked_signals.insert(Signal::WINCH);
        linux::sigprocmask(SigprocmaskHow::BLOCK, Some(&blocked_signals))?;
    }

    log::run_with_logger(config.log_level, |log| {
        main_with_logger(blocked_signals, config, extra_options, bg_proc, log)
    })
}
