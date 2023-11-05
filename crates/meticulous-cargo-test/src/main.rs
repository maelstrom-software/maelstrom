use anyhow::{Error, Result};
use cargo_metadata::{
    Artifact as CargoArtifact, Message as CargoMessage, MessageIter as CargoMessageIter,
};
use clap::Parser;
use colored::{ColoredString, Colorize as _};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use meticulous_base::{
    stats::JobState, ClientJobId, JobDetails, JobOutputResult, JobResult, JobStatus,
};
use meticulous_client::Client;
use meticulous_util::process::{ExitCode, ExitCodeAccumulator};
use regex::Regex;
use std::io::IsTerminal as _;
use std::{
    collections::HashMap,
    io::{self, BufReader},
    net::{SocketAddr, ToSocketAddrs as _},
    process::{Child, ChildStdout, Command, Stdio},
    str,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use unicode_truncate::UnicodeTruncateStr as _;
use unicode_width::UnicodeWidthStr as _;

fn parse_socket_addr(arg: &str) -> io::Result<SocketAddr> {
    let addrs: Vec<SocketAddr> = arg.to_socket_addrs()?.collect();
    // It's not clear how we could end up with an empty iterator. We'll assume
    // that's impossible until proven wrong.
    Ok(*addrs.get(0).unwrap())
}

/// The meticulous client. This process sends work to the broker to be executed by workers.
#[derive(Parser)]
#[command(version, bin_name = "cargo")]
struct Cli {
    #[clap(subcommand)]
    subcommand: Subcommand,
}

impl Cli {
    fn subcommand(self) -> MetestCli {
        match self.subcommand {
            Subcommand::Metest(cmd) => cmd,
        }
    }
}

#[derive(Debug, clap::Args)]
struct MetestCli {
    /// Socket address of broker. Examples: 127.0.0.1:5000 host.example.com:2000".
    #[arg(value_parser = parse_socket_addr)]
    broker: SocketAddr,
    #[arg(short, long)]
    quiet: bool,
}

#[derive(Debug, clap::Subcommand)]
enum Subcommand {
    Metest(MetestCli),
}

struct CargoBuild {
    child: Child,
}

impl CargoBuild {
    fn new() -> Result<Self> {
        let color = std::io::stderr().is_terminal();
        let child = Command::new("cargo")
            .arg("test")
            .arg("--no-run")
            .arg("--message-format=json-render-diagnostics")
            .arg(&format!(
                "--color={}",
                if color { "always" } else { "never" }
            ))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        Ok(Self { child })
    }

    fn artifact_stream(&mut self) -> TestArtifactStream {
        TestArtifactStream {
            stream: CargoMessage::parse_stream(BufReader::new(self.child.stdout.take().unwrap())),
        }
    }

    fn check_status(mut self) -> Result<()> {
        let exit_status = self.child.wait()?;
        if !exit_status.success() {
            std::io::copy(
                self.child.stderr.as_mut().unwrap(),
                &mut std::io::stderr().lock(),
            )?;
            return Err(Error::msg("build failure".to_string()));
        }

        Ok(())
    }
}

struct TestArtifactStream {
    stream: CargoMessageIter<BufReader<ChildStdout>>,
}

impl Iterator for TestArtifactStream {
    type Item = Result<CargoArtifact>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.stream.next()? {
                Err(e) => return Some(Err(e.into())),
                Ok(CargoMessage::CompilerArtifact(artifact)) => {
                    if artifact.executable.is_some() && artifact.profile.test {
                        return Some(Ok(artifact));
                    }
                }
                _ => continue,
            }
        }
    }
}

fn get_cases_from_binary(binary: &str) -> Result<Vec<String>> {
    let output = Command::new(binary)
        .arg("--list")
        .arg("--format")
        .arg("terse")
        .output()?;
    Ok(Regex::new(r"\b([^ ]*): test")?
        .captures_iter(str::from_utf8(&output.stdout)?)
        .map(|capture| capture.get(1).unwrap().as_str().trim().to_string())
        .collect())
}

trait ProgressIndicatorScope: Clone + Send + Sync + 'static {
    /// Prints a line to stdout while not interfering with any progress bars
    fn println(&self, msg: String);

    /// Prints a line to stdout while not interfering with any progress bars and indicating it was
    /// stderr
    fn eprintln(&self, msg: impl AsRef<str>) {
        for line in msg.as_ref().lines() {
            self.println(format!("{} {line}", "stderr:".red()))
        }
    }

    /// Meant to be called with the job is complete, it updates the complete bar with this status
    fn job_finished(&self);

    /// Update the number of pending jobs indicated
    fn update_length(&self, new_length: u64);
}

trait ProgressIndicator {
    type Scope: ProgressIndicatorScope;

    /// Potentially runs background thread while body is running allowing implementations to update
    /// progress in the background.
    fn run(
        &self,
        client: Mutex<Client>,
        body: impl FnOnce(&Mutex<Client>, &Self::Scope) -> Result<()>,
    ) -> Result<()>;
}

#[derive(Default)]
struct JobStatusTracker {
    statuses: Mutex<Vec<(String, ExitCode)>>,
    exit_code: ExitCodeAccumulator,
}

impl JobStatusTracker {
    fn job_exited(&self, case: String, exit_code: ExitCode) {
        let mut statuses = self.statuses.lock().unwrap();
        statuses.push((case, exit_code));
        self.exit_code.add(exit_code);
    }

    fn print_summary(&self, width: Option<usize>) {
        println!();
        let width = width.unwrap_or(80);
        let heading = " Test Summary ";
        let equal_width = (width - heading.width()) / 2;
        println!(
            "{empty:=<equal_width$}{heading}{empty:=<equal_width$}",
            empty = ""
        );

        let success = "Successful Tests";
        let failure = "Failed Tests";
        let column1_width = std::cmp::max(success.width(), failure.width());
        let max_digits = 9;
        let statuses = self.statuses.lock().unwrap();
        let failed = statuses
            .iter()
            .filter(|(_, exit_code)| exit_code != &ExitCode::SUCCESS);
        let num_failed = failed.clone().count();
        let num_succeeded = statuses.len() - num_failed;

        println!(
            "{:<column1_width$}: {num_succeeded:>max_digits$}",
            success.green(),
        );
        println!(
            "{:<column1_width$}: {num_failed:>max_digits$}",
            failure.red(),
        );
        let failed_width = failed.clone().map(|(n, _)| n.width()).max().unwrap_or(0);
        for (failed, _) in failed {
            println!("    {failed:<failed_width$}: {}", "failure".red());
        }
    }
}

struct JobStatusVisitor<ProgressIndicatorT> {
    tracker: Arc<JobStatusTracker>,
    case: String,
    width: Option<usize>,
    ind: ProgressIndicatorT,
}

impl<ProgressIndicatorT> JobStatusVisitor<ProgressIndicatorT> {
    fn new(
        tracker: Arc<JobStatusTracker>,
        case: String,
        width: Option<usize>,
        ind: ProgressIndicatorT,
    ) -> Self {
        Self {
            tracker,
            case,
            width,
            ind,
        }
    }
}

impl<ProgressIndicatorT: ProgressIndicatorScope> JobStatusVisitor<ProgressIndicatorT> {
    fn job_finished(&self, cjid: ClientJobId, result: JobResult) -> Result<()> {
        let result_str: ColoredString;
        let mut result_details: Option<String> = None;
        match result {
            JobResult::Ran { status, stderr, .. } => {
                match stderr {
                    JobOutputResult::None => {}
                    JobOutputResult::Inline(bytes) => {
                        self.ind.eprintln(String::from_utf8_lossy(&bytes));
                    }
                    JobOutputResult::Truncated { first, truncated } => {
                        self.ind.eprintln(String::from_utf8_lossy(&first));
                        self.ind.eprintln(format!(
                            "job {cjid}: stderr truncated, {truncated} bytes lost"
                        ));
                    }
                }
                match status {
                    JobStatus::Exited(code) => {
                        result_str = if code == 0 {
                            "OK".green()
                        } else {
                            "FAIL".red()
                        };
                        self.tracker
                            .job_exited(self.case.clone(), ExitCode::from(code));
                    }
                    JobStatus::Signalled(signo) => {
                        result_str = "FAIL".red();
                        result_details = Some(format!("killed by signal {signo}"));
                        self.tracker
                            .job_exited(self.case.clone(), ExitCode::FAILURE);
                    }
                };
            }
            JobResult::ExecutionError(err) => {
                result_str = "ERR".yellow();
                result_details = Some(format!("execution error: {err}"));
                self.tracker
                    .job_exited(self.case.clone(), ExitCode::FAILURE);
            }
            JobResult::SystemError(err) => {
                result_str = "ERR".yellow();
                result_details = Some(format!("system error: {err}"));
                self.tracker
                    .job_exited(self.case.clone(), ExitCode::FAILURE);
            }
        }
        match self.width {
            Some(width) if width > 10 => {
                let case_width = self.case.width();
                let result_width = result_str.width();
                if case_width + result_width < width {
                    let dots_width = width - result_width - case_width;
                    let case = self.case.bold();
                    self.ind.println(format!(
                        "{case}{empty:.<dots_width$}{result_str}",
                        empty = ""
                    ));
                } else {
                    let (case, case_width) =
                        self.case.unicode_truncate_start(width - 2 - result_width);
                    let case = case.bold();
                    let dots_width = width - result_width - case_width - 1;
                    self.ind.println(format!(
                        "<{case}{empty:.<dots_width$}{result_str}",
                        empty = ""
                    ));
                }
            }
            _ => {
                self.ind
                    .println(format!("{case} {result_str}", case = self.case));
            }
        }
        if let Some(details_str) = result_details {
            self.ind.println(details_str);
        }
        self.ind.job_finished();
        Ok(())
    }
}

//                      waiting for artifacts, pending, running, complete
const COLORS: [&str; 4] = ["red", "yellow", "blue", "green"];

fn make_progress_bar(color: &str, message: impl Into<String>, msg_len: usize) -> ProgressBar {
    ProgressBar::new(0).with_message(message.into()).with_style(
        ProgressStyle::with_template(&format!(
            "{{wide_bar:.{color}}} {{pos}}/{{len}} {{msg:{msg_len}}}"
        ))
        .unwrap()
        .progress_chars("##-"),
    )
}

struct MultipleProgressBars {
    scope: ProgressBarsScope,
    done_queuing_jobs: AtomicBool,
    build_spinner: ProgressBar,
}

impl MultipleProgressBars {
    fn new() -> Self {
        let multi_bar = MultiProgress::new();
        let build_spinner =
            multi_bar.add(ProgressBar::new_spinner().with_message("building artifacts..."));
        build_spinner.enable_steady_tick(Duration::from_millis(500));

        let mut bars = HashMap::new();
        for (state, color) in JobState::iter().zip(COLORS) {
            let bar = multi_bar.add(make_progress_bar(color, state.to_string(), 21));
            bars.insert(state, bar);
        }
        multi_bar.set_draw_target(ProgressDrawTarget::stdout());
        Self {
            scope: ProgressBarsScope { bars },
            build_spinner,
            done_queuing_jobs: AtomicBool::new(false),
        }
    }

    fn update_progress(&self, client: &Mutex<Client>) -> Result<()> {
        while !self.finished() {
            let counts = client.lock().unwrap().get_job_state_counts()?;
            for state in JobState::iter().filter(|s| s != &JobState::Complete) {
                let jobs = JobState::iter()
                    .filter(|s| s >= &state)
                    .map(|s| counts[s])
                    .sum();
                self.scope.bars.get(&state).unwrap().set_position(jobs);
            }
        }
        Ok(())
    }

    fn done_queuing_jobs(&self) {
        self.done_queuing_jobs.store(true, Ordering::Relaxed);

        self.build_spinner.disable_steady_tick();
        self.build_spinner.finish_and_clear();
    }

    fn finished(&self) -> bool {
        let com = self.scope.bars.get(&JobState::Complete).unwrap();
        self.done_queuing_jobs.load(Ordering::Relaxed) && com.position() >= com.length().unwrap()
    }
}

#[derive(Clone)]
struct ProgressBarsScope {
    bars: HashMap<JobState, ProgressBar>,
}

impl ProgressIndicatorScope for ProgressBarsScope {
    fn println(&self, msg: String) {
        let com = self.bars.get(&JobState::Complete).unwrap();
        com.println(msg);
    }

    fn job_finished(&self) {
        let com = self.bars.get(&JobState::Complete).unwrap();
        com.inc(1);
    }

    fn update_length(&self, new_length: u64) {
        for bar in self.bars.values() {
            bar.set_length(new_length);
        }
    }
}

impl ProgressIndicator for MultipleProgressBars {
    type Scope = ProgressBarsScope;

    fn run(
        &self,
        client: Mutex<Client>,
        body: impl FnOnce(&Mutex<Client>, &ProgressBarsScope) -> Result<()>,
    ) -> Result<()> {
        std::thread::scope(|scope| -> Result<()> {
            let update_thread = scope.spawn(|| self.update_progress(&client));
            let res = body(&client, &self.scope);
            self.done_queuing_jobs();

            res?;
            update_thread.join().unwrap()?;
            Ok(())
        })?;

        // not necessary, but might as well
        client.into_inner().unwrap().wait_for_outstanding_jobs()?;

        Ok(())
    }
}

#[derive(Clone)]
struct QuietProgressBar {
    bar: ProgressBar,
}

impl QuietProgressBar {
    fn new() -> Self {
        Self {
            bar: make_progress_bar("white", "jobs", 4),
        }
    }
}

impl ProgressIndicator for QuietProgressBar {
    type Scope = Self;

    fn run(
        &self,
        client: Mutex<Client>,
        body: impl FnOnce(&Mutex<Client>, &Self) -> Result<()>,
    ) -> Result<()> {
        let res = body(&client, self);
        client.into_inner().unwrap().wait_for_outstanding_jobs()?;
        res
    }
}

impl ProgressIndicatorScope for QuietProgressBar {
    fn println(&self, _msg: String) {
        // quiet mode doesn't print anything
    }

    fn job_finished(&self) {
        self.bar.inc(1);
    }

    fn update_length(&self, new_length: u64) {
        self.bar.set_length(new_length);
    }
}

#[derive(Clone)]
struct QuietNoBar;

impl QuietNoBar {
    fn new() -> Self {
        Self
    }
}

impl ProgressIndicator for QuietNoBar {
    type Scope = Self;

    fn run(
        &self,
        client: Mutex<Client>,
        body: impl FnOnce(&Mutex<Client>, &Self) -> Result<()>,
    ) -> Result<()> {
        let res = body(&client, self);
        client.into_inner().unwrap().wait_for_outstanding_jobs()?;
        println!("all jobs completed");
        res
    }
}

impl ProgressIndicatorScope for QuietNoBar {
    fn println(&self, _msg: String) {
        // quiet mode doesn't print anything
    }

    fn job_finished(&self) {
        // nothing to do
    }

    fn update_length(&self, _new_length: u64) {
        // nothing to do
    }
}

#[derive(Clone)]
struct NoBar;

impl NoBar {
    fn new() -> Self {
        Self
    }
}

impl ProgressIndicator for NoBar {
    type Scope = Self;

    fn run(
        &self,
        client: Mutex<Client>,
        body: impl FnOnce(&Mutex<Client>, &Self) -> Result<()>,
    ) -> Result<()> {
        let res = body(&client, self);
        client.into_inner().unwrap().wait_for_outstanding_jobs()?;
        println!("all jobs completed");
        res
    }
}

impl ProgressIndicatorScope for NoBar {
    fn println(&self, msg: String) {
        println!("{}", msg);
    }

    fn job_finished(&self) {
        // nothing to do
    }

    fn update_length(&self, _new_length: u64) {
        // nothing to do
    }
}

fn queue_jobs_and_wait<ProgressIndicatorT>(
    client: &Mutex<Client>,
    tracker: Arc<JobStatusTracker>,
    width: Option<usize>,
    ind: ProgressIndicatorT,
    mut cb: impl FnMut(u64),
) -> Result<()>
where
    ProgressIndicatorT: ProgressIndicatorScope,
{
    let mut total_jobs = 0;
    let mut cargo_build = CargoBuild::new()?;

    for artifact in cargo_build.artifact_stream() {
        let artifact = artifact?;
        let binary = artifact.executable.unwrap().to_string();
        let package_name = artifact.target.name;
        for case in get_cases_from_binary(&binary)? {
            total_jobs += 1;
            cb(total_jobs);

            let case_str = format!("{package_name} {case}");
            let visitor = JobStatusVisitor::new(tracker.clone(), case_str, width, ind.clone());
            client.lock().unwrap().add_job(
                JobDetails {
                    program: binary.clone(),
                    arguments: vec!["--exact".into(), "--nocapture".into(), case.clone()],
                    layers: vec![],
                },
                Box::new(move |cjid, result| visitor.job_finished(cjid, result)),
            );
        }
    }

    cargo_build.check_status()?;

    Ok(())
}

fn main_with_progress<ProgressIndicatorT: ProgressIndicator>(
    prog: ProgressIndicatorT,
    client: Mutex<Client>,
) -> Result<ExitCode> {
    let tracker = Arc::new(JobStatusTracker::default());
    let width = term_size::dimensions().map(|(w, _)| w);

    prog.run(client, |client, bar_scope| {
        queue_jobs_and_wait(
            client,
            tracker.clone(),
            width,
            bar_scope.clone(),
            |num_jobs| bar_scope.update_length(num_jobs),
        )
    })?;
    drop(prog);

    tracker.print_summary(width);
    Ok(tracker.exit_code.get())
}

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub fn main() -> Result<ExitCode> {
    let cli_options = Cli::parse().subcommand();
    let client = Mutex::new(Client::new(cli_options.broker)?);
    let is_tty = std::io::stdout().is_terminal();

    match (is_tty, cli_options.quiet) {
        (true, true) => Ok(main_with_progress(QuietProgressBar::new(), client)?),
        (true, false) => Ok(main_with_progress(MultipleProgressBars::new(), client)?),
        (false, true) => Ok(main_with_progress(QuietNoBar::new(), client)?),
        (false, false) => Ok(main_with_progress(NoBar::new(), client)?),
    }
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
