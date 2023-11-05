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
use std::{
    collections::HashMap,
    io::{self, BufReader, Write as _},
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
    fn broker(&self) -> SocketAddr {
        match &self.subcommand {
            Subcommand::Metest { broker } => *broker,
        }
    }
}

#[derive(Debug, clap::Subcommand)]
enum Subcommand {
    Metest {
        /// Socket address of broker. Examples: 127.0.0.1:5000 host.example.com:2000".
        #[arg(value_parser = parse_socket_addr)]
        broker: SocketAddr,
    },
}

struct CargoBuild {
    child: Child,
}

impl CargoBuild {
    fn new() -> Result<Self> {
        let child = Command::new("cargo")
            .arg("test")
            .arg("--no-run")
            .arg("--message-format=json-render-diagnostics")
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
            return Err(Error::msg(format!("build failure")));
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

struct JobStatusVisitor {
    accum: Arc<ExitCodeAccumulator>,
    case: String,
    width: Option<usize>,
    bar: ProgressBar,
}

impl JobStatusVisitor {
    fn new(
        accum: Arc<ExitCodeAccumulator>,
        case: String,
        width: Option<usize>,
        bar: ProgressBar,
    ) -> Self {
        Self {
            accum,
            case,
            width,
            bar,
        }
    }
}

impl JobStatusVisitor {
    fn job_finished(&self, cjid: ClientJobId, result: JobResult) -> Result<()> {
        let result_str: ColoredString;
        let mut result_details: Option<String> = None;
        match result {
            JobResult::Ran { status, stderr, .. } => {
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
                match status {
                    JobStatus::Exited(code) => {
                        result_str = if code == 0 {
                            "OK".green()
                        } else {
                            "FAIL".red()
                        };
                        self.accum.add(ExitCode::from(code));
                    }
                    JobStatus::Signalled(signo) => {
                        result_str = "FAIL".red();
                        result_details = Some(format!("killed by signal {signo}"));
                        self.accum.add(ExitCode::FAILURE);
                    }
                };
            }
            JobResult::ExecutionError(err) => {
                result_str = "ERR".yellow();
                result_details = Some(format!("execution error: {err}"));
                self.accum.add(ExitCode::FAILURE);
            }
            JobResult::SystemError(err) => {
                result_str = "ERR".yellow();
                result_details = Some(format!("system error: {err}"));
                self.accum.add(ExitCode::FAILURE);
            }
        }
        match self.width {
            Some(width) if width > 10 => {
                let case_width = self.case.width();
                let result_width = result_str.width();
                if case_width + result_width < width {
                    let dots_width = width - result_width - case_width;
                    let case = self.case.bold();
                    self.bar.println(format!(
                        "{case}{empty:.<dots_width$}{result_str}",
                        empty = ""
                    ));
                } else {
                    let (case, case_width) =
                        self.case.unicode_truncate_start(width - 2 - result_width);
                    let case = case.bold();
                    let dots_width = width - result_width - case_width - 1;
                    self.bar.println(format!(
                        "<{case}{empty:.<dots_width$}{result_str}",
                        empty = ""
                    ));
                }
            }
            _ => {
                self.bar
                    .println(format!("{case} {result_str}", case = self.case));
            }
        }
        if let Some(details_str) = result_details {
            self.bar.println(details_str);
        }
        self.bar.inc(1);
        Ok(())
    }
}

//                      waiting for artifacts, pending, running, complete
const COLORS: [&str; 4] = ["red", "yellow", "blue", "green"];

struct ProgressBars {
    _multi_bar: MultiProgress,
    bars: HashMap<JobState, ProgressBar>,
    done_queuing_jobs: AtomicBool,
    build_spinner: ProgressBar,
}

impl ProgressBars {
    fn new() -> Self {
        let multi_bar = MultiProgress::new();
        let build_spinner =
            multi_bar.add(ProgressBar::new_spinner().with_message("building artifacts..."));
        build_spinner.enable_steady_tick(Duration::from_millis(500));

        let mut bars = HashMap::new();
        for (state, color) in JobState::iter().zip(COLORS) {
            let bar = multi_bar.add(
                ProgressBar::new(0)
                    .with_message(state.to_string())
                    .with_style(
                        ProgressStyle::with_template(&format!(
                            "{{wide_bar:.{color}}} {{pos}}/{{len}} {{msg:21}}"
                        ))
                        .unwrap()
                        .progress_chars("##-"),
                    ),
            );
            bars.insert(state, bar);
        }
        multi_bar.set_draw_target(ProgressDrawTarget::stdout());
        Self {
            _multi_bar: multi_bar,
            bars,
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
                self.bars.get(&state).unwrap().set_position(jobs);
            }
        }
        Ok(())
    }

    fn update_length(&self, new_length: u64) {
        for bar in self.bars.values() {
            bar.set_length(new_length);
        }
    }

    fn finished(&self) -> bool {
        let com = self.bars.get(&JobState::Complete).unwrap();
        self.done_queuing_jobs.load(Ordering::Relaxed) && com.position() >= com.length().unwrap()
    }

    fn done_queuing_jobs(&self) {
        self.done_queuing_jobs.store(true, Ordering::Relaxed);

        self.build_spinner.disable_steady_tick();
        self.build_spinner.finish_and_clear();
    }
}

fn queue_jobs_and_wait(
    client: &Mutex<Client>,
    accum: Arc<ExitCodeAccumulator>,
    width: Option<usize>,
    bar: ProgressBar,
    mut cb: impl FnMut(u64),
) -> Result<()> {
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
            let visitor = JobStatusVisitor::new(accum.clone(), case_str, width, bar.clone());
            client.lock().unwrap().add_job(
                JobDetails {
                    program: binary.clone(),
                    arguments: vec![case.clone()],
                    layers: vec![],
                },
                Box::new(move |cjid, result| visitor.job_finished(cjid, result)),
            );
        }
    }

    cargo_build.check_status()?;

    Ok(())
}

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub fn main() -> Result<ExitCode> {
    let cli_options = Cli::parse();
    let accum = Arc::new(ExitCodeAccumulator::default());
    let client = Mutex::new(Client::new(cli_options.broker())?);
    let width = term_size::dimensions().map(|(w, _)| w);

    let bars = ProgressBars::new();
    let complete_bar = bars.bars.get(&JobState::Complete).unwrap().clone();
    std::thread::scope(|scope| -> Result<()> {
        let bars_thread = scope.spawn(|| bars.update_progress(&client));
        let res = queue_jobs_and_wait(&client, accum.clone(), width, complete_bar, |num_jobs| {
            bars.update_length(num_jobs)
        });
        bars.done_queuing_jobs();

        res?;
        bars_thread.join().unwrap()?;
        Ok(())
    })?;

    Ok(accum.get())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
