use anyhow::Result;
use clap::Parser;
use colored::{ColoredString, Colorize as _};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use meticulous_base::{
    stats::JobState, ClientJobId, JobDetails, JobOutputResult, JobResult, JobStatus,
};
use meticulous_client::Client;
use meticulous_util::process::{ExitCode, ExitCodeAccumulator};
use regex::Regex;
use std::{
    collections::HashMap,
    io::{self, Write as _},
    net::{SocketAddr, ToSocketAddrs as _},
    process::Command,
    str,
    sync::Arc,
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

fn get_test_binaries() -> Result<Vec<String>> {
    use cargo_metadata::Message;
    let output = Command::new("cargo")
        .arg("test")
        .arg("--no-run")
        .arg("--message-format=json-render-diagnostics")
        .output()?;
    let mut paths = vec![];
    for message in Message::parse_stream(std::io::BufReader::new(&output.stdout[..])) {
        match message? {
            Message::CompilerArtifact(artifact) => {
                if let Some(path) = artifact.executable {
                    if artifact.profile.test {
                        paths.push(path.to_string());
                    }
                }
            }
            _ => continue,
        }
    }
    Ok(paths)
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

fn visitor(
    cjid: ClientJobId,
    result: JobResult,
    accum: Arc<ExitCodeAccumulator>,
    case: String,
    width: Option<usize>,
    bar: ProgressBar,
) -> Result<()> {
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
                    accum.add(ExitCode::from(code));
                }
                JobStatus::Signalled(signo) => {
                    result_str = "FAIL".red();
                    result_details = Some(format!("killed by signal {signo}"));
                    accum.add(ExitCode::FAILURE);
                }
            };
        }
        JobResult::ExecutionError(err) => {
            result_str = "ERR".yellow();
            result_details = Some(format!("execution error: {err}"));
            accum.add(ExitCode::FAILURE);
        }
        JobResult::SystemError(err) => {
            result_str = "ERR".yellow();
            result_details = Some(format!("system error: {err}"));
            accum.add(ExitCode::FAILURE);
        }
    }
    match width {
        Some(width) if width > 10 => {
            let case_width = case.width();
            let result_width = result_str.width();
            if case_width + result_width < width {
                let dots_width = width - result_width - case_width;
                let case = case.bold();
                bar.println(format!(
                    "{case}{empty:.<dots_width$}{result_str}",
                    empty = ""
                ));
            } else {
                let (case, case_width) = case.unicode_truncate_start(width - 2 - result_width);
                let case = case.bold();
                let dots_width = width - result_width - case_width - 1;
                bar.println(format!(
                    "<{case}{empty:.<dots_width$}{result_str}",
                    empty = ""
                ));
            }
        }
        _ => {
            bar.println(format!("{case} {result_str}"));
        }
    }
    if let Some(details_str) = result_details {
        bar.println(format!("{details_str}"));
    }
    bar.inc(1);
    Ok(())
}

//                      waiting for artifacts, pending, running, complete
const COLORS: [&str; 4] = ["red", "yellow", "blue", "green"];

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub fn main() -> Result<ExitCode> {
    let cli_options = Cli::parse();
    let accum = Arc::new(ExitCodeAccumulator::default());
    let mut client = Client::new(cli_options.broker())?;
    let width = term_size::dimensions().map(|(w, _)| w);
    let mut cases = vec![];
    for binary in get_test_binaries()? {
        for case in get_cases_from_binary(&binary)? {
            cases.push((binary.clone(), case));
        }
    }
    let total_jobs = cases.len().try_into().unwrap();
    let multi_bar = MultiProgress::new();
    let mut bars = HashMap::new();
    for (state, color) in JobState::iter().zip(COLORS) {
        let bar = multi_bar.add(
            ProgressBar::new(total_jobs)
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

    for (binary, case) in cases.into_iter() {
        let accum_clone = accum.clone();
        let bar_clone = bars.get(&JobState::Complete).unwrap().clone();
        client.add_job(
            JobDetails {
                program: binary.clone(),
                arguments: vec![case.clone()],
                layers: vec![],
            },
            Box::new(move |cjid, result| {
                visitor(cjid, result, accum_clone, case, width, bar_clone)
            }),
        );
    }

    while bars[&JobState::Complete].position() < total_jobs {
        let counts = client.get_job_state_counts()?;
        for state in JobState::iter().filter(|s| s != &JobState::Complete) {
            let jobs = JobState::iter()
                .filter(|s| s >= &state)
                .map(|s| counts[s])
                .sum();
            bars.get(&state).unwrap().set_position(jobs);
        }
    }
    Ok(accum.get())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
