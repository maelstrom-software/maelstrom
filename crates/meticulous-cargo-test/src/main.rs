use anyhow::Result;
use clap::Parser;
use colored::{ColoredString, Colorize as _};
use meticulous_base::{ClientJobId, JobDetails, JobOutputResult, JobResult, JobStatus};
use meticulous_client::Client;
use regex::Regex;
use std::{
    io::{self, Write as _},
    net::{SocketAddr, ToSocketAddrs as _},
    process::{Command, ExitCode},
    str,
    sync::{Arc, Mutex},
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
#[command(version)]
struct Cli {
    /// Socket address of broker. Examples: 127.0.0.1:5000 host.example.com:2000".
    #[arg(value_parser = parse_socket_addr)]
    broker: SocketAddr,
}

fn get_test_binaries() -> Result<Vec<String>> {
    let output = Command::new("cargo").arg("test").arg("--no-run").output()?;
    Ok(Regex::new(r"Executable unittests.*\((.*)\)")?
        .captures_iter(str::from_utf8(&output.stderr)?)
        .map(|capture| capture.get(1).unwrap().as_str().trim().to_string())
        .collect())
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

struct ExitCodeAccumulator(Mutex<Option<ExitCode>>);

impl Default for ExitCodeAccumulator {
    fn default() -> Self {
        ExitCodeAccumulator(Mutex::new(None))
    }
}

impl ExitCodeAccumulator {
    fn add(&self, code: ExitCode) {
        self.0.lock().unwrap().get_or_insert(code);
    }

    fn get(&self) -> ExitCode {
        self.0.lock().unwrap().unwrap_or(ExitCode::SUCCESS)
    }
}

fn visitor(
    cjid: ClientJobId,
    result: JobResult,
    accum: Arc<ExitCodeAccumulator>,
    case: String,
    width: Option<usize>,
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
                JobStatus::Exited(0) => {
                    result_str = "OK".green();
                }
                JobStatus::Exited(code) => {
                    result_str = "FAIL".red();
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
                println!("{case}{empty:.<dots_width$}{result_str}", empty = "");
            } else {
                let (case, case_width) = case.unicode_truncate_start(width - 2 - result_width);
                let case = case.bold();
                let dots_width = width - result_width - case_width - 1;
                println!("<{case}{empty:.<dots_width$}{result_str}", empty = "");
            }
        }
        _ => {
            println!("{case} {result_str}");
        }
    }
    if let Some(details_str) = result_details {
        println!("{details_str}");
    }
    Ok(())
}

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub fn main() -> Result<ExitCode> {
    let cli_options = Cli::parse();
    let accum = Arc::new(ExitCodeAccumulator::default());
    let mut client = Client::new(cli_options.broker)?;
    let width = term_size::dimensions().map(|(w, _)| w);
    for binary in get_test_binaries()? {
        for case in get_cases_from_binary(&binary)? {
            let accum_clone = accum.clone();
            client.add_job(
                JobDetails {
                    program: binary.clone(),
                    arguments: vec![case.clone()],
                    layers: vec![],
                },
                Box::new(move |cjid, result| visitor(cjid, result, accum_clone, case, width)),
            );
        }
    }
    client.wait_for_oustanding_jobs()?;
    Ok(accum.get())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
