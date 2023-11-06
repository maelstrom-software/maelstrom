use anyhow::Result;
use cargo::{get_cases_from_binary, CargoBuild};
use clap::Parser;
use meticulous_base::JobDetails;
use meticulous_client::Client;
use meticulous_util::process::ExitCode;
use progress::{
    MultipleProgressBars, NoBar, ProgressIndicator, ProgressIndicatorScope, QuietNoBar,
    QuietProgressBar,
};
use std::io::IsTerminal as _;
use std::{
    io::{self},
    net::{SocketAddr, ToSocketAddrs as _},
    str,
    sync::{Arc, Mutex},
};
use visitor::{JobStatusTracker, JobStatusVisitor};

mod cargo;
mod progress;
mod visitor;

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

fn queue_jobs_and_wait<ProgressIndicatorT>(
    client: &Mutex<Client>,
    tracker: Arc<JobStatusTracker>,
    width: Option<usize>,
    ind: ProgressIndicatorT,
    stderr: impl io::Write,
    stderr_color: bool,
    mut cb: impl FnMut(u64),
) -> Result<()>
where
    ProgressIndicatorT: ProgressIndicatorScope,
{
    let mut total_jobs = 0;
    let mut cargo_build = CargoBuild::new(stderr_color)?;

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

    cargo_build.check_status(stderr)?;

    Ok(())
}

pub struct MainApp<StdErr> {
    client: Mutex<Client>,
    stderr: StdErr,
    stderr_color: bool,
}

impl<StdErr> MainApp<StdErr> {
    fn new(client: Mutex<Client>, stderr: StdErr, stderr_color: bool) -> Self {
        Self {
            client,
            stderr,
            stderr_color,
        }
    }
}

impl<StdErr: io::Write> MainApp<StdErr> {
    fn run_with_progress<ProgressIndicatorT: ProgressIndicator>(
        self,
        prog: ProgressIndicatorT,
    ) -> Result<ExitCode> {
        let tracker = Arc::new(JobStatusTracker::default());
        let width = term_size::dimensions().map(|(w, _)| w);

        prog.run(self.client, |client, bar_scope| {
            queue_jobs_and_wait(
                client,
                tracker.clone(),
                width,
                bar_scope.clone(),
                self.stderr,
                self.stderr_color,
                |num_jobs| bar_scope.update_length(num_jobs),
            )
        })?;

        tracker.print_summary(width);
        Ok(tracker.exit_code())
    }

    fn run(self, stdout_tty: bool, quiet: bool) -> Result<ExitCode> {
        match (stdout_tty, quiet) {
            (true, true) => Ok(self.run_with_progress(QuietProgressBar::new())?),
            (true, false) => Ok(self.run_with_progress(MultipleProgressBars::new())?),
            (false, true) => Ok(self.run_with_progress(QuietNoBar::new())?),
            (false, false) => Ok(self.run_with_progress(NoBar::new())?),
        }
    }
}

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub fn main() -> Result<ExitCode> {
    let cli_options = Cli::parse().subcommand();
    let client = Mutex::new(Client::new(cli_options.broker)?);
    let app = MainApp::new(
        client,
        std::io::stderr().lock(),
        std::io::stderr().is_terminal(),
    );

    let stdout_tty = std::io::stdout().is_terminal();
    app.run(stdout_tty, cli_options.quiet)
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
