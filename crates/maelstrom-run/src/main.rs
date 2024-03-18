use anyhow::{Context as _, Result};
use clap::command;
use indicatif::ProgressBar;
use maelstrom_base::{
    ClientJobId, JobEffects, JobError, JobOutcome, JobOutcomeResult, JobOutputResult, JobStatus,
};
use maelstrom_client::{
    spec::{std_env_lookup, ImageConfig},
    Client, ClientBgProcess,
};
use maelstrom_config::{AsCommandLineOptions, ConfigBuilder};
use maelstrom_run::spec::job_spec_iter_from_reader;
use maelstrom_util::{
    config::{BrokerAddr, LogLevel},
    process::{ExitCode, ExitCodeAccumulator},
};
use slog::Drain as _;
use std::{
    io::{self, Read, Write as _},
    path::PathBuf,
    sync::Arc,
};
use xdg::BaseDirectories;

#[derive(Debug)]
pub struct Config {
    /// Socket address of broker.
    pub broker: BrokerAddr,
    /// Minimum log level to output.
    pub log_level: LogLevel,
}

impl AsCommandLineOptions for Config {
    fn as_command_line_options(builder: ConfigBuilder) -> ConfigBuilder {
        builder
            .value(
                "broker",
                'b',
                "SOCKADDR",
                None,
                r#"Socket address of broker. Examples: "[::]:5000", "host.example.com:2000"."#,
            )
            .value(
                "log_level",
                'l',
                "LEVEL",
                Some("info".to_string()),
                "Minimum log level to output.",
            )
    }
}

impl maelstrom_config::Config for Config {
    fn from_config_bag(config: &mut maelstrom_config::ConfigBag) -> Result<Self> {
        Ok(Self {
            broker: config.get("broker")?,
            log_level: config.get_or("log-level", LogLevel::Info)?,
        })
    }
}

fn print_effects(cjid: ClientJobId, JobEffects { stdout, stderr }: JobEffects) -> Result<()> {
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

fn visitor(cjid: ClientJobId, result: JobOutcomeResult, accum: Arc<ExitCodeAccumulator>) {
    match result {
        Ok(JobOutcome::Completed { status, effects }) => {
            print_effects(cjid, effects).ok();
            match status {
                JobStatus::Exited(0) => {}
                JobStatus::Exited(code) => {
                    io::stdout().lock().flush().ok();
                    eprintln!("job {cjid}: exited with code {code}");
                    accum.add(ExitCode::from(code));
                }
                JobStatus::Signaled(signum) => {
                    io::stdout().lock().flush().ok();
                    eprintln!("job {cjid}: killed by signal {signum}");
                    accum.add(ExitCode::FAILURE);
                }
            };
        }
        Ok(JobOutcome::TimedOut(effects)) => {
            print_effects(cjid, effects).ok();
            io::stdout().lock().flush().ok();
            eprintln!("job {cjid}: timed out");
            accum.add(ExitCode::FAILURE);
        }
        Err(JobError::Execution(err)) => {
            eprintln!("job {cjid}: execution error: {err}");
            accum.add(ExitCode::FAILURE);
        }
        Err(JobError::System(err)) => {
            eprintln!("job {cjid}: system error: {err}");
            accum.add(ExitCode::FAILURE);
        }
    }
}

fn cache_dir() -> PathBuf {
    BaseDirectories::with_prefix("maelstrom/client")
        .expect("failed to find cache dir")
        .get_cache_file("")
}

fn main() -> Result<ExitCode> {
    let base_directories =
        BaseDirectories::with_prefix("maelstrom/run").context("searching for config files")?;
    let env_var_prefix = "MAELSTROM_RUN";
    let config =
        maelstrom_config::new_config::<Config>(command!(), &base_directories, env_var_prefix)?;

    let bg_proc = ClientBgProcess::new_from_fork()?;

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = slog::LevelFilter::new(drain, config.log_level.as_slog_level()).fuse();
    let log = slog::Logger::root(drain, slog::o!());

    let accum = Arc::new(ExitCodeAccumulator::default());
    let client = Client::new(
        bg_proc,
        Default::default(),
        config.broker,
        ".",
        cache_dir(),
        log,
    )?;
    let reader: Box<dyn Read> = Box::new(io::stdin().lock());
    let image_lookup = |image: &str| {
        let (image, version) = image.split_once(':').unwrap_or((image, "latest"));
        let prog = ProgressBar::hidden();
        let image = client.get_container_image(image, version, prog)?;
        Ok(ImageConfig {
            layers: image.layers.clone(),
            environment: image.env().cloned(),
            working_directory: image.working_dir().map(From::from),
        })
    };
    let job_specs = job_spec_iter_from_reader(
        reader,
        |layer| client.add_layer(layer),
        std_env_lookup,
        image_lookup,
    );
    for job_spec in job_specs {
        let accum_clone = accum.clone();
        client.add_job(
            job_spec?,
            Box::new(move |cjid, result| visitor(cjid, result, accum_clone)),
        )?;
    }
    client.wait_for_outstanding_jobs()?;
    Ok(accum.get())
}
