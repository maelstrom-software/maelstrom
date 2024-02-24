use anyhow::{Context as _, Result};
use clap::Parser;
use figment::{
    error::Kind,
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use indicatif::ProgressBar;
use maelstrom_base::{
    ClientJobId, JobEffects, JobError, JobOutcome, JobOutcomeResult, JobOutputResult, JobStatus,
};
use maelstrom_client::{
    spec::{std_env_lookup, ImageConfig},
    Client, ClientBgProcess,
};
use maelstrom_run::spec::job_spec_iter_from_reader;
use maelstrom_util::{
    config::BrokerAddr,
    process::{ExitCode, ExitCodeAccumulator},
};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{
    cell::RefCell,
    io::{self, Read, Write as _},
    path::PathBuf,
    sync::Arc,
};
use xdg::BaseDirectories;

/// The maelstrom run client. This process sends jobs to the broker to be executed.
#[derive(Parser)]
#[command(
    after_help = r#"Configuration values can be specified in three ways: fields in a config file, environment variables, or command-line options. Command-line options have the highest precendence, followed by environment variables.

The configuration value 'config_value' would be set via the '--config-value' command-line option, the MAELSTROM_RUN_CONFIG_VALUE environment variable, and the 'config_value' key in a configuration file.

All values except for 'broker' have reasonable defaults.
"#
)]
#[command(version)]
#[command(styles = maelstrom_util::clap::styles())]
struct CliOptions {
    /// Configuration file. Values set in the configuration file will be overridden by values set
    /// through environment variables and values set on the command line.
    #[arg(
        long,
        short = 'c',
        value_name="PATH",
        default_value = PathBuf::from(".config/maelstrom-run.toml").into_os_string()
    )]
    config_file: PathBuf,

    /// Print configuration and exit.
    #[arg(long, short = 'P')]
    print_config: bool,

    /// Socket address of broker. Examples: "[::]:5000", "host.example.com:2000".
    #[arg(long, short = 'b', value_name = "SOCKADDR")]
    broker: Option<String>,
}

impl CliOptions {
    fn to_config_options(&self) -> ConfigOptions {
        ConfigOptions {
            broker: self.broker.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Socket address of broker.
    pub broker: BrokerAddr,
}

#[skip_serializing_none]
#[derive(Default, Serialize)]
pub struct ConfigOptions {
    pub broker: Option<String>,
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
    let bg_proc = ClientBgProcess::new_from_fork()?;

    let cli_options = CliOptions::parse();
    let print_config = cli_options.print_config;
    let config: Config = Figment::new()
        .merge(Serialized::defaults(ConfigOptions::default()))
        .merge(Toml::file(&cli_options.config_file))
        .merge(Env::prefixed("MAELSTROM_CLIENT_"))
        .merge(Serialized::globals(cli_options.to_config_options()))
        .extract()
        .map_err(|mut e| {
            if let Kind::MissingField(field) = &e.kind {
                e.kind = Kind::Message(format!("configuration value \"{field}\" was no provided"));
                e
            } else {
                e
            }
        })
        .context("reading configuration")?;

    if print_config {
        println!("{config:#?}");
        return Ok(ExitCode::SUCCESS);
    }
    let accum = Arc::new(ExitCodeAccumulator::default());
    let client = Client::new(bg_proc, Default::default(), config.broker, ".", cache_dir())?;
    let client = RefCell::new(client);
    let reader: Box<dyn Read> = Box::new(io::stdin().lock());
    let image_lookup = |image: &str| {
        let (image, version) = image.split_once(':').unwrap_or((image, "latest"));
        let prog = ProgressBar::hidden();
        let mut client = client.borrow_mut();
        let image = client.get_container_image(image, version, prog)?;
        Ok(ImageConfig {
            layers: image.layers.clone(),
            environment: image.env().cloned(),
            working_directory: image.working_dir().map(From::from),
        })
    };
    let job_specs = job_spec_iter_from_reader(
        reader,
        |layer| client.borrow_mut().add_layer(layer),
        std_env_lookup,
        image_lookup,
    );
    for job_spec in job_specs {
        let accum_clone = accum.clone();
        client.borrow_mut().add_job(
            job_spec?,
            Box::new(move |cjid, result| visitor(cjid, result, accum_clone)),
        )?;
    }
    client.into_inner().wait_for_outstanding_jobs()?;
    Ok(accum.get())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    CliOptions::command().debug_assert()
}
