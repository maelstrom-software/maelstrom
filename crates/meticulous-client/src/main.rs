use anyhow::{Context, Error, Result};
use clap::Parser;
use figment::{
    error::Kind,
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use indicatif::ProgressBar;
use meticulous_base::{
    ClientJobId, JobError, JobOutputResult, JobResult, JobStatus, JobSuccess, NonEmpty,
    Sha256Digest,
};
use meticulous_client::{
    spec::{self, ContainerImage},
    Client, DefaultClientDriver,
};
use meticulous_util::{
    config::BrokerAddr,
    process::{ExitCode, ExitCodeAccumulator},
};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{
    cell::RefCell,
    env::{self, VarError},
    io::{self, Read, Write as _},
    path::{Path, PathBuf},
    sync::Arc,
};

/// The meticulous client. This process sends jobs to the broker to be executed.
#[derive(Parser)]
#[command(
    after_help = r#"Configuration values can be specified in three ways: fields in a config file, environment variables, or command-line options. Command-line options have the highest precendence, followed by environment variables.

The configuration value 'config_value' would be set via the '--config-value' command-line option, the METICULOUS_WORKER_CONFIG_VALUE environment variable, and the 'config_value' key in a configuration file.

All values except for 'broker' have reasonable defaults.
"#
)]
#[command(version)]
struct CliOptions {
    /// Configuration file. Values set in the configuration file will be overridden by values set
    /// through environment variables and values set on the command line.
    #[arg(short = 'c', long, default_value=PathBuf::from(".config/meticulous-client.toml").into_os_string())]
    config_file: PathBuf,

    /// Print configuration and exit
    #[arg(short = 'P', long)]
    print_config: bool,

    /// Socket address of broker. Examples: 127.0.0.1:5000 host.example.com:2000"
    #[arg(short = 'b', long)]
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

fn visitor(cjid: ClientJobId, result: JobResult, accum: Arc<ExitCodeAccumulator>) -> Result<()> {
    match result {
        Ok(JobSuccess {
            status,
            stdout,
            stderr,
        }) => {
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
            match status {
                JobStatus::Exited(0) => {}
                JobStatus::Exited(code) => {
                    io::stdout().lock().flush()?;
                    eprintln!("job {cjid}: exited with code {code}");
                    accum.add(ExitCode::from(code));
                }
                JobStatus::Signaled(signum) => {
                    io::stdout().lock().flush()?;
                    eprintln!("job {cjid}: killed by signal {signum}");
                    accum.add(ExitCode::FAILURE);
                }
            };
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
    Ok(())
}

fn add_artifact(client: &mut Client, layer: &str) -> Result<NonEmpty<Sha256Digest>> {
    Ok(if layer.starts_with("docker:") {
        let pkg = layer.split(':').nth(1).unwrap();
        client.add_container(pkg, "latest", None)?
    } else {
        NonEmpty::singleton(client.add_artifact(Path::new(layer))?)
    })
}

fn cache_dir() -> PathBuf {
    directories::BaseDirs::new()
        .expect("failed to find cache dir")
        .cache_dir()
        .join("meticulous")
}

fn std_env_lookup(var: &str) -> Result<Option<String>> {
    match env::var(var) {
        Ok(val) => Ok(Some(val)),
        Err(VarError::NotPresent) => Ok(None),
        Err(err) => Err(Error::new(err)),
    }
}

fn main() -> Result<ExitCode> {
    let cli_options = CliOptions::parse();
    let print_config = cli_options.print_config;
    let config: Config = Figment::new()
        .merge(Serialized::defaults(ConfigOptions::default()))
        .merge(Toml::file(&cli_options.config_file))
        .merge(Env::prefixed("METICULOUS_CLIENT_"))
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
    let client = Client::new(
        DefaultClientDriver::default(),
        config.broker,
        ".",
        cache_dir(),
    )?;
    let client = RefCell::new(client);
    let reader: Box<dyn Read> = Box::new(io::stdin().lock());
    let image_lookup = |image: &str| {
        let (image, version) = image.split_once(':').unwrap_or((image, "latest"));
        let prog = ProgressBar::hidden();
        let mut client = client.borrow_mut();
        let container_image_depot = client.container_image_depot_mut();
        let image = container_image_depot.get_container_image(image, version, prog)?;
        Ok(ContainerImage {
            layers: image.layers.clone(),
            environment: image.env().cloned(),
            working_directory: image.working_dir().map(PathBuf::from),
        })
    };
    let job_specs = spec::job_spec_iter_from_reader(
        reader,
        |layer| add_artifact(&mut client.borrow_mut(), layer.as_str()),
        std_env_lookup,
        image_lookup,
    );
    for job_spec in job_specs {
        let accum_clone = accum.clone();
        client.borrow_mut().add_job(
            job_spec?,
            Box::new(move |cjid, result| visitor(cjid, result, accum_clone)),
        );
    }
    client.into_inner().wait_for_outstanding_jobs()?;
    Ok(accum.get())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    CliOptions::command().debug_assert()
}
