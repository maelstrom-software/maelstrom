use anyhow::{Context, Result};
use clap::Parser;
use figment::{
    error::Kind,
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use meticulous_base::{
    ClientJobId, JobDetails, JobError, JobOutputResult, JobResult, JobStatus, JobSuccess,
    Sha256Digest,
};
use meticulous_client::Client;
use meticulous_util::{
    config::BrokerAddr,
    process::{ExitCode, ExitCodeAccumulator},
};
use serde::{
    ser::{SerializeMap, Serializer},
    Deserialize, Serialize,
};
use serde_json::{self, Deserializer};
use std::{
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

impl Serialize for CliOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        // Don't serialize 'config_file'.
        // Don't serialize 'print_config'.
        if let Some(broker) = &self.broker {
            map.serialize_entry("broker", broker)?;
        }
        map.end()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Socket address of broker.
    pub broker: BrokerAddr,
}

#[derive(Deserialize, Debug)]
struct JobDescription {
    program: String,
    arguments: Option<Vec<String>>,
    environment: Option<Vec<String>>,
    layers: Option<Vec<String>>,
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

fn add_artifact(client: &mut Client, layer: &str) -> Result<Vec<Sha256Digest>> {
    let mut digests = vec![];
    if layer.starts_with("docker:") {
        let pkg = layer.split(':').nth(1).unwrap();
        digests.extend(client.add_container(pkg, "latest")?);
    } else {
        digests.push(client.add_artifact(Path::new(layer))?);
    }
    Ok(digests)
}

fn main() -> Result<ExitCode> {
    let cli_options = CliOptions::parse();
    let print_config = cli_options.print_config;
    let config: Config = Figment::new()
        .merge(Toml::file(&cli_options.config_file))
        .merge(Env::prefixed("METICULOUS_CLIENT_"))
        .merge(Serialized::globals(cli_options))
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
    let mut client = Client::new(config.broker.into_inner())?;
    let reader: Box<dyn Read> = Box::new(io::stdin().lock());
    let jobs = Deserializer::from_reader(reader).into_iter::<JobDescription>();
    for job in jobs {
        let job = job?;
        let mut layers = vec![];
        for layer in job.layers.unwrap_or(vec![]) {
            layers.extend(add_artifact(&mut client, &layer)?);
        }
        let accum_clone = accum.clone();
        client.add_job(
            JobDetails {
                program: job.program,
                arguments: job.arguments.unwrap_or(vec![]),
                environment: job.environment.unwrap_or(vec![]),
                layers,
            },
            Box::new(move |cjid, result| visitor(cjid, result, accum_clone)),
        );
    }
    client.wait_for_outstanding_jobs()?;
    Ok(accum.get())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    CliOptions::command().debug_assert()
}
