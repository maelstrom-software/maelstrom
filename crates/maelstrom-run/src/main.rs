use anyhow::{Context as _, Result};
use clap::{command, Arg, ArgAction, ArgMatches, Command};
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
    config::{BrokerAddr, LogLevel},
    process::{ExitCode, ExitCodeAccumulator},
};
use slog::Drain as _;
use std::{
    env, fs,
    io::{self, Read, Write as _},
    path::PathBuf,
    process,
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

impl Config {
    pub fn add_command_line_options(command: Command) -> Command {
        command
        .styles(maelstrom_util::clap::styles())
        .after_help(
            "Configuration values can be specified in three ways: fields in a config file, \
            environment variables, or command-line options. Command-line options have the \
            highest precendence, followed by environment variables.\n\
            \n\
            The configuration value 'config_value' would be set via the '--config-value' \
            command-line option, the MAELSTROM_WORKER_CONFIG_VALUE environment variable, \
            and the 'config_value' key in a configuration file.\n\
            \n\
            All values except for 'broker' have reasonable defaults.")
        .arg(
            Arg::new("config-file")
                .long("config-file")
                .short('c')
                .value_name("PATH")
                .action(ArgAction::Set)
                .help(
                    "Configuration file. Values set in the configuration file will be overridden by \
                    values set through environment variables and values set on the command line"
                )
        )
        .arg(
            Arg::new("print-config")
                .long("print-config")
                .short('P')
                .action(ArgAction::SetTrue)
                .help("Print configuration and exit"),
        )
        .arg(
            Arg::new("broker")
                .long("broker")
                .short('b')
                .value_name("SOCKADDR")
                .action(ArgAction::Set)
                .help(r#"Socket address of broker. Examples: "[::]:5000", "host.example.com:2000""#)
        )
        .arg(
            Arg::new("log-level")
                .long("log-level")
                .short('l')
                .value_name("LEVEL")
                .action(ArgAction::Set)
                .help("Minimum log level to output")
        )
    }

    pub fn new(mut args: ArgMatches) -> Result<Self> {
        let env = env::vars().filter(|(key, _)| key.starts_with("MAELSTROM_RUN_"));

        let config_files = match args.remove_one::<String>("config-file").as_deref() {
            Some("-") => vec![],
            Some(config_file) => vec![PathBuf::from(config_file)],
            None => BaseDirectories::with_prefix("maelstrom/run")
                .context("searching for config files")?
                .find_config_files("config.toml")
                .rev()
                .collect(),
        };
        let mut files = vec![];
        for config_file in config_files {
            let contents = fs::read_to_string(&config_file).with_context(|| {
                format!("reading config file `{}`", config_file.to_string_lossy())
            })?;
            files.push((config_file.clone(), contents));
        }

        let print_config = args.remove_one::<bool>("print-config").unwrap();

        let config = maelstrom_config::Config::new(args, "MAELSTROM_RUN_", env, files)
            .context("loading configuration from environment variables and config files")?;

        let config = Self {
            broker: config.get("broker")?,
            log_level: config.get_or("log-level", LogLevel::Info)?,
        };

        if print_config {
            println!("{config:#?}");
            process::exit(0);
        }

        Ok(config)
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
    let args = Config::add_command_line_options(command!()).get_matches();
    let config = Config::new(args)?;

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
