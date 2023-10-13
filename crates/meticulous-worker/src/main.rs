use anyhow::{Context, Result};
use clap::Parser;
use figment::{
    error::Kind,
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use meticulous_worker::config::Config;
use serde::{
    ser::{SerializeMap, Serializer},
    Serialize,
};
use slog::{o, Drain, Logger};
use slog_async::Async;
use slog_term::{CompactFormat, TermDecorator};
use std::path::PathBuf;
use tokio::runtime::Runtime;

/// The meticulous worker. This process executes jobs as directed by the broker.
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
    #[arg(short = 'c', long, default_value=PathBuf::from("meticulous-worker.toml").into_os_string())]
    config_file: PathBuf,

    /// Print configuration and exit
    #[arg(short = 'P', long)]
    print_config: bool,

    /// Socket address of broker. Examples: 127.0.0.1:5000 host.example.com:2000"
    #[arg(short = 'b', long)]
    broker: Option<String>,

    /// Name of the worker provided to the broker. The broker will reject workers with duplicate
    /// names.
    #[arg(short = 'n', long)]
    name: Option<String>,

    /// The number of job slots available. Most program jobs will take one job slot
    #[arg(short = 's', long)]
    slots: Option<u16>,

    /// The directory to use for the cache
    #[arg(short = 'r', long)]
    cache_root: Option<PathBuf>,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative
    #[arg(short = 'B', long)]
    cache_bytes_used_target: Option<u64>,
}

impl Default for CliOptions {
    fn default() -> Self {
        CliOptions {
            config_file: "".into(),
            print_config: false,
            broker: None,
            name: Some(gethostname::gethostname().into_string().unwrap()),
            slots: Some(num_cpus::get().try_into().unwrap()),
            cache_root: Some("var/cache/meticulous-worker".into()),
            cache_bytes_used_target: Some(100000000),
        }
    }
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
        if let Some(name) = &self.name {
            map.serialize_entry("name", name)?;
        }
        if let Some(slots) = &self.slots {
            map.serialize_entry("slots", slots)?;
        }
        if let Some(cache_root) = &self.cache_root {
            map.serialize_entry("cache_root", cache_root)?;
        }
        if let Some(cache_bytes_used_target) = &self.cache_bytes_used_target {
            map.serialize_entry("cache_bytes_used_target", cache_bytes_used_target)?;
        }
        map.end()
    }
}

fn main() -> Result<()> {
    let cli_options = CliOptions::parse();
    let print_config = cli_options.print_config;
    let config: Config = Figment::new()
        .merge(Serialized::defaults(CliOptions::default()))
        .merge(Toml::file(&cli_options.config_file))
        .merge(Env::prefixed("METICULOUS_WORKER_"))
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
        return Ok(());
    }
    let decorator = TermDecorator::new().build();
    let drain = CompactFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!());
    Runtime::new()
        .context("starting tokio runtime")?
        .block_on(async move { meticulous_worker::main(config, log).await })?;
    Ok(())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    CliOptions::command().debug_assert()
}
