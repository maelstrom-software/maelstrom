use anyhow::Result;
use clap::Parser;
use figment::{
    error::Kind,
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{
    ser::{SerializeMap, Serializer},
    Deserialize, Serialize,
};
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// Socket address of broker.
    broker: meticulous_worker::config::Broker,

    /// Name of the worker provided to the broker.
    name: meticulous_worker::config::Name,

    /// The number of job slots available.
    slots: meticulous_worker::config::Slots,

    /// The directory to use for the cache.
    cache_root: meticulous_worker::config::CacheRoot,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative.
    cache_bytes_used_target: meticulous_worker::config::CacheBytesUsedTarget,
}

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
    #[arg(short = 'p', long)]
    print_config: bool,

    /// Socket address of broker. Examples: 127.0.0.1:5000 host.example.com:2000".
    #[arg(short = 'b', long)]
    broker: Option<String>,

    /// Name of the worker provided to the broker. The broker will reject workers with duplicate
    /// names.
    #[arg(short = 'n', long)]
    name: Option<String>,

    /// The number of job slots available. Most program jobs will take one job slot.
    #[arg(short = 's', long)]
    slots: Option<u32>,

    /// The directory to use for the cache.
    #[arg(short = 'r', long)]
    cache_root: Option<PathBuf>,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative.
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
        if let Some(cache_directory) = &self.cache_root {
            map.serialize_entry("cache_directory", cache_directory)?;
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
        })?;
    if print_config {
        println!("{config:#?}");
        return Ok(());
    }
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        meticulous_worker::main(
            config.name,
            config.slots,
            config.cache_root,
            config.cache_bytes_used_target,
            config.broker,
        )
        .await
    })?;
    Ok(())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    CliOptions::command().debug_assert()
}
