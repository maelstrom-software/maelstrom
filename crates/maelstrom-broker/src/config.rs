use anyhow::{Context as _, Result};
use clap::{Arg, ArgAction, ArgMatches, Command};
use derive_more::From;
use maelstrom_util::config::{CacheBytesUsedTarget, CacheRoot, LogLevel};
use serde::Deserialize;
use std::{
    env,
    fmt::{self, Debug, Formatter},
    fs,
    path::PathBuf,
    process, result,
    str::FromStr,
};
use xdg::BaseDirectories;

#[derive(Deserialize, From)]
#[serde(from = "u16")]
pub struct BrokerPort(u16);

impl BrokerPort {
    pub fn inner(&self) -> &u16 {
        &self.0
    }

    pub fn into_inner(self) -> u16 {
        self.0
    }
}

impl Debug for BrokerPort {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        Debug::fmt(&self.0, f)
    }
}

impl FromStr for BrokerPort {
    type Err = <u16 as FromStr>::Err;
    fn from_str(slots: &str) -> result::Result<Self, Self::Err> {
        Ok(Self::from(u16::from_str(slots)?))
    }
}

#[derive(Deserialize, From)]
#[serde(from = "u16")]
pub struct HttpPort(u16);

impl HttpPort {
    pub fn inner(&self) -> &u16 {
        &self.0
    }

    pub fn into_inner(self) -> u16 {
        self.0
    }
}

impl Debug for HttpPort {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        Debug::fmt(&self.0, f)
    }
}

impl FromStr for HttpPort {
    type Err = <u16 as FromStr>::Err;
    fn from_str(slots: &str) -> result::Result<Self, Self::Err> {
        Ok(Self::from(u16::from_str(slots)?))
    }
}

#[derive(Debug)]
pub struct Config {
    /// The port the broker listens for connections from workers and clients on
    pub port: BrokerPort,

    /// The port the HTTP UI is served up on
    pub http_port: HttpPort,

    /// The directory to use for the cache.
    pub cache_root: CacheRoot,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative.
    pub cache_bytes_used_target: CacheBytesUsedTarget,

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
            and the 'config_value' key in a configuration file.")
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
            Arg::new("port")
                .long("port")
                .short('p')
                .value_name("PORT")
                .action(ArgAction::Set)
                .help("The port the broker listens on for connections from workers and clients")
        )
        .arg(
            Arg::new("http-port")
                .long("http-port")
                .short('H')
                .value_name("PORT")
                .action(ArgAction::Set)
                .help("The port the HTTP UI is served on")
        )
        .arg(
            Arg::new("cache-root")
                .long("cache-root")
                .short('r')
                .value_name("PATH")
                .action(ArgAction::Set)
                .help("The directory to use for the cache")
        )
        .arg(
            Arg::new("cache-bytes-used-target")
                .long("cache-bytes-used-target")
                .short('B')
                .value_name("BYTES")
                .action(ArgAction::Set)
                .help(
                    "The target amount of disk space to use for the cache. \
                    This bound won't be followed strictly, so it's best to be conservative"
                )
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
        let env = env::vars().filter(|(key, _)| key.starts_with("MAELSTROM_BROKER_"));

        let config_files = match args.remove_one::<String>("config-file").as_deref() {
            Some("-") => vec![],
            Some(config_file) => vec![PathBuf::from(config_file)],
            None => BaseDirectories::with_prefix("maelstrom/broker")
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

        let config = maelstrom_config::Config::new(args, "MAELSTROM_BROKER_", env, files)
            .context("loading configuration from environment variables and config files")?;

        let config = Self {
            port: config.get_or_else("port", || BrokerPort::from(0))?,
            http_port: config.get_or_else("http-port", || HttpPort::from(0))?,
            cache_root: config.get_or_else("cache-root", || {
                PathBuf::from(".cache/maelstrom-worker").into()
            })?,
            cache_bytes_used_target: config.get_or_else("cache-bytes-used-target", || {
                CacheBytesUsedTarget::from(1_000_000_000)
            })?,
            log_level: config.get_or("log-level", LogLevel::Info)?,
        };

        if print_config {
            println!("{config:#?}");
            process::exit(0);
        }

        Ok(config)
    }
}
