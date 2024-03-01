use anyhow::{Context as _, Result};
use bytesize::ByteSize;
use clap::{ArgMatches, Command};
use derive_more::From;
use maelstrom_config::ConfigBuilder;
use maelstrom_util::config::{BrokerAddr, CacheBytesUsedTarget, CacheRoot, LogLevel};
use serde::Deserialize;
use std::{
    env, error,
    fmt::{self, Debug, Display, Formatter},
    fs,
    num::ParseIntError,
    path::PathBuf,
    process, result,
    str::FromStr,
};
use xdg::BaseDirectories;

#[derive(Deserialize)]
#[serde(try_from = "u16")]
pub struct Slots(u16);

impl Slots {
    pub fn inner(&self) -> &u16 {
        &self.0
    }

    pub fn into_inner(self) -> u16 {
        self.0
    }
}

impl TryFrom<u16> for Slots {
    type Error = String;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        if value < 1 {
            Err("value must be at least 1".to_string())
        } else if value > 1000 {
            Err("value must be less than 1000".to_string())
        } else {
            Ok(Slots(value))
        }
    }
}

impl Debug for Slots {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl FromStr for Slots {
    type Err = SlotsFromStrError;
    fn from_str(slots: &str) -> result::Result<Self, Self::Err> {
        let slots = u16::from_str(slots).map_err(SlotsFromStrError::Parse)?;
        Self::try_from(slots).map_err(SlotsFromStrError::Bounds)
    }
}

#[derive(Debug)]
pub enum SlotsFromStrError {
    Parse(ParseIntError),
    Bounds(String),
}

impl Display for SlotsFromStrError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::Parse(inner) => Display::fmt(inner, f),
            Self::Bounds(inner) => write!(f, "{inner}"),
        }
    }
}

impl error::Error for SlotsFromStrError {}

#[derive(Clone, Copy, Deserialize, From)]
#[serde(from = "u64")]
pub struct InlineLimit(u64);

impl InlineLimit {
    pub fn into_inner(self) -> u64 {
        self.0
    }
}

impl Debug for InlineLimit {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        Debug::fmt(&ByteSize::b(self.0), f)
    }
}

impl FromStr for InlineLimit {
    type Err = InlineLimitFromStrError;
    fn from_str(slots: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(
            ByteSize::from_str(slots)
                .map_err(InlineLimitFromStrError)?
                .as_u64(),
        ))
    }
}

#[derive(Debug)]
pub struct InlineLimitFromStrError(String);

impl Display for InlineLimitFromStrError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl error::Error for InlineLimitFromStrError {}

#[derive(Debug)]
pub struct Config {
    /// Socket address of broker.
    pub broker: BrokerAddr,

    /// The number of job slots available.
    pub slots: Slots,

    /// The directory to use for the cache.
    pub cache_root: CacheRoot,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative.
    pub cache_bytes_used_target: CacheBytesUsedTarget,

    /// The maximum amount of bytes to return inline for captured stdout and stderr.
    pub inline_limit: InlineLimit,

    /// Minimum log level to output.
    pub log_level: LogLevel,
}

impl Config {
    pub fn add_command_line_options() -> Result<Command> {
        Ok(ConfigBuilder::new()?
            .value(
                "broker",
                'b',
                "SOCKADDR",
                r#"Socket address of broker. Examples: "[::]:5000", "host.example.com:2000""#,
            )
            .value(
                "slots",
                's',
                "N",
                "The number of job slots available. Most jobs will take one job slot",
            )
            .value(
                "cache-root",
                'r',
                "PATH",
                "The directory to use for the cache",
            )
            .value(
                "cache-bytes-used-target",
                'B',
                "BYTES",
                "The target amount of disk space to use for the cache. \
                This bound won't be followed strictly, so it's best to be conservative",
            )
            .value(
                "inline-limit",
                'i',
                "BYTES",
                "The maximum amount of bytes to return inline for captured stdout and stderr",
            )
            .value("log-level", 'l', "LEVEL", "Minimum log level to output")
            .build())
    }

    pub fn new(mut args: ArgMatches) -> Result<Self> {
        let env = env::vars().filter(|(key, _)| key.starts_with("MAELSTROM_WORKER_"));

        let config_files = match args.remove_one::<String>("config-file").as_deref() {
            Some("-") => vec![],
            Some(config_file) => vec![PathBuf::from(config_file)],
            None => BaseDirectories::with_prefix("maelstrom/worker")
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

        let config = maelstrom_config::Config::new(args, "MAELSTROM_WORKER_", env, files)
            .context("loading configuration from environment variables and config files")?;

        let config = Self {
            broker: config.get("broker")?,
            slots: config.get_or_else("slots", || {
                Slots::try_from(u16::try_from(num_cpus::get()).unwrap()).unwrap()
            })?,
            cache_root: config.get_or_else("cache-root", || {
                PathBuf::from(".cache/maelstrom-worker").into()
            })?,
            cache_bytes_used_target: config.get_or_else("cache-bytes-used-target", || {
                CacheBytesUsedTarget::from(1_000_000_000)
            })?,
            inline_limit: config.get_or_else("inline-limit", || InlineLimit::from(1_000_000))?,
            log_level: config.get_or("log-level", LogLevel::Info)?,
        };

        if print_config {
            println!("{config:#?}");
            process::exit(0);
        }

        Ok(config)
    }
}
