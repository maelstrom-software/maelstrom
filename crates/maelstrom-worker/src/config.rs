use anyhow::Result;
use bytesize::ByteSize;
use derive_more::From;
use maelstrom_config::CommandBuilder;
use maelstrom_util::config::{BrokerAddr, CacheBytesUsedTarget, CacheRoot, LogLevel};
use serde::Deserialize;
use std::{
    error,
    fmt::{self, Debug, Display, Formatter},
    num::ParseIntError,
    path::PathBuf,
    result,
    str::FromStr,
};

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

impl TryFrom<usize> for Slots {
    type Error = String;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        if value < 1 {
            Err("value must be at least 1".to_string())
        } else if value > 1000 {
            Err("value must be less than 1000".to_string())
        } else {
            Ok(Slots(value.try_into().unwrap()))
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

impl maelstrom_config::Config for Config {
    fn add_command_line_options(builder: CommandBuilder) -> CommandBuilder {
        builder
            .value(
                "broker",
                Some('b'),
                "SOCKADDR",
                None,
                r#"Socket address of broker. Examples: "[::]:5000", "host.example.com:2000"."#,
            )
            .value(
                "slots",
                Some('s'),
                "N",
                Some(num_cpus::get().to_string()),
                "The number of job slots available. Most jobs will take one job slot.",
            )
            .value(
                "cache_root",
                Some('r'),
                "PATH",
                Some(".cache/maelstrom-worker".to_string()),
                "The directory to use for the cache.",
            )
            .value(
                "cache_bytes_used_target",
                Some('B'),
                "BYTES",
                Some(1_000_000_000.to_string()),
                "The target amount of disk space to use for the cache. \
                This bound won't be followed strictly, so it's best to be conservative.",
            )
            .value(
                "inline_limit",
                Some('i'),
                "BYTES",
                Some(1_000_000.to_string()),
                "The maximum amount of bytes to return inline for captured stdout and stderr.",
            )
            .value(
                "log_level",
                Some('l'),
                "LEVEL",
                Some("info".to_string()),
                "Minimum log level to output.",
            )
    }

    fn from_config_bag(config: &mut maelstrom_config::ConfigBag) -> Result<Self> {
        Ok(Self {
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
        })
    }
}
