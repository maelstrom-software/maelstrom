use anyhow::Result;
use derive_more::From;
use maelstrom_config::CommandBuilder;
use maelstrom_util::config::{CacheBytesUsedTarget, CacheRoot, LogLevel};
use serde::Deserialize;
use std::{
    fmt::{self, Debug, Formatter},
    path::PathBuf,
    result,
    str::FromStr,
};

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

impl maelstrom_config::Config for Config {
    fn add_command_line_options(builder: CommandBuilder) -> CommandBuilder {
        builder
            .value(
                "port",
                Some('p'),
                "PORT",
                Some(0.to_string()),
                "The port the broker listens on for connections from workers and clients.",
            )
            .value(
                "http-port",
                Some('H'),
                "PORT",
                Some(0.to_string()),
                "The port the HTTP UI is served on.",
            )
            .value(
                "cache_root",
                Some('r'),
                "PATH",
                Some(".cache/maelstrom-broker".to_string()),
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
                "log_level",
                Some('l'),
                "LEVEL",
                Some("info".to_string()),
                "Minimum log level to output.",
            )
    }

    fn from_config_bag(config: &mut maelstrom_config::ConfigBag) -> Result<Self> {
        Ok(Self {
            port: config.get_or_else("port", || BrokerPort::from(0))?,
            http_port: config.get_or_else("http_port", || HttpPort::from(0))?,
            cache_root: config.get_or_else("cache_root", || {
                PathBuf::from(".cache/maelstrom-worker").into()
            })?,
            cache_bytes_used_target: config.get_or_else("cache_bytes-used_target", || {
                CacheBytesUsedTarget::from(1_000_000_000)
            })?,
            log_level: config.get_or("log_level", LogLevel::Info)?,
        })
    }
}
