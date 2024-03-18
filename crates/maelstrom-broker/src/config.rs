use anyhow::Result;
use clap::{command, Command};
use derive_more::From;
use maelstrom_config::{ConfigBuilder, FromConfig};
use maelstrom_util::config::{CacheBytesUsedTarget, CacheRoot, LogLevel};
use serde::Deserialize;
use std::{
    env,
    fmt::{self, Debug, Formatter},
    path::PathBuf,
    result,
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
    pub fn add_command_line_options(
        base_directories: &BaseDirectories,
        env_var_prefix: &'static str,
    ) -> Command {
        ConfigBuilder::new(command!(), base_directories, env_var_prefix)
            .value(
                "port",
                'p',
                "PORT",
                Some(0.to_string()),
                "The port the broker listens on for connections from workers and clients.",
            )
            .value(
                "http-port",
                'H',
                "PORT",
                Some(0.to_string()),
                "The port the HTTP UI is served on.",
            )
            .value(
                "cache_root",
                'r',
                "PATH",
                Some(".cache/maelstrom-broker".to_string()),
                "The directory to use for the cache.",
            )
            .value(
                "cache_bytes_used_target",
                'B',
                "BYTES",
                Some(1_000_000_000.to_string()),
                "The target amount of disk space to use for the cache. \
                This bound won't be followed strictly, so it's best to be conservative.",
            )
            .value(
                "log_level",
                'l',
                "LEVEL",
                Some("info".to_string()),
                "Minimum log level to output.",
            )
            .build()
    }
}

impl FromConfig for Config {
    fn from_config(config: &mut maelstrom_config::Config) -> Result<Self> {
        Ok(Self {
            port: config.get_or_else("port", || BrokerPort::from(0))?,
            http_port: config.get_or_else("http-port", || HttpPort::from(0))?,
            cache_root: config.get_or_else("cache-root", || {
                PathBuf::from(".cache/maelstrom-worker").into()
            })?,
            cache_bytes_used_target: config.get_or_else("cache-bytes-used-target", || {
                CacheBytesUsedTarget::from(1_000_000_000)
            })?,
            log_level: config.get_or("log-level", LogLevel::Info)?,
        })
    }
}
