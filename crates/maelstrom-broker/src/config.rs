use anyhow::Result;
use derive_more::From;
use maelstrom_util::config::{CacheBytesUsedTarget, CacheRoot, LogLevel};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{
    fmt::{self, Debug, Formatter},
    path::PathBuf,
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
        self.0.fmt(f)
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
        self.0.fmt(f)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[skip_serializing_none]
#[derive(Serialize)]
pub struct ConfigOptions {
    pub port: Option<u16>,
    pub http_port: Option<u16>,
    pub cache_root: Option<PathBuf>,
    pub cache_bytes_used_target: Option<u64>,
    pub log_level: Option<LogLevel>,
}

impl Default for ConfigOptions {
    fn default() -> Self {
        ConfigOptions {
            port: Some(0),
            http_port: Some(0),
            cache_root: Some(".cache/maelstrom-broker".into()),
            cache_bytes_used_target: Some(1_000_000_000),
            log_level: Some(LogLevel::Info),
        }
    }
}
