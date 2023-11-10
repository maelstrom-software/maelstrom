use anyhow::Result;
use meticulous_util::config::{CacheBytesUsedTarget, CacheRoot, LogLevel};
use serde::Deserialize;
use std::fmt::{self, Debug, Formatter};

#[derive(Deserialize)]
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

impl From<u16> for BrokerPort {
    fn from(value: u16) -> Self {
        BrokerPort(value)
    }
}

impl Debug for BrokerPort {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

#[derive(Deserialize)]
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

impl From<u16> for HttpPort {
    fn from(value: u16) -> Self {
        HttpPort(value)
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
