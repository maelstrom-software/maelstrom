use anyhow::Result;
use serde::Deserialize;
use std::{
    fmt::{self, Debug, Formatter},
    path::PathBuf,
};

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

#[derive(Deserialize)]
#[serde(from = "PathBuf")]
pub struct CacheRoot(PathBuf);

impl CacheRoot {
    pub fn into_inner(self) -> PathBuf {
        self.0
    }
}

impl From<PathBuf> for CacheRoot {
    fn from(value: PathBuf) -> Self {
        CacheRoot(value)
    }
}

impl Debug for CacheRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Deserialize)]
#[serde(from = "u64")]
pub struct CacheBytesUsedTarget(u64);

impl CacheBytesUsedTarget {
    pub fn into_inner(self) -> u64 {
        self.0
    }
}

impl From<u64> for CacheBytesUsedTarget {
    fn from(value: u64) -> Self {
        CacheBytesUsedTarget(value)
    }
}

impl Debug for CacheBytesUsedTarget {
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
}
