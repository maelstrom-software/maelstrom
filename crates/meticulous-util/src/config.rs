use bytesize::ByteSize;
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Debug, Formatter},
    io,
    net::{SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Deserialize)]
#[serde(try_from = "String")]
pub struct BrokerAddr(SocketAddr);

impl BrokerAddr {
    pub fn inner(&self) -> &SocketAddr {
        &self.0
    }

    pub fn into_inner(self) -> SocketAddr {
        self.0
    }
}

impl TryFrom<String> for BrokerAddr {
    type Error = io::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let addrs: Vec<SocketAddr> = value.to_socket_addrs()?.collect();
        // It's not clear how we could end up with an empty iterator. We'll assume that's
        // impossible until proven wrong.
        Ok(BrokerAddr(*addrs.first().unwrap()))
    }
}

impl Debug for BrokerAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

#[derive(Deserialize)]
#[serde(from = "PathBuf")]
pub struct CacheRoot(PathBuf);

impl CacheRoot {
    pub fn inner(&self) -> &Path {
        &self.0
    }

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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
        ByteSize::b(self.0).fmt(f)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, ValueEnum)]
#[clap(rename_all = "kebab_case")]
#[serde(rename_all = "kebab-case")]
pub enum LogLevel {
    Error,
    Warning,
    Info,
    Debug,
}
