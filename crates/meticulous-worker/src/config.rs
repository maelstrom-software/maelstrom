use serde::Deserialize;
use std::{
    fmt::{self, Debug, Formatter},
    io,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
};

#[derive(Clone, Copy, Deserialize)]
#[serde(try_from = "String")]
pub struct Broker(SocketAddr);

impl Broker {
    pub fn inner(&self) -> &SocketAddr {
        &self.0
    }
}

impl TryFrom<String> for Broker {
    type Error = io::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let addrs: Vec<SocketAddr> = value.to_socket_addrs()?.collect();
        // It's not clear how we could end up with an empty iterator. We'll assume that's
        // impossible until proven wrong.
        Ok(Broker(*addrs.get(0).unwrap()))
    }
}

impl Debug for Broker {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

#[derive(Deserialize)]
#[serde(try_from = "String")]
pub struct Name(String);

impl TryFrom<String> for Name {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err("value must not be empty".to_string())
        } else {
            Ok(Name(value))
        }
    }
}

impl Debug for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

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
