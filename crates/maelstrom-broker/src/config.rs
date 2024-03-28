use anyhow::Result;
use derive_more::From;
use maelstrom_macro::Config;
use maelstrom_util::config::common::{CacheRoot, CacheSize, LogLevel};
use serde::Deserialize;
use std::{
    fmt::{self, Debug, Formatter},
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

#[derive(Config, Debug)]
pub struct Config {
    /// The port the broker listens on for connections from workers and clients.
    #[config(short = 'p', value_name = "PORT", default = "0")]
    pub port: BrokerPort,

    /// The port the HTTP UI is served on.
    #[config(short = 'H', value_name = "PORT", default = "0")]
    pub http_port: HttpPort,

    /// The directory to use for the cache.
    #[config(
        short = 'r',
        value_name = "PATH",
        default = "|bd: &BaseDirectories| bd.get_cache_home().into_os_string().into_string().unwrap()"
    )]
    pub cache_root: CacheRoot,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative.
    #[config(
        short = 's',
        value_name = "BYTES",
        default = "bytesize::ByteSize::gb(1)"
    )]
    pub cache_size: CacheSize,

    /// Minimum log level to output.
    #[config(short = 'l', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,
}
