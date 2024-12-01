use crate::scheduler_task::CacheDir;
use derive_more::{Debug, From};
use maelstrom_macro::Config;
use maelstrom_util::{
    config::common::{CacheSize, LogLevel},
    root::RootBuf,
};
use serde::Deserialize;
use std::{result, str::FromStr};
use xdg::BaseDirectories;

#[derive(Deserialize, Debug, From)]
#[serde(from = "u16")]
#[debug("{_0:?}")]
pub struct BrokerPort(u16);

impl BrokerPort {
    pub fn inner(&self) -> &u16 {
        &self.0
    }

    pub fn into_inner(self) -> u16 {
        self.0
    }
}

impl FromStr for BrokerPort {
    type Err = <u16 as FromStr>::Err;
    fn from_str(slots: &str) -> result::Result<Self, Self::Err> {
        Ok(Self::from(u16::from_str(slots)?))
    }
}

#[derive(Deserialize, Debug, From)]
#[serde(from = "u16")]
#[debug("{_0:?}")]
pub struct HttpPort(u16);

impl HttpPort {
    pub fn inner(&self) -> &u16 {
        &self.0
    }

    pub fn into_inner(self) -> u16 {
        self.0
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
        value_name = "PATH",
        default = r#"|bd: &BaseDirectories| {
            bd.get_cache_home()
                .into_os_string()
                .into_string()
                .unwrap()
        }"#
    )]
    pub cache_root: RootBuf<CacheDir>,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative.
    #[config(value_name = "BYTES", default = "bytesize::ByteSize::gb(1)")]
    pub cache_size: CacheSize,

    /// Minimum log level to output.
    #[config(short = 'l', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,
}
