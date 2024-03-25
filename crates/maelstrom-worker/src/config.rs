use anyhow::Result;
use bytesize::ByteSize;
use derive_more::From;
use maelstrom_macro::Config;
use maelstrom_util::config::common::{BrokerAddr, CacheRoot, CacheSize, LogLevel};
use serde::Deserialize;
use std::{
    error,
    fmt::{self, Debug, Display, Formatter},
    num::ParseIntError,
    result,
    str::FromStr,
};
use xdg::BaseDirectories;

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

#[derive(Config, Debug)]
pub struct Config {
    /// Socket address of broker.
    #[config(short = 'b', value_name = "SOCKADDR")]
    pub broker: BrokerAddr,

    /// The number of job slots available.
    #[config(short = 'S', value_name = "N", default = "num_cpus::get()")]
    pub slots: Slots,

    /// The directory to use for the cache.
    #[config(
        short = 'r',
        value_name = "PATH",
        default = "|bd: &BaseDirectories| bd.get_cache_home().into_os_string().into_string().unwrap()"
    )]
    pub cache_root: CacheRoot,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative.
    #[config(short = 's', value_name = "BYTES", default = "1_000_000_000")]
    pub cache_size: CacheSize,

    /// The maximum amount of bytes to return inline for captured stdout and stderr.
    #[config(short = 'i', value_name = "BYTES", default = "1_000_000")]
    pub inline_limit: InlineLimit,

    /// Minimum log level to output.
    #[config(short = 'l', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,
}
