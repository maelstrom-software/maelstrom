use bytesize::ByteSize;
use clap::ValueEnum;
use derive_more::From;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use slog::Level;
use std::{
    error,
    fmt::{self, Debug, Display, Formatter},
    io,
    net::{SocketAddr, ToSocketAddrs},
    num::ParseIntError,
    path::{Path, PathBuf},
    result,
    str::FromStr,
};
use strum::EnumString;

#[serde_as]
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct BrokerAddr(#[serde_as(as = "DisplayFromStr")] SocketAddr);

impl BrokerAddr {
    pub fn new(inner: SocketAddr) -> Self {
        BrokerAddr(inner)
    }

    pub fn inner(&self) -> &SocketAddr {
        &self.0
    }

    pub fn into_inner(self) -> SocketAddr {
        self.0
    }
}

impl FromStr for BrokerAddr {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let addrs: Vec<SocketAddr> = value.to_socket_addrs()?.collect();
        // It's not clear how we could end up with an empty iterator. We'll assume that's
        // impossible until proven wrong.
        Ok(BrokerAddr(*addrs.first().unwrap()))
    }
}

impl From<BrokerAddr> for String {
    fn from(b: BrokerAddr) -> Self {
        b.to_string()
    }
}

impl Display for BrokerAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Debug for BrokerAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

#[derive(Deserialize, From)]
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

impl Debug for CacheRoot {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for CacheRoot {
    type Err = <PathBuf as FromStr>::Err;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(PathBuf::from_str(value)?))
    }
}

impl TryFrom<String> for CacheRoot {
    type Error = <CacheRoot as FromStr>::Err;
    fn try_from(from: String) -> Result<Self, Self::Error> {
        CacheRoot::from_str(from.as_str())
    }
}

#[derive(Debug)]
pub struct StringError(pub String);

impl Display for StringError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl error::Error for StringError {}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(transparent)]
pub struct CacheSize(#[serde(with = "bytesize_serde")] ByteSize);

impl CacheSize {
    pub fn as_bytes(self) -> u64 {
        self.0 .0
    }

    pub fn from_bytes(bytes: u64) -> Self {
        Self(ByteSize(bytes))
    }
}

impl From<ByteSize> for CacheSize {
    fn from(inner: ByteSize) -> Self {
        Self(inner)
    }
}

impl Debug for CacheSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Default for CacheSize {
    fn default() -> Self {
        Self(ByteSize::gb(1))
    }
}

impl Display for CacheSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl FromStr for CacheSize {
    type Err = StringError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            <ByteSize as FromStr>::from_str(s).map_err(StringError)?,
        ))
    }
}

#[derive(Debug)]
pub struct CacheSizeFromStrError(String);

impl Display for CacheSizeFromStrError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl error::Error for CacheSizeFromStrError {}

#[derive(Clone, Copy, Debug, Deserialize, EnumString, Serialize, ValueEnum)]
#[clap(rename_all = "kebab_case")]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum LogLevel {
    Error,
    Warning,
    Info,
    Debug,
}

impl LogLevel {
    pub fn as_slog_level(&self) -> Level {
        match self {
            LogLevel::Error => slog::Level::Error,
            LogLevel::Warning => slog::Level::Warning,
            LogLevel::Info => slog::Level::Info,
            LogLevel::Debug => slog::Level::Debug,
        }
    }
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(transparent)]
pub struct InlineLimit(#[serde(with = "bytesize_serde")] ByteSize);

impl InlineLimit {
    pub fn as_bytes(self) -> u64 {
        self.0 .0
    }

    pub fn from_bytes(bytes: u64) -> Self {
        Self(ByteSize(bytes))
    }
}

impl From<ByteSize> for InlineLimit {
    fn from(inner: ByteSize) -> Self {
        Self(inner)
    }
}

impl Debug for InlineLimit {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Default for InlineLimit {
    fn default() -> Self {
        Self(ByteSize::mb(1))
    }
}

impl Display for InlineLimit {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl FromStr for InlineLimit {
    type Err = StringError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            <ByteSize as FromStr>::from_str(s).map_err(StringError)?,
        ))
    }
}

#[derive(Clone, Copy, Deserialize)]
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

impl Default for Slots {
    fn default() -> Self {
        Self::try_from(num_cpus::get()).unwrap()
    }
}

impl Display for Slots {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct TwoCacheSizes {
        one: CacheSize,
        two: CacheSize,
    }

    #[test]
    fn cache_size() {
        assert_eq!(
            CacheSize::from(ByteSize::b(1234)),
            CacheSize::from(ByteSize::b(1234))
        );
        assert_eq!(
            format!("{:?}", CacheSize::from(ByteSize::b(1234))),
            "1.2 KB"
        );
        assert_eq!(format!("{}", CacheSize::from(ByteSize::b(1234))), "1.2 KB");
        assert_eq!(
            CacheSize::from_str("10 MB").unwrap(),
            CacheSize::from(ByteSize::mb(10))
        );
        let two_cache_sizes: TwoCacheSizes = toml::from_str(
            r#"
            one = 1000
            two = "1.23 MB"
        "#,
        )
        .unwrap();
        assert_eq!(
            two_cache_sizes,
            TwoCacheSizes {
                one: "1 kB".parse().unwrap(),
                two: "1.23 MB".parse().unwrap()
            }
        );
    }

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct TwoInlineLimits {
        one: InlineLimit,
        two: InlineLimit,
    }

    #[test]
    fn inline_limit() {
        assert_eq!(
            InlineLimit::from(ByteSize::b(1234)),
            InlineLimit::from(ByteSize::b(1234))
        );
        assert_eq!(
            format!("{:?}", InlineLimit::from(ByteSize::b(1234))),
            "1.2 KB"
        );
        assert_eq!(
            format!("{}", InlineLimit::from(ByteSize::b(1234))),
            "1.2 KB"
        );
        assert_eq!(
            InlineLimit::from_str("10 MB").unwrap(),
            InlineLimit::from(ByteSize::mb(10))
        );
        let two_inline_limits: TwoInlineLimits = toml::from_str(
            r#"
            one = 1000
            two = "1.23 MB"
        "#,
        )
        .unwrap();
        assert_eq!(
            two_inline_limits,
            TwoInlineLimits {
                one: "1 kB".parse().unwrap(),
                two: "1.23 MB".parse().unwrap()
            }
        );
    }
}
