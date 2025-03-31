use bytesize::ByteSize;
use clap::ValueEnum;
use derive_more::{Debug, Display, From, Into};
use maelstrom_macro::pocket_definition;
use serde::{
    de::{self, Deserializer, Visitor},
    Deserialize, Serialize, Serializer,
};
use slog::Level;
use std::{
    error, fmt, io,
    net::{SocketAddr, ToSocketAddrs},
    num::ParseIntError,
    result,
    str::FromStr,
};
use strum::EnumString;

#[pocket_definition(export)]
#[derive(Clone, Copy, Debug, Display, PartialEq, Serialize)]
#[serde(transparent)]
#[debug("{_0:?}")]
#[display("{_0}")]
pub struct BrokerAddr(SocketAddr);

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

impl TryFrom<String> for BrokerAddr {
    type Error = io::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<BrokerAddr> for String {
    fn from(b: BrokerAddr) -> Self {
        b.to_string()
    }
}

impl<'de> Deserialize<'de> for BrokerAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BrokerAddrVisitor;
        impl Visitor<'_> for BrokerAddrVisitor {
            type Value = BrokerAddr;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a socket address")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                BrokerAddr::from_str(v).map_err(de::Error::custom)
            }
        }
        if deserializer.is_human_readable() {
            deserializer.deserialize_str(BrokerAddrVisitor)
        } else {
            SocketAddr::deserialize(deserializer).map(Self::new)
        }
    }
}

#[derive(Debug, Display)]
#[display("{_0}")]
pub struct StringError(pub String);

impl StringError {
    pub fn new<T: ToString>(inner: T) -> Self {
        Self(inner.to_string())
    }
}

macro_rules! byte_size_u64_from_impls {
    ($name:ident) => {
        impl From<$name> for u64 {
            fn from(l: $name) -> u64 {
                (l.0).0
            }
        }

        impl From<u64> for $name {
            fn from(v: u64) -> Self {
                ByteSize(v).into()
            }
        }

        impl FromStr for $name {
            type Err = StringError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(
                    <ByteSize as FromStr>::from_str(s).map_err(StringError)?,
                ))
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                if serializer.is_human_readable() {
                    <str>::serialize(self.0.to_string().as_str(), serializer)
                } else {
                    self.0 .0.serialize(serializer)
                }
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct ByteSizeVisitor;
                impl Visitor<'_> for ByteSizeVisitor {
                    type Value = ByteSize;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("an integer or string")
                    }

                    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                        Ok(ByteSize(value))
                    }

                    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                        Ok(ByteSize(value.try_into().map_err(de::Error::custom)?))
                    }

                    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        ByteSize::from_str(v).map_err(de::Error::custom)
                    }
                }
                (if deserializer.is_human_readable() {
                    deserializer.deserialize_any(ByteSizeVisitor)
                } else {
                    deserializer.deserialize_u64(ByteSizeVisitor)
                })
                .map($name)
            }
        }
    };
}

impl error::Error for StringError {}

#[pocket_definition(export)]
#[derive(Clone, Copy, Debug, Display, Eq, PartialEq, From, Into)]
#[debug("{_0:?}")]
#[display("{_0}")]
pub struct CacheSize(ByteSize);

byte_size_u64_from_impls!(CacheSize);

impl Default for CacheSize {
    fn default() -> Self {
        Self(ByteSize::gb(1))
    }
}

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

#[pocket_definition(export)]
#[derive(Clone, Copy, Debug, Display, Eq, PartialEq, From, Into)]
#[debug("{_0:?}")]
#[display("{_0}")]
pub struct InlineLimit(ByteSize);

byte_size_u64_from_impls!(InlineLimit);

impl Default for InlineLimit {
    fn default() -> Self {
        Self(ByteSize::mb(1))
    }
}

#[pocket_definition(export)]
#[derive(Clone, Copy, Debug, Display, Deserialize, Into)]
#[serde(try_from = "u16")]
#[into(usize, u16, u32)]
#[debug("{_0:?}")]
#[display("{_0}")]
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

macro_rules! slots_try_from_impl {
    ($($int_type:ty),*) => {$(
        impl TryFrom<$int_type> for Slots {
            type Error = String;

            fn try_from(value: $int_type) -> Result<Self, Self::Error> {
                let value: u16 = value
                    .try_into()
                    .map_err(|_| "value must be less than 1000".to_string())?;
                value.try_into()
            }
        }
    )*}
}

slots_try_from_impl!(usize, u32);

impl Default for Slots {
    fn default() -> Self {
        Self::try_from(num_cpus::get()).unwrap()
    }
}

impl FromStr for Slots {
    type Err = SlotsFromStrError;
    fn from_str(slots: &str) -> result::Result<Self, Self::Err> {
        let slots = u16::from_str(slots).map_err(SlotsFromStrError::Parse)?;
        Self::try_from(slots).map_err(SlotsFromStrError::Bounds)
    }
}

#[derive(Debug, Display)]
pub enum SlotsFromStrError {
    #[display("{_0}")]
    Parse(ParseIntError),
    #[display("{_0}")]
    Bounds(String),
}

impl error::Error for SlotsFromStrError {}

#[pocket_definition(export)]
#[derive(Copy, Clone, Debug, EnumString, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum ClusterCommunicationStrategy {
    Tcp = 0,
    #[serde(rename = "github")]
    #[strum(serialize = "github")]
    GitHub = 1,
}

impl ValueEnum for ClusterCommunicationStrategy {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Tcp, Self::GitHub][..]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            Self::Tcp => Some(clap::builder::PossibleValue::new("tcp")),
            Self::GitHub => Some(clap::builder::PossibleValue::new("github")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Debug, *};
    use serde_test::{assert_de_tokens, assert_tokens, Configure, Token};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    const LOCALHOST4: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234);
    const LOCALHOST6: SocketAddr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 1234);

    #[test]
    fn broker_addr_from_str() {
        assert_eq!(
            BrokerAddr::from_str("127.0.0.1:1234").unwrap(),
            BrokerAddr::new(LOCALHOST4),
        );
        assert_eq!(
            BrokerAddr::from_str("localhost:1234").unwrap(),
            BrokerAddr::new(LOCALHOST4),
        );
        assert_eq!(
            BrokerAddr::from_str("[::1]:1234").unwrap(),
            BrokerAddr::new(LOCALHOST6),
        );
        assert_eq!(
            BrokerAddr::from_str("ip6-localhost:1234").unwrap(),
            BrokerAddr::new(LOCALHOST6),
        );
    }

    #[test]
    fn broker_addr_into_string() {
        assert_eq!(String::from(BrokerAddr::new(LOCALHOST4)), "127.0.0.1:1234");
        assert_eq!(String::from(BrokerAddr::new(LOCALHOST6)), "[::1]:1234");
    }

    #[test]
    fn broker_addr_display() {
        assert_eq!(
            format!("{}", BrokerAddr::new(LOCALHOST4)),
            format!("{LOCALHOST4}")
        );
        assert_eq!(
            format!("{}", BrokerAddr::new(LOCALHOST6)),
            format!("{LOCALHOST6}")
        );
    }

    #[test]
    fn broker_addr_debug() {
        assert_eq!(
            format!("{:?}", BrokerAddr::new(LOCALHOST4)),
            format!("{LOCALHOST4:?}")
        );
        assert_eq!(
            format!("{:?}", BrokerAddr::new(LOCALHOST6)),
            format!("{LOCALHOST6:?}")
        );
    }

    #[test]
    fn broker_addr_serialize_and_deserialize_human_readable() {
        assert_tokens(
            &BrokerAddr::new(LOCALHOST4).readable(),
            &[Token::String("127.0.0.1:1234")],
        );
        assert_tokens(
            &BrokerAddr::new(LOCALHOST6).readable(),
            &[Token::String("[::1]:1234")],
        );
    }

    #[test]
    fn broker_addr_serialize_and_deserialize_compact() {
        assert_tokens(
            &BrokerAddr::new(LOCALHOST4).compact(),
            &[
                Token::NewtypeVariant {
                    name: "SocketAddr",
                    variant: "V4",
                },
                Token::Tuple { len: 2 },
                Token::Tuple { len: 4 },
                Token::U8(127),
                Token::U8(0),
                Token::U8(0),
                Token::U8(1),
                Token::TupleEnd,
                Token::U16(1234),
                Token::TupleEnd,
            ],
        );
        assert_tokens(
            &BrokerAddr::new(LOCALHOST6).compact(),
            &[
                Token::NewtypeVariant {
                    name: "SocketAddr",
                    variant: "V6",
                },
                Token::Tuple { len: 2 },
                Token::Tuple { len: 16 },
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(0),
                Token::U8(1),
                Token::TupleEnd,
                Token::U16(1234),
                Token::TupleEnd,
            ],
        );
    }

    #[test]
    fn broker_addr_deserialize_from_string() {
        assert_de_tokens(
            &BrokerAddr::new(LOCALHOST4).readable(),
            &[Token::String("127.0.0.1:1234")],
        );
        assert_de_tokens(
            &BrokerAddr::new(LOCALHOST6).readable(),
            &[Token::String("[::1]:1234")],
        );
    }

    #[test]
    fn broker_addr_deserialize_from_hostname_string() {
        assert_de_tokens(
            &BrokerAddr::new(LOCALHOST4).readable(),
            &[Token::String("localhost:1234")],
        );
        assert_de_tokens(
            &BrokerAddr::new(LOCALHOST6).readable(),
            &[Token::String("ip6-localhost:1234")],
        );
    }

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

    #[test]
    fn cache_size_serialize_and_deserialize_human_readable() {
        assert_tokens(
            &CacheSize::from_str("10MB").unwrap().readable(),
            &[Token::String("10.0 MB")],
        );
    }

    #[test]
    fn cache_size_serialize_and_deserialize_compact() {
        assert_tokens(
            &CacheSize::from_str("10MB").unwrap().compact(),
            &[Token::U64(10000000)],
        );
    }

    #[test]
    fn cache_size_deserialize_from_string() {
        assert_de_tokens(
            &CacheSize::from_str("10 MB").unwrap().readable(),
            &[Token::String("10 MB")],
        );
    }

    #[test]
    fn cache_size_deserialize_from_unsigned_integer() {
        assert_de_tokens(
            &CacheSize::from_str("10 MB").unwrap().readable(),
            &[Token::U64(10000000)],
        );
        assert_de_tokens(
            &CacheSize::from_str("10 MB").unwrap().readable(),
            &[Token::U32(10000000)],
        );
        assert_de_tokens(
            &CacheSize::from_str("10 kB").unwrap().readable(),
            &[Token::U16(10000)],
        );
        assert_de_tokens(
            &CacheSize::from_str("10 B").unwrap().readable(),
            &[Token::U8(10)],
        );
    }

    #[test]
    fn cache_size_deserialize_from_signed_integer() {
        assert_de_tokens(
            &CacheSize::from_str("10 MB").unwrap().readable(),
            &[Token::I64(10000000)],
        );
        assert_de_tokens(
            &CacheSize::from_str("10 MB").unwrap().readable(),
            &[Token::I32(10000000)],
        );
        assert_de_tokens(
            &CacheSize::from_str("10 kB").unwrap().readable(),
            &[Token::I16(10000)],
        );
        assert_de_tokens(
            &CacheSize::from_str("10 B").unwrap().readable(),
            &[Token::I8(10)],
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

    #[test]
    fn inline_limit_serialize_and_deserialize_human_readable() {
        assert_tokens(
            &InlineLimit::from_str("10MB").unwrap().readable(),
            &[Token::String("10.0 MB")],
        );
    }

    #[test]
    fn inline_limit_serialize_and_deserialize_compact() {
        assert_tokens(
            &InlineLimit::from_str("10MB").unwrap().compact(),
            &[Token::U64(10000000)],
        );
    }

    #[test]
    fn inline_limit_deserialize_from_string() {
        assert_de_tokens(
            &InlineLimit::from_str("10 MB").unwrap().readable(),
            &[Token::String("10 MB")],
        );
    }

    #[test]
    fn inline_limit_deserialize_from_unsigned_integer() {
        assert_de_tokens(
            &InlineLimit::from_str("10 MB").unwrap().readable(),
            &[Token::U64(10000000)],
        );
        assert_de_tokens(
            &InlineLimit::from_str("10 MB").unwrap().readable(),
            &[Token::U32(10000000)],
        );
        assert_de_tokens(
            &InlineLimit::from_str("10 kB").unwrap().readable(),
            &[Token::U16(10000)],
        );
        assert_de_tokens(
            &InlineLimit::from_str("10 B").unwrap().readable(),
            &[Token::U8(10)],
        );
    }

    #[test]
    fn inline_limit_deserialize_from_signed_integer() {
        assert_de_tokens(
            &InlineLimit::from_str("10 MB").unwrap().readable(),
            &[Token::I64(10000000)],
        );
        assert_de_tokens(
            &InlineLimit::from_str("10 MB").unwrap().readable(),
            &[Token::I32(10000000)],
        );
        assert_de_tokens(
            &InlineLimit::from_str("10 kB").unwrap().readable(),
            &[Token::I16(10000)],
        );
        assert_de_tokens(
            &InlineLimit::from_str("10 B").unwrap().readable(),
            &[Token::I8(10)],
        );
    }
}
