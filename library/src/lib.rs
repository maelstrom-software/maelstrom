use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::hash::Hash;

pub mod broker;
mod channel_reader;
pub mod client;
mod heap;
mod proto;
pub mod worker;

#[cfg(test)]
pub mod test;

pub type Error = anyhow::Error;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ClientId(u32);

#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ClientExecutionId(u32);

#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ExecutionId(ClientId, ClientExecutionId);

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExecutionDetails {
    pub program: String,
    pub arguments: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ExecutionResult {
    Exited(u8),
    Signalled(u8),
    Error(String),
}

#[derive(
    Copy, Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct WorkerId(u32);

#[derive(Clone, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Sha256Digest(pub [u8; 32]);

impl From<u32> for Sha256Digest {
    fn from(input: u32) -> Self {
        let mut bytes = [0; 32];
        bytes[28..].copy_from_slice(&input.to_be_bytes());
        Sha256Digest(bytes)
    }
}

impl From<u64> for Sha256Digest {
    fn from(input: u64) -> Self {
        let mut bytes = [0; 32];
        bytes[24..].copy_from_slice(&input.to_be_bytes());
        Sha256Digest(bytes)
    }
}

impl std::str::FromStr for Sha256Digest {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let digits: Option<Vec<_>> = value
            .chars()
            .map(|c| c.to_digit(16).and_then(|x| x.try_into().ok()))
            .collect();
        match digits {
            None => Err("Input string must consist of only hexadecimal digits"),
            Some(ref digits) if digits.len() != 64 => {
                Err("Input string must be exactly 64 hexadecimal digits long")
            }
            Some(mut digits) => {
                digits = digits
                    .chunks(2)
                    .map(|chunk| chunk[0] * 16 + chunk[1])
                    .collect();
                let mut bytes = [0; 32];
                bytes.clone_from_slice(&digits);
                Ok(Sha256Digest(bytes))
            }
        }
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(
            &self
                .0
                .iter()
                .flat_map(|byte| {
                    [
                        char::from_digit((byte / 16).into(), 16).unwrap(),
                        char::from_digit((byte % 16).into(), 16).unwrap(),
                    ]
                })
                .collect::<String>(),
        )
    }
}

impl Debug for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.debug_tuple("Sha256Digest").field(&self.0).finish()
        } else {
            write!(f, "Sha256Digest({})", self)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_u32() {
        assert_eq!(
            Sha256Digest::from(0x12345678u32),
            Sha256Digest([
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0x12, 0x34, 0x56, 0x78,
            ])
        );
    }

    #[test]
    fn from_u64() {
        assert_eq!(
            Sha256Digest::from(0x123456789ABCDEF0u64),
            Sha256Digest([
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x12, 0x34,
                0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
            ])
        );
    }

    #[test]
    fn from_str_ok() {
        assert_eq!(
            "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f"
                .parse::<Sha256Digest>()
                .unwrap(),
            Sha256Digest([
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
                0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b,
                0x2c, 0x2d, 0x2e, 0x2f,
            ])
        );
    }

    #[test]
    fn from_str_wrong_length() {
        let wrong_length_strs = [
            "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f0",
            "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2",
            "",
            "1",
            "10",
        ];

        for s in wrong_length_strs {
            match s.parse::<Sha256Digest>() {
                Err(s) => assert_eq!(s, "Input string must be exactly 64 hexadecimal digits long"),
                Ok(_) => panic!("expected error with input {s}"),
            }
        }
    }

    #[test]
    fn from_str_bad_chars() {
        let bad_chars_strs = [
            " 101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f",
            "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2g",
        ];

        for s in bad_chars_strs {
            match s.parse::<Sha256Digest>() {
                Err(s) => assert_eq!(s, "Input string must consist of only hexadecimal digits"),
                Ok(_) => panic!("expected error with input {s}"),
            }
        }
    }

    #[test]
    fn display_round_trip() {
        let s = "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f";
        assert_eq!(s, s.parse::<Sha256Digest>().unwrap().to_string());
    }

    #[test]
    fn display_padding() {
        let d = "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f"
            .parse::<Sha256Digest>()
            .unwrap();
        assert_eq!(
            format!("{d:<70}"),
            "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f      "
        );
        assert_eq!(
            format!("{d:0>70}"),
            "000000101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f"
        );
    }

    #[test]
    fn debug() {
        let d = "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f"
            .parse::<Sha256Digest>()
            .unwrap();
        assert_eq!(
            format!("{d:?}"),
            "Sha256Digest(101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f)"
        );
        assert_eq!(
            format!("{d:#?}"),
            "Sha256Digest(
    [
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
        24,
        25,
        26,
        27,
        28,
        29,
        30,
        31,
        32,
        33,
        34,
        35,
        36,
        37,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        45,
        46,
        47,
    ],
)"
        );
    }
}
