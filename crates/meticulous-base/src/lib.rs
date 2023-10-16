//! Core structs used by the broker, worker, and clients. Everything in this crate must be usable
//! from wasm.

pub mod proto;

use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    hash::Hash,
    result::Result,
    str::FromStr,
};

/// ID of a client connection. These share the same ID space as [`WorkerId`].
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ClientId(u32);

impl From<u32> for ClientId {
    fn from(input: u32) -> Self {
        ClientId(input)
    }
}

impl Display for ClientId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}", self.0))
    }
}

/// A client-relative job ID. Clients can assign these however they like.
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ClientJobId(u32);

impl From<u32> for ClientJobId {
    fn from(input: u32) -> Self {
        ClientJobId(input)
    }
}

/// An absolute job ID that includes a [`ClientId`] for disambiguation.
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct JobId {
    pub cid: ClientId,
    pub cjid: ClientJobId,
}

/// All necessary information for the worker to execute a job.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobDetails {
    pub program: String,
    pub arguments: Vec<String>,
    pub layers: Vec<Sha256Digest>,
}

/// All relevant information about the outcome of a job.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum JobResult {
    Exited(u8),
    Signalled(u8),
    Error(String),
}

/// ID of a worker connection. These share the same ID space as [`ClientId`].
#[derive(
    Copy, Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct WorkerId(u32);

impl From<u32> for WorkerId {
    fn from(input: u32) -> Self {
        WorkerId(input)
    }
}

impl Display for WorkerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}", self.0))
    }
}

/// Useful information for a client to display about the broker's state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrokerStatistics {
    pub num_clients: u64,
    pub num_workers: u64,
    pub num_requests: u64,
}

/// A SHA-256 digest.
#[derive(Clone, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    pub fn new(input: [u8; 32]) -> Self {
        Sha256Digest(input)
    }

    /// Verify that two digests match. If not, return a [`Sha256DigestVerificationError`].
    pub fn verify(&self, expected: &Self) -> Result<(), Sha256DigestVerificationError> {
        if *self != *expected {
            Err(Sha256DigestVerificationError::new(
                self.clone(),
                expected.clone(),
            ))
        } else {
            Ok(())
        }
    }
}

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

impl FromStr for Sha256Digest {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
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

impl Display for Sha256Digest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.debug_tuple("Sha256Digest").field(&self.0).finish()
        } else {
            write!(f, "Sha256Digest({})", self)
        }
    }
}

/// Error indicating that two digests that should have matched didn't.
#[derive(Debug)]
pub struct Sha256DigestVerificationError {
    pub actual: Sha256Digest,
    pub expected: Sha256Digest,
}

impl Sha256DigestVerificationError {
    pub fn new(actual: Sha256Digest, expected: Sha256Digest) -> Self {
        Sha256DigestVerificationError { actual, expected }
    }
}

impl Display for Sha256DigestVerificationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "mismatched SHA-256 digest (expected {}, found {})",
            self.expected, self.actual,
        )
    }
}

impl Error for Sha256DigestVerificationError {}

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
