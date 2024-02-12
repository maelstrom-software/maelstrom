//! Core structs used by the broker, worker, and clients. Everything in this crate must be usable
//! from wasm.

pub mod manifest;
pub mod proto;
pub mod ring_buffer;
pub mod stats;

pub use camino::{Utf8Path, Utf8PathBuf};
use derive_more::{Constructor, Display, From};
use enumset::EnumSetType;
pub use enumset::{enum_set, EnumSet};
use hex::{self, FromHexError};
pub use nonempty::{nonempty, NonEmpty};
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fmt::{self, Debug, Formatter},
    hash::Hash,
    result::Result,
    str::{self, FromStr},
};

/// ID of a client connection. These share the same ID space as [`WorkerId`].
#[derive(
    Copy, Clone, Debug, Deserialize, Display, Eq, From, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct ClientId(u32);

/// A client-relative job ID. Clients can assign these however they like.
#[derive(
    Copy, Clone, Debug, Deserialize, Display, Eq, From, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct ClientJobId(u32);

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub enum ArtifactType {
    /// A .tar file
    Tar,
    /// A serialized `Manifest`
    Manifest,
}

/// An absolute job ID that includes a [`ClientId`] for disambiguation.
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct JobId {
    pub cid: ClientId,
    pub cjid: ClientJobId,
}

#[derive(Debug, Deserialize, EnumSetType, Serialize)]
#[enumset(serialize_deny_unknown)]
pub enum JobDevice {
    Full,
    Null,
    Random,
    Tty,
    Urandom,
    Zero,
}

#[derive(Debug, Deserialize, EnumSetType, Serialize)]
#[serde(rename_all = "kebab-case")]
#[enumset(serialize_repr = "list")]
pub enum JobDeviceListDeserialize {
    Full,
    Null,
    Random,
    Tty,
    Urandom,
    Zero,
}

impl From<JobDeviceListDeserialize> for JobDevice {
    fn from(value: JobDeviceListDeserialize) -> JobDevice {
        match value {
            JobDeviceListDeserialize::Full => JobDevice::Full,
            JobDeviceListDeserialize::Null => JobDevice::Null,
            JobDeviceListDeserialize::Random => JobDevice::Random,
            JobDeviceListDeserialize::Tty => JobDevice::Tty,
            JobDeviceListDeserialize::Urandom => JobDevice::Urandom,
            JobDeviceListDeserialize::Zero => JobDevice::Zero,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum JobMountFsType {
    Proc,
    Tmp,
    Sys,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JobMount {
    pub fs_type: JobMountFsType,
    pub mount_point: Utf8PathBuf,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct PrefixOptions {
    pub strip_prefix: Option<Utf8PathBuf>,
    pub prepend_prefix: Option<Utf8PathBuf>,
    #[serde(default)]
    pub canonicalize: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SymlinkSpec {
    pub link: Utf8PathBuf,
    pub target: Utf8PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(untagged, deny_unknown_fields, rename_all = "snake_case")]
pub enum Layer {
    Tar {
        #[serde(rename = "tar")]
        path: Utf8PathBuf,
    },
    Glob {
        glob: String,
        #[serde(flatten)]
        prefix_options: PrefixOptions,
    },
    Paths {
        paths: Vec<Utf8PathBuf>,
        #[serde(flatten)]
        prefix_options: PrefixOptions,
    },
    Stubs {
        stubs: Vec<Utf8PathBuf>,
    },
    Symlinks {
        symlinks: Vec<SymlinkSpec>,
    },
}

/// ID of a user. This should be compatible with uid_t.
#[derive(
    Copy, Clone, Debug, Deserialize, Display, Eq, From, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct UserId(u32);

/// ID of a group. This should be compatible with gid_t.
#[derive(
    Copy, Clone, Debug, Deserialize, Display, Eq, From, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct GroupId(u32);

/// All necessary information for the worker to execute a job.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobSpec {
    pub program: Utf8PathBuf,
    pub arguments: Vec<String>,
    pub environment: Vec<String>,
    pub layers: NonEmpty<(Sha256Digest, ArtifactType)>,
    pub devices: EnumSet<JobDevice>,
    pub mounts: Vec<JobMount>,
    pub enable_loopback: bool,
    pub enable_writable_file_system: bool,
    pub working_directory: Utf8PathBuf,
    pub user: UserId,
    pub group: GroupId,
}

impl JobSpec {
    pub fn new(
        program: impl Into<String>,
        layers: impl Into<NonEmpty<(Sha256Digest, ArtifactType)>>,
    ) -> Self {
        JobSpec {
            program: program.into().into(),
            layers: layers.into(),
            arguments: Default::default(),
            environment: Default::default(),
            devices: Default::default(),
            mounts: Default::default(),
            enable_loopback: false,
            enable_writable_file_system: Default::default(),
            working_directory: Utf8PathBuf::from("/"),
            user: UserId::from(0),
            group: GroupId::from(0),
        }
    }

    pub fn arguments<I, T>(mut self, arguments: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.arguments = arguments.into_iter().map(Into::into).collect();
        self
    }

    pub fn environment<I, T>(mut self, environment: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.environment = environment.into_iter().map(Into::into).collect();
        self
    }

    pub fn devices(mut self, devices: impl IntoIterator<Item = JobDevice>) -> Self {
        self.devices = devices.into_iter().collect();
        self
    }

    pub fn mounts(mut self, mounts: impl IntoIterator<Item = JobMount>) -> Self {
        self.mounts = mounts.into_iter().collect();
        self
    }

    pub fn enable_loopback(mut self, enable_loopback: bool) -> Self {
        self.enable_loopback = enable_loopback;
        self
    }

    pub fn enable_writable_file_system(mut self, enable_writable_file_system: bool) -> Self {
        self.enable_writable_file_system = enable_writable_file_system;
        self
    }

    pub fn working_directory(mut self, working_directory: impl Into<Utf8PathBuf>) -> Self {
        self.working_directory = working_directory.into();
        self
    }

    pub fn user(mut self, user: impl Into<UserId>) -> Self {
        self.user = user.into();
        self
    }

    pub fn group(mut self, group: impl Into<GroupId>) -> Self {
        self.group = group.into();
        self
    }
}

/// How a job's process terminated. A process can either exit of its own accord or be killed by a
/// signal.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum JobStatus {
    Exited(u8),
    Signaled(u8),
}

/// The result for stdout or stderr for a job.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub enum JobOutputResult {
    /// There was no output.
    None,

    /// The output is contained in the provided slice.
    Inline(Box<[u8]>),

    /// The output was truncated to the provided slice, the size of which is based on the job
    /// request. The actual size of the output is also provided, though the remaining bytes will
    /// have been thrown away.
    Truncated { first: Box<[u8]>, truncated: u64 },
    /*
     * To come:
    /// The output was stored in a digest, and is of the provided size.
    External(Sha256Digest, u64),
    */
}

impl Debug for JobOutputResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            JobOutputResult::None => f.debug_tuple("None").finish(),
            JobOutputResult::Inline(bytes) => {
                let pretty_bytes = String::from_utf8_lossy(bytes);
                f.debug_tuple("Inline").field(&pretty_bytes).finish()
            }
            JobOutputResult::Truncated { first, truncated } => {
                let pretty_first = String::from_utf8_lossy(first);
                f.debug_struct("Truncated")
                    .field("first", &pretty_first)
                    .field("truncated", truncated)
                    .finish()
            }
        }
    }
}

/// The output of a job that was successfully executed. This doesn't mean that the client will be
/// happy with the job's output. Maybe the job was killed by a signal, or it exited with an error
/// status code. From our point of view, it doesn't matter. We ran the job until it was terminated,
/// and gathered its output.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobSuccess {
    pub status: JobStatus,
    pub stdout: JobOutputResult,
    pub stderr: JobOutputResult,
}

/// A job failed to execute for some reason. We separate the universe of errors into "execution"
/// errors and "system" errors.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum JobError<T> {
    /// There was something wrong with the job that made it unable to be executed. This error
    /// indicates that there was something wrong with the job itself, and thus is obstensibly the
    /// fault of the client. An error of this type might happen if the execution path wasn't found, or
    /// if the binary couldn't be executed because it was for the wrong architecture.
    Execution(T),

    /// There was something wrong with the system that made it impossible to execute the job. There
    /// isn't anything different the client could do to mitigate this error. An error of this type
    /// might happen if the broker ran out of disk space, or there was a software error.
    System(T),
}

impl<T> JobError<T> {
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> JobError<U> {
        match self {
            JobError::Execution(e) => JobError::Execution(f(e)),
            JobError::System(e) => JobError::System(f(e)),
        }
    }
}

/// A common Result type in the worker.
pub type JobResult<T, E> = Result<T, JobError<E>>;

/// All relevant information about the outcome of a job. This is what's sent around between the
/// Worker, Broker, and Client.
pub type JobStringResult = JobResult<JobSuccess, String>;

/// ID of a worker connection. These share the same ID space as [`ClientId`].
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    Deserialize,
    Display,
    Eq,
    From,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub struct WorkerId(u32);

/// A SHA-256 digest.
#[derive(Clone, Constructor, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
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

impl From<u64> for Sha256Digest {
    fn from(input: u64) -> Self {
        let mut bytes = [0; 32];
        bytes[24..].copy_from_slice(&input.to_be_bytes());
        Sha256Digest(bytes)
    }
}

impl From<u32> for Sha256Digest {
    fn from(input: u32) -> Self {
        Sha256Digest::from(u64::from(input))
    }
}

impl FromStr for Sha256Digest {
    type Err = FromHexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut bytes = [0; 32];
        hex::decode_to_slice(value, &mut bytes).map(|_| Sha256Digest(bytes))
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut bytes = [0; 64];
        hex::encode_to_slice(self.0, &mut bytes).unwrap();
        f.pad(unsafe { str::from_utf8_unchecked(&bytes) })
    }
}

impl Debug for Sha256Digest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.debug_tuple("Sha256Digest").field(&self.0).finish()
        } else {
            f.pad(&format!("Sha256Digest({})", self))
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

impl fmt::Display for Sha256DigestVerificationError {
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
    fn client_id_display() {
        assert_eq!(format!("{}", ClientId::from(100)), "100");
        assert_eq!(format!("{}", ClientId::from(0)), "0");
        assert_eq!(format!("{:03}", ClientId::from(0)), "000");
        assert_eq!(format!("{:3}", ClientId::from(43)), " 43");
    }

    #[test]
    fn client_job_id_display() {
        assert_eq!(format!("{}", ClientJobId::from(100)), "100");
        assert_eq!(format!("{}", ClientJobId::from(0)), "0");
        assert_eq!(format!("{:03}", ClientJobId::from(0)), "000");
        assert_eq!(format!("{:3}", ClientJobId::from(43)), " 43");
    }

    #[test]
    fn user_id_display() {
        assert_eq!(format!("{}", UserId::from(100)), "100");
        assert_eq!(format!("{}", UserId::from(0)), "0");
        assert_eq!(format!("{:03}", UserId::from(0)), "000");
        assert_eq!(format!("{:3}", UserId::from(43)), " 43");
    }

    #[test]
    fn group_id_display() {
        assert_eq!(format!("{}", GroupId::from(100)), "100");
        assert_eq!(format!("{}", GroupId::from(0)), "0");
        assert_eq!(format!("{:03}", GroupId::from(0)), "000");
        assert_eq!(format!("{:3}", GroupId::from(43)), " 43");
    }

    #[test]
    fn worker_id_display() {
        assert_eq!(format!("{}", WorkerId::from(100)), "100");
        assert_eq!(format!("{}", WorkerId::from(0)), "0");
        assert_eq!(format!("{:03}", WorkerId::from(0)), "000");
        assert_eq!(format!("{:3}", WorkerId::from(43)), " 43");
    }

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
        assert_eq!(
            "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f0"
                .parse::<Sha256Digest>()
                .unwrap_err(),
            FromHexError::OddLength
        );
        assert_eq!(
            "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f0f"
                .parse::<Sha256Digest>()
                .unwrap_err(),
            FromHexError::InvalidStringLength
        );
        assert_eq!(
            "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e"
                .parse::<Sha256Digest>()
                .unwrap_err(),
            FromHexError::InvalidStringLength
        );
    }

    #[test]
    fn from_str_bad_chars() {
        assert_eq!(
            " 01112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f"
                .parse::<Sha256Digest>()
                .unwrap_err(),
            FromHexError::InvalidHexCharacter { c: ' ', index: 0 }
        );
        assert_eq!(
            "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2g"
                .parse::<Sha256Digest>()
                .unwrap_err(),
            FromHexError::InvalidHexCharacter { c: 'g', index: 63 }
        );
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
            format!("{d:80?}"),
            "Sha256Digest(101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f)  "
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
