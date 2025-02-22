//! Core structs used by the broker, worker, and clients. Everything in this crate must be usable
//! from wasm.

pub mod manifest;
pub mod proto;
pub mod ring_buffer;
pub mod stats;
pub mod tty;

pub use camino::{Utf8Component, Utf8Path, Utf8PathBuf};
pub use enumset::{enum_set, EnumSet};
pub use nonempty::{nonempty, NonEmpty};

use derive_more::{with_trait::Debug, Constructor, Display, From, Into};
use enumset::EnumSetType;
use get_size::GetSize;
use hex::{self, FromHexError};
use maelstrom_macro::pocket_definition;
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fmt::{self, Formatter},
    hash::Hash,
    num::NonZeroU32,
    result::Result,
    str::{self, FromStr},
    time::Duration,
};
use strum::{EnumCount, EnumIter};

/// ID of a client connection. These share the same ID space as [`WorkerId`] and [`MonitorId`].
#[derive(
    Copy, Clone, Debug, Deserialize, Display, Eq, From, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct ClientId(u32);

impl ClientId {
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

/// A client-relative job ID. Clients can assign these however they like.
#[pocket_definition(export)]
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    From,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    Into,
)]
pub struct ClientJobId(u32);

impl ClientJobId {
    pub fn from_u32(v: u32) -> Self {
        Self(v)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum ArtifactType {
    /// A .tar file
    Tar,
    /// A serialized `Manifest`
    Manifest,
}

#[macro_export]
macro_rules! tar_digest {
    ($digest:expr) => {
        ($crate::digest!($digest), $crate::ArtifactType::Tar)
    };
}

#[macro_export]
macro_rules! manifest_digest {
    ($digest:expr) => {
        ($crate::digest!($digest), $crate::ArtifactType::Manifest)
    };
}

/// An absolute job ID that includes a [`ClientId`] for disambiguation.
#[derive(
    Copy, Clone, Debug, Deserialize, Display, Eq, From, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[display("{cid}.{cjid}")]
#[from(forward)]
pub struct JobId {
    pub cid: ClientId,
    pub cjid: ClientJobId,
}

#[pocket_definition(export)]
#[derive(Debug, Deserialize, EnumIter, EnumSetType, Serialize)]
pub enum JobDevice {
    Full,
    Fuse,
    Null,
    Random,
    Shm,
    Tty,
    Urandom,
    Zero,
}

#[derive(Debug, Deserialize, EnumCount, EnumSetType, Serialize)]
#[serde(rename_all = "kebab-case")]
#[enumset(serialize_repr = "list")]
pub enum JobDeviceForTomlAndJson {
    Full,
    Fuse,
    Null,
    Random,
    Shm,
    Tty,
    Urandom,
    Zero,
}

impl From<JobDeviceForTomlAndJson> for JobDevice {
    fn from(value: JobDeviceForTomlAndJson) -> JobDevice {
        match value {
            JobDeviceForTomlAndJson::Full => JobDevice::Full,
            JobDeviceForTomlAndJson::Fuse => JobDevice::Fuse,
            JobDeviceForTomlAndJson::Null => JobDevice::Null,
            JobDeviceForTomlAndJson::Random => JobDevice::Random,
            JobDeviceForTomlAndJson::Shm => JobDevice::Shm,
            JobDeviceForTomlAndJson::Tty => JobDevice::Tty,
            JobDeviceForTomlAndJson::Urandom => JobDevice::Urandom,
            JobDeviceForTomlAndJson::Zero => JobDevice::Zero,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Into)]
#[serde(transparent)]
pub struct NonRootUtf8PathBuf(Utf8PathBuf);

impl<'de> Deserialize<'de> for NonRootUtf8PathBuf {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        use serde::de::Error as _;
        let path = Utf8PathBuf::deserialize(deserializer)?;
        path.try_into()
            .map_err(|e: NonRootUtf8PathBufTryFromError| D::Error::custom(e.to_string()))
    }
}

#[derive(Debug, Display)]
#[display("a path of \"/\" is not allowed")]
pub struct NonRootUtf8PathBufTryFromError;

impl Error for NonRootUtf8PathBufTryFromError {}

impl TryFrom<Utf8PathBuf> for NonRootUtf8PathBuf {
    type Error = NonRootUtf8PathBufTryFromError;

    fn try_from(v: Utf8PathBuf) -> Result<Self, Self::Error> {
        if v == "/" {
            return Err(NonRootUtf8PathBufTryFromError);
        }
        Ok(Self(v))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub enum JobMountForTomlAndJson {
    Bind {
        mount_point: NonRootUtf8PathBuf,
        local_path: Utf8PathBuf,
        #[serde(default)]
        read_only: bool,
    },
    Devices {
        devices: EnumSet<JobDeviceForTomlAndJson>,
    },
    Devpts {
        mount_point: NonRootUtf8PathBuf,
    },
    Mqueue {
        mount_point: NonRootUtf8PathBuf,
    },
    Proc {
        mount_point: NonRootUtf8PathBuf,
    },
    Sys {
        mount_point: NonRootUtf8PathBuf,
    },
    Tmp {
        mount_point: NonRootUtf8PathBuf,
    },
}

#[pocket_definition(export)]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum JobMount {
    Bind {
        mount_point: Utf8PathBuf,
        local_path: Utf8PathBuf,
        read_only: bool,
    },
    Devices {
        devices: EnumSet<JobDevice>,
    },
    Devpts {
        mount_point: Utf8PathBuf,
    },
    Mqueue {
        mount_point: Utf8PathBuf,
    },
    Proc {
        mount_point: Utf8PathBuf,
    },
    Sys {
        mount_point: Utf8PathBuf,
    },
    Tmp {
        mount_point: Utf8PathBuf,
    },
}

#[macro_export]
macro_rules! sys_mount {
    ($mount_point:expr) => {
        $crate::JobMount::Sys {
            mount_point: $mount_point.into(),
        }
    };
}

#[macro_export]
macro_rules! proc_mount {
    ($mount_point:expr) => {
        $crate::JobMount::Proc {
            mount_point: $mount_point.into(),
        }
    };
}

#[macro_export]
macro_rules! tmp_mount {
    ($mount_point:expr) => {
        $crate::JobMount::Tmp {
            mount_point: $mount_point.into(),
        }
    };
}

#[macro_export]
macro_rules! devices_mount {
    ($devices:expr) => {
        $crate::JobMount::Devices {
            devices: $devices.into_iter().map($crate::JobDevice::from).collect(),
        }
    };
}

impl From<JobMountForTomlAndJson> for JobMount {
    fn from(job_mount: JobMountForTomlAndJson) -> JobMount {
        match job_mount {
            JobMountForTomlAndJson::Bind {
                mount_point,
                local_path,
                read_only,
            } => JobMount::Bind {
                mount_point: mount_point.into(),
                local_path,
                read_only,
            },
            JobMountForTomlAndJson::Devices { devices } => JobMount::Devices {
                devices: devices.into_iter().map(JobDevice::from).collect(),
            },
            JobMountForTomlAndJson::Devpts { mount_point } => JobMount::Devpts {
                mount_point: mount_point.into(),
            },
            JobMountForTomlAndJson::Mqueue { mount_point } => JobMount::Mqueue {
                mount_point: mount_point.into(),
            },
            JobMountForTomlAndJson::Proc { mount_point } => JobMount::Proc {
                mount_point: mount_point.into(),
            },
            JobMountForTomlAndJson::Sys { mount_point } => JobMount::Sys {
                mount_point: mount_point.into(),
            },
            JobMountForTomlAndJson::Tmp { mount_point } => JobMount::Tmp {
                mount_point: mount_point.into(),
            },
        }
    }
}

#[pocket_definition(export)]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum JobNetwork {
    #[default]
    Disabled,
    Loopback,
    Local,
}

#[pocket_definition(export)]
#[derive(Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CaptureFileSystemChanges {
    pub upper: Utf8PathBuf,
    pub work: Utf8PathBuf,
}

#[pocket_definition(export)]
#[derive(Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum JobRootOverlay {
    #[default]
    None,
    Tmp,
    Local(CaptureFileSystemChanges),
}

/// ID of a user. This should be compatible with uid_t.
#[pocket_definition(export)]
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    From,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    Into,
)]
pub struct UserId(u32);

impl UserId {
    pub fn new(v: u32) -> Self {
        Self(v)
    }
}

/// ID of a group. This should be compatible with gid_t.
#[pocket_definition(export)]
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    From,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    Into,
)]
pub struct GroupId(u32);

impl GroupId {
    pub fn new(v: u32) -> Self {
        Self(v)
    }
}

/// A count of seconds.
#[pocket_definition(export)]
#[derive(
    Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Into,
)]
#[into(u32)]
pub struct Timeout(NonZeroU32);

impl Timeout {
    pub fn new(timeout: u32) -> Option<Self> {
        NonZeroU32::new(timeout).map(Self)
    }
}

impl TryFrom<u32> for Timeout {
    type Error = std::num::TryFromIntError;

    fn try_from(timeout: u32) -> std::result::Result<Self, std::num::TryFromIntError> {
        Ok(Self(timeout.try_into()?))
    }
}

impl From<Timeout> for Duration {
    fn from(timeout: Timeout) -> Duration {
        Duration::from_secs(timeout.0.get().into())
    }
}

/// The size of a terminal in characters.
#[pocket_definition(export)]
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WindowSize {
    pub rows: u16,
    pub columns: u16,
}

impl WindowSize {
    pub fn new(rows: u16, columns: u16) -> Self {
        Self { rows, columns }
    }
}

/// The parameters for a TTY for a job.
#[pocket_definition(export)]
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct JobTty {
    /// A Unix domain socket abstract address. We use exactly 6 bytes because that's how many bytes
    /// the autobind feature in Linux uses. The first byte will always be 0.
    pub socket_address: [u8; 6],

    /// The initial window size of the TTY. Window size updates may follow.
    pub window_size: WindowSize,
}

impl JobTty {
    pub fn new(socket_address: &[u8; 6], window_size: WindowSize) -> Self {
        let socket_address = *socket_address;
        Self {
            socket_address,
            window_size,
        }
    }
}

/// All necessary information for the worker to execute a job.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct JobSpec {
    pub program: Utf8PathBuf,
    pub arguments: Vec<String>,
    pub environment: Vec<String>,
    pub layers: NonEmpty<(Sha256Digest, ArtifactType)>,
    pub mounts: Vec<JobMount>,
    pub network: JobNetwork,
    pub root_overlay: JobRootOverlay,
    pub working_directory: Utf8PathBuf,
    pub user: UserId,
    pub group: GroupId,
    pub timeout: Option<Timeout>,
    pub estimated_duration: Option<Duration>,
    pub allocate_tty: Option<JobTty>,
    pub priority: i8,
}

impl JobSpec {
    pub fn must_be_run_locally(&self) -> bool {
        self.network == JobNetwork::Local
            || self
                .mounts
                .iter()
                .any(|mount| matches!(mount, JobMount::Bind { .. }))
            || matches!(&self.root_overlay, JobRootOverlay::Local { .. })
            || self.allocate_tty.is_some()
    }
}

#[macro_export]
macro_rules! job_spec {
    (@expand [$program:expr, [$($layer:expr),+ $(,)?]] [] -> [$($($fields:tt)+)?]) => {
        $crate::JobSpec {
            $($($fields)+,)?
            .. $crate::JobSpec {
                program: $program.into(),
                arguments: Default::default(),
                environment: Default::default(),
                layers: $crate::nonempty![$($layer),+],
                mounts: Default::default(),
                network: Default::default(),
                root_overlay: Default::default(),
                working_directory: "/".into(),
                user: 0.into(),
                group: 0.into(),
                timeout: Default::default(),
                estimated_duration: Default::default(),
                allocate_tty: Default::default(),
                priority: Default::default(),
            }
        }
    };
    (@expand [$($required:tt)+] [arguments: [$($($argument:expr),+ $(,)?)?] $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? arguments: vec![$($($argument.into()),+)?]
        ])
    };
    (@expand [$($required:tt)+] [environment: [$($($var:expr),+ $(,)?)?] $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? environment: vec![$($($var.into()),+)?]
        ])
    };
    (@expand [$($required:tt)+] [mounts: [$($mount:tt)*] $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? mounts: vec![$($mount)*]])
    };
    (@expand [$($required:tt)+] [network: $network:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? network: $network])
    };
    (@expand [$($required:tt)+] [root_overlay: $root_overlay:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? root_overlay: $root_overlay])
    };
    (@expand [$($required:tt)+] [working_directory: $working_directory:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? working_directory: $working_directory.into()])
    };
    (@expand [$($required:tt)+] [user: $user:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? user: $user.into()])
    };
    (@expand [$($required:tt)+] [group: $group:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? group: $group.into()])
    };
    (@expand [$($required:tt)+] [timeout: $timeout:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? timeout: $crate::Timeout::new($timeout)])
    };
    (@expand [$($required:tt)+] [estimated_duration: $duration:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? estimated_duration: Some($duration)])
    };
    (@expand [$($required:tt)+] [allocate_tty: $tty:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? allocate_tty: Some($tty)])
    };
    (@expand [$($required:tt)+] [priority: $priority:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        $crate::job_spec!(@expand [$($required)+] [$($($field_in)*)?] ->
            [$($($field_out)+,)? priority: $priority])
    };
    ($program:expr, [$($layer:expr),+ $(,)?] $(,$($field_in:tt)*)?) => {
        $crate::job_spec!(@expand [$program, [$($layer),+]] [$($($field_in)*)?] -> [])
    };
}

/// How a job's process terminated. A process can either exit of its own accord or be killed by a
/// signal.
#[pocket_definition(export)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum JobTerminationStatus {
    Exited(u8),
    Signaled(u8),
}

/// The result for stdout or stderr for a job.
#[pocket_definition(export)]
#[derive(Clone, Debug, Display, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum JobOutputResult {
    /// There was no output.
    None,

    /// The output is contained in the provided slice.
    #[display("{}", String::from_utf8_lossy(_0))]
    Inline(#[debug("{}", String::from_utf8_lossy(_0))] Box<[u8]>),

    /// The output was truncated to the provided slice, the size of which is based on the job
    /// request. The actual size of the output is also provided, though the remaining bytes will
    /// have been thrown away.
    #[display("{}<{truncated} bytes truncated>", String::from_utf8_lossy(first))]
    Truncated {
        #[debug("{}", String::from_utf8_lossy(first))]
        first: Box<[u8]>,
        truncated: u64,
    },
    /*
     * To come:
    /// The output was stored in a digest, and is of the provided size.
    External(Sha256Digest, u64),
    */
}

/// The output and duration of a job that ran for some amount of time. This is generated regardless
/// of how the job terminated. From our point of view, it doesn't matter. We ran the job until it
/// was terminated, and gathered its output.
#[pocket_definition(export)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct JobEffects {
    pub stdout: JobOutputResult,
    pub stderr: JobOutputResult,
    pub duration: Duration,
}

/// The outcome of a completed job. That is, a job that ran to completion, instead of timing out,
/// being canceled, etc.
#[pocket_definition(export)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct JobCompleted {
    pub status: JobTerminationStatus,
    pub effects: JobEffects,
}

/// The outcome of a job. This doesn't include error outcomes, which are handled with JobError.
#[pocket_definition(export)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum JobOutcome {
    Completed(JobCompleted),
    TimedOut(JobEffects),
}

/// A job failed to execute for some reason. We separate the universe of errors into "execution"
/// errors and "system" errors.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
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
pub type JobOutcomeResult = JobResult<JobOutcome, String>;

#[pocket_definition(export)]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum JobWorkerStatus {
    WaitingForLayers,
    WaitingToExecute,
    Executing,
}

#[pocket_definition(export)]
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum JobBrokerStatus {
    WaitingForLayers,
    WaitingForWorker,
    AtWorker(WorkerId, JobWorkerStatus),
}

/// ID of a worker connection. These share the same ID space as [`ClientId`] and [`MonitorId`].
#[pocket_definition(export)]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Display,
    Eq,
    From,
    Hash,
    Into,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub struct WorkerId(u32);

/// ID of a monitor connection. These share the same ID space as [`ClientId`] and [`WorkerId`].
#[derive(
    Copy, Clone, Debug, Deserialize, Display, Eq, From, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct MonitorId(u32);

impl MonitorId {
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ArtifactUploadLocation {
    TcpUpload,
    Remote,
}

/// A SHA-256 digest.
#[derive(
    Clone, Constructor, Debug, Deserialize, Eq, GetSize, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct Sha256Digest(#[debug("{self}")] [u8; 32]);

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

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Debug, Display)]
#[display("failed to convert to SHA-256 digest")]
pub struct Sha256DigestTryFromError;

impl Error for Sha256DigestTryFromError {}

impl TryFrom<Vec<u8>> for Sha256Digest {
    type Error = Sha256DigestTryFromError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(
            bytes.try_into().map_err(|_| Sha256DigestTryFromError)?,
        ))
    }
}

impl From<Sha256Digest> for Vec<u8> {
    fn from(d: Sha256Digest) -> Self {
        d.0.to_vec()
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

#[macro_export]
macro_rules! digest {
    ($n:expr) => {
        $crate::Sha256Digest::from(u64::try_from($n).unwrap())
    };
}

/// Error indicating that two digests that should have matched didn't.
#[derive(Debug, Display)]
#[display("mismatched SHA-256 digest (expected {expected}, found {actual})")]
pub struct Sha256DigestVerificationError {
    pub actual: Sha256Digest,
    pub expected: Sha256Digest,
}

impl Sha256DigestVerificationError {
    pub fn new(actual: Sha256Digest, expected: Sha256Digest) -> Self {
        Sha256DigestVerificationError { actual, expected }
    }
}

impl Error for Sha256DigestVerificationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use enumset::enum_set;
    use heck::ToKebabCase;
    use indoc::indoc;
    use strum::IntoEnumIterator as _;

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
    fn job_id_from() {
        assert_eq!(
            JobId {
                cid: ClientId::from(1),
                cjid: ClientJobId::from(2)
            },
            JobId::from((1, 2))
        );
    }

    #[test]
    fn job_id_display() {
        assert_eq!(format!("{}", JobId::from((0, 0))), "0.0");
    }

    #[test]
    fn sha256_digest_from_u64() {
        assert_eq!(
            Sha256Digest::from(0x123456789ABCDEF0u64),
            Sha256Digest([
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x12, 0x34,
                0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
            ])
        );
    }

    #[test]
    fn sha256_digest_from_str_ok() {
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
    fn sha256_digest_from_str_wrong_length() {
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
    fn sha256_digest_from_str_bad_chars() {
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
    fn sha256_digest_display_round_trip() {
        let s = "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f";
        assert_eq!(s, s.parse::<Sha256Digest>().unwrap().to_string());
    }

    #[test]
    fn sha256_digest_display_padding() {
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
    fn sha256_digest_debug() {
        let d = "101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f"
            .parse::<Sha256Digest>()
            .unwrap();
        assert_eq!(
            format!("{d:?}"),
            "Sha256Digest(101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f)"
        );
        assert_eq!(
            format!("{d:80?}"),
            "Sha256Digest(101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f)"
        );
        assert_eq!(
            format!("{d:#?}"),
            indoc! {
                "Sha256Digest(
                    101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f,
                )"
            }
        );
    }

    trait AssertError {
        fn assert_error(&self, expected: &str);
    }

    impl AssertError for toml::de::Error {
        fn assert_error(&self, expected: &str) {
            let message = self.message();
            assert!(message.starts_with(expected), "message: {message}");
        }
    }

    #[track_caller]
    fn deserialize_value<T: for<'a> Deserialize<'a>>(file: &str) -> T {
        T::deserialize(toml::de::ValueDeserializer::new(file)).unwrap()
    }

    #[track_caller]
    fn deserialize_value_error<T: for<'a> Deserialize<'a> + Debug>(file: &str) -> toml::de::Error {
        match T::deserialize(toml::de::ValueDeserializer::new(file)) {
            Err(err) => err,
            Ok(val) => panic!("expected a toml error but instead got value: {val:?}"),
        }
    }

    #[test]
    fn job_device_for_toml_and_json_enum_set_deserializes_from_list() {
        let devices: EnumSet<JobDeviceForTomlAndJson> = deserialize_value(r#"["full", "null"]"#);
        let devices: EnumSet<_> = devices.into_iter().map(Into::<JobDevice>::into).collect();
        assert_eq!(devices, enum_set!(JobDevice::Full | JobDevice::Null));
    }

    #[test]
    fn job_device_for_toml_and_json_enum_set_deserialize_unknown_field() {
        deserialize_value_error::<EnumSet<JobDeviceForTomlAndJson>>(r#"["bull", "null"]"#)
            .assert_error("unknown variant `bull`");
    }

    #[test]
    fn job_device_for_toml_and_json_and_job_device_match() {
        for job_device in JobDevice::iter() {
            let repr = format!(r#""{}""#, format!("{job_device:?}").to_kebab_case());
            assert_eq!(
                JobDevice::from(deserialize_value::<JobDeviceForTomlAndJson>(&repr)),
                job_device
            );
        }
        assert_eq!(JobDevice::iter().count(), JobDeviceForTomlAndJson::COUNT);
    }

    #[test]
    fn non_root_utf8_path_buf_deserialize_not_root() {
        let path_buf: NonRootUtf8PathBuf = deserialize_value(r#""foo""#);
        assert_eq!(path_buf, NonRootUtf8PathBuf("foo".into()));
    }

    #[test]
    fn non_root_utf8_path_buf_deserialize_root() {
        deserialize_value_error::<NonRootUtf8PathBuf>(r#""/""#)
            .assert_error(r#"a path of "/" is not allowed"#);
    }

    #[test]
    fn job_mount_for_toml_and_json_deserialize() {
        let job_mounts: Vec<JobMountForTomlAndJson> = deserialize_value(
            r#"[
                { type = "bind", mount_point = "/mnt", local_path = "/a", read_only = true },
                { type = "devices", devices = [ "tty", "shm" ] },
                { type = "devpts", mount_point = "/dev/pts" },
                { type = "mqueue", mount_point = "/dev/mqueue" },
                { type = "proc", mount_point = "/proc" },
                { type = "sys", mount_point = "/sys" },
                { type = "tmp", mount_point = "/tmp" },
            ]"#,
        );
        let job_mounts: Vec<JobMount> = job_mounts.into_iter().map(|mount| mount.into()).collect();
        assert_eq!(
            job_mounts,
            vec![
                JobMount::Bind {
                    mount_point: "/mnt".into(),
                    local_path: "/a".into(),
                    read_only: true,
                },
                JobMount::Devices {
                    devices: enum_set!(JobDevice::Tty | JobDevice::Shm),
                },
                JobMount::Devpts {
                    mount_point: "/dev/pts".into(),
                },
                JobMount::Mqueue {
                    mount_point: "/dev/mqueue".into(),
                },
                JobMount::Proc {
                    mount_point: "/proc".into(),
                },
                JobMount::Sys {
                    mount_point: "/sys".into(),
                },
                JobMount::Tmp {
                    mount_point: "/tmp".into(),
                },
            ]
        );
    }

    #[test]
    fn job_mount_for_toml_and_json_deserialize_invalid() {
        deserialize_value_error::<JobMountForTomlAndJson>(
            r#"{ type = "foo", mount_point = "/mnt" }"#,
        )
        .assert_error("unknown variant `foo`");
    }

    #[test]
    fn job_mount_for_toml_and_json_deserialize_bind_mount_missing_read_only() {
        let job_mount: JobMountForTomlAndJson =
            deserialize_value(r#"{ type = "bind", mount_point = "/mnt", local_path = "/a" }"#);
        let job_mount: JobMount = job_mount.into();
        assert_eq!(
            job_mount,
            JobMount::Bind {
                mount_point: "/mnt".into(),
                local_path: "/a".into(),
                read_only: false,
            },
        );
    }

    #[test]
    fn job_mount_for_toml_and_json_deserialize_root_mount_point() {
        let mounts = [
            r#"{ type = "bind", mount_point = "/", local_path = "/a" }"#,
            r#"{ type = "devpts", mount_point = "/" }"#,
            r#"{ type = "mqueue", mount_point = "/" }"#,
            r#"{ type = "proc", mount_point = "/" }"#,
            r#"{ type = "sys", mount_point = "/" }"#,
            r#"{ type = "tmp", mount_point = "/" }"#,
        ];
        for mount in mounts {
            deserialize_value_error::<JobMountForTomlAndJson>(mount)
                .assert_error(r#"a path of "/" is not allowed"#);
        }
    }

    #[test]
    fn job_network_deserialize() {
        assert_eq!(
            deserialize_value::<JobNetwork>(r#""disabled""#),
            JobNetwork::Disabled
        );
        assert_eq!(
            deserialize_value::<JobNetwork>(r#""loopback""#),
            JobNetwork::Loopback
        );
        assert_eq!(
            deserialize_value::<JobNetwork>(r#""local""#),
            JobNetwork::Local
        );
        deserialize_value_error::<JobNetwork>(r#""foo""#).assert_error("unknown variant `foo`");
    }

    #[test]
    fn job_spec_must_be_run_locally_network() {
        let spec = job_spec!("foo", [tar_digest!(0)]);
        assert!(!spec.must_be_run_locally());

        let spec = job_spec!("foo", [tar_digest!(0)], network: JobNetwork::Loopback);
        assert!(!spec.must_be_run_locally());

        let spec = job_spec!("foo", [tar_digest!(0)], network: JobNetwork::Local);
        assert!(spec.must_be_run_locally());

        let spec = job_spec!("foo", [tar_digest!(0)], network: JobNetwork::Disabled);
        assert!(!spec.must_be_run_locally());
    }

    #[test]
    fn job_spec_must_be_run_locally_mounts() {
        let spec = job_spec!("foo", [tar_digest!(0)]);
        assert!(!spec.must_be_run_locally());

        let spec = job_spec! {
            "foo",
            [tar_digest!(0)],
            mounts: [
                JobMount::Sys {
                    mount_point: Utf8PathBuf::from("/sys"),
                },
                JobMount::Bind {
                    mount_point: Utf8PathBuf::from("/bind"),
                    local_path: Utf8PathBuf::from("/a"),
                    read_only: false,
                },
            ],
        };
        assert!(spec.must_be_run_locally());
    }

    #[test]
    fn job_spec_must_be_run_locally_root_overlay() {
        let spec = job_spec!("foo", [tar_digest!(0)]);
        assert!(!spec.must_be_run_locally());

        let spec = job_spec!("foo", [tar_digest!(0)], root_overlay: JobRootOverlay::None);
        assert!(!spec.must_be_run_locally());

        let spec = job_spec!("foo", [tar_digest!(0)], root_overlay: JobRootOverlay::Tmp);
        assert!(!spec.must_be_run_locally());

        let spec = job_spec! {
            "foo",
            [tar_digest!(0)],
            root_overlay: JobRootOverlay::Local(CaptureFileSystemChanges {
                upper: "upper".into(),
                work: "work".into(),
            }),
        };
        assert!(spec.must_be_run_locally());
    }

    #[test]
    fn job_spec_must_be_run_locally_allocate_tty() {
        let spec = job_spec!("foo", [tar_digest!(0)]);
        assert!(!spec.must_be_run_locally());

        let spec = job_spec! {
            "foo",
            [tar_digest!(0)],
            allocate_tty: JobTty::new(b"\0abcde", WindowSize::new(20, 80)),
        };
        assert!(spec.must_be_run_locally());
    }
}
