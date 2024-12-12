use crate::proto;
use anyhow::{anyhow, Result};
use enumset::{EnumSet, EnumSetType};
use maelstrom_base::{
    capture_file_system_changes_pocket_definition, client_job_id_pocket_definition,
    group_id_pocket_definition, job_broker_status_pocket_definition,
    job_completed_pocket_definition, job_device_pocket_definition, job_effects_pocket_definition,
    job_mount_pocket_definition, job_network_pocket_definition, job_outcome_pocket_definition,
    job_output_result_pocket_definition, job_termination_status_pocket_definition,
    job_tty_pocket_definition, job_worker_status_pocket_definition, timeout_pocket_definition,
    user_id_pocket_definition, window_size_pocket_definition, worker_id_pocket_definition,
    CaptureFileSystemChanges, ClientJobId, GroupId, JobBrokerStatus, JobCompleted, JobDevice,
    JobEffects, JobMount, JobNetwork, JobOutcome, JobOutputResult, JobTerminationStatus, JobTty,
    JobWorkerStatus, Timeout, UserId, Utf8PathBuf, WindowSize, WorkerId,
};
use maelstrom_macro::{
    into_proto_buf_remote_derive, remote_derive, try_from_proto_buf_remote_derive,
};
use maelstrom_util::{
    artifact_transfer_strategy_pocket_definition, broker_addr_pocket_definition,
    cache_size_pocket_definition,
    config::common::{ArtifactTransferStrategy, BrokerAddr, CacheSize, InlineLimit, Slots},
    inline_limit_pocket_definition, slots_pocket_definition,
};
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::hash::Hash;
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Path, PathBuf};

pub trait IntoProtoBuf {
    type ProtoBufType;

    fn into_proto_buf(self) -> Self::ProtoBufType;
}

pub trait TryFromProtoBuf: Sized {
    type ProtoBufType;

    fn try_from_proto_buf(v: Self::ProtoBufType) -> Result<Self>;
}

//  _           _ _ _        _
// | |__  _   _(_) | |_     (_)_ __
// | '_ \| | | | | | __|____| | '_ \
// | |_) | |_| | | | ||_____| | | | |
// |_.__/ \__,_|_|_|\__|    |_|_| |_|
//

impl IntoProtoBuf for () {
    type ProtoBufType = proto::Void;

    fn into_proto_buf(self) -> proto::Void {
        proto::Void {}
    }
}

impl TryFromProtoBuf for () {
    type ProtoBufType = proto::Void;

    fn try_from_proto_buf(_: proto::Void) -> Result<()> {
        Ok(())
    }
}

impl IntoProtoBuf for bool {
    type ProtoBufType = bool;

    fn into_proto_buf(self) -> bool {
        self
    }
}

impl TryFromProtoBuf for bool {
    type ProtoBufType = bool;

    fn try_from_proto_buf(v: bool) -> Result<Self> {
        Ok(v)
    }
}

impl IntoProtoBuf for i8 {
    type ProtoBufType = i32;

    fn into_proto_buf(self) -> i32 {
        self as i32
    }
}

impl TryFromProtoBuf for i8 {
    type ProtoBufType = i32;

    fn try_from_proto_buf(v: i32) -> Result<Self> {
        Ok(v.try_into()?)
    }
}

impl IntoProtoBuf for u8 {
    type ProtoBufType = u32;

    fn into_proto_buf(self) -> u32 {
        self as u32
    }
}

impl TryFromProtoBuf for u8 {
    type ProtoBufType = u32;

    fn try_from_proto_buf(v: u32) -> Result<Self> {
        Ok(v.try_into()?)
    }
}

impl IntoProtoBuf for u16 {
    type ProtoBufType = u32;

    fn into_proto_buf(self) -> u32 {
        self as u32
    }
}

impl TryFromProtoBuf for u16 {
    type ProtoBufType = u32;

    fn try_from_proto_buf(v: u32) -> Result<Self> {
        Ok(v.try_into()?)
    }
}

impl IntoProtoBuf for u32 {
    type ProtoBufType = u32;

    fn into_proto_buf(self) -> u32 {
        self
    }
}

impl TryFromProtoBuf for u32 {
    type ProtoBufType = u32;

    fn try_from_proto_buf(v: u32) -> Result<Self> {
        Ok(v)
    }
}

impl IntoProtoBuf for u64 {
    type ProtoBufType = u64;

    fn into_proto_buf(self) -> u64 {
        self
    }
}

impl TryFromProtoBuf for u64 {
    type ProtoBufType = u64;

    fn try_from_proto_buf(v: u64) -> Result<Self> {
        Ok(v)
    }
}

impl<const LEN: usize> IntoProtoBuf for [u8; LEN] {
    type ProtoBufType = Vec<u8>;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        self.as_slice().to_owned()
    }
}

impl<const LEN: usize> TryFromProtoBuf for [u8; LEN] {
    type ProtoBufType = Vec<u8>;

    fn try_from_proto_buf(v: Self::ProtoBufType) -> Result<Self> {
        v.try_into().map_err(|_| anyhow!("malformed array"))
    }
}

impl IntoProtoBuf for Box<[u8]> {
    type ProtoBufType = Vec<u8>;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        self.into()
    }
}

impl TryFromProtoBuf for Box<[u8]> {
    type ProtoBufType = Vec<u8>;

    fn try_from_proto_buf(v: Self::ProtoBufType) -> Result<Self> {
        Ok(v.into())
    }
}

//      _      _
//  ___| |_ __| |
// / __| __/ _` |
// \__ \ || (_| |
// |___/\__\__,_|
//

impl IntoProtoBuf for String {
    type ProtoBufType = String;

    fn into_proto_buf(self) -> String {
        self
    }
}

impl TryFromProtoBuf for String {
    type ProtoBufType = String;

    fn try_from_proto_buf(v: String) -> Result<Self> {
        Ok(v)
    }
}

impl IntoProtoBuf for &Path {
    type ProtoBufType = Vec<u8>;

    fn into_proto_buf(self) -> Vec<u8> {
        self.as_os_str().as_encoded_bytes().to_vec()
    }
}

impl IntoProtoBuf for PathBuf {
    type ProtoBufType = Vec<u8>;

    fn into_proto_buf(self) -> Vec<u8> {
        self.into_os_string().into_encoded_bytes()
    }
}

impl TryFromProtoBuf for PathBuf {
    type ProtoBufType = Vec<u8>;

    fn try_from_proto_buf(b: Vec<u8>) -> Result<Self> {
        Ok(OsString::from_vec(b).into())
    }
}

impl<V: IntoProtoBuf> IntoProtoBuf for Option<V> {
    type ProtoBufType = Option<V::ProtoBufType>;

    fn into_proto_buf(self) -> Option<V::ProtoBufType> {
        self.map(|v| v.into_proto_buf())
    }
}

impl<V: TryFromProtoBuf> TryFromProtoBuf for Option<V> {
    type ProtoBufType = Option<V::ProtoBufType>;

    fn try_from_proto_buf(v: Self::ProtoBufType) -> Result<Self> {
        v.map(|e| TryFromProtoBuf::try_from_proto_buf(e))
            .transpose()
    }
}

impl<V: IntoProtoBuf> IntoProtoBuf for Vec<V> {
    type ProtoBufType = Vec<V::ProtoBufType>;

    fn into_proto_buf(self) -> Vec<V::ProtoBufType> {
        self.into_iter().map(|v| v.into_proto_buf()).collect()
    }
}

impl<V: TryFromProtoBuf> TryFromProtoBuf for Vec<V> {
    type ProtoBufType = Vec<V::ProtoBufType>;

    fn try_from_proto_buf(v: Self::ProtoBufType) -> Result<Self> {
        v.into_iter()
            .map(|e| TryFromProtoBuf::try_from_proto_buf(e))
            .collect()
    }
}

impl<K: IntoProtoBuf + Eq + Hash, V: IntoProtoBuf> IntoProtoBuf for HashMap<K, V>
where
    K::ProtoBufType: Eq + Hash,
{
    type ProtoBufType = HashMap<K::ProtoBufType, V::ProtoBufType>;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        self.into_iter()
            .map(|(k, v)| (k.into_proto_buf(), v.into_proto_buf()))
            .collect()
    }
}

impl<K: TryFromProtoBuf + Eq + Hash, V: TryFromProtoBuf> TryFromProtoBuf for HashMap<K, V> {
    type ProtoBufType = HashMap<K::ProtoBufType, V::ProtoBufType>;

    fn try_from_proto_buf(v: Self::ProtoBufType) -> Result<Self> {
        v.into_iter()
            .map(|(k, v)| {
                Ok((
                    TryFromProtoBuf::try_from_proto_buf(k)?,
                    TryFromProtoBuf::try_from_proto_buf(v)?,
                ))
            })
            .collect()
    }
}

impl<K: IntoProtoBuf + Eq + Ord, V: IntoProtoBuf> IntoProtoBuf for BTreeMap<K, V>
where
    K::ProtoBufType: Eq + Ord,
{
    type ProtoBufType = BTreeMap<K::ProtoBufType, V::ProtoBufType>;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        self.into_iter()
            .map(|(k, v)| (k.into_proto_buf(), v.into_proto_buf()))
            .collect()
    }
}

impl<K: TryFromProtoBuf + Eq + Ord, V: TryFromProtoBuf> TryFromProtoBuf for BTreeMap<K, V> {
    type ProtoBufType = BTreeMap<K::ProtoBufType, V::ProtoBufType>;

    fn try_from_proto_buf(v: Self::ProtoBufType) -> Result<Self> {
        v.into_iter()
            .map(|(k, v)| {
                Ok((
                    TryFromProtoBuf::try_from_proto_buf(k)?,
                    TryFromProtoBuf::try_from_proto_buf(v)?,
                ))
            })
            .collect()
    }
}

impl IntoProtoBuf for std::time::Duration {
    type ProtoBufType = proto::Duration;

    fn into_proto_buf(self) -> proto::Duration {
        proto::Duration {
            seconds: self.as_secs(),
            nano_seconds: self.subsec_nanos(),
        }
    }
}

impl TryFromProtoBuf for std::time::Duration {
    type ProtoBufType = proto::Duration;

    fn try_from_proto_buf(v: Self::ProtoBufType) -> Result<Self> {
        Ok(Self::new(v.seconds, v.nano_seconds))
    }
}

//  _____         _                        _
// |___ / _ __ __| |      _ __   __ _ _ __| |_ _   _
//   |_ \| '__/ _` |_____| '_ \ / _` | '__| __| | | |
//  ___) | | | (_| |_____| |_) | (_| | |  | |_| |_| |
// |____/|_|  \__,_|     | .__/ \__,_|_|   \__|\__, |
//                       |_|                   |___/
//

impl IntoProtoBuf for Utf8PathBuf {
    type ProtoBufType = String;

    fn into_proto_buf(self) -> String {
        self.into_string()
    }
}

impl TryFromProtoBuf for Utf8PathBuf {
    type ProtoBufType = String;

    fn try_from_proto_buf(s: String) -> Result<Self> {
        Ok(s.into())
    }
}

impl<V: IntoProtoBuf + EnumSetType> IntoProtoBuf for EnumSet<V> {
    type ProtoBufType = Vec<V::ProtoBufType>;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        self.iter().map(|v| v.into_proto_buf()).collect()
    }
}

impl<V: TryFromProtoBuf + EnumSetType> TryFromProtoBuf for EnumSet<V> {
    type ProtoBufType = Vec<V::ProtoBufType>;

    fn try_from_proto_buf(p: Self::ProtoBufType) -> Result<Self> {
        p.into_iter()
            .map(|v| V::try_from_proto_buf(v))
            .collect::<Result<_>>()
    }
}

impl IntoProtoBuf for slog::Level {
    type ProtoBufType = i32;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        (match self {
            slog::Level::Critical => proto::LogLevel::Critical,
            slog::Level::Error => proto::LogLevel::Error,
            slog::Level::Warning => proto::LogLevel::Warning,
            slog::Level::Info => proto::LogLevel::Info,
            slog::Level::Debug => proto::LogLevel::Debug,
            slog::Level::Trace => proto::LogLevel::Trace,
        }) as i32
    }
}

impl TryFromProtoBuf for slog::Level {
    type ProtoBufType = i32;

    fn try_from_proto_buf(p: Self::ProtoBufType) -> Result<Self> {
        Ok(match proto::LogLevel::try_from(p)? {
            proto::LogLevel::Critical => slog::Level::Critical,
            proto::LogLevel::Error => slog::Level::Error,
            proto::LogLevel::Warning => slog::Level::Warning,
            proto::LogLevel::Info => slog::Level::Info,
            proto::LogLevel::Debug => slog::Level::Debug,
            proto::LogLevel::Trace => slog::Level::Trace,
        })
    }
}

//                       _     _                             _
//  _ __ ___   __ _  ___| |___| |_ _ __ ___  _ __ ___       | |__   __ _ ___  ___
// | '_ ` _ \ / _` |/ _ \ / __| __| '__/ _ \| '_ ` _ \ _____| '_ \ / _` / __|/ _ \
// | | | | | | (_| |  __/ \__ \ |_| | | (_) | | | | | |_____| |_) | (_| \__ \  __/
// |_| |_| |_|\__,_|\___|_|___/\__|_|  \___/|_| |_| |_|     |_.__/ \__,_|___/\___|
//
//
//
impl<V: IntoProtoBuf> IntoProtoBuf for maelstrom_base::NonEmpty<V> {
    type ProtoBufType = Vec<V::ProtoBufType>;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        self.into_iter().map(|v| v.into_proto_buf()).collect()
    }
}

impl<V: TryFromProtoBuf> TryFromProtoBuf for maelstrom_base::NonEmpty<V> {
    type ProtoBufType = Vec<V::ProtoBufType>;

    fn try_from_proto_buf(v: Vec<V::ProtoBufType>) -> Result<Self> {
        maelstrom_base::NonEmpty::from_vec(
            v.into_iter()
                .map(|v| TryFromProtoBuf::try_from_proto_buf(v))
                .collect::<Result<Vec<_>>>()?,
        )
        .ok_or_else(|| anyhow!("malformed NonEmpty"))
    }
}

remote_derive!(
    UserId,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = u32, try_from_into)
);

remote_derive!(
    GroupId,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = u32, try_from_into)
);

remote_derive!(
    Timeout,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = u32, try_from_into)
);

remote_derive!(
    ClientJobId,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = u32, try_from_into)
);

remote_derive!(
    WindowSize,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = "proto::WindowSize")
);

remote_derive!(
    WorkerId,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = u32, try_from_into)
);

//      _       _
//     | | ___ | |__ __/\__
//  _  | |/ _ \| '_ \\    /
// | |_| | (_) | |_) /_  _\
//  \___/ \___/|_.__/  \/

remote_derive!(
    JobDevice,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = "proto::JobDevice")
);

remote_derive!(
    JobTerminationStatus,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = "proto::job_completed::Status")
);

remote_derive!(
    JobNetwork,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = "proto::JobNetwork")
);

remote_derive!(
    CaptureFileSystemChanges,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = "proto::CaptureFileSystemChanges")
);

remote_derive!(
    JobEffects,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = "proto::JobEffects", option_all)
);

remote_derive!(
    JobTty,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = "proto::JobTty"),
    @window_size: proto(option)
);

remote_derive!(
    JobMount,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = "proto::JobMount", enum_type = "proto::job_mount::Mount"),
    @Bind: proto(proto_buf_type = "proto::BindMount"),
    @Devices: proto(proto_buf_type = "proto::DevicesMount"),
    @Devpts: proto(proto_buf_type = "proto::DevptsMount"),
    @Mqueue: proto(proto_buf_type = "proto::MqueueMount"),
    @Proc: proto(proto_buf_type = "proto::ProcMount"),
    @Sys: proto(proto_buf_type = "proto::SysMount"),
    @Tmp: proto(proto_buf_type = "proto::TmpMount"),
);

remote_derive!(
    JobOutputResult,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(
        proto_buf_type = "proto::JobOutputResult",
        enum_type = "proto::job_output_result::Result"
    ),
    @Truncated: proto(proto_buf_type = "proto::JobOutputResultTruncated"),
);

remote_derive!(
    JobOutcome,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(
        proto_buf_type = "proto::JobOutcome",
        enum_type = "proto::job_outcome::Outcome"
    ),
);

remote_derive!(
    JobCompleted,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = "proto::JobCompleted", option_all),
);

remote_derive!(
    JobWorkerStatus,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = "proto::JobWorkerStatus"),
);

remote_derive!(
    JobBrokerStatus,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = "proto::JobBrokerStatus", enum_type = "proto::job_broker_status::Status"),
    @AtWorker: proto(proto_buf_type = "proto::JobBrokerAtWorkerStatus")
);

impl From<(u32, i32)> for proto::JobBrokerAtWorkerStatus {
    fn from((worker_id, status): (u32, i32)) -> Self {
        Self { worker_id, status }
    }
}

impl From<proto::JobBrokerAtWorkerStatus> for (u32, i32) {
    fn from(p: proto::JobBrokerAtWorkerStatus) -> Self {
        (p.worker_id, p.status)
    }
}

impl IntoProtoBuf for maelstrom_base::JobError<String> {
    type ProtoBufType = proto::JobError;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        use proto::job_error::Kind;
        proto::JobError {
            kind: Some(match self {
                Self::Execution(msg) => Kind::Execution(msg),
                Self::System(msg) => Kind::System(msg),
            }),
        }
    }
}

impl TryFromProtoBuf for maelstrom_base::JobError<String> {
    type ProtoBufType = proto::JobError;

    fn try_from_proto_buf(t: proto::JobError) -> Result<Self> {
        use proto::job_error::Kind;
        match t.kind.ok_or_else(|| anyhow!("malformed JobError"))? {
            Kind::Execution(msg) => Ok(Self::Execution(msg)),
            Kind::System(msg) => Ok(Self::System(msg)),
        }
    }
}

impl IntoProtoBuf for maelstrom_base::JobOutcomeResult {
    type ProtoBufType = proto::JobOutcomeResult;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        use proto::job_outcome_result::Result as JobOutcomeResult;
        proto::JobOutcomeResult {
            result: Some(match self {
                Ok(v) => JobOutcomeResult::Outcome(v.into_proto_buf()),
                Err(e) => JobOutcomeResult::Error(e.into_proto_buf()),
            }),
        }
    }
}

impl TryFromProtoBuf for maelstrom_base::JobOutcomeResult {
    type ProtoBufType = proto::JobOutcomeResult;

    fn try_from_proto_buf(t: proto::JobOutcomeResult) -> Result<Self> {
        use proto::job_outcome_result::Result as JobOutcomeResult;
        match t
            .result
            .ok_or_else(|| anyhow!("malformed JobOutcomeResult"))?
        {
            JobOutcomeResult::Error(e) => Ok(Self::Err(TryFromProtoBuf::try_from_proto_buf(e)?)),
            JobOutcomeResult::Outcome(v) => Ok(Self::Ok(TryFromProtoBuf::try_from_proto_buf(v)?)),
        }
    }
}

//                       _     _                                   _   _ _
//  _ __ ___   __ _  ___| |___| |_ _ __ ___  _ __ ___        _   _| |_(_) |
// | '_ ` _ \ / _` |/ _ \ / __| __| '__/ _ \| '_ ` _ \ _____| | | | __| | |
// | | | | | | (_| |  __/ \__ \ |_| | | (_) | | | | | |_____| |_| | |_| | |
// |_| |_| |_|\__,_|\___|_|___/\__|_|  \___/|_| |_| |_|      \__,_|\__|_|_|
//

remote_derive!(
    BrokerAddr,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = String, try_from_into),
);

remote_derive!(
    CacheSize,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = u64, try_from_into),
);

remote_derive!(
    InlineLimit,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = u64, try_from_into),
);

remote_derive!(
    Slots,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(proto_buf_type = u32, try_from_into),
);

remote_derive!(
    ArtifactTransferStrategy,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(
        proto_buf_type = "proto::ArtifactTransferStrategy",
        try_from_into
    ),
);

impl<'a, T> IntoProtoBuf for &'a maelstrom_util::root::Root<T> {
    type ProtoBufType = <&'a Path as IntoProtoBuf>::ProtoBufType;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        (**self).into_proto_buf()
    }
}

impl<T> IntoProtoBuf for maelstrom_util::root::RootBuf<T> {
    type ProtoBufType = <PathBuf as IntoProtoBuf>::ProtoBufType;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        self.into_path_buf().into_proto_buf()
    }
}

impl<T> TryFromProtoBuf for maelstrom_util::root::RootBuf<T> {
    type ProtoBufType = <PathBuf as TryFromProtoBuf>::ProtoBufType;

    fn try_from_proto_buf(pbt: Self::ProtoBufType) -> Result<Self> {
        Ok(Self::new(PathBuf::try_from_proto_buf(pbt)?))
    }
}
