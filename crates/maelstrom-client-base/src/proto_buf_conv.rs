use crate::proto;
use anyhow::{anyhow, Result};
use enum_map::{enum_map, EnumMap};
use enumset::{EnumSet, EnumSetType};
use maelstrom_base::Utf8PathBuf;
use maelstrom_base::{
    job_device_pocket_definition, job_effects_pocket_definition, job_network_pocket_definition,
    job_status_pocket_definition, JobDevice, JobEffects, JobNetwork, JobStatus,
};
use maelstrom_macro::{
    into_proto_buf_remote_derive, remote_derive, try_from_proto_buf_remote_derive,
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

impl<'a> IntoProtoBuf for &'a Path {
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

impl From<proto::layer::Layer> for proto::Layer {
    fn from(layer: proto::layer::Layer) -> Self {
        Self { layer: Some(layer) }
    }
}

impl TryFrom<proto::Layer> for proto::layer::Layer {
    type Error = anyhow::Error;

    fn try_from(layer: proto::Layer) -> Result<Self> {
        layer.layer.ok_or_else(|| anyhow!("malformed Layer"))
    }
}

impl IntoProtoBuf for maelstrom_base::UserId {
    type ProtoBufType = u32;

    fn into_proto_buf(self) -> u32 {
        self.as_u32()
    }
}

impl TryFromProtoBuf for maelstrom_base::UserId {
    type ProtoBufType = u32;

    fn try_from_proto_buf(v: u32) -> Result<Self> {
        Ok(Self::new(v))
    }
}

impl IntoProtoBuf for maelstrom_base::GroupId {
    type ProtoBufType = u32;

    fn into_proto_buf(self) -> u32 {
        self.as_u32()
    }
}

impl TryFromProtoBuf for maelstrom_base::GroupId {
    type ProtoBufType = u32;

    fn try_from_proto_buf(v: u32) -> Result<Self> {
        Ok(Self::new(v))
    }
}

impl IntoProtoBuf for maelstrom_base::Timeout {
    type ProtoBufType = u32;

    fn into_proto_buf(self) -> u32 {
        self.as_u32()
    }
}

impl TryFromProtoBuf for maelstrom_base::Timeout {
    type ProtoBufType = u32;

    fn try_from_proto_buf(v: u32) -> Result<Self> {
        Self::new(v).ok_or_else(|| anyhow!("malformed Timeout"))
    }
}

impl IntoProtoBuf for maelstrom_base::ClientJobId {
    type ProtoBufType = u32;

    fn into_proto_buf(self) -> u32 {
        self.as_u32()
    }
}

impl TryFromProtoBuf for maelstrom_base::ClientJobId {
    type ProtoBufType = u32;

    fn try_from_proto_buf(v: u32) -> Result<Self> {
        Ok(Self::from_u32(v))
    }
}

impl IntoProtoBuf for maelstrom_base::WindowSize {
    type ProtoBufType = proto::WindowSize;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        Self::ProtoBufType {
            rows: self.rows.into(),
            columns: self.columns.into(),
        }
    }
}

impl TryFromProtoBuf for maelstrom_base::WindowSize {
    type ProtoBufType = proto::WindowSize;

    fn try_from_proto_buf(window_size: Self::ProtoBufType) -> Result<Self> {
        Ok(Self {
            rows: window_size.rows.try_into()?,
            columns: window_size.columns.try_into()?,
        })
    }
}

impl IntoProtoBuf for maelstrom_base::JobTty {
    type ProtoBufType = proto::JobTty;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        Self::ProtoBufType {
            socket_address: self.socket_address.as_slice().to_vec(),
            window_size: Some(self.window_size.into_proto_buf()),
        }
    }
}

impl TryFromProtoBuf for maelstrom_base::JobTty {
    type ProtoBufType = proto::JobTty;

    fn try_from_proto_buf(job_tty: Self::ProtoBufType) -> Result<Self> {
        Ok(Self {
            socket_address: job_tty
                .socket_address
                .try_into()
                .map_err(|_| anyhow!("malformed JobTty"))?,
            window_size: maelstrom_base::WindowSize::try_from_proto_buf(
                job_tty
                    .window_size
                    .ok_or_else(|| anyhow!("malformed JobTty"))?,
            )?,
        })
    }
}

remote_derive!(
    JobDevice,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(other_type = "proto::JobDevice")
);

remote_derive!(
    JobStatus,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(other_type = "proto::job_completed::Status")
);

remote_derive!(
    JobNetwork,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(other_type = "proto::JobNetwork")
);

remote_derive!(
    JobEffects,
    (IntoProtoBuf, TryFromProtoBuf),
    proto(other_type = "proto::JobEffects", option_all)
);

//      _       _
//     | | ___ | |__ __/\__
//  _  | |/ _ \| '_ \\    /
// | |_| | (_) | |_) /_  _\
//  \___/ \___/|_.__/  \/
//

impl IntoProtoBuf for maelstrom_base::JobMount {
    type ProtoBufType = proto::JobMount;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        let mount = match self {
            Self::Bind {
                mount_point,
                local_path,
                read_only,
            } => proto::job_mount::Mount::Bind(proto::BindMount {
                mount_point: mount_point.into_proto_buf(),
                local_path: local_path.into_proto_buf(),
                read_only: read_only.into_proto_buf(),
            }),
            Self::Devices { devices } => proto::job_mount::Mount::Devices(proto::DevicesMount {
                devices: devices.into_proto_buf(),
            }),
            Self::Devpts { mount_point } => proto::job_mount::Mount::Devpts(proto::DevptsMount {
                mount_point: mount_point.into_proto_buf(),
            }),
            Self::Mqueue { mount_point } => proto::job_mount::Mount::Mqueue(proto::MqueueMount {
                mount_point: mount_point.into_proto_buf(),
            }),
            Self::Proc { mount_point } => proto::job_mount::Mount::Proc(proto::ProcMount {
                mount_point: mount_point.into_proto_buf(),
            }),
            Self::Sys { mount_point } => proto::job_mount::Mount::Sys(proto::SysMount {
                mount_point: mount_point.into_proto_buf(),
            }),
            Self::Tmp { mount_point } => proto::job_mount::Mount::Tmp(proto::TmpMount {
                mount_point: mount_point.into_proto_buf(),
            }),
        };
        Self::ProtoBufType { mount: Some(mount) }
    }
}

impl TryFromProtoBuf for maelstrom_base::JobMount {
    type ProtoBufType = proto::JobMount;

    fn try_from_proto_buf(protobuf: Self::ProtoBufType) -> Result<Self> {
        let mount = protobuf
            .mount
            .ok_or_else(|| anyhow!("malformed JobMount"))?;
        Ok(match mount {
            proto::job_mount::Mount::Bind(bind_mount) => maelstrom_base::JobMount::Bind {
                mount_point: TryFromProtoBuf::try_from_proto_buf(bind_mount.mount_point)?,
                local_path: TryFromProtoBuf::try_from_proto_buf(bind_mount.local_path)?,
                read_only: TryFromProtoBuf::try_from_proto_buf(bind_mount.read_only)?,
            },
            proto::job_mount::Mount::Devices(devices_mount) => maelstrom_base::JobMount::Devices {
                devices: TryFromProtoBuf::try_from_proto_buf(devices_mount.devices)?,
            },
            proto::job_mount::Mount::Devpts(devpts_mount) => maelstrom_base::JobMount::Devpts {
                mount_point: TryFromProtoBuf::try_from_proto_buf(devpts_mount.mount_point)?,
            },
            proto::job_mount::Mount::Mqueue(mqueue_mount) => maelstrom_base::JobMount::Mqueue {
                mount_point: TryFromProtoBuf::try_from_proto_buf(mqueue_mount.mount_point)?,
            },
            proto::job_mount::Mount::Proc(proc_mount) => maelstrom_base::JobMount::Proc {
                mount_point: TryFromProtoBuf::try_from_proto_buf(proc_mount.mount_point)?,
            },
            proto::job_mount::Mount::Sys(sys_mount) => maelstrom_base::JobMount::Sys {
                mount_point: TryFromProtoBuf::try_from_proto_buf(sys_mount.mount_point)?,
            },
            proto::job_mount::Mount::Tmp(tmp_mount) => maelstrom_base::JobMount::Tmp {
                mount_point: TryFromProtoBuf::try_from_proto_buf(tmp_mount.mount_point)?,
            },
        })
    }
}

impl IntoProtoBuf for maelstrom_base::JobRootOverlay {
    type ProtoBufType = Option<proto::JobRootOverlay>;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        let overlay = match self {
            Self::None => proto::job_root_overlay::Overlay::None(proto::Void {}),
            Self::Tmp => proto::job_root_overlay::Overlay::Tmp(proto::Void {}),
            Self::Local { upper, work } => {
                proto::job_root_overlay::Overlay::Local(proto::LocalJobRootOverlay {
                    upper: upper.into_proto_buf(),
                    work: work.into_proto_buf(),
                })
            }
        };
        Some(proto::JobRootOverlay {
            overlay: Some(overlay),
        })
    }
}

impl TryFromProtoBuf for maelstrom_base::JobRootOverlay {
    type ProtoBufType = Option<proto::JobRootOverlay>;

    fn try_from_proto_buf(protobuf: Self::ProtoBufType) -> Result<Self> {
        let Some(overlay) = protobuf else {
            return Ok(Default::default());
        };
        let overlay = overlay
            .overlay
            .ok_or_else(|| anyhow!("malformed JobRootOverlay"))?;
        Ok(match overlay {
            proto::job_root_overlay::Overlay::None(proto::Void {}) => {
                maelstrom_base::JobRootOverlay::None
            }
            proto::job_root_overlay::Overlay::Tmp(proto::Void {}) => {
                maelstrom_base::JobRootOverlay::Tmp
            }
            proto::job_root_overlay::Overlay::Local(local) => {
                maelstrom_base::JobRootOverlay::Local {
                    upper: TryFromProtoBuf::try_from_proto_buf(local.upper)?,
                    work: TryFromProtoBuf::try_from_proto_buf(local.work)?,
                }
            }
        })
    }
}

impl IntoProtoBuf for maelstrom_base::JobOutputResult {
    type ProtoBufType = proto::JobOutputResult;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        use proto::job_output_result::Result as ProtoJobOutputResult;
        proto::JobOutputResult {
            result: Some(match self {
                Self::None => ProtoJobOutputResult::None(proto::Void {}),
                Self::Inline(bytes) => ProtoJobOutputResult::Inline(bytes.into()),
                Self::Truncated { first, truncated } => {
                    ProtoJobOutputResult::Truncated(proto::JobOutputResultTruncated {
                        first: first.into(),
                        truncated,
                    })
                }
            }),
        }
    }
}

impl TryFromProtoBuf for maelstrom_base::JobOutputResult {
    type ProtoBufType = proto::JobOutputResult;

    fn try_from_proto_buf(v: proto::JobOutputResult) -> Result<Self> {
        use proto::job_output_result::Result as ProtoJobOutputResult;
        match v
            .result
            .ok_or_else(|| anyhow!("malformed JobOutputResult"))?
        {
            ProtoJobOutputResult::None(_) => Ok(Self::None),
            ProtoJobOutputResult::Inline(bytes) => Ok(Self::Inline(bytes.into())),
            ProtoJobOutputResult::Truncated(proto::JobOutputResultTruncated {
                first,
                truncated,
            }) => Ok(Self::Truncated {
                first: first.into(),
                truncated,
            }),
        }
    }
}

impl IntoProtoBuf for maelstrom_base::JobOutcome {
    type ProtoBufType = proto::JobOutcome;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        use proto::job_outcome::Outcome;
        proto::JobOutcome {
            outcome: Some(match self {
                Self::Completed(maelstrom_base::JobCompleted { status, effects }) => {
                    Outcome::Completed(proto::JobCompleted {
                        status: Some(status.into_proto_buf()),
                        effects: Some(effects.into_proto_buf()),
                    })
                }
                Self::TimedOut(effects) => Outcome::TimedOut(effects.into_proto_buf()),
            }),
        }
    }
}

impl TryFromProtoBuf for maelstrom_base::JobOutcome {
    type ProtoBufType = proto::JobOutcome;

    fn try_from_proto_buf(v: proto::JobOutcome) -> Result<Self> {
        use proto::job_outcome::Outcome;
        match v.outcome.ok_or_else(|| anyhow!("malformed JobOutcome"))? {
            Outcome::Completed(proto::JobCompleted { status, effects }) => {
                Ok(Self::Completed(maelstrom_base::JobCompleted {
                    status: TryFromProtoBuf::try_from_proto_buf(
                        status.ok_or_else(|| anyhow!("malformed JobOutcome::Completed"))?,
                    )?,
                    effects: TryFromProtoBuf::try_from_proto_buf(
                        effects.ok_or_else(|| anyhow!("malformed JobOutcome::Completed"))?,
                    )?,
                }))
            }
            Outcome::TimedOut(effects) => Ok(Self::TimedOut(TryFromProtoBuf::try_from_proto_buf(
                effects,
            )?)),
        }
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

impl IntoProtoBuf for EnumMap<maelstrom_base::stats::JobState, u64> {
    type ProtoBufType = proto::JobStateCounts;

    fn into_proto_buf(self) -> Self::ProtoBufType {
        use maelstrom_base::stats::JobState;
        proto::JobStateCounts {
            waiting_for_artifacts: self[JobState::WaitingForArtifacts],
            pending: self[JobState::Pending],
            running: self[JobState::Running],
            complete: self[JobState::Complete],
        }
    }
}

impl TryFromProtoBuf for EnumMap<maelstrom_base::stats::JobState, u64> {
    type ProtoBufType = proto::JobStateCounts;

    fn try_from_proto_buf(b: proto::JobStateCounts) -> Result<Self> {
        use maelstrom_base::stats::JobState;
        Ok(enum_map! {
            JobState::WaitingForArtifacts => b.waiting_for_artifacts,
            JobState::Pending => b.pending,
            JobState::Running => b.running,
            JobState::Complete => b.complete,
        })
    }
}

//                       _     _                                   _   _ _
//  _ __ ___   __ _  ___| |___| |_ _ __ ___  _ __ ___        _   _| |_(_) |
// | '_ ` _ \ / _` |/ _ \ / __| __| '__/ _ \| '_ ` _ \ _____| | | | __| | |
// | | | | | | (_| |  __/ \__ \ |_| | | (_) | | | | | |_____| |_| | |_| | |
// |_| |_| |_|\__,_|\___|_|___/\__|_|  \___/|_| |_| |_|      \__,_|\__|_|_|
//

impl IntoProtoBuf for maelstrom_util::config::common::BrokerAddr {
    type ProtoBufType = String;

    fn into_proto_buf(self) -> String {
        self.into()
    }
}

impl TryFromProtoBuf for maelstrom_util::config::common::BrokerAddr {
    type ProtoBufType = String;

    fn try_from_proto_buf(v: String) -> Result<Self> {
        Ok(v.parse()?)
    }
}

impl IntoProtoBuf for maelstrom_util::config::common::CacheSize {
    type ProtoBufType = u64;

    fn into_proto_buf(self) -> u64 {
        self.as_bytes()
    }
}

impl TryFromProtoBuf for maelstrom_util::config::common::CacheSize {
    type ProtoBufType = u64;

    fn try_from_proto_buf(v: u64) -> Result<Self> {
        Ok(Self::from_bytes(v))
    }
}

impl IntoProtoBuf for maelstrom_util::config::common::InlineLimit {
    type ProtoBufType = u64;

    fn into_proto_buf(self) -> u64 {
        self.as_bytes()
    }
}

impl TryFromProtoBuf for maelstrom_util::config::common::InlineLimit {
    type ProtoBufType = u64;

    fn try_from_proto_buf(v: u64) -> Result<Self> {
        Ok(Self::from_bytes(v))
    }
}

impl IntoProtoBuf for maelstrom_util::config::common::Slots {
    type ProtoBufType = u32;

    fn into_proto_buf(self) -> u32 {
        self.into_inner().into()
    }
}

impl TryFromProtoBuf for maelstrom_util::config::common::Slots {
    type ProtoBufType = u32;

    fn try_from_proto_buf(v: u32) -> Result<Self> {
        Self::try_from(u16::try_from(v)?).map_err(|s| anyhow!("error deserializing slots: {s}"))
    }
}

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

//                       _     _
//  _ __ ___   __ _  ___| |___| |_ _ __ ___  _ __ ___
// | '_ ` _ \ / _` |/ _ \ / __| __| '__/ _ \| '_ ` _ \ _____
// | | | | | | (_| |  __/ \__ \ |_| | | (_) | | | | | |_____|
// |_| |_| |_|\__,_|\___|_|___/\__|_|  \___/|_| |_| |_|
//
//                  _        _
//   ___ ___  _ __ | |_ __ _(_)_ __   ___ _ __
//  / __/ _ \| '_ \| __/ _` | | '_ \ / _ \ '__|
// | (_| (_) | | | | || (_| | | | | |  __/ |
//  \___\___/|_| |_|\__\__,_|_|_| |_|\___|_|
//

impl IntoProtoBuf for maelstrom_container::Arch {
    type ProtoBufType = String;

    fn into_proto_buf(self) -> String {
        self.to_string()
    }
}

impl TryFromProtoBuf for maelstrom_container::Arch {
    type ProtoBufType = String;

    fn try_from_proto_buf(v: String) -> Result<Self> {
        if v.is_empty() {
            return Err(anyhow!("malformed `Arch`"));
        }
        Ok(v.as_str().into())
    }
}

impl IntoProtoBuf for maelstrom_container::Os {
    type ProtoBufType = String;

    fn into_proto_buf(self) -> String {
        self.to_string()
    }
}

impl TryFromProtoBuf for maelstrom_container::Os {
    type ProtoBufType = String;

    fn try_from_proto_buf(v: String) -> Result<Self> {
        if v.is_empty() {
            return Err(anyhow!("malformed `Os`"));
        }
        Ok(v.as_str().into())
    }
}

impl IntoProtoBuf for maelstrom_container::ContainerImageVersion {
    type ProtoBufType = u32;

    fn into_proto_buf(self) -> u32 {
        self as u32
    }
}

impl TryFromProtoBuf for maelstrom_container::ContainerImageVersion {
    type ProtoBufType = u32;

    fn try_from_proto_buf(v: u32) -> Result<Self> {
        if v != Self::default() as u32 {
            Err(anyhow!("wrong ContainerImage version"))
        } else {
            Ok(Self::default())
        }
    }
}
