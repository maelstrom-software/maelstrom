use crate::proto;
use anyhow::{anyhow, Result};
use enum_map::{enum_map, EnumMap};
use enumset::{EnumSet, EnumSetType};
use maelstrom_base::Utf8PathBuf;
use std::collections::HashMap;
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
        .ok_or(anyhow!("malformed NonEmpty"))
    }
}

impl IntoProtoBuf for maelstrom_base::Sha256Digest {
    type ProtoBufType = Vec<u8>;

    fn into_proto_buf(self) -> Vec<u8> {
        self.into()
    }
}

impl TryFromProtoBuf for maelstrom_base::Sha256Digest {
    type ProtoBufType = Vec<u8>;

    fn try_from_proto_buf(v: Vec<u8>) -> Result<Self> {
        Ok(maelstrom_base::Sha256Digest::try_from(v)?)
    }
}

impl IntoProtoBuf for (maelstrom_base::Sha256Digest, maelstrom_base::ArtifactType) {
    type ProtoBufType = proto::LayerSpec;

    fn into_proto_buf(self) -> proto::LayerSpec {
        proto::LayerSpec {
            digest: self.0.into_proto_buf(),
            r#type: self.1.into_proto_buf(),
        }
    }
}

impl TryFromProtoBuf for (maelstrom_base::Sha256Digest, maelstrom_base::ArtifactType) {
    type ProtoBufType = proto::LayerSpec;

    fn try_from_proto_buf(p: Self::ProtoBufType) -> Result<Self> {
        Ok((
            TryFromProtoBuf::try_from_proto_buf(p.digest)?,
            TryFromProtoBuf::try_from_proto_buf(p.r#type)?,
        ))
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
        Self::new(v).ok_or(anyhow!("malformed Timeout"))
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

//      _       _
//     | | ___ | |__ __/\__
//  _  | |/ _ \| '_ \\    /
// | |_| | (_) | |_) /_  _\
//  \___/ \___/|_.__/  \/
//

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
        match v.result.ok_or(anyhow!("malformed JobOutputResult"))? {
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
        match v.outcome.ok_or(anyhow!("malformed JobOutcome"))? {
            Outcome::Completed(proto::JobCompleted { status, effects }) => {
                Ok(Self::Completed(maelstrom_base::JobCompleted {
                    status: TryFromProtoBuf::try_from_proto_buf(
                        status.ok_or(anyhow!("malformed JobOutcome::Completed"))?,
                    )?,
                    effects: TryFromProtoBuf::try_from_proto_buf(
                        effects.ok_or(anyhow!("malformed JobOutcome::Completed"))?,
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
        match t.kind.ok_or(anyhow!("malformed JobError"))? {
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
        match t.result.ok_or(anyhow!("malformed JobOutcomeResult"))? {
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
