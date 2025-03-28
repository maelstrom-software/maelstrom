use crate::dispatcher;
use maelstrom_base::{proto::WorkerToBroker, JobId, Sha256Digest};
use maelstrom_util::cache::{self, fs::std::Fs as StdFs, GotArtifact};
use std::path::PathBuf;
use strum::{EnumString, IntoStaticStr, VariantNames};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(
    Clone,
    Copy,
    Debug,
    EnumString,
    Eq,
    Hash,
    IntoStaticStr,
    Ord,
    PartialEq,
    VariantNames,
    PartialOrd,
)]
#[strum(serialize_all = "snake_case")]
pub enum CacheKeyKind {
    Blob,
    BottomFsLayer,
    UpperFsLayer,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CacheKey {
    kind: CacheKeyKind,
    digest: Sha256Digest,
}

impl CacheKey {
    pub fn blob(digest: Sha256Digest) -> Self {
        Self {
            kind: CacheKeyKind::Blob,
            digest,
        }
    }

    pub fn bottom_fs_layer(digest: Sha256Digest) -> Self {
        Self {
            kind: CacheKeyKind::BottomFsLayer,
            digest,
        }
    }

    pub fn upper_fs_layer(digest: Sha256Digest) -> Self {
        Self {
            kind: CacheKeyKind::UpperFsLayer,
            digest,
        }
    }
}

impl cache::Key for CacheKey {
    fn kinds() -> impl Iterator<Item = &'static str> {
        CacheKeyKind::VARIANTS.iter().copied()
    }

    fn from_kind_and_digest(kind: &'static str, digest: Sha256Digest) -> Self {
        Self {
            kind: kind.try_into().unwrap(),
            digest,
        }
    }

    fn kind(&self) -> &'static str {
        self.kind.into()
    }

    fn digest(&self) -> &Sha256Digest {
        &self.digest
    }
}

pub enum CacheGetStrategy {}

impl cache::GetStrategy for CacheGetStrategy {
    type Getter = ();
    fn getter_from_job_id(_jid: JobId) -> Self::Getter {}
}

pub type Cache = cache::Cache<StdFs, CacheKey, CacheGetStrategy>;
pub type TempFile = cache::fs::std::TempFile;
pub type TempFileFactory = cache::TempFileFactory<StdFs>;

/// The standard implementation of [`Cache`] that just calls into [`cache::Cache`].
impl dispatcher::Cache for Cache {
    type Fs = StdFs;

    fn get_artifact(&mut self, key: CacheKey, jid: JobId) -> cache::GetArtifact {
        self.get_artifact(key, jid)
    }

    fn got_artifact_failure(&mut self, key: &CacheKey) -> Vec<JobId> {
        self.got_artifact_failure(key)
    }

    fn got_artifact_success(
        &mut self,
        key: &CacheKey,
        artifact: GotArtifact<StdFs>,
    ) -> Result<Vec<JobId>, (anyhow::Error, Vec<JobId>)> {
        self.got_artifact_success(key, artifact)
    }

    fn decrement_ref_count(&mut self, key: &CacheKey) {
        self.decrement_ref_count(key)
    }

    fn cache_path(&self, key: &CacheKey) -> PathBuf {
        self.cache_path(key).into_path_buf()
    }
}

pub type DispatcherReceiver = UnboundedReceiver<dispatcher::Message<StdFs>>;
pub type DispatcherSender = UnboundedSender<dispatcher::Message<StdFs>>;
pub type BrokerSocketOutgoingSender = UnboundedSender<WorkerToBroker>;
