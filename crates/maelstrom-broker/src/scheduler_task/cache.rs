use maelstrom_base::{ClientId, JobId, Sha256Digest};
use maelstrom_util::cache::{
    fs::{Fs, TempFile},
    Cache, GetArtifact, GetStrategy, GotArtifact, Key,
};
use ref_cast::RefCast;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, Hash, PartialEq, RefCast)]
#[repr(transparent)]
pub struct BrokerKey(pub Sha256Digest);

impl Key for BrokerKey {
    fn kinds() -> impl Iterator<Item = &'static str> {
        ["blob"].into_iter()
    }

    fn from_kind_and_digest(_kind: &'static str, digest: Sha256Digest) -> Self {
        Self(digest)
    }

    fn kind(&self) -> &'static str {
        "blob"
    }

    fn digest(&self) -> &Sha256Digest {
        &self.0
    }
}

pub enum BrokerGetStrategy {}

impl GetStrategy for BrokerGetStrategy {
    type Getter = ClientId;
    fn getter_from_job_id(jid: JobId) -> Self::Getter {
        jid.cid
    }
}

/// The required interface for the cache that is provided to the [`Scheduler`]. This mirrors the
/// API for [`Cache`]. We keep them separate so that we can test the [`Scheduler`] more easily.
///
/// Unlike with [`SchedulerDeps`], all of these functions are immediate. In production, the
/// [`Cache`] is owned by the [`Scheduler`] and the two live on the same task. So these methods
/// sometimes return actual values which can be handled immediately, unlike [`SchedulerDeps`].
pub trait SchedulerCache {
    type TempFile: TempFile;

    /// See [`Cache::get_artifact`].
    fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact;

    /// See [`Cache::got_artifact`].
    fn got_artifact(&mut self, digest: &Sha256Digest, file: Self::TempFile) -> Vec<JobId>;

    /// See [`Cache::decrement_refcount`].
    fn decrement_refcount(&mut self, digest: &Sha256Digest);

    /// See [`Cache::client_disconnected`].
    fn client_disconnected(&mut self, cid: ClientId);

    /// See [`Cache::get_artifact_for_worker`].
    fn get_artifact_for_worker(&mut self, digest: &Sha256Digest) -> Option<(PathBuf, u64)>;
}

impl<FsT: Fs> SchedulerCache for Cache<FsT, BrokerKey, BrokerGetStrategy> {
    type TempFile = FsT::TempFile;

    fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact {
        self.get_artifact(BrokerKey(digest), jid)
    }

    fn got_artifact(&mut self, digest: &Sha256Digest, file: FsT::TempFile) -> Vec<JobId> {
        self.got_artifact_success(BrokerKey::ref_cast(digest), GotArtifact::file(file))
            .map_err(|(err, _)| err)
            .unwrap()
    }

    fn decrement_refcount(&mut self, digest: &Sha256Digest) {
        self.decrement_ref_count(BrokerKey::ref_cast(digest))
    }

    fn client_disconnected(&mut self, cid: ClientId) {
        self.getter_disconnected(cid)
    }

    fn get_artifact_for_worker(&mut self, digest: &Sha256Digest) -> Option<(PathBuf, u64)> {
        let cache_path = self.cache_path(BrokerKey::ref_cast(digest)).into_path_buf();
        self.try_increment_ref_count(BrokerKey::ref_cast(digest))
            .map(|size| (cache_path, size))
    }
}
