use maelstrom_base::{ClientId, JobId, Sha256Digest};
use maelstrom_util::cache::{fs::Fs, Cache, GetArtifact, GetStrategy, GotArtifact, Key};
use ref_cast::RefCast;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;

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

/// The required interface for the cache that is provided to the [`Scheduler`].
///
/// Unlike with [`SchedulerDeps`], all of these functions are immediate. In production, the
/// [`SchedulerCache`] is owned by the [`Scheduler`] and the two live on the same task. So these
/// methods sometimes return actual values which can be handled immediately, unlike
/// [`SchedulerDeps`].
pub trait SchedulerCache {
    type TempFile;
    type ArtifactStream;

    /// Try to get the artifact from the cache. This increases the refcount by one.
    /// - If [`GetArtifact::Success`] is returned, the artifact is in the cache.
    /// - If [`GetArtifact::Wait`] is returned, the artifact is being fetched, and the given job id
    ///   will be returned from [`SchedulerCache::got_artifact`]
    fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact;

    /// Enter something into the cache.
    fn got_artifact(&mut self, digest: &Sha256Digest, file: Self::TempFile) -> Vec<JobId>;

    /// Decrement the refcount for the given artifact.
    fn decrement_refcount(&mut self, digest: &Sha256Digest);

    /// Any artifacts that are being fetched have any job ids matching the given client id removed.
    fn client_disconnected(&mut self, cid: ClientId);

    /// Get the path and size of a given artifact if it has a non-zero refcount, `None` otherwise.
    /// If the refcount is non-zero, it is increased by one.
    fn get_artifact_for_worker(&mut self, digest: &Sha256Digest) -> Option<(PathBuf, u64)>;

    /// Get a stream that reads the contents of an artifact. You must have keep a non-zero refcount
    /// on the artifact the whole time the stream is being used.
    fn read_artifact(&mut self, digest: &Sha256Digest) -> (Self::ArtifactStream, u64);
}

type LazyRead<FileT> = maelstrom_util::io::LazyRead<
    Pin<Box<dyn Future<Output = io::Result<FileT>> + Send + Sync>>,
    FileT,
>;

impl<FsT: Fs> SchedulerCache for Cache<FsT, BrokerKey, BrokerGetStrategy> {
    type TempFile = FsT::TempFile;
    type ArtifactStream = LazyRead<maelstrom_util::async_fs::File>;

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

    fn read_artifact(&mut self, digest: &Sha256Digest) -> (Self::ArtifactStream, u64) {
        let key = BrokerKey::ref_cast(digest);
        let cache_path = self.cache_path(key).into_path_buf();
        let stream = LazyRead::new(Box::pin(async move {
            let fs = maelstrom_util::async_fs::Fs::new();
            fs.open_file(cache_path)
                .await
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
        }));
        let bytes_used = self.try_get_size(key).expect("non-zero refcount");

        (stream, bytes_used)
    }
}
