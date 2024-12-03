pub mod github;
pub mod local;

use maelstrom_base::{ClientId, JobId, Sha256Digest};
use maelstrom_util::cache::GetArtifact;
use std::path::PathBuf;

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
    fn got_artifact(&mut self, digest: &Sha256Digest, file: Option<Self::TempFile>) -> Vec<JobId>;

    /// Decrement the refcount for the given artifact.
    fn decrement_refcount(&mut self, digest: &Sha256Digest);

    /// Any artifacts that are being fetched have any job ids matching the given client id removed.
    fn client_disconnected(&mut self, cid: ClientId);

    /// Get the path and size of a given artifact if it has a non-zero refcount, `None` otherwise.
    /// If the refcount is non-zero, it is increased by one.
    fn get_artifact_for_worker(&mut self, digest: &Sha256Digest) -> Option<(PathBuf, u64)>;

    /// Get a stream that reads the contents of an artifact. Returns said stream and the size of
    /// the artifact. You must have a non-zero refcount on the artifact before calling this
    /// function and keep it the whole time the stream is being used.
    fn read_artifact(&mut self, digest: &Sha256Digest) -> (Self::ArtifactStream, u64);
}
