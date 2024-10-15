use crate::{
    dispatcher,
    types::{Cache, CacheKeyKind},
};
use maelstrom_base::{JobId, Sha256Digest};
use maelstrom_util::cache::{self, fs, GotArtifact};
use std::path::PathBuf;

/*                 _
 *   ___ __ _  ___| |__   ___
 *  / __/ _` |/ __| '_ \ / _ \
 * | (_| (_| | (__| | | |  __/
 *  \___\__,_|\___|_| |_|\___|
 *  FIGLET: cache
 */

pub enum CacheGetStrategy {}

impl cache::GetStrategy for CacheGetStrategy {
    type Getter = ();
    fn getter_from_job_id(_jid: JobId) -> Self::Getter {}
}

/// The standard implementation of [`Cache`] that just calls into [`cache::Cache`].
impl dispatcher::Cache for Cache {
    type Fs = fs::std::Fs;

    fn get_artifact(
        &mut self,
        kind: CacheKeyKind,
        artifact: Sha256Digest,
        jid: JobId,
    ) -> cache::GetArtifact {
        self.get_artifact(kind, artifact, jid)
    }

    fn got_artifact_failure(&mut self, kind: CacheKeyKind, digest: &Sha256Digest) -> Vec<JobId> {
        self.got_artifact_failure(kind, digest)
    }

    fn got_artifact_success(
        &mut self,
        kind: CacheKeyKind,
        digest: &Sha256Digest,
        artifact: GotArtifact<fs::std::Fs>,
    ) -> Result<Vec<JobId>, (anyhow::Error, Vec<JobId>)> {
        self.got_artifact_success(kind, digest, artifact)
    }

    fn decrement_ref_count(&mut self, kind: CacheKeyKind, digest: &Sha256Digest) {
        self.decrement_ref_count(kind, digest)
    }

    fn cache_path(&self, kind: CacheKeyKind, digest: &Sha256Digest) -> PathBuf {
        self.cache_path(kind, digest).into_path_buf()
    }
}
