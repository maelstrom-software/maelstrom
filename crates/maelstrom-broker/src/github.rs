use crate::{scheduler_task::SchedulerCache, BrokerCache, TempFileFactory};
use anyhow::{bail, Result};
use maelstrom_base::{ClientId, JobId, Sha256Digest};
use maelstrom_util::cache::{fs::TempFile, GetArtifact};
use std::collections::{hash_map::Entry as HashEntry, HashMap, HashSet};
use std::mem;
use std::path::{Path, PathBuf};
use tokio::io::AsyncRead;

enum Entry {
    Getting {
        jobs: Vec<JobId>,
        clients: HashSet<ClientId>,
    },
    InCache,
}

#[derive(Default)]
pub struct ArtifactsCache {
    entries: HashMap<Sha256Digest, Entry>,
}

#[derive(Debug)]
pub struct PanicTempFile;

impl TempFile for PanicTempFile {
    fn path(&self) -> &Path {
        panic!()
    }
}

#[derive(Clone)]
pub struct ErroringTempFileFactory;

impl TempFileFactory for ErroringTempFileFactory {
    type TempFile = PanicTempFile;

    fn temp_file(&self) -> Result<Self::TempFile> {
        bail!("Broker not accepting TCP uploads")
    }
}

pub struct RemoteArtifactStream;

impl AsyncRead for RemoteArtifactStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        todo!()
    }
}

impl SchedulerCache for ArtifactsCache {
    type TempFile = PanicTempFile;
    type ArtifactStream = RemoteArtifactStream;

    fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact {
        match self.entries.entry(digest) {
            HashEntry::Vacant(entry) => {
                entry.insert(Entry::Getting {
                    jobs: vec![jid],
                    clients: [jid.cid].into_iter().collect(),
                });
                GetArtifact::Get
            }
            HashEntry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Entry::Getting { jobs, clients } => {
                        jobs.push(jid);
                        if clients.insert(jid.cid) {
                            GetArtifact::Get
                        } else {
                            GetArtifact::Wait
                        }
                    }
                    Entry::InCache => GetArtifact::Success,
                }
            }
        }
    }

    fn got_artifact(&mut self, digest: &Sha256Digest, file: Option<Self::TempFile>) -> Vec<JobId> {
        assert!(file.is_none(), "expecting remote file");
        let Some(entry) = self.entries.get_mut(digest) else {
            return vec![];
        };
        let Entry::Getting { jobs, .. } = entry else {
            return vec![];
        };

        let jobs = mem::take(jobs);
        *entry = Entry::InCache;
        jobs
    }

    fn decrement_refcount(&mut self, _digest: &Sha256Digest) {
        // nothing to do
    }

    fn client_disconnected(&mut self, cid: ClientId) {
        for entry in self.entries.values_mut() {
            if let Entry::Getting { jobs, clients } = entry {
                jobs.retain(|jid| jid.cid != cid);
                clients.remove(&cid);
            }
        }
    }

    fn get_artifact_for_worker(&mut self, _digest: &Sha256Digest) -> Option<(PathBuf, u64)> {
        None
    }

    fn read_artifact(&mut self, _digest: &Sha256Digest) -> (Self::ArtifactStream, u64) {
        todo!()
    }
}

pub enum GithubCache {}

impl BrokerCache for GithubCache {
    type Cache = ArtifactsCache;
    type TempFileFactory = ErroringTempFileFactory;

    fn new(
        _config: crate::config::Config,
        _log: slog::Logger,
    ) -> Result<(Self::Cache, Self::TempFileFactory)> {
        Ok((ArtifactsCache::default(), ErroringTempFileFactory))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maelstrom_test::*;

    #[test]
    fn get_artifact_different_clients() {
        let mut cache = ArtifactsCache::default();

        assert_eq!(
            cache.get_artifact(jid![1, 1], digest![42]),
            GetArtifact::Get
        );
        assert_eq!(
            cache.get_artifact(jid![1, 2], digest![42]),
            GetArtifact::Wait
        );
        assert_eq!(
            cache.get_artifact(jid![1, 2], digest![43]),
            GetArtifact::Get
        );

        assert_eq!(
            cache.get_artifact(jid![2, 1], digest![42]),
            GetArtifact::Get
        );
        assert_eq!(
            cache.get_artifact(jid![2, 2], digest![42]),
            GetArtifact::Wait
        );
        assert_eq!(
            cache.get_artifact(jid![2, 2], digest![43]),
            GetArtifact::Get
        );
    }

    #[test]
    fn get_artifact_existing() {
        let mut cache = ArtifactsCache::default();

        cache.get_artifact(jid![1, 1], digest![42]);
        cache.got_artifact(&digest![42], None);

        assert_eq!(
            cache.get_artifact(jid![1, 2], digest![42]),
            GetArtifact::Success
        );
        assert_eq!(
            cache.get_artifact(jid![2, 1], digest![42]),
            GetArtifact::Success
        );
    }

    #[test]
    fn got_artifact() {
        let mut cache = ArtifactsCache::default();

        cache.get_artifact(jid![1, 1], digest![42]);
        cache.get_artifact(jid![1, 2], digest![42]);
        cache.get_artifact(jid![1, 2], digest![43]);
        cache.get_artifact(jid![2, 1], digest![42]);
        cache.get_artifact(jid![2, 2], digest![42]);

        assert_eq!(
            cache.got_artifact(&digest![42], None),
            vec![jid![1, 1], jid![1, 2], jid![2, 1], jid![2, 2]]
        );
    }

    #[test]
    fn got_artifact_after_client_disconnect() {
        let mut cache = ArtifactsCache::default();

        cache.get_artifact(jid![1, 1], digest![42]);
        cache.get_artifact(jid![1, 2], digest![42]);
        cache.get_artifact(jid![1, 2], digest![43]);
        cache.get_artifact(jid![2, 1], digest![42]);
        cache.get_artifact(jid![2, 2], digest![42]);
        cache.client_disconnected(cid![1]);

        assert_eq!(
            cache.got_artifact(&digest![42], None),
            vec![jid![2, 1], jid![2, 2]]
        );
    }
}
