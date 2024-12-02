use crate::{scheduler_task::SchedulerCache, BrokerCache, TempFileFactory};
use anyhow::{bail, Result};
use maelstrom_base::{ClientId, JobId, Sha256Digest};
use maelstrom_util::cache::{fs::TempFile, GetArtifact};
use std::path::{Path, PathBuf};
use tokio::io::AsyncRead;

pub struct ArtifactsCache;

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

    fn get_artifact(&mut self, _jid: JobId, _digest: Sha256Digest) -> GetArtifact {
        todo!()
    }

    fn got_artifact(&mut self, _digest: &Sha256Digest, _file: Self::TempFile) -> Vec<JobId> {
        todo!()
    }

    fn decrement_refcount(&mut self, _digest: &Sha256Digest) {
        todo!()
    }

    fn client_disconnected(&mut self, _cid: ClientId) {
        todo!()
    }

    fn get_artifact_for_worker(&mut self, _digest: &Sha256Digest) -> Option<(PathBuf, u64)> {
        todo!()
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
        Ok((ArtifactsCache, ErroringTempFileFactory))
    }
}
