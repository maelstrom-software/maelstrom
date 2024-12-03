use crate::cache::{remote, BrokerCache};
use anyhow::Result;
use maelstrom_base::Sha256Digest;
use tokio::io::AsyncRead;

pub struct GithubArtifactStream;

impl AsyncRead for GithubArtifactStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        todo!()
    }
}

#[derive(Default)]
pub struct GithubArtifactReader;

impl remote::RemoteArtifactReader for GithubArtifactReader {
    type ArtifactStream = GithubArtifactStream;

    fn read(&self, _digest: &Sha256Digest) -> Self::ArtifactStream {
        todo!()
    }
}

pub enum GithubCache {}

impl BrokerCache for GithubCache {
    type Cache = remote::RemoteCache<GithubArtifactReader>;
    type TempFileFactory = remote::ErroringTempFileFactory;

    fn new(
        _config: crate::config::Config,
        _log: slog::Logger,
    ) -> Result<(Self::Cache, Self::TempFileFactory)> {
        Ok((Self::Cache::default(), remote::ErroringTempFileFactory))
    }
}
