use crate::{
    cache::LazyRead,
    cache::{remote, BrokerCache},
};
use anyhow::{anyhow, Result};
use maelstrom_base::Sha256Digest;
use maelstrom_github::GitHubClient;
use std::{io, pin::Pin, sync::Arc};
use tokio::io::AsyncRead;
use url::Url;

pub struct GitHubArtifactReader {
    client: Arc<GitHubClient>,
}

impl GitHubArtifactReader {
    fn new(token: &str, url: Url) -> Result<Self> {
        Ok(Self {
            client: Arc::new(GitHubClient::new(token, url)?),
        })
    }
}

type GitHubArtifactStream = Pin<Box<dyn AsyncRead + Send + Sync + 'static>>;

async fn download_artifact(
    client: Arc<GitHubClient>,
    digest: Sha256Digest,
) -> Result<GitHubArtifactStream> {
    let artifact_name = format!("maelstrom-cache-sha256-{digest}");
    let artifact = client
        .get(&artifact_name)
        .await?
        .ok_or_else(|| anyhow!("artifact {digest} not found in broker cache"))?;
    let stream = client
        .download(artifact.backend_ids, &artifact_name)
        .await?;
    Ok(Box::pin(stream))
}

impl remote::RemoteArtifactReader for GitHubArtifactReader {
    type ArtifactStream = LazyRead<GitHubArtifactStream>;

    fn read(&self, digest: &Sha256Digest) -> Self::ArtifactStream {
        let client = self.client.clone();
        let digest = digest.clone();
        LazyRead::new(Box::pin(async move {
            download_artifact(client, digest)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        }))
    }
}

pub enum GithubCache {}

impl BrokerCache for GithubCache {
    type Cache = remote::RemoteCache<GitHubArtifactReader>;
    type TempFileFactory = remote::ErroringTempFileFactory;

    fn new(
        config: &crate::config::Config,
        _log: slog::Logger,
    ) -> Result<(Self::Cache, Self::TempFileFactory)> {
        Ok((
            Self::Cache::new(GitHubArtifactReader::new(
                config.github_actions_token.as_ref().unwrap().as_str(),
                config.github_actions_url.as_ref().unwrap().clone(),
            )?),
            remote::ErroringTempFileFactory,
        ))
    }
}
