use crate::cache::LazyRead;
use crate::cache::{remote, BrokerCache};
use anyhow::{anyhow, Result};
use maelstrom_base::Sha256Digest;
use maelstrom_github::GitHubClient;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncRead;
use url::Url;

pub struct GitHubArtifactReader {
    client: Arc<GitHubClient>,
}

fn env_or_error(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| anyhow!("{key} environment variable missing"))
}

impl GitHubArtifactReader {
    fn new() -> Result<Self> {
        // XXX remi: I would prefer if we didn't read these from environment variables.
        let token = env_or_error("ACTIONS_RUNTIME_TOKEN")?;
        let base_url = Url::parse(&env_or_error("ACTIONS_RESULTS_URL")?)?;
        Ok(Self {
            client: Arc::new(GitHubClient::new(&token, base_url)?),
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
        .ok_or_else(|| anyhow!("artifact {digest} not found"))?;
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
        _config: crate::config::Config,
        _log: slog::Logger,
    ) -> Result<(Self::Cache, Self::TempFileFactory)> {
        Ok((
            Self::Cache::new(GitHubArtifactReader::new()?),
            remote::ErroringTempFileFactory,
        ))
    }
}
