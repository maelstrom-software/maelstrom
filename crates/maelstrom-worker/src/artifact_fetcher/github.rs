use crate::{
    dispatcher::{self, Message},
    types::{DispatcherSender, TempFile, TempFileFactory},
};
use anyhow::{anyhow, Result};
use maelstrom_base::Sha256Digest;
use maelstrom_github::GitHubClient;
use maelstrom_util::{
    async_fs::Fs,
    cache::{fs::TempFile as _, GotArtifact},
    r#async::Pool,
};
use slog::{debug, o, warn, Logger};
use std::{num::NonZeroU32, sync::Arc};
use tokio::task;

pub struct GitHubArtifactFetcher {
    github_client: Arc<GitHubClient>,
    dispatcher_sender: DispatcherSender,
    log: Logger,
    pool: Arc<Pool<()>>,
    temp_file_factory: TempFileFactory,
}

impl GitHubArtifactFetcher {
    pub fn new(
        max_simultaneous_fetches: NonZeroU32,
        github_client: GitHubClient,
        dispatcher_sender: DispatcherSender,
        log: Logger,
        temp_file_factory: TempFileFactory,
    ) -> Self {
        let github_client = Arc::new(github_client);
        Self {
            github_client,
            dispatcher_sender,
            log,
            pool: Arc::new(Pool::new(max_simultaneous_fetches)),
            temp_file_factory,
        }
    }
}

impl dispatcher::ArtifactFetcher for GitHubArtifactFetcher {
    fn start_artifact_fetch(&mut self, digest: Sha256Digest) {
        let log = self.log.new(o!(
            "digest" => digest.to_string(),
        ));
        debug!(log, "artifact fetcher request enqueued");
        let github_client = self.github_client.clone();
        let dispatcher_sender = self.dispatcher_sender.clone();
        let pool = self.pool.clone();
        let temp_file_factory = self.temp_file_factory.clone();
        task::spawn(async move {
            let result = pool
                .call_with_item(|_: Option<()>| {
                    main(github_client, &digest, &log, temp_file_factory)
                })
                .await;
            debug!(log, "artifact fetcher request completed"; "result" => ?result);
            let _ = dispatcher_sender.send(Message::ArtifactFetchCompleted(
                digest,
                result.map(GotArtifact::file),
            ));
        });
    }
}

async fn main(
    github_client: Arc<GitHubClient>,
    digest: &Sha256Digest,
    log: &Logger,
    temp_file_factory: TempFileFactory,
) -> Result<((), TempFile)> {
    let temp_file = task::spawn_blocking(move || temp_file_factory.temp_file())
        .await
        .unwrap()
        .inspect_err(|err| {
            warn!(log, "artifact fetcher failed to create a temporary file"; "error" => %err);
        })?;

    let artifact_name = format!("maelstrom-cache-sha256-{digest}");
    let artifact = github_client
        .get(&artifact_name)
        .await?
        .ok_or_else(|| anyhow!("artifact {digest} not found"))?;
    let mut stream = github_client
        .download(artifact.backend_ids, &artifact_name)
        .await?;

    let fs = Fs::new();
    let mut file = fs.create_file(temp_file.path()).await?;
    tokio::io::copy(&mut stream, &mut file).await?;

    Ok(((), temp_file))
}
