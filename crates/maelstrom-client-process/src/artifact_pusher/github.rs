use crate::artifact_pusher::{construct_upload_name, start_task_inner, Receiver, SuccessCb};
use crate::progress::{ProgressTracker, UploadProgressReader};
use anyhow::{anyhow, Result};
use maelstrom_base::{proto::ArtifactUploadLocation, Sha256Digest};
use maelstrom_github::{FileStreamBuilder, GitHubClient, SeekableStream};
use maelstrom_util::async_fs::Fs;
use std::{path::PathBuf, sync::Arc};
use tokio::task::JoinSet;
use url::Url;

pub async fn push_one_artifact(
    github_client: Arc<GitHubClient>,
    upload_tracker: ProgressTracker,
    path: PathBuf,
    digest: Sha256Digest,
    success_callback: SuccessCb,
) -> Result<()> {
    let fs = Fs::new();
    let file = fs.open_file(&path).await?;
    let size = file.metadata().await?.len();

    let upload_name = construct_upload_name(&digest, &path);
    let prog = upload_tracker.new_task(&upload_name, size);

    let artifact_name = format!("maelstrom-cache-sha256-{digest}");
    let file_stream = FileStreamBuilder::new(file.into_inner()).build().await?;
    let stream = Box::new(UploadProgressReader::new(prog, file_stream)) as Box<dyn SeekableStream>;
    github_client.upload(&artifact_name, stream).await?;

    success_callback(ArtifactUploadLocation::Remote);

    Ok(())
}

fn env_or_error(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| anyhow!("{key} environment variable missing"))
}

pub fn start_task(
    join_set: &mut JoinSet<Result<()>>,
    receiver: Receiver,
    upload_tracker: ProgressTracker,
) -> Result<()> {
    // XXX remi: I would prefer if we didn't read these from environment variables.
    let token = env_or_error("ACTIONS_RUNTIME_TOKEN")?;
    let base_url = Url::parse(&env_or_error("ACTIONS_RESULTS_URL")?)?;
    let github_client = Arc::new(GitHubClient::new(&token, base_url)?);

    start_task_inner(
        join_set,
        receiver,
        move |_, path, digest, success_callback| {
            let upload_tracker = upload_tracker.clone();
            let github_client = github_client.clone();
            async move {
                push_one_artifact(
                    github_client,
                    upload_tracker,
                    path,
                    digest,
                    success_callback,
                )
                .await
                .map(|()| ((), ()))
            }
        },
    );
    Ok(())
}
