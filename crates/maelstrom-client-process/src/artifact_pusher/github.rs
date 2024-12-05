use crate::artifact_pusher::{construct_upload_name, start_task_inner, Receiver, SuccessCb};
use crate::progress::{ProgressTracker, UploadProgressReader};
use anyhow::Result;
use maelstrom_base::{proto::ArtifactUploadLocation, Sha256Digest};
use maelstrom_github::{FileStreamBuilder, GitHubClient, SeekableStream};
use maelstrom_util::async_fs::Fs;
use std::{path::PathBuf, sync::Arc};
use tokio::task::JoinSet;

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

#[expect(dead_code)]
pub fn start_task(
    join_set: &mut JoinSet<Result<()>>,
    receiver: Receiver,
    github_client: Arc<GitHubClient>,
    upload_tracker: ProgressTracker,
) {
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
    )
}
