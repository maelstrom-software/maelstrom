use crate::{
    artifact_pusher::{construct_upload_name, start_task_inner, Receiver, SuccessCb},
    progress::{ProgressTracker, UploadProgressReader},
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use maelstrom_base::{ArtifactUploadLocation, Sha256Digest};
use maelstrom_github::{Body, GitHubClient};
use maelstrom_util::async_fs::Fs;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::task::JoinSet;
use tokio_util::io::ReaderStream;

fn two_hours_from_now() -> DateTime<Utc> {
    Utc::now() + Duration::from_secs(60 * 60 * 2)
}

pub async fn push_one_artifact(
    github_client: Arc<GitHubClient>,
    upload_tracker: ProgressTracker,
    path: PathBuf,
    digest: Sha256Digest,
    success_callback: SuccessCb,
) -> Result<()> {
    let fs = Fs::new();
    let size = fs.metadata(&path).await?.len();

    let upload_name = construct_upload_name(&digest, &path);
    let prog = Arc::new(upload_tracker.new_task(&upload_name, size));

    let artifact_name = format!("maelstrom-cache-sha256-{digest}");
    github_client
        .upload_with(&artifact_name, Some(two_hours_from_now()), size, || {
            // This is called again if the upload is retried, so start over each time.
            let prog = prog.clone();
            let path = path.clone();
            async move {
                prog.reset();
                let file = Fs::new().open_file(&path).await?.into_inner();
                let reader = UploadProgressReader::new(prog, file);
                Ok(Body::wrap_stream(ReaderStream::new(reader)))
            }
        })
        .await?;

    success_callback(ArtifactUploadLocation::Remote);

    Ok(())
}

pub fn start_task(
    github_client: GitHubClient,
    join_set: &mut JoinSet<Result<()>>,
    receiver: Receiver,
    upload_tracker: ProgressTracker,
) {
    let github_client = Arc::new(github_client);

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
}
