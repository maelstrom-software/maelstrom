use crate::artifact_pusher::{construct_upload_name, start_task_inner, Receiver, SuccessCb};
use crate::progress::{ProgressTracker, UploadProgressReader};
use anyhow::{anyhow, Context as _, Result};
use maelstrom_base::{
    proto::{ArtifactPusherToBroker, ArtifactUploadLocation, BrokerToArtifactPusher, Hello},
    Sha256Digest,
};
use maelstrom_util::{async_fs::Fs, config::common::BrokerAddr, net, net::AsRawFdExt as _};
use slog::Logger;
use std::path::{Path, PathBuf};
use tokio::{
    io::{self, AsyncReadExt as _},
    net::TcpStream,
    task::JoinSet,
};

pub async fn push_one_artifact(
    stream: Option<TcpStream>,
    upload_tracker: ProgressTracker,
    broker_addr: BrokerAddr,
    path: PathBuf,
    digest: Sha256Digest,
    success_callback: SuccessCb,
    log: Logger,
) -> Result<(TcpStream, ())> {
    push_one_artifact_inner(
        stream,
        upload_tracker,
        broker_addr,
        &path,
        digest,
        success_callback,
        log,
    )
    .await
    .with_context(|| format!("pushing artifact {}", path.display()))
}

async fn push_one_artifact_inner(
    stream: Option<TcpStream>,
    upload_tracker: ProgressTracker,
    broker_addr: BrokerAddr,
    path: &Path,
    digest: Sha256Digest,
    success_callback: SuccessCb,
    log: Logger,
) -> Result<(TcpStream, ())> {
    let mut stream = match stream {
        Some(stream) => stream,
        None => {
            let mut stream = TcpStream::connect(broker_addr.inner())
                .await?
                .set_socket_options()?;
            net::write_message_to_async_socket(&mut stream, Hello::ArtifactPusher, &log).await?;
            stream
        }
    };

    let fs = Fs::new();
    let file = fs.open_file(&path).await?;
    let size = file.metadata().await?.len();

    let upload_name = construct_upload_name(&digest, path);
    let prog = upload_tracker.new_task(&upload_name, size);

    let mut file = UploadProgressReader::new(prog, file.chain(io::repeat(0)).take(size));

    net::write_message_to_async_socket(&mut stream, ArtifactPusherToBroker(digest, size), &log)
        .await?;
    let copied = io::copy(&mut file, &mut stream).await?;
    assert_eq!(copied, size);

    let BrokerToArtifactPusher(resp) =
        net::read_message_from_async_socket(&mut stream, &log).await?;

    resp.map_err(|e| anyhow!("Error from broker: {e}"))?;

    success_callback(ArtifactUploadLocation::TcpUpload);

    Ok((stream, ()))
}

pub fn start_task(
    join_set: &mut JoinSet<Result<()>>,
    receiver: Receiver,
    broker_addr: BrokerAddr,
    upload_tracker: ProgressTracker,
    log: Logger,
) {
    start_task_inner(
        join_set,
        receiver,
        move |stream, path, digest, success_callback| {
            push_one_artifact(
                stream,
                upload_tracker.clone(),
                broker_addr,
                path,
                digest,
                success_callback,
                log.clone(),
            )
        },
    )
}
