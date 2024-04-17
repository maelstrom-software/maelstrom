mod artifact_pusher;
mod client;
mod digest_repo;
mod dispatcher;
mod local_broker;
mod rpc;
mod stream_wrapper;

use anyhow::Result;
use maelstrom_base::Sha256Digest;
use maelstrom_util::{async_fs, io::Sha256Stream};
pub use rpc::client_process_main;
use std::{path::Path, time::SystemTime};

async fn calculate_digest(path: &Path) -> Result<(SystemTime, Sha256Digest)> {
    let fs = async_fs::Fs::new();
    let mut f = fs.open_file(path).await?;
    let mut hasher = Sha256Stream::new(tokio::io::sink());
    tokio::io::copy(&mut f, &mut hasher).await?;
    let mtime = f.metadata().await?.modified()?;

    Ok((mtime, hasher.finalize().1))
}
