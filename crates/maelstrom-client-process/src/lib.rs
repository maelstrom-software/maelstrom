mod artifact_pusher;
mod client;
mod digest_repo;
mod dispatcher;
mod local_broker;
mod rpc;
mod stream_wrapper;

use anyhow::Result;
use maelstrom_base::Sha256Digest;
use maelstrom_client_base::{MANIFEST_DIR, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR};
use maelstrom_util::{async_fs, io::Sha256Stream, net};
pub use rpc::client_process_main;
use std::{path::Path, time::SystemTime};
use tokio::{net::tcp, sync::mpsc::UnboundedSender};

async fn calculate_digest(path: &Path) -> Result<(SystemTime, Sha256Digest)> {
    let fs = async_fs::Fs::new();
    let mut f = fs.open_file(path).await?;
    let mut hasher = Sha256Stream::new(tokio::io::sink());
    tokio::io::copy(&mut f, &mut hasher).await?;
    let mtime = f.metadata().await?.modified()?;

    Ok((mtime, hasher.finalize().1))
}

pub struct SocketReader {
    stream: tcp::OwnedReadHalf,
    channel: local_broker::Sender,
}

impl SocketReader {
    fn new(stream: tcp::OwnedReadHalf, channel: UnboundedSender<local_broker::Message>) -> Self {
        Self { stream, channel }
    }

    pub async fn process_one(&mut self) -> bool {
        let Ok(message) = net::read_message_from_async_socket(&mut self.stream).await else {
            return false;
        };
        self.channel
            .send(local_broker::Message::Broker(message))
            .is_ok()
    }
}
