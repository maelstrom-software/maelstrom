mod artifact_pusher;
mod client;
mod digest_repo;
mod progress;
mod router;
mod rpc;
mod stream_wrapper;

pub use maelstrom_worker::clone_into_pid_and_user_namespace;

use anyhow::Result;
use client::Client;
use futures::stream::StreamExt as _;
use maelstrom_base::Sha256Digest;
use maelstrom_client_base::proto::client_process_server::ClientProcessServer;
use maelstrom_util::{async_fs, io::Sha256Stream, log::LoggerFactory};
use rpc::Handler;
use std::{error, os::unix::net::UnixStream as StdUnixStream, path::Path, time::SystemTime};
use stream_wrapper::StreamWrapper;
use tokio::net::UnixStream as TokioUnixStream;
use tonic::transport::Server;

async fn calculate_digest(path: &Path) -> Result<(SystemTime, Sha256Digest)> {
    let fs = async_fs::Fs::new();
    let mut f = fs.open_file(path).await?;
    let mut hasher = Sha256Stream::new(tokio::io::sink());
    tokio::io::copy(&mut f, &mut hasher).await?;
    let mtime = f.metadata().await?.modified()?;

    Ok((mtime, hasher.finalize().1))
}

type TokioError<T> = Result<T, Box<dyn error::Error + Send + Sync>>;

#[tokio::main]
pub async fn main_after_clone(sock: StdUnixStream, log: LoggerFactory) -> Result<()> {
    sock.set_nonblocking(true)?;
    let (sock, receiver) = StreamWrapper::new(TokioUnixStream::from_std(sock)?);
    Server::builder()
        .add_service(ClientProcessServer::new(Handler::new(Client::new(log))))
        .serve_with_incoming_shutdown(
            tokio_stream::once(TokioError::<_>::Ok(sock)).chain(tokio_stream::pending()),
            receiver,
        )
        .await?;
    Ok(())
}

pub fn main(sock: StdUnixStream, log: LoggerFactory) -> Result<()> {
    clone_into_pid_and_user_namespace()?;
    main_after_clone(sock, log)
}
