mod artifact_pusher;
mod client;
mod digest_repo;
mod local_broker;
mod rpc;
mod stream_wrapper;

use anyhow::Result;
use client::Client;
use futures::stream::StreamExt as _;
use maelstrom_base::Sha256Digest;
use maelstrom_client_base::proto::client_process_server::ClientProcessServer;
use maelstrom_util::{async_fs, io::Sha256Stream};
use rpc::Handler;
use slog::Logger;
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
async fn main_inner(sock: StdUnixStream, log: Option<Logger>) -> Result<()> {
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

pub fn main(sock: StdUnixStream, log: Option<Logger>) -> Result<()> {
    maelstrom_worker::clone_into_pid_and_user_namespace()?;
    main_inner(sock, log)
}
