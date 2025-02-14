mod artifact_pusher;
mod client;
mod collapsed_job_spec;
mod digest_repo;
mod log;
mod preparer;
mod progress;
mod router;
mod rpc;
mod stream_wrapper;

pub use maelstrom_util::process::clone_into_pid_and_user_namespace;

use anyhow::Result;
use futures::stream::{self, StreamExt as _};
use maelstrom_base::Sha256Digest;
use maelstrom_client_base::proto::client_process_server::ClientProcessServer;
use maelstrom_util::{async_fs, config::common::LogLevel, io::Sha256Stream};
use rpc::{ArcHandler, Handler};
use std::{error, os::unix::net::UnixStream as StdUnixStream, path::Path, time::SystemTime};
use stream_wrapper::StreamWrapper;
use tokio::net::UnixStream as TokioUnixStream;
use tonic::transport::Server;

// This hack makes some macros in maelstrom_test work correctly
#[cfg(test)]
extern crate maelstrom_client_base as maelstrom_client;

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
pub async fn main_after_clone(
    sock: StdUnixStream,
    log: Option<slog::Logger>,
    rpc_log_level: LogLevel,
) -> Result<()> {
    let handler = ArcHandler::new(Handler::new(log, rpc_log_level));

    sock.set_nonblocking(true)?;
    let (sock, receiver) = StreamWrapper::new(TokioUnixStream::from_std(sock)?);
    let res = Server::builder()
        .add_service(ClientProcessServer::new(handler.clone()))
        .serve_with_incoming_shutdown(
            stream::once(async move { TokioError::<_>::Ok(sock) }).chain(stream::pending()),
            receiver,
        )
        .await;
    handler.client.read().await.shutdown().await;

    res?;
    Ok(())
}

/// The main function for the process when invoked using the "fork" method, described at
/// [`maelstrom_client::ClientBgProcess`].
pub fn main_for_fork(sock: StdUnixStream, rpc_log_level: LogLevel) -> Result<()> {
    clone_into_pid_and_user_namespace()?;
    main_after_clone(sock, None, rpc_log_level)
}
