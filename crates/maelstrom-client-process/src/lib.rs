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
mod util;

pub use maelstrom_util::process::clone_into_pid_and_user_namespace;

use anyhow::Result;
use futures::stream::{self, StreamExt as _};
use maelstrom_client_base::proto::client_process_server::ClientProcessServer;
use maelstrom_linux as linux;
use maelstrom_util::{config::common::LogLevel, process::ExitCode};
use rpc::{ArcHandler, Handler};
use slog::{info, Logger};
use std::{
    error,
    os::unix::net::{UnixListener as StdUnixListener, UnixStream as StdUnixStream},
    str,
};
use stream_wrapper::StreamWrapper;
use tokio::net::UnixStream as TokioUnixStream;
use tonic::transport::Server;

// This hack makes some macros in maelstrom_test work correctly
#[cfg(test)]
extern crate maelstrom_client_base as maelstrom_client;

type TokioError<T> = Result<T, Box<dyn error::Error + Send + Sync>>;

#[tokio::main]
async fn main_after_clone(
    sock: StdUnixStream,
    log: Option<Logger>,
    rpc_log_level: LogLevel,
) -> Result<ExitCode> {
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

    // Tell the local worker to shut down, and then wait for its task to complete.
    handler.client.read().await.shutdown().await;

    res?;
    Ok(ExitCode::SUCCESS)
}

/// The main function for the process when invoked using the "fork" method, described at
/// [`maelstrom_client::ClientProcess`].
pub fn main_for_fork(sock: StdUnixStream, rpc_log_level: LogLevel) -> Result<ExitCode> {
    clone_into_pid_and_user_namespace()?;
    main_after_clone(sock, None, rpc_log_level)
}

/// The main function for the process when invoked using the "spawn" method, described at
/// [`maelstrom_client::ClientProcess`].
pub fn main_for_spawn() -> Result<ExitCode> {
    clone_into_pid_and_user_namespace()?;

    maelstrom_util::log::run_with_logger(LogLevel::Debug, |log| {
        let (sock, path) = linux::autobound_unix_listener(Default::default(), 1)?;
        let name = str::from_utf8(&path[1..]).unwrap();

        info!(log, "listening on unix-abstract:{name}");
        println!("{name}");

        let (sock, addr) = StdUnixListener::from(sock).accept()?;
        info!(log, "got connection"; "address" => ?addr);

        let result = main_after_clone(sock, Some(log.clone()), LogLevel::Debug);
        info!(log, "shutting down"; "result" => ?result);
        result
    })
}
