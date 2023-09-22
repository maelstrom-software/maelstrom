//! Code for the broker binary.

pub mod cache;
mod connection;
mod http;
pub mod scheduler;
mod scheduler_task;

use meticulous_util::error::{Error, Result};
use std::sync::Arc;

/// "Main loop" for a signal handler. This function will block until it receives the indicated
/// signal, then it will return an error.
async fn signal_handler(kind: tokio::signal::unix::SignalKind) -> Result<()> {
    tokio::signal::unix::signal(kind)?.recv().await;
    Err(Error::msg(format!("received signal {kind:?}")))
}

/// The main function for the broker. This should be called on a task of its own. It will return
/// when a signal is received, or when the broker or http listener socket returns an error at
/// accept time.
pub async fn main(listener: tokio::net::TcpListener, http_listener: tokio::net::TcpListener) {
    let scheduler_task = scheduler_task::SchedulerTask::default();
    let id_vendor = Arc::new(connection::IdVendor::default());
    let mut join_set = tokio::task::JoinSet::new();
    join_set.spawn(http::listener_main(
        http_listener,
        scheduler_task.scheduler_sender().clone(),
        id_vendor.clone(),
    ));
    join_set.spawn(connection::listener_main(
        listener,
        scheduler_task.scheduler_sender().clone(),
        id_vendor,
    ));
    join_set.spawn(async move {
        scheduler_task.run().await;
        Ok(())
    });
    join_set.spawn(signal_handler(tokio::signal::unix::SignalKind::interrupt()));
    join_set.spawn(signal_handler(tokio::signal::unix::SignalKind::terminate()));
    join_set.join_next().await;
}
