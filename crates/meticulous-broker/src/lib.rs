//! Code for the broker binary.

mod artifact_fetcher;
mod artifact_pusher;
pub mod config;
mod connection;
mod http;
mod scheduler_task;

use config::{CacheBytesUsedTarget, CacheRoot};
use meticulous_base::stats::BROKER_STATISTICS_INTERVAL;
use scheduler_task::{SchedulerMessage, SchedulerSender, SchedulerTask};
use slog::{error, Logger};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tokio::{
    net::TcpListener,
    signal::unix::{self, SignalKind},
    task::JoinSet,
};

/// Simple wrapper around a [AtomicU32] used to vend [meticulous_base::ClientId]s and
/// [meticulous_base::WorkerId]s.
pub struct IdVendor {
    id: AtomicU32,
}

impl IdVendor {
    pub fn vend<T: From<u32>>(&self) -> T {
        self.id.fetch_add(1, Ordering::SeqCst).into()
    }
}

/// "Main loop" for a signal handler. This function will block until it receives the indicated
/// signal, then it will return an error.
async fn signal_handler(kind: SignalKind, log: Logger, signame: &'static str) {
    unix::signal(kind)
        .expect("failed to register signal handler")
        .recv()
        .await;
    error!(log, "received {signame}")
}

async fn stats_heartbeat(sender: SchedulerSender) {
    let mut interval = tokio::time::interval(BROKER_STATISTICS_INTERVAL);
    while sender.send(SchedulerMessage::StatisticsHeartbeat).is_ok() {
        interval.tick().await;
    }
}

/// The main function for the broker. This should be called on a task of its own. It will return
/// when a signal is received, or when the broker or http listener socket returns an error at
/// accept time.
pub async fn main(
    listener: TcpListener,
    http_listener: TcpListener,
    cache_root: CacheRoot,
    cache_bytes_used_target: CacheBytesUsedTarget,
    log: Logger,
) {
    let scheduler_task = SchedulerTask::new(cache_root, cache_bytes_used_target, log.clone());
    let id_vendor = Arc::new(IdVendor {
        id: AtomicU32::new(0),
    });

    let mut join_set = JoinSet::new();

    join_set.spawn(http::listener_main(
        http_listener,
        scheduler_task.scheduler_sender().clone(),
        id_vendor.clone(),
        log.clone(),
    ));
    join_set.spawn(connection::listener_main(
        listener,
        scheduler_task.scheduler_sender().clone(),
        id_vendor,
        scheduler_task.cache_tmp_path().to_owned(),
        log.clone(),
    ));
    join_set.spawn(stats_heartbeat(scheduler_task.scheduler_sender().clone()));
    join_set.spawn(scheduler_task.run());
    join_set.spawn(signal_handler(
        SignalKind::interrupt(),
        log.clone(),
        "SIGINT",
    ));
    join_set.spawn(signal_handler(
        SignalKind::terminate(),
        log.clone(),
        "SIGTERM",
    ));

    join_set.join_next().await;
}
