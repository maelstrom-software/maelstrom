//! Code for the broker binary.

use scheduler_task::SchedulerTask;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tokio::{
    net::TcpListener,
    signal::unix::{self, SignalKind},
};

pub mod cache;
mod connection;
mod http;
mod scheduler;
mod scheduler_task;

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
async fn signal_handler(kind: SignalKind) {
    unix::signal(kind)
        .expect("failed to register signal handler")
        .recv()
        .await;
}

/// The main function for the broker. This should be called on a task of its own. It will return
/// when a signal is received, or when the broker or http listener socket returns an error at
/// accept time.
pub async fn main(listener: TcpListener, http_listener: TcpListener) {
    let scheduler_task = SchedulerTask::default();
    let id_vendor = Arc::new(IdVendor {
        id: AtomicU32::new(0),
    });

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
    join_set.spawn(scheduler_task.run());
    join_set.spawn(signal_handler(SignalKind::interrupt()));
    join_set.spawn(signal_handler(SignalKind::terminate()));

    join_set.join_next().await;
}
