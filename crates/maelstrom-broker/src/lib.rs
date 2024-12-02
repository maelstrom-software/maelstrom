//! Code for the broker binary.

mod artifact_fetcher;
mod artifact_pusher;
pub mod config;
mod connection;
mod http;
mod scheduler_task;

use anyhow::{Context as _, Result};
use config::Config;
use maelstrom_base::stats::BROKER_STATISTICS_INTERVAL;
use scheduler_task::{SchedulerCache, SchedulerMessage, SchedulerSender, SchedulerTask};
use slog::{error, info, Logger};
use std::{
    net::{Ipv6Addr, SocketAddrV6},
    process,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};
use tokio::{
    net::TcpListener,
    signal::unix::{self, SignalKind},
    task::JoinSet,
};

/// Simple wrapper around a [AtomicU32] used to vend [maelstrom_base::ClientId]s and
/// [maelstrom_base::WorkerId]s.
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

async fn stats_heartbeat<TempFileT>(sender: SchedulerSender<TempFileT>) {
    let mut interval = tokio::time::interval(BROKER_STATISTICS_INTERVAL);
    while sender.send(SchedulerMessage::StatisticsHeartbeat).is_ok() {
        interval.tick().await;
    }
}

trait TempFileFactory: Clone {
    type TempFile: maelstrom_util::cache::fs::TempFile;

    fn temp_file(&self) -> Result<Self::TempFile>;
}

impl TempFileFactory
    for maelstrom_util::cache::TempFileFactory<maelstrom_util::cache::fs::std::Fs>
{
    type TempFile = <maelstrom_util::cache::fs::std::Fs as maelstrom_util::cache::fs::Fs>::TempFile;

    fn temp_file(&self) -> Result<Self::TempFile> {
        self.temp_file()
    }
}

/// The main function for the broker. It will return when a signal is received, or when the broker
/// or http listener socket returns an error at accept time.
async fn main_inner_inner<BrokerCacheT>(
    listener: TcpListener,
    http_listener: TcpListener,
    config: Config,
    log: Logger,
) -> Result<()>
where
    BrokerCacheT: BrokerCache,
    <BrokerCacheT::Cache as SchedulerCache>::ArtifactStream: Send + 'static,
    <BrokerCacheT::Cache as SchedulerCache>::TempFile: Send + Sync + 'static,
    <BrokerCacheT::Cache as SchedulerCache>::ArtifactStream:
        tokio::io::AsyncRead + Unpin + Send + Sync,
{
    let (cache, temp_file_factory) = BrokerCacheT::new(config, log.clone())?;
    let scheduler_task = SchedulerTask::new(cache);
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
        temp_file_factory,
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

    Ok(())
}

pub fn main(config: Config, log: Logger) -> Result<()> {
    main_inner(config, log)
}

trait BrokerCache {
    type Cache: SchedulerCache + Sized + Send + 'static;
    type TempFileFactory: TempFileFactory<TempFile = <Self::Cache as SchedulerCache>::TempFile>
        + Sized
        + Send
        + 'static;

    fn new(config: Config, log: Logger) -> Result<(Self::Cache, Self::TempFileFactory)>;
}

enum TcpUploadLocalCache {}

impl BrokerCache for TcpUploadLocalCache {
    type Cache = maelstrom_util::cache::Cache<
        maelstrom_util::cache::fs::std::Fs,
        scheduler_task::cache::BrokerKey,
        scheduler_task::cache::BrokerGetStrategy,
    >;

    type TempFileFactory =
        maelstrom_util::cache::TempFileFactory<maelstrom_util::cache::fs::std::Fs>;

    fn new(config: Config, log: Logger) -> Result<(Self::Cache, Self::TempFileFactory)> {
        maelstrom_util::cache::Cache::new(
            maelstrom_util::cache::fs::std::Fs,
            config.cache_root,
            config.cache_size,
            log.clone(),
            true,
        )
    }
}

#[tokio::main]
async fn main_inner(config: Config, log: Logger) -> Result<()> {
    let sock_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, *config.port.inner(), 0, 0);
    let listener = TcpListener::bind(sock_addr)
        .await
        .context("binding listener socket")?;

    let sock_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, *config.http_port.inner(), 0, 0);
    let http_listener = TcpListener::bind(sock_addr)
        .await
        .context("binding http listener socket")?;

    let listener_addr = listener
        .local_addr()
        .context("retrieving listener local address")?;
    let http_listener_addr = http_listener
        .local_addr()
        .context("retrieving listener local address")?;
    info!(log, "started";
        "config" => ?config,
        "addr" => listener_addr,
        "http_addr" => http_listener_addr,
        "pid" => process::id());

    main_inner_inner::<TcpUploadLocalCache>(listener, http_listener, config, log.clone()).await?;
    info!(log, "exiting");
    Ok(())
}
