//! Code for the broker binary.

mod artifact_fetcher;
mod artifact_pusher;
mod cache;
pub mod config;
mod connection;
mod http;
mod scheduler_task;

use anyhow::{anyhow, Context as _, Result};
use cache::{github::GithubCache, local::TcpUploadLocalCache, BrokerCache, SchedulerCache};
use config::Config;
use maelstrom_base::stats::BROKER_STATISTICS_INTERVAL;
use maelstrom_github::GitHubClient;
use maelstrom_util::config::common::ArtifactTransferStrategy;
use scheduler_task::{SchedulerMessage, SchedulerSender, SchedulerTask};
use slog::{error, info, Logger};
use std::{
    net::{Ipv6Addr, SocketAddrV6},
    process,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
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

fn env_or_error(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| anyhow!("{key} environment variable missing"))
}

fn github_client() -> Result<Arc<GitHubClient>> {
    // XXX remi: I would prefer if we didn't read these from environment variables.
    let token = env_or_error("ACTIONS_RUNTIME_TOKEN")?;
    let base_url = url::Url::parse(&env_or_error("ACTIONS_RESULTS_URL")?)?;
    Ok(Arc::new(GitHubClient::new(&token, base_url)?))
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
    <BrokerCacheT::Cache as SchedulerCache>::TempFile: Send + Sync + 'static,
    <BrokerCacheT::Cache as SchedulerCache>::ArtifactStream:
        tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let (cache, temp_file_factory) = BrokerCacheT::new(config, log.clone())?;
    let scheduler_task = SchedulerTask::new(cache);
    let id_vendor = Arc::new(IdVendor {
        id: AtomicU32::new(0),
    });

    let github_connection_tasks = Arc::new(Mutex::new(JoinSet::new()));
    let mut join_set = JoinSet::new();

    join_set.spawn(http::listener_main(
        http_listener,
        scheduler_task.scheduler_sender().clone(),
        id_vendor.clone(),
        log.clone(),
    ));
    join_set.spawn(connection::tcp_listener_main(
        listener,
        scheduler_task.scheduler_sender().clone(),
        id_vendor.clone(),
        temp_file_factory,
        log.clone(),
    ));
    if let Ok(client) = github_client() {
        join_set.spawn(connection::github_acceptor_main(
            client,
            scheduler_task.scheduler_sender().clone(),
            id_vendor,
            log.clone(),
            github_connection_tasks.clone(),
        ));
    } else {
        info!(log, "not listening for GitHub connections");
    }

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

    drop(join_set);
    let github_connection_tasks = Arc::into_inner(github_connection_tasks)
        .unwrap()
        .into_inner()
        .unwrap();
    github_connection_tasks.join_all().await;

    Ok(())
}

pub fn main(config: Config, log: Logger) -> Result<()> {
    main_inner(config, log)
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

    match config.artifact_transfer_strategy {
        ArtifactTransferStrategy::GitHub => {
            main_inner_inner::<GithubCache>(listener, http_listener, config, log.clone()).await?;
        }
        ArtifactTransferStrategy::TcpUpload => {
            main_inner_inner::<TcpUploadLocalCache>(listener, http_listener, config, log.clone())
                .await?;
        }
    }

    info!(log, "exiting");
    Ok(())
}
