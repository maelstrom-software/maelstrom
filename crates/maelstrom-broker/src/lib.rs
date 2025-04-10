//! Code for the broker binary.

mod artifact_fetcher;
mod artifact_pusher;
mod cache;
pub mod config;
mod connection;
#[cfg(feature = "web-ui")]
mod http;
mod scheduler_task;

use anyhow::{bail, Context as _, Result};
use cache::{github::GithubCache, local::TcpUploadLocalCache, BrokerCache, SchedulerCache};
use config::Config;
use maelstrom_base::stats::BROKER_STATISTICS_INTERVAL;
use maelstrom_github::GitHubClient;
use maelstrom_util::config::common::ClusterCommunicationStrategy;
use scheduler_task::SchedulerTask;
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

async fn stats_heartbeat<TempFileT>(sender: scheduler_task::Sender<TempFileT>) {
    let mut interval = tokio::time::interval(BROKER_STATISTICS_INTERVAL);
    while sender
        .send(scheduler_task::Message::StatisticsHeartbeat)
        .is_ok()
    {
        interval.tick().await;
    }
}

struct Listeners {
    listener: TcpListener,
    #[cfg(feature = "web-ui")]
    http_listener: TcpListener,
}

/// The main function for the broker. It will return when a signal is received, or when the broker
/// or http listener socket returns an error at accept time.
async fn main_inner_inner<BrokerCacheT>(
    listeners: Listeners,
    config: Config,
    log: Logger,
) -> Result<()>
where
    BrokerCacheT: BrokerCache,
    <BrokerCacheT::Cache as SchedulerCache>::TempFile: Send + Sync + 'static,
    <BrokerCacheT::Cache as SchedulerCache>::ArtifactStream:
        tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let (cache, temp_file_factory) = BrokerCacheT::new(&config, log.clone())?;
    let scheduler_task = SchedulerTask::new(cache);
    let id_vendor = Arc::new(IdVendor {
        id: AtomicU32::new(0),
    });

    let github_connection_tasks = Arc::new(Mutex::new(JoinSet::new()));
    let mut join_set = JoinSet::new();

    #[cfg(feature = "web-ui")]
    join_set.spawn(http::listener_main(
        listeners.http_listener,
        scheduler_task.scheduler_task_sender().clone(),
        id_vendor.clone(),
        log.clone(),
    ));
    join_set.spawn(connection::tcp_listener_main(
        listeners.listener,
        scheduler_task.scheduler_task_sender().clone(),
        id_vendor.clone(),
        temp_file_factory,
        log.clone(),
    ));
    if let ClusterCommunicationStrategy::GitHub = config.cluster_communication_strategy {
        let client = GitHubClient::new(
            config.github_actions_token.unwrap().as_str(),
            config.github_actions_url.unwrap().clone(),
        )?;
        join_set.spawn(connection::github_acceptor_main(
            client,
            scheduler_task.scheduler_task_sender().clone(),
            id_vendor,
            log.clone(),
            github_connection_tasks.clone(),
        ));
    } else {
        info!(log, "not listening for GitHub connections");
    }

    join_set.spawn(stats_heartbeat(
        scheduler_task.scheduler_task_sender().clone(),
    ));
    join_set.spawn(scheduler_task.run(log.clone()));
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

    join_set.shutdown().await;
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

#[cfg(feature = "web-ui")]
async fn start_listeners(config: &Config, log: &Logger) -> Result<Listeners> {
    let sock_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, *config.port.inner(), 0, 0);
    let listener = TcpListener::bind(sock_addr)
        .await
        .context("binding listener socket")?;
    let listener_addr = listener
        .local_addr()
        .context("retrieving listener local address")?;

    let sock_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, *config.http_port.inner(), 0, 0);
    let http_listener = TcpListener::bind(sock_addr)
        .await
        .context("binding http listener socket")?;
    let http_listener_addr = http_listener
        .local_addr()
        .context("retrieving http listener local address")?;

    info!(log, "started";
        "config" => ?config,
        "addr" => listener_addr,
        "http_addr" => http_listener_addr,
        "pid" => process::id());

    Ok(Listeners {
        listener,
        http_listener,
    })
}

#[cfg(not(feature = "web-ui"))]
async fn start_listeners(config: &Config, log: &Logger) -> Result<Listeners> {
    let sock_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, *config.port.inner(), 0, 0);
    let listener = TcpListener::bind(sock_addr)
        .await
        .context("binding listener socket")?;
    let listener_addr = listener
        .local_addr()
        .context("retrieving listener local address")?;

    info!(log, "started";
        "config" => ?config,
        "addr" => listener_addr,
        "pid" => process::id());

    Ok(Listeners { listener })
}

#[tokio::main]
async fn main_inner(config: Config, log: Logger) -> Result<()> {
    let listeners = start_listeners(&config, &log).await?;

    match config.cluster_communication_strategy {
        ClusterCommunicationStrategy::Tcp => {
            main_inner_inner::<TcpUploadLocalCache>(listeners, config, log.clone()).await?;
        }
        ClusterCommunicationStrategy::GitHub => {
            if config.github_actions_token.is_none() {
                bail!(
                    "because config value `cluster-communication-strategy` is set to \
                    `github`, config value `github-actions-token` must be set"
                );
            }
            if config.github_actions_url.is_none() {
                bail!(
                    "because config value `cluster-communication-strategy` is set to \
                            `github`, config value `github-actions-url` must be set"
                );
            }
            main_inner_inner::<GithubCache>(listeners, config, log.clone()).await?;
        }
    }

    info!(log, "exiting");
    Ok(())
}
