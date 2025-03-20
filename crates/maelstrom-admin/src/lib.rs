//! Code for the admin binary.

pub mod config;

use anyhow::{bail, Result};
use config::Config;
use maelstrom_base::{
    proto::{BrokerToMonitor, Hello, MonitorToBroker},
    stats::BrokerStatistics,
};
use maelstrom_util::{
    broker_connection::{
        BrokerConnectionFactory, BrokerReadConnection as _, BrokerWriteConnection as _,
        GitHubQueueBrokerConnectionFactory, TcpBrokerConnectionFactory,
    },
    config::common::ClusterCommunicationStrategy,
};
use slog::Logger;

#[derive(clap::Subcommand)]
#[command(disable_help_subcommand = true)]
pub enum Subcommand {
    /// Print cluster status and exit.
    #[command(
        aliases = ["stats", "stat"],
        verbatim_doc_comment
    )]
    Status,

    /// Tell broker to stop.
    #[command(
        aliases = ["shut-down", "shutdown"],
        verbatim_doc_comment
    )]
    Stop,
}

pub fn main(config: Config, log: Logger, subcommand: Subcommand) -> Result<()> {
    match config.cluster_communication_strategy {
        ClusterCommunicationStrategy::Tcp => {
            let Some(broker) = config.broker else {
                bail!(
                    "because config value `cluster-communication-strategy` is set to `tcp`, \
                    config value `broker` must be set via `--broker` command-line option, \
                    `MAELSTROM_ADMIN_BROKER` or `MAELSTROM_BROKER` environment variables, or \
                    `broker` key in config file"
                );
            };
            main_inner(
                TcpBrokerConnectionFactory::new(broker, &log),
                &log,
                subcommand,
            )
        }
        ClusterCommunicationStrategy::GitHub => {
            let Some(token) = config.github_actions_token else {
                bail!(
                    "because config value `cluster-communication-strategy` is set to `github`, \
                    config value `github-actions-token` must be set via `--github-actions-token` \
                    command-line option, `MAELSTROM_ADMIN_GITHUB_ACTIONS_TOKEN` or \
                    `MAELSTROM_GITHUB_ACTIONS_TOKEN` environment variables, or \
                    `github-actions-token` key in config file"
                );
            };
            let Some(url) = config.github_actions_url else {
                bail!(
                    "because config value `cluster-communication-strategy` is set to `github`, \
                    config value `github-actions-url` must be set via `--github-actions-url` \
                    command-line option, `MAELSTROM_ADMIN_GITHUB_ACTIONS_URL` or \
                    `MAELSTROM_GITHUB_ACTIONS_URL` environment variables, or \
                    `github-actions-url` key in config file"
                );
            };
            main_inner(
                GitHubQueueBrokerConnectionFactory::new(&log, token, url)?,
                &log,
                subcommand,
            )
        }
    }
}

#[tokio::main]
async fn main_inner(
    broker_connection_factory: impl BrokerConnectionFactory,
    log: &Logger,
    subcommand: Subcommand,
) -> Result<()> {
    let hello = Hello::Monitor;
    let (mut read_stream, mut write_stream) = broker_connection_factory.connect(&hello).await?;
    match subcommand {
        Subcommand::Status => {
            write_stream
                .write_message(&MonitorToBroker::StatisticsRequest, log)
                .await?;
            let BrokerToMonitor::StatisticsResponse(BrokerStatistics {
                worker_statistics,
                job_statistics,
            }) = read_stream.read_message(log).await?;
            println!("Number of workers: {}", worker_statistics.len());
            println!(
                "Number of clients: {}",
                job_statistics
                    .iter()
                    .last()
                    .map(|sample| sample.client_to_stats.len())
                    .unwrap_or(0)
            );
        }
        Subcommand::Stop => {
            write_stream
                .write_message(&MonitorToBroker::StopRequest, log)
                .await?;
            if let Ok(message) = read_stream.read_message::<BrokerToMonitor>(log).await {
                bail!("expected broker to disconnect, but it sent message instead: {message:#?}");
            }
        }
    }
    Ok(())
}
