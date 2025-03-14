use anyhow::Result;
use derive_more::Constructor;
use maelstrom_base::proto::Hello;
use maelstrom_github::{GitHubClient, GitHubQueue, GitHubReadQueue, GitHubWriteQueue};
use crate::{
    config::common::BrokerAddr,
    net::{self, AsRawFdExt as _},
};
use serde::{de::DeserializeOwned, Serialize};
use slog::{error, Logger};
use std::{fmt::Debug, future::Future};
use tokio::{
    io::BufReader,
    net::TcpStream,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use url::Url;

pub trait BrokerConnectionFactory: Sized {
    type Read: BrokerReadConnection;
    type Write: BrokerWriteConnection;

    fn connect(&self, hello: &Hello) -> impl Future<Output = Result<(Self::Read, Self::Write)>>;
}

pub trait BrokerReadConnection: Send + Sync + 'static {
    fn read_messages<MessageT: Debug + DeserializeOwned, TransformedT: Send>(
        self,
        channel: UnboundedSender<TransformedT>,
        log: Logger,
        transform: impl Fn(MessageT) -> TransformedT + Send,
    ) -> impl Future<Output = Result<()>> + Send;
}

pub trait BrokerWriteConnection: Send + Sync + 'static {
    fn write_messages<MessageT: Debug + Send + Serialize + Sync>(
        self,
        channel: UnboundedReceiver<MessageT>,
        log: Logger,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Constructor)]
pub struct TcpBrokerConnectionFactory<'a> {
    broker: BrokerAddr,
    log: &'a Logger,
}

impl BrokerConnectionFactory for TcpBrokerConnectionFactory<'_> {
    type Read = BufReader<tokio::net::tcp::OwnedReadHalf>;
    type Write = tokio::net::tcp::OwnedWriteHalf;

    async fn connect(&self, hello: &Hello) -> Result<(Self::Read, Self::Write)> {
        let (read, mut write) = TcpStream::connect(self.broker.inner())
            .await
            .map_err(|err| {
                error!(self.log, "error connecting to broker"; "error" => %err);
                err
            })?
            .set_socket_options()?
            .into_split();

        net::write_message_to_async_socket(&mut write, hello, self.log).await?;

        Ok((BufReader::new(read), write))
    }
}

impl BrokerReadConnection for BufReader<tokio::net::tcp::OwnedReadHalf> {
    async fn read_messages<MessageT: Debug + DeserializeOwned, TransformedT: Send>(
        self,
        channel: UnboundedSender<TransformedT>,
        log: Logger,
        transform: impl Fn(MessageT) -> TransformedT + Send,
    ) -> Result<()> {
        net::async_socket_reader(self, channel, transform, log, "reading from broker socket").await
    }
}

impl BrokerWriteConnection for tokio::net::tcp::OwnedWriteHalf {
    async fn write_messages<MessageT: Debug + Send + Serialize + Sync>(
        self,
        channel: UnboundedReceiver<MessageT>,
        log: Logger,
    ) -> Result<()> {
        net::async_socket_writer(channel, self, log, "writing to broker socket").await
    }
}

pub struct GitHubQueueBrokerConnectionFactory<'a> {
    log: &'a Logger,
    token: String,
    url: Url,
}

impl<'a> GitHubQueueBrokerConnectionFactory<'a> {
    pub fn new(log: &'a Logger, token: String, url: Url) -> Result<Self> {
        Ok(Self { log, token, url })
    }
}

impl BrokerConnectionFactory for GitHubQueueBrokerConnectionFactory<'_> {
    type Read = GitHubReadQueue;
    type Write = GitHubWriteQueue;

    async fn connect(&self, hello: &Hello) -> Result<(Self::Read, Self::Write)> {
        let client = GitHubClient::new(&self.token, self.url.clone())?;
        let (read, mut write) = GitHubQueue::connect(client, "maelstrom-broker")
            .await
            .map_err(|err| {
                error!(self.log, "error connecting to broker"; "error" => %err);
                err
            })?
            .into_split();

        net::write_message_to_github_queue(&mut write, hello, self.log).await?;

        Ok((read, write))
    }
}

impl BrokerReadConnection for GitHubReadQueue {
    async fn read_messages<MessageT: Debug + DeserializeOwned, TransformedT: Send>(
        mut self,
        channel: UnboundedSender<TransformedT>,
        log: Logger,
        transform: impl Fn(MessageT) -> TransformedT + Send,
    ) -> Result<()> {
        net::github_queue_reader(
            &mut self,
            channel,
            transform,
            log,
            "reading from broker github queue",
        )
        .await
    }
}

impl BrokerWriteConnection for GitHubWriteQueue {
    async fn write_messages<MessageT: Debug + Send + Serialize + Sync>(
        mut self,
        channel: UnboundedReceiver<MessageT>,
        log: Logger,
    ) -> Result<()> {
        net::github_queue_writer(channel, &mut self, log, "reading from broker github queue").await
    }
}

