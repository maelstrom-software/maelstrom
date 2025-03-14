use crate::{
    dispatcher::Message,
    types::{BrokerSocketOutgoingReceiver, DispatcherSender},
};
use anyhow::Result;
use derive_more::Constructor;
use maelstrom_base::proto::Hello;
use maelstrom_github::{GitHubQueue, GitHubReadQueue, GitHubWriteQueue};
use maelstrom_util::{
    config::common::{BrokerAddr, Slots},
    net::{self, AsRawFdExt as _},
};
use slog::{error, Logger};
use std::future::Future;
use tokio::{io::BufReader, net::TcpStream};

pub trait BrokerConnectionFactory: Sized {
    type Read: BrokerReadConnection;
    type Write: BrokerWriteConnection;

    async fn connect(&self, slots: Slots, log: &Logger) -> Result<(Self::Read, Self::Write)>;
}

#[derive(Constructor)]
pub struct TcpBrokerConnectionFactory(BrokerAddr);

impl BrokerConnectionFactory for TcpBrokerConnectionFactory {
    type Read = BufReader<tokio::net::tcp::OwnedReadHalf>;
    type Write = tokio::net::tcp::OwnedWriteHalf;

    async fn connect(&self, slots: Slots, log: &Logger) -> Result<(Self::Read, Self::Write)> {
        let (read, mut write) = TcpStream::connect(self.0.inner())
            .await
            .map_err(|err| {
                error!(log, "error connecting to broker"; "error" => %err);
                err
            })?
            .set_socket_options()?
            .into_split();

        net::write_message_to_async_socket(
            &mut write,
            Hello::Worker {
                slots: slots.into_inner().into(),
            },
            log,
        )
        .await?;

        Ok((BufReader::new(read), write))
    }
}

pub trait BrokerReadConnection: Send + Sync + 'static {
    fn read_messages(
        self,
        dispatcher_sender: DispatcherSender,
        log: Logger,
    ) -> impl Future<Output = Result<()>> + Send;
}

impl BrokerReadConnection for BufReader<tokio::net::tcp::OwnedReadHalf> {
    async fn read_messages(self, dispatcher_sender: DispatcherSender, log: Logger) -> Result<()> {
        net::async_socket_reader(
            self,
            dispatcher_sender,
            Message::Broker,
            log,
            "reading from broker socket",
        )
        .await
    }
}

pub trait BrokerWriteConnection: Send + Sync + 'static {
    fn write_messages(
        self,
        broker_socket_outgoing_receiver: BrokerSocketOutgoingReceiver,
        log: Logger,
    ) -> impl Future<Output = Result<()>> + Send;
}

impl BrokerWriteConnection for tokio::net::tcp::OwnedWriteHalf {
    async fn write_messages(
        self,
        broker_socket_outgoing_receiver: BrokerSocketOutgoingReceiver,
        log: Logger,
    ) -> Result<()> {
        net::async_socket_writer(
            broker_socket_outgoing_receiver,
            self,
            log,
            "writing to broker socket",
        )
        .await
    }
}

pub struct GitHubQueueBrokerConnectionFactory;

impl BrokerConnectionFactory for GitHubQueueBrokerConnectionFactory {
    type Read = GitHubReadQueue;
    type Write = GitHubWriteQueue;

    async fn connect(&self, slots: Slots, log: &Logger) -> Result<(Self::Read, Self::Write)> {
        let client = crate::github_client_factory()?;
        let (read, mut write) = GitHubQueue::connect(client, "maelstrom-broker")
            .await
            .map_err(|err| {
                error!(log, "error connecting to broker"; "error" => %err);
                err
            })?
            .into_split();

        net::write_message_to_github_queue(
            &mut write,
            &Hello::Worker {
                slots: slots.into_inner().into(),
            },
            log,
        )
        .await?;

        Ok((read, write))
    }
}

impl BrokerReadConnection for GitHubReadQueue {
    async fn read_messages(
        mut self,
        dispatcher_sender: DispatcherSender,
        log: Logger,
    ) -> Result<()> {
        net::github_queue_reader(
            &mut self,
            dispatcher_sender,
            Message::Broker,
            log,
            "writing to broker github queue",
        )
        .await
    }
}

impl BrokerWriteConnection for GitHubWriteQueue {
    async fn write_messages(
        mut self,
        broker_socket_outgoing_receiver: BrokerSocketOutgoingReceiver,
        log: Logger,
    ) -> Result<()> {
        net::github_queue_writer(
            broker_socket_outgoing_receiver,
            &mut self,
            log,
            "reading from broker github queue",
        )
        .await
    }
}
