//! Code for the worker binary.

mod cache;
mod dispatcher;
mod executor;
mod fetcher;

use meticulous_base::{proto, JobDetails, JobId, Sha256Digest};
use meticulous_util::{error::Result, net};
use std::{net::SocketAddr, path::PathBuf};

type DispatcherReceiver = tokio::sync::mpsc::UnboundedReceiver<dispatcher::Message>;
type DispatcherSender = tokio::sync::mpsc::UnboundedSender<dispatcher::Message>;
type BrokerSocketSender = tokio::sync::mpsc::UnboundedSender<proto::WorkerToBroker>;

struct DispatcherAdapter {
    dispatcher_sender: DispatcherSender,
    broker_socket_sender: BrokerSocketSender,
    broker_addr: SocketAddr,
}

impl DispatcherAdapter {
    fn new(
        dispatcher_sender: DispatcherSender,
        broker_socket_sender: BrokerSocketSender,
        broker_addr: SocketAddr,
    ) -> Self {
        DispatcherAdapter {
            dispatcher_sender,
            broker_socket_sender,
            broker_addr,
        }
    }
}

impl dispatcher::DispatcherDeps for DispatcherAdapter {
    type JobHandle = executor::Handle;

    fn start_job(
        &mut self,
        id: JobId,
        details: &JobDetails,
        _layers: Vec<PathBuf>,
    ) -> Self::JobHandle {
        let sender = self.dispatcher_sender.clone();
        executor::start(details, move |result| {
            sender.send(dispatcher::Message::Executor(id, result)).ok();
        })
    }

    fn start_artifact_fetch(&mut self, digest: Sha256Digest, path: PathBuf) {
        let sender = self.dispatcher_sender.clone();
        let broker_addr = self.broker_addr;
        std::thread::spawn(move || {
            let result = fetcher::main(&digest, path, broker_addr).ok();
            sender
                .send(dispatcher::Message::ArtifactFetcher(digest, result))
                .ok();
        });
    }

    fn send_message_to_broker(&mut self, message: proto::WorkerToBroker) {
        self.broker_socket_sender.send(message).ok();
    }
}

async fn dispatcher_main(
    slots: usize,
    cache_root: PathBuf,
    cache_bytes_used_goal: u64,
    dispatcher_receiver: DispatcherReceiver,
    dispatcher_sender: DispatcherSender,
    broker_socket_sender: BrokerSocketSender,
    broker_addr: SocketAddr,
) {
    let cache = cache::Cache::new(cache::StdCacheFs, cache_root, cache_bytes_used_goal);
    let adapter = DispatcherAdapter::new(dispatcher_sender, broker_socket_sender, broker_addr);
    let mut dispatcher = dispatcher::Dispatcher::new(adapter, cache, slots);
    net::channel_reader(dispatcher_receiver, |msg| dispatcher.receive_message(msg)).await
}

async fn signal_handler(kind: tokio::signal::unix::SignalKind) {
    tokio::signal::unix::signal(kind)
        .expect("failed to register signal handler")
        .recv()
        .await;
}

/// The main function for the worker. This should be called on a task of its own. It will return
/// when a signal is received or when one of the worker tasks completes because of an error.
pub async fn main(
    _name: String,
    slots: usize,
    cache_root: PathBuf,
    cache_bytes_used_goal: u64,
    broker_addr: SocketAddr,
) -> Result<()> {
    let (read_stream, mut write_stream) = tokio::net::TcpStream::connect(&broker_addr)
        .await?
        .into_split();
    let read_stream = tokio::io::BufReader::new(read_stream);

    net::write_message_to_async_socket(
        &mut write_stream,
        proto::Hello::Worker {
            slots: slots as u32,
        },
    )
    .await?;

    let (dispatcher_sender, dispatcher_receiver) = tokio::sync::mpsc::unbounded_channel();
    let dispatcher_sender_clone = dispatcher_sender.clone();
    let (broker_socket_sender, broker_socket_receiver) = tokio::sync::mpsc::unbounded_channel();

    let mut join_set = tokio::task::JoinSet::new();
    join_set.spawn(net::async_socket_reader(
        read_stream,
        dispatcher_sender_clone,
        dispatcher::Message::Broker,
    ));
    join_set.spawn(net::async_socket_writer(
        broker_socket_receiver,
        write_stream,
    ));
    join_set.spawn(dispatcher_main(
        slots,
        cache_root,
        cache_bytes_used_goal,
        dispatcher_receiver,
        dispatcher_sender,
        broker_socket_sender,
        broker_addr,
    ));
    join_set.spawn(signal_handler(tokio::signal::unix::SignalKind::interrupt()));
    join_set.spawn(signal_handler(tokio::signal::unix::SignalKind::terminate()));
    join_set.join_next().await;
    Ok(())
}
