//! Code for the worker binary.

mod cache;
mod dispatcher;
mod executor;

use meticulous_util::channel_reader;
use meticulous::{proto, Error, ExecutionDetails, ExecutionId, Result, Sha256Digest};
use std::path::PathBuf;

type DispatcherReceiver = tokio::sync::mpsc::UnboundedReceiver<dispatcher::Message>;
type DispatcherSender = tokio::sync::mpsc::UnboundedSender<dispatcher::Message>;
type BrokerSocketSender = tokio::sync::mpsc::UnboundedSender<proto::WorkerToBroker>;

struct CacheAdapter {
    dispatcher_sender: DispatcherSender,
    fs: cache::StdCacheFs,
}

impl CacheAdapter {
    fn new(dispatcher_sender: DispatcherSender) -> Self {
        CacheAdapter {
            dispatcher_sender,
            fs: cache::StdCacheFs,
        }
    }
}

impl cache::CacheDeps for CacheAdapter {
    type RequestId = ExecutionId;

    fn download_and_extract(&mut self, _digest: Sha256Digest, _path: PathBuf) {
        todo!()
    }

    fn get_completed(&mut self, id: Self::RequestId, digest: Sha256Digest, path: Option<PathBuf>) {
        self.dispatcher_sender
            .send(dispatcher::Message::Cache(id, digest, path))
            .ok();
    }

    type Fs = cache::StdCacheFs;

    fn fs(&mut self) -> &mut Self::Fs {
        &mut self.fs
    }
}

struct DispatcherAdapter<'a> {
    dispatcher_sender: DispatcherSender,
    broker_socket_sender: BrokerSocketSender,
    cache: &'a mut cache::Cache<CacheAdapter>,
    cache_adapter: &'a mut CacheAdapter,
}

impl<'a> DispatcherAdapter<'a> {
    fn new(
        dispatcher_sender: DispatcherSender,
        broker_socket_sender: BrokerSocketSender,
        cache: &'a mut cache::Cache<CacheAdapter>,
        cache_adapter: &'a mut CacheAdapter,
    ) -> Self {
        DispatcherAdapter {
            dispatcher_sender,
            broker_socket_sender,
            cache,
            cache_adapter,
        }
    }
}

impl<'a> dispatcher::DispatcherDeps for DispatcherAdapter<'a> {
    type ExecutionHandle = executor::Handle;

    fn start_execution(
        &mut self,
        id: ExecutionId,
        details: &ExecutionDetails,
        _layers: Vec<PathBuf>,
    ) -> Self::ExecutionHandle {
        let sender = self.dispatcher_sender.clone();
        executor::start(details, move |result| {
            sender.send(dispatcher::Message::Executor(id, result)).ok();
        })
    }

    fn send_response_to_broker(&mut self, message: proto::WorkerToBroker) {
        self.broker_socket_sender.send(message).ok();
    }

    fn send_get_request_to_cache(&mut self, id: ExecutionId, digest: Sha256Digest) {
        self.cache
            .receive_message(self.cache_adapter, cache::Message::GetRequest(id, digest))
    }

    fn send_decrement_refcount_to_cache(&mut self, _digest: Sha256Digest) {
        todo!()
    }
}

async fn dispatcher_main(
    slots: usize,
    cache_root: PathBuf,
    cache_bytes_used_goal: u64,
    dispatcher_receiver: DispatcherReceiver,
    dispatcher_sender: DispatcherSender,
    broker_socket_sender: BrokerSocketSender,
) {
    let mut cache_adapter = CacheAdapter::new(dispatcher_sender.clone());
    let mut cache = cache::Cache::new(&cache_root, &mut cache_adapter, cache_bytes_used_goal);
    let adapter = DispatcherAdapter::new(
        dispatcher_sender,
        broker_socket_sender,
        &mut cache,
        &mut cache_adapter,
    );
    let mut dispatcher = dispatcher::Dispatcher::new(adapter, slots);
    channel_reader::run(dispatcher_receiver, |msg| dispatcher.receive_message(msg)).await;
}

async fn signal_handler(kind: tokio::signal::unix::SignalKind) -> Result<()> {
    tokio::signal::unix::signal(kind)?.recv().await;
    Err(Error::msg(format!("received signal {kind:?}")))
}

/// The main function for the worker. This should be called on a task of its own. It will return
/// when a signal is received or when one of the worker tasks completes because of an error.
pub async fn main(
    _name: String,
    slots: usize,
    cache_root: PathBuf,
    cache_bytes_used_goal: u64,
    broker_addr: std::net::SocketAddr,
) -> Result<()> {
    let (read_stream, mut write_stream) = tokio::net::TcpStream::connect(&broker_addr)
        .await?
        .into_split();
    let read_stream = tokio::io::BufReader::new(read_stream);

    proto::write_message(
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
    join_set.spawn(proto::socket_reader(
        read_stream,
        dispatcher_sender_clone,
        dispatcher::Message::Broker,
    ));
    join_set.spawn(proto::socket_writer(broker_socket_receiver, write_stream));
    join_set.spawn(async move {
        dispatcher_main(
            slots,
            cache_root,
            cache_bytes_used_goal,
            dispatcher_receiver,
            dispatcher_sender,
            broker_socket_sender,
        )
        .await;
        Ok(())
    });
    join_set.spawn(signal_handler(tokio::signal::unix::SignalKind::interrupt()));
    join_set.spawn(signal_handler(tokio::signal::unix::SignalKind::terminate()));

    loop {
        join_set
            .join_next()
            .await
            .expect("at least one task should return an error")
            .expect("no task should panic or be canceled")?;
    }
}
