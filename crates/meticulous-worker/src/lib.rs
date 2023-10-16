//! Code for the worker binary.

mod cache;
pub mod config;
mod dispatcher;
mod executor;
mod fetcher;

use anyhow::Result;
use cache::{Cache, StdCacheFs};
use config::{Broker, Config};
use dispatcher::{Dispatcher, DispatcherDeps, Message};
use executor::Handle;
use meticulous_base::{
    proto::{Hello, WorkerToBroker},
    JobDetails, JobId, Sha256Digest,
};
use meticulous_util::net;
use slog::{debug, error, info, o, Logger};
use std::{path::PathBuf, process, thread};
use tokio::{
    io::BufReader,
    net::TcpStream,
    signal::unix::{self, SignalKind},
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};

type DispatcherReceiver = UnboundedReceiver<Message>;
type DispatcherSender = UnboundedSender<Message>;
type BrokerSocketSender = UnboundedSender<WorkerToBroker>;

struct DispatcherAdapter {
    dispatcher_sender: DispatcherSender,
    broker_socket_sender: BrokerSocketSender,
    broker_addr: Broker,
    log: Logger,
}

impl DispatcherAdapter {
    fn new(
        dispatcher_sender: DispatcherSender,
        broker_socket_sender: BrokerSocketSender,
        broker_addr: Broker,
        log: Logger,
    ) -> Self {
        DispatcherAdapter {
            dispatcher_sender,
            broker_socket_sender,
            broker_addr,
            log,
        }
    }
}

impl DispatcherDeps for DispatcherAdapter {
    type JobHandle = Handle;

    fn start_job(
        &mut self,
        id: JobId,
        details: &JobDetails,
        _layers: Vec<PathBuf>,
    ) -> Self::JobHandle {
        let sender = self.dispatcher_sender.clone();
        let log = self
            .log
            .new(o!("id" => format!("{id:?}"), "details" => format!("{details:?}")));
        debug!(log, "job starting");
        executor::start(details, move |result| {
            debug!(log, "job completed"; "result" => ?result);
            sender.send(Message::Executor(id, result)).ok();
        })
    }

    fn start_artifact_fetch(&mut self, digest: Sha256Digest, path: PathBuf) {
        let sender = self.dispatcher_sender.clone();
        let broker_addr = self.broker_addr;
        let mut log = self.log.new(o!(
            "digest" => digest.to_string(),
            "broker_addr" => broker_addr.inner().to_string()
        ));
        debug!(log, "artifact fetcher starting");
        thread::spawn(move || {
            let result = fetcher::main(&digest, path, broker_addr, &mut log).ok();
            debug!(log, "artifact fetcher completed"; "result" => ?result);
            sender.send(Message::ArtifactFetcher(digest, result)).ok();
        });
    }

    fn send_message_to_broker(&mut self, message: WorkerToBroker) {
        self.broker_socket_sender.send(message).ok();
    }
}

async fn dispatcher_main(
    config: Config,
    dispatcher_receiver: DispatcherReceiver,
    dispatcher_sender: DispatcherSender,
    broker_socket_sender: BrokerSocketSender,
    log: Logger,
) {
    let cache = Cache::new(
        StdCacheFs,
        config.cache_root,
        config.cache_bytes_used_target,
        log.clone(),
    );
    let adapter =
        DispatcherAdapter::new(dispatcher_sender, broker_socket_sender, config.broker, log);
    let mut dispatcher = Dispatcher::new(adapter, cache, config.slots);
    net::channel_reader(dispatcher_receiver, |msg| dispatcher.receive_message(msg)).await
}

async fn signal_handler(kind: SignalKind, log: Logger, signame: &'static str) {
    unix::signal(kind)
        .expect("failed to register signal handler")
        .recv()
        .await;
    error!(log, "received {signame}")
}

/// The main function for the worker. This should be called on a task of its own. It will return
/// when a signal is received or when one of the worker tasks completes because of an error.
pub async fn main(config: Config, log: Logger) -> Result<()> {
    info!(log, "started"; "config" => ?config, "pid" => process::id());

    let (read_stream, mut write_stream) = TcpStream::connect(config.broker.inner())
        .await
        .map_err(|err| {
            error!(log, "error connecting to broker"; "err" => %err);
            err
        })?
        .into_split();
    let read_stream = BufReader::new(read_stream);

    net::write_message_to_async_socket(
        &mut write_stream,
        Hello::Worker {
            slots: (*config.slots.inner()).into(),
        },
    )
    .await
    .map_err(|err| {
        error!(log, "error writing hello message"; "err" => %err);
        err
    })?;

    let (dispatcher_sender, dispatcher_receiver) = mpsc::unbounded_channel();
    let dispatcher_sender_clone = dispatcher_sender.clone();
    let (broker_socket_sender, broker_socket_receiver) = mpsc::unbounded_channel();

    let mut join_set = JoinSet::new();

    let log_clone = log.clone();
    join_set.spawn(net::async_socket_reader(
        read_stream,
        dispatcher_sender_clone,
        move |msg| {
            debug!(log_clone, "received broker message"; "msg" => ?msg);
            Message::Broker(msg)
        },
    ));
    let log_clone = log.clone();
    join_set.spawn(net::async_socket_writer(
        broker_socket_receiver,
        write_stream,
        move |msg| {
            debug!(log_clone, "sending broker message"; "msg" => ?msg);
        },
    ));
    join_set.spawn(dispatcher_main(
        config,
        dispatcher_receiver,
        dispatcher_sender,
        broker_socket_sender,
        log.clone(),
    ));
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
    info!(log, "exiting");
    Ok(())
}
