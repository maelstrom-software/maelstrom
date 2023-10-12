//! Code for the worker binary.

mod cache;
pub mod config;
mod dispatcher;
mod executor;
mod fetcher;

use meticulous_base::{proto, JobDetails, JobId, Sha256Digest};
use meticulous_util::{error::Result, net};
use slog::{debug, error, info, o};
use std::{path::PathBuf, process};
use tokio::signal::unix::{self, SignalKind};

type DispatcherReceiver = tokio::sync::mpsc::UnboundedReceiver<dispatcher::Message>;
type DispatcherSender = tokio::sync::mpsc::UnboundedSender<dispatcher::Message>;
type BrokerSocketSender = tokio::sync::mpsc::UnboundedSender<proto::WorkerToBroker>;

struct DispatcherAdapter {
    dispatcher_sender: DispatcherSender,
    broker_socket_sender: BrokerSocketSender,
    broker_addr: config::Broker,
    log: slog::Logger,
}

impl DispatcherAdapter {
    fn new(
        dispatcher_sender: DispatcherSender,
        broker_socket_sender: BrokerSocketSender,
        broker_addr: config::Broker,
        log: slog::Logger,
    ) -> Self {
        DispatcherAdapter {
            dispatcher_sender,
            broker_socket_sender,
            broker_addr,
            log,
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
        let log = self
            .log
            .new(o!("id" => format!("{id:?}"), "details" => format!("{details:?}")));
        debug!(log, "job starting");
        executor::start(details, move |result| {
            debug!(log, "job completed"; "result" => ?result);
            sender.send(dispatcher::Message::Executor(id, result)).ok();
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
        std::thread::spawn(move || {
            let result = fetcher::main(&digest, path, broker_addr, &mut log).ok();
            debug!(log, "artifact fetcher completed"; "result" => ?result);
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
    config: config::Config,
    dispatcher_receiver: DispatcherReceiver,
    dispatcher_sender: DispatcherSender,
    broker_socket_sender: BrokerSocketSender,
    log: slog::Logger,
) {
    let cache = cache::Cache::new(
        cache::StdCacheFs,
        config.cache_root,
        config.cache_bytes_used_target,
        log.clone(),
    );
    let adapter =
        DispatcherAdapter::new(dispatcher_sender, broker_socket_sender, config.broker, log);
    let mut dispatcher = dispatcher::Dispatcher::new(adapter, cache, config.slots);
    net::channel_reader(dispatcher_receiver, |msg| dispatcher.receive_message(msg)).await
}

async fn signal_handler(kind: SignalKind, log: slog::Logger, signame: &'static str) {
    unix::signal(kind)
        .expect("failed to register signal handler")
        .recv()
        .await;
    error!(log, "received {signame}")
}

/// The main function for the worker. This should be called on a task of its own. It will return
/// when a signal is received or when one of the worker tasks completes because of an error.
pub async fn main(config: config::Config, log: slog::Logger) -> Result<()> {
    info!(log, "started"; "config" => ?config, "pid" => process::id());

    let (read_stream, mut write_stream) = tokio::net::TcpStream::connect(config.broker.inner())
        .await
        .map_err(|err| {
            error!(log, "error connecting to broker"; "err" => %err);
            err
        })?
        .into_split();
    let read_stream = tokio::io::BufReader::new(read_stream);

    net::write_message_to_async_socket(
        &mut write_stream,
        proto::Hello::Worker {
            slots: (*config.slots.inner()).into(),
        },
    )
    .await
    .map_err(|err| {
        error!(log, "error writing hello message"; "err" => %err);
        err
    })?;

    let (dispatcher_sender, dispatcher_receiver) = tokio::sync::mpsc::unbounded_channel();
    let dispatcher_sender_clone = dispatcher_sender.clone();
    let (broker_socket_sender, broker_socket_receiver) = tokio::sync::mpsc::unbounded_channel();

    let mut join_set = tokio::task::JoinSet::new();

    let log_clone = log.clone();
    join_set.spawn(net::async_socket_reader(
        read_stream,
        dispatcher_sender_clone,
        move |msg| {
            debug!(log_clone, "received broker message"; "msg" => ?msg);
            dispatcher::Message::Broker(msg)
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
