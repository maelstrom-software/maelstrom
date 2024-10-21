//! Code for the worker binary.

pub mod config;
pub mod local_worker;

mod artifact_fetcher;
mod dispatcher;
mod dispatcher_adapter;
mod executor;
mod layer_fs;
mod manifest_digest_cache;
mod types;

use anyhow::{anyhow, bail, Context as _, Result};
use artifact_fetcher::ArtifactFetcher;
use config::Config;
use dispatcher::Message;
use dispatcher_adapter::DispatcherAdapter;
use executor::{MountDir, TmpfsDir};
use maelstrom_base::proto::Hello;
use maelstrom_layer_fs::BlobDir;
use maelstrom_linux::{self as linux};
use maelstrom_util::{
    cache::{fs::std::Fs as StdFs, CacheDir},
    config::common::Slots,
    net, signal,
};
use slog::{debug, error, info, Logger};
use std::{future::Future, process};
use tokio::{io::BufReader, net::TcpStream, sync::mpsc};
use types::{
    BrokerSender, BrokerSocketIncomingReceiver, BrokerSocketOutgoingSender, Cache, Dispatcher,
    DispatcherReceiver, DispatcherSender,
};

pub struct WorkerCacheDir;

pub const MAX_IN_FLIGHT_LAYERS_BUILDS: usize = 10;
pub const MAX_ARTIFACT_FETCHES: usize = 1;

pub fn main(config: Config, log: Logger) -> Result<()> {
    main_inner(config, log)
}

/// The main function for the worker. This should be called on a task of its own. It will return
/// when a signal is received or when one of the worker tasks completes because of an error.
#[tokio::main]
pub async fn main_inner(config: Config, log: Logger) -> Result<()> {
    info!(log, "started"; "config" => ?config, "pid" => process::id());

    check_open_file_limit(&log, config.slots, 0)?;

    let stream = TcpStream::connect(config.broker.inner())
        .await
        .map_err(|err| {
            error!(log, "error connecting to broker"; "error" => %err);
            err
        })?;
    net::set_socket_options(&stream)?;
    let (read_stream, mut write_stream) = stream.into_split();
    let read_stream = BufReader::new(read_stream);

    net::write_message_to_async_socket(
        &mut write_stream,
        Hello::Worker {
            slots: (*config.slots.inner()).into(),
        },
        &log,
    )
    .await?;

    let (dispatcher_sender, dispatcher_receiver) = mpsc::unbounded_channel();
    let (broker_socket_outgoing_sender, broker_socket_outgoing_receiver) =
        mpsc::unbounded_channel();
    let (broker_socket_incoming_sender, broker_socket_incoming_receiver) =
        mpsc::unbounded_channel();

    let log_clone = log.clone();
    tokio::task::spawn(shutdown_on_error(
        async move {
            net::async_socket_reader(
                read_stream,
                broker_socket_incoming_sender,
                |msg| msg,
                &log_clone,
            )
            .await
            .context("error communicating with broker")
        },
        dispatcher_sender.clone(),
    ));

    let log_clone = log.clone();
    tokio::task::spawn(shutdown_on_error(
        async move {
            net::async_socket_writer(broker_socket_outgoing_receiver, write_stream, &log_clone)
                .await
                .context("error communicating with broker")
        },
        dispatcher_sender.clone(),
    ));

    tokio::task::spawn(shutdown_on_error(
        wait_for_signal(log.clone()),
        dispatcher_sender.clone(),
    ));

    dispatcher_main(
        config,
        dispatcher_receiver,
        dispatcher_sender,
        broker_socket_outgoing_sender,
        broker_socket_incoming_receiver,
        log.clone(),
    )
    .await;

    info!(log, "exiting");

    Ok(())
}

/// Check if the open file limit is high enough to fit our estimate of how many files we need.
pub fn check_open_file_limit(log: &Logger, slots: Slots, extra: u64) -> Result<()> {
    let limit = linux::getrlimit(linux::RlimitResource::NoFile)?;
    let estimate = open_file_max(slots) + extra;
    debug!(log, "checking open file limit"; "limit" => ?limit.current, "estimate" => estimate);
    if limit.current < estimate {
        let estimate = round_to_multiple(estimate, 1024);
        bail!("Open file limit is too low. Increase limit by running `ulimit -n {estimate}`");
    }
    Ok(())
}

/// For the number of slots, what is the maximum number of files we will open. This attempts to
/// come up with a number by doing some math, but nothing is guaranteeing the result.
fn open_file_max(slots: Slots) -> u64 {
    let existing_open_files: u64 = 3 /* stdout, stdin, stderr */;
    let per_slot_estimate: u64 = 6 /* unix socket, FUSE connection, (stdout, stderr) * 2 */ +
        maelstrom_fuse::MAX_INFLIGHT as u64 /* each FUSE request opens a file */;
    existing_open_files
        + (maelstrom_layer_fs::READER_CACHE_SIZE * 2) // 1 for socket, 1 for the file
        + MAX_ARTIFACT_FETCHES as u64
        + per_slot_estimate * u16::from(slots) as u64
        + (MAX_IN_FLIGHT_LAYERS_BUILDS * maelstrom_layer_fs::LAYER_BUILDING_FILE_MAX) as u64
}

fn round_to_multiple(n: u64, k: u64) -> u64 {
    if n % k == 0 {
        n
    } else {
        n + (k - (n % k))
    }
}

async fn shutdown_on_error(
    fut: impl Future<Output = Result<()>>,
    dispatcher_sender: DispatcherSender,
) {
    if let Err(error) = fut.await {
        let _ = dispatcher_sender.send(Message::Shutdown(error));
    }
}

async fn wait_for_signal(log: Logger) -> Result<()> {
    let signal = signal::wait_for_signal(log).await;
    Err(anyhow!("signal {signal}"))
}

async fn dispatcher_main(
    config: Config,
    dispatcher_receiver: DispatcherReceiver,
    dispatcher_sender: DispatcherSender,
    broker_socket_outgoing_sender: BrokerSocketOutgoingSender,
    broker_socket_incoming_receiver: BrokerSocketIncomingReceiver,
    log: Logger,
) {
    let mount_dir = config.cache_root.join::<MountDir>("mount");
    let tmpfs_dir = config.cache_root.join::<TmpfsDir>("upper");
    let cache_root = config.cache_root.join::<CacheDir>("artifacts");
    let blob_dir = cache_root.join::<BlobDir>("sha256/blob");

    let broker_sender = BrokerSender::new(broker_socket_outgoing_sender);
    let (cache, temp_file_factory) =
        match Cache::new(StdFs, cache_root, config.cache_size, log.clone()) {
            Err(err) => {
                error!(log, "could not start cache"; "error" => %err);
                return;
            }
            Ok((cache, temp_file_factory)) => (cache, temp_file_factory),
        };
    let artifact_fetcher = ArtifactFetcher::new(
        u32::try_from(MAX_ARTIFACT_FETCHES)
            .unwrap()
            .try_into()
            .unwrap(),
        dispatcher_sender.clone(),
        config.broker,
        log.clone(),
        temp_file_factory.clone(),
    );
    match DispatcherAdapter::new(
        dispatcher_sender,
        config.inline_limit,
        log.clone(),
        mount_dir,
        tmpfs_dir,
        blob_dir,
        temp_file_factory,
    ) {
        Err(err) => {
            error!(log, "could not start executor"; "error" => %err);
        }
        Ok(adapter) => {
            let dispatcher = Dispatcher::new(
                adapter,
                artifact_fetcher,
                broker_sender,
                cache,
                config.slots,
            );
            handle_incoming_messages(
                log,
                dispatcher_receiver,
                broker_socket_incoming_receiver,
                dispatcher,
            )
            .await;
        }
    }
}

async fn handle_incoming_messages(
    log: Logger,
    mut dispatcher_receiver: DispatcherReceiver,
    mut broker_socket_incoming_recevier: BrokerSocketIncomingReceiver,
    mut dispatcher: Dispatcher,
) {
    // Multiplex messages from broker and others sources
    let err = loop {
        let res = tokio::select! {
            msg = dispatcher_receiver.recv() => {
                handle_dispatcher_message(msg.expect("missing shutdown"), &mut dispatcher)
            },
            msg = broker_socket_incoming_recevier.recv() => {
                let Some(msg) = msg else { continue };
                handle_dispatcher_message(Message::Broker(msg), &mut dispatcher)
            },
        };
        if let Err(err) = res {
            break err;
        }
    };

    error!(log, "shutting down due to {err}");

    // This should close the connection with the broker, and canceling running jobs.
    info!(
        log,
        "canceling {} running jobs",
        dispatcher.num_jobs_executing()
    );
    dispatcher.receive_message(Message::Shutdown(err));
    drop(broker_socket_incoming_recevier);

    // Wait for the running jobs to finish.
    while dispatcher.num_jobs_executing() > 0 {
        let msg = dispatcher_receiver.recv().await.expect("missing shutdown");
        let _ = handle_dispatcher_message(msg, &mut dispatcher);
    }
}

/// Returns error from shutdown message, or delivers message to dispatcher.
fn handle_dispatcher_message(msg: Message<StdFs>, dispatcher: &mut Dispatcher) -> Result<()> {
    if let Message::Shutdown(error) = msg {
        return Err(error);
    }

    dispatcher.receive_message(msg);
    Ok(())
}
