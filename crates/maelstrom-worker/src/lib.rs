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

use anyhow::{anyhow, bail, Context as _, Error, Result};
use artifact_fetcher::ArtifactFetcher;
use config::Config;
use dispatcher::{Dispatcher, Message};
use dispatcher_adapter::DispatcherAdapter;
use executor::{MountDir, TmpfsDir};
use maelstrom_base::proto::Hello;
use maelstrom_layer_fs::BlobDir;
use maelstrom_linux::{self as linux};
use maelstrom_util::{
    cache::{self, fs::std::Fs as StdFs, TempFileFactory},
    config::common::{Slots, InlineLimit},
    net::{self, AsRawFdExt as _},
    signal,
    root::RootBuf,
};
use slog::{debug, error, info, Logger};
use std::{future::Future, process};
use tokio::{
    io::BufReader,
    net::TcpStream,
    sync::mpsc,
    task::{self, JoinHandle},
};
use types::{
    BrokerSender, BrokerSocketOutgoingSender, Cache, DispatcherReceiver, DispatcherSender,
};

const MAX_IN_FLIGHT_LAYERS_BUILDS: usize = 10;
const MAX_ARTIFACT_FETCHES: usize = 1;

pub fn main(config: Config, log: Logger) -> Result<()> {
    info!(log, "started"; "config" => ?config, "pid" => process::id());
    let err = main_inner(config, &log).unwrap_err();
    error!(log, "exiting"; "error" => %err);
    Err(err)
}

/// The main function for the worker. This should be called on a task of its own. It will return
/// when a signal is received or when one of the worker tasks completes because of an error.
#[tokio::main]
async fn main_inner(config: Config, log: &Logger) -> Result<()> {
    check_open_file_limit(log, config.slots, 0)?;

    let (read_stream, mut write_stream) = TcpStream::connect(config.broker.inner())
        .await
        .map_err(|err| {
            error!(log, "error connecting to broker"; "error" => %err);
            err
        })?
        .set_socket_options()?
        .into_split();
    let read_stream = BufReader::new(read_stream);

    net::write_message_to_async_socket(
        &mut write_stream,
        Hello::Worker {
            slots: (*config.slots.inner()).into(),
        },
        log,
    )
    .await?;

    let (dispatcher_sender, dispatcher_receiver) = mpsc::unbounded_channel();
    let (broker_socket_outgoing_sender, broker_socket_outgoing_receiver) =
        mpsc::unbounded_channel();

    let log_clone = log.clone();
    let dispatcher_sender_clone = dispatcher_sender.clone();
    task::spawn(shutdown_on_error(
        async move {
            net::async_socket_reader(
                read_stream,
                dispatcher_sender_clone,
                Message::Broker,
                &log_clone,
            )
            .await
            .context("error communicating with broker")
        },
        dispatcher_sender.clone(),
    ));

    let log_clone = log.clone();
    task::spawn(shutdown_on_error(
        async move {
            net::async_socket_writer(broker_socket_outgoing_receiver, write_stream, &log_clone)
                .await
                .context("error communicating with broker")
        },
        dispatcher_sender.clone(),
    ));

    task::spawn(shutdown_on_error(
        wait_for_signal(log.clone()),
        dispatcher_sender.clone(),
    ));

    Err(start_dispatcher_task(
        config,
        dispatcher_receiver,
        dispatcher_sender,
        broker_socket_outgoing_sender,
        log,
    )?
    .await?)
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
        let _ = dispatcher_sender.send(Message::ShutDown(error));
    }
}

async fn wait_for_signal(log: Logger) -> Result<()> {
    let signal = signal::wait_for_signal(log).await;
    Err(anyhow!("signal {signal}"))
}

fn start_dispatcher_task(
    config: Config,
    dispatcher_receiver: DispatcherReceiver,
    dispatcher_sender: DispatcherSender,
    broker_socket_outgoing_sender: BrokerSocketOutgoingSender,
    log: &Logger,
) -> Result<JoinHandle<Error>> {
    let broker_sender = BrokerSender::new(broker_socket_outgoing_sender);

    let cache_root = config.cache_root.join::<cache::CacheDir>("artifacts");

    let (cache, temp_file_factory) =
        Cache::new(StdFs, cache_root, config.cache_size, log.clone(), true)?;

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

    start_dispatcher_task_common(
        artifact_fetcher,
        broker_sender,
        cache,
        config.cache_root,
        dispatcher_receiver,
        dispatcher_sender,
        config.inline_limit,
        log,
        config.slots,
        temp_file_factory,
    )
}

#[allow(clippy::too_many_arguments)]
fn start_dispatcher_task_common<
    ArtifactFetcherT: dispatcher::ArtifactFetcher + Send + 'static,
    BrokerSenderT: dispatcher::BrokerSender + Send + 'static,
>(
    artifact_fetcher: ArtifactFetcherT,
    broker_sender: BrokerSenderT,
    cache: Cache,
    cache_root: RootBuf<config::CacheDir>,
    mut dispatcher_receiver: DispatcherReceiver,
    dispatcher_sender: DispatcherSender,
    inline_limit: InlineLimit,
    log: &Logger,
    slots: Slots,
    temp_file_factory: TempFileFactory<StdFs>,
) -> Result<JoinHandle<Error>> {

    let dispatcher_adapter = DispatcherAdapter::new(
        dispatcher_sender,
        inline_limit,
        log.clone(),
        cache_root.join::<MountDir>("mount"),
        cache_root.join::<TmpfsDir>("upper"),
        cache.root().join::<BlobDir>("sha256/blob"),
        temp_file_factory,
    )?;

    let mut dispatcher = Dispatcher::new(
        dispatcher_adapter,
        artifact_fetcher,
        broker_sender,
        cache,
        slots,
    );

    let dispatcher_main = async move {
        loop {
            let msg = dispatcher_receiver
                .recv()
                .await
                .expect("missing shut down message");
            if let Err(err) = dispatcher.receive_message(msg) {
                break err;
            }
        }
    };
    Ok(task::spawn(dispatcher_main))
}
