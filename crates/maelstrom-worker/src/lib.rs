//! Code for the worker binary.

pub mod config;
pub mod local_worker;

mod artifact_fetcher;
mod connection;
mod dispatcher;
mod dispatcher_adapter;
mod executor;
mod layer_fs;
mod manifest_digest_cache;
mod types;

use anyhow::{anyhow, bail, Context as _, Error, Result};
use artifact_fetcher::{GitHubArtifactFetcher, TcpArtifactFetcher};
use config::Config;
use connection::{BrokerConnection, BrokerReadConnection as _, BrokerWriteConnection as _};
use dispatcher::{Dispatcher, Message};
use dispatcher_adapter::DispatcherAdapter;
use executor::{MountDir, TmpfsDir};
use maelstrom_github::{GitHubClient, GitHubQueue};
use maelstrom_layer_fs::BlobDir;
use maelstrom_linux::{self as linux};
use maelstrom_util::{
    cache::{self, fs::std::Fs as StdFs, TempFileFactory},
    config::common::{
        ArtifactTransferStrategy, BrokerConnection as ConfigBrokerConnection, CacheSize,
        InlineLimit, Slots,
    },
    root::RootBuf,
    signal,
};
use num::integer;
use slog::{debug, info, o, Logger};
use std::sync::Arc;
use tokio::{
    net::TcpStream,
    sync::mpsc,
    task::{self, JoinHandle, JoinSet},
};
use types::{BrokerSocketOutgoingSender, Cache, DispatcherReceiver, DispatcherSender};

fn env_or_error(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| anyhow!("{key} environment variable missing"))
}

fn github_client_factory() -> Result<Arc<GitHubClient>> {
    // XXX remi: I would prefer if we didn't read these from environment variables.
    let token = env_or_error("ACTIONS_RUNTIME_TOKEN")?;
    let base_url = url::Url::parse(&env_or_error("ACTIONS_RESULTS_URL")?)?;
    Ok(Arc::new(GitHubClient::new(&token, base_url)?))
}

const MAX_PENDING_LAYERS_BUILDS: usize = 10;
const MAX_ARTIFACT_FETCHES: usize = 1;

pub fn main(config: Config, log: Logger) -> Result<()> {
    info!(log, "started"; "config" => ?config);
    let err = match config.broker_connection {
        ConfigBrokerConnection::Tcp => main_inner::<TcpStream>(config, &log).unwrap_err(),
        ConfigBrokerConnection::GitHub => main_inner::<GitHubQueue>(config, &log).unwrap_err(),
    };
    info!(log, "exiting"; "error" => %err);
    Err(err)
}

/// The main function for the worker. This should be called on a task of its own. It will return
/// when a signal is received or when one of the worker tasks completes because of an error.
#[tokio::main]
async fn main_inner<ConnectionT: BrokerConnection>(config: Config, log: &Logger) -> Result<()> {
    check_open_file_limit(log, config.slots, 0)?;

    let (read_stream, write_stream) =
        ConnectionT::connect(&config.broker, config.slots, log).await?;

    let (dispatcher_sender, dispatcher_receiver) = mpsc::unbounded_channel();
    let (broker_socket_outgoing_sender, broker_socket_outgoing_receiver) =
        mpsc::unbounded_channel();

    let mut join_set = JoinSet::new();

    let reader_log = log.new(o!("task" => "reader"));
    join_set.spawn(read_stream.read_messages(dispatcher_sender.clone(), reader_log));

    let writer_log = log.new(o!("task" => "writer"));
    join_set.spawn(write_stream.write_messages(broker_socket_outgoing_receiver, writer_log));

    let tasks = start_dispatcher_task(
        config,
        dispatcher_receiver,
        dispatcher_sender.clone(),
        broker_socket_outgoing_sender,
        log,
        join_set,
    )
    .context("starting dispatcher task")?;

    tasks.run_to_completion().await
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
        maelstrom_fuse::MAX_PENDING as u64 /* each FUSE request opens a file */;
    existing_open_files
        + (maelstrom_layer_fs::READER_CACHE_SIZE * 2) // 1 for socket, 1 for the file
        + MAX_ARTIFACT_FETCHES as u64
        + per_slot_estimate * u16::from(slots) as u64
        + (MAX_PENDING_LAYERS_BUILDS * maelstrom_layer_fs::LAYER_BUILDING_FILE_MAX) as u64
}

fn round_to_multiple(n: u64, k: u64) -> u64 {
    integer::div_ceil(n, k) * k
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
    tasks: JoinSet<Result<()>>,
) -> Result<Tasks> {
    let dispatcher_sender_clone = dispatcher_sender.clone();
    let max_simultaneous_fetches = u32::try_from(MAX_ARTIFACT_FETCHES)
        .unwrap()
        .try_into()
        .unwrap();
    let broker_sender = move |msg| broker_socket_outgoing_sender.send(msg);

    match config.artifact_transfer_strategy {
        ArtifactTransferStrategy::TcpUpload => {
            let artifact_fetcher_factory = move |temp_file_factory| {
                TcpArtifactFetcher::new(
                    max_simultaneous_fetches,
                    dispatcher_sender_clone,
                    config.broker,
                    log.clone(),
                    temp_file_factory,
                )
            };
            start_dispatcher_task_common(
                artifact_fetcher_factory,
                broker_sender,
                config.cache_size,
                config.cache_root,
                dispatcher_receiver,
                dispatcher_sender,
                config.inline_limit,
                log,
                true, /* log_initial_cache_message_at_info */
                config.slots,
                tasks,
            )
        }
        ArtifactTransferStrategy::GitHub => {
            let github_client = github_client_factory().context("creating GitHub client")?;
            let artifact_fetcher_factory = move |temp_file_factory| {
                GitHubArtifactFetcher::new(
                    max_simultaneous_fetches,
                    github_client,
                    dispatcher_sender_clone,
                    log.clone(),
                    temp_file_factory,
                )
            };
            start_dispatcher_task_common(
                artifact_fetcher_factory,
                broker_sender,
                config.cache_size,
                config.cache_root,
                dispatcher_receiver,
                dispatcher_sender,
                config.inline_limit,
                log,
                true, /* log_initial_cache_message_at_info */
                config.slots,
                tasks,
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn start_dispatcher_task_common<
    ArtifactFetcherT: dispatcher::ArtifactFetcher + Send + 'static,
    ArtifactFetcherFactoryT: FnOnce(TempFileFactory<StdFs>) -> ArtifactFetcherT,
    BrokerSenderT: dispatcher::BrokerSender + Send + 'static,
>(
    artifact_fetcher_factory: ArtifactFetcherFactoryT,
    broker_sender: BrokerSenderT,
    cache_size: CacheSize,
    cache_root: RootBuf<config::CacheDir>,
    mut dispatcher_receiver: DispatcherReceiver,
    dispatcher_sender: DispatcherSender,
    inline_limit: InlineLimit,
    log: &Logger,
    log_initial_cache_message_at_info: bool,
    slots: Slots,
    mut tasks: JoinSet<Result<()>>,
) -> Result<Tasks> {
    // Start the signal handler.
    tasks.spawn(wait_for_signal(log.clone()));

    let log = log.new(o!("task" => "dispatcher"));

    let (cache, temp_file_factory) = Cache::new(
        StdFs,
        cache_root.join::<cache::CacheDir>("artifacts"),
        cache_size,
        log.clone(),
        log_initial_cache_message_at_info,
    )
    .context("creating cache")?;

    let artifact_fetcher = artifact_fetcher_factory(temp_file_factory.clone());

    let dispatcher_adapter = DispatcherAdapter::new(
        dispatcher_sender.clone(),
        inline_limit,
        log,
        cache_root.join::<MountDir>("mount"),
        cache_root.join::<TmpfsDir>("upper"),
        cache.root().join::<BlobDir>("sha256/blob"),
        temp_file_factory,
    )
    .context("creating dispatcher adapter")?;

    let mut dispatcher = Dispatcher::new(
        dispatcher_adapter,
        artifact_fetcher,
        broker_sender,
        cache,
        slots,
    );

    let dispatcher_task = task::spawn(async move {
        loop {
            let msg = dispatcher_receiver
                .recv()
                .await
                .expect("all senders should never be closed");
            if let Err(err) = dispatcher.receive_message(msg) {
                break err;
            }
        }
    });

    Ok(Tasks::new(dispatcher_task, dispatcher_sender, tasks))
}

// The tasks in the JoinSet are all of the supporting tasks. If these complete with Ok(()), that
// means they ran out of work to do. This should only happen at shutdown time, like when the
// channel a task is reading from or writing to is closed. When we see these return Ok(()) results,
// we just ignore them, since we expect the root cause to surface somewhere else.
//
// When a task in the JoinSet completes with Err(_), then we need to tell the dispatcher to
// cleanly shut down. We do that by sending it a ShutDown message with the error returned
// from the task. It's okay to send multiple ShutDown messages: the dispatcher will ignore
// all but the first.
pub struct Tasks {
    dispatcher: JoinHandle<Error>,
    dispatcher_sender: DispatcherSender,
    other_tasks_monitor: JoinHandle<()>,
}

impl Tasks {
    fn new(
        dispatcher: JoinHandle<Error>,
        dispatcher_sender: DispatcherSender,
        mut other_tasks: JoinSet<Result<()>>,
    ) -> Self {
        let dispatcher_sender_clone = dispatcher_sender.clone();
        let other_tasks_monitor = task::spawn(async move {
            while let Some(result) = other_tasks.join_next().await {
                match result.context("joining worker task") {
                    Err(join_error) => {
                        let _ = dispatcher_sender_clone.send(Message::ShutDown(join_error));
                        break;
                    }
                    Ok(Err(error)) => {
                        let _ = dispatcher_sender_clone.send(Message::ShutDown(error));
                        break;
                    }
                    Ok(Ok(())) => {}
                }
            }
        });
        Self {
            dispatcher,
            dispatcher_sender,
            other_tasks_monitor,
        }
    }

    pub async fn run_to_completion(self) -> Result<()> {
        let result = Err(self.dispatcher.await?);
        self.other_tasks_monitor.abort();
        result
    }

    pub async fn shut_down(self, error: Error) -> Result<()> {
        let _ = self.dispatcher_sender.send(Message::ShutDown(error));
        self.run_to_completion().await
    }
}
