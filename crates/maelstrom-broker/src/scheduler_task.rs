mod scheduler;

pub use maelstrom_util::cache::CacheDir;

use maelstrom_base::proto::{BrokerToClient, BrokerToMonitor, BrokerToWorker};
use maelstrom_util::{
    cache::{self, Cache, TempFileFactory},
    config::common::CacheSize,
    manifest::ManifestReader,
    root::RootBuf,
    sync,
};
use scheduler::{Message, Scheduler, SchedulerDeps};
use slog::Logger;
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::mpsc as std_mpsc,
};
use tokio::sync::mpsc as tokio_mpsc;

#[derive(Debug)]
pub struct PassThroughDeps;

/// The production implementation of [SchedulerDeps]. This implementation just hands the
/// message to the provided sender.
impl SchedulerDeps for PassThroughDeps {
    type ClientSender = tokio_mpsc::UnboundedSender<BrokerToClient>;
    type WorkerSender = tokio_mpsc::UnboundedSender<BrokerToWorker>;
    type MonitorSender = tokio_mpsc::UnboundedSender<BrokerToMonitor>;
    type WorkerArtifactFetcherSender = std_mpsc::Sender<Option<(PathBuf, u64)>>;
    type ManifestError = io::Error;
    type ManifestIterator = ManifestReader<fs::File>;

    fn send_message_to_client(&mut self, sender: &mut Self::ClientSender, message: BrokerToClient) {
        sender.send(message).ok();
    }

    fn send_message_to_worker(&mut self, sender: &mut Self::WorkerSender, message: BrokerToWorker) {
        sender.send(message).ok();
    }

    fn send_message_to_monitor(
        &mut self,
        sender: &mut Self::MonitorSender,
        message: BrokerToMonitor,
    ) {
        sender.send(message).ok();
    }

    fn send_message_to_worker_artifact_fetcher(
        &mut self,
        sender: &mut Self::WorkerArtifactFetcherSender,
        message: Option<(PathBuf, u64)>,
    ) {
        sender.send(message).ok();
    }

    fn read_manifest(&mut self, path: &Path) -> io::Result<ManifestReader<fs::File>> {
        ManifestReader::new(fs::File::open(path)?)
    }
}

/// The production scheduler message type. Some [Message] arms contain a
/// [SchedulerDeps], so it's defined as a generic type. But in this module, we only use
/// one implementation of [SchedulerDeps].
pub type SchedulerMessage = Message<PassThroughDeps, cache::fs::std::TempFile>;

/// This type is used often enough to warrant an alias.
pub type SchedulerSender = tokio_mpsc::UnboundedSender<SchedulerMessage>;

pub struct SchedulerTask {
    scheduler: Scheduler<
        Cache<cache::fs::std::Fs, scheduler::BrokerKeyKind, scheduler::BrokerGetStrategy>,
        PassThroughDeps,
    >,
    sender: SchedulerSender,
    receiver: tokio_mpsc::UnboundedReceiver<SchedulerMessage>,
    temp_file_factory: TempFileFactory<cache::fs::std::Fs>,
}

impl SchedulerTask {
    pub fn new(cache_root: RootBuf<CacheDir>, cache_size: CacheSize, log: Logger) -> Self {
        let (sender, receiver) = tokio_mpsc::unbounded_channel();
        let (cache, temp_file_factory) =
            Cache::new(cache::fs::std::Fs, cache_root, cache_size, log).unwrap();
        SchedulerTask {
            scheduler: Scheduler::new(cache),
            sender,
            receiver,
            temp_file_factory,
        }
    }

    pub fn scheduler_sender(&self) -> &SchedulerSender {
        &self.sender
    }

    pub fn temp_file_factory(&self) -> &TempFileFactory<cache::fs::std::Fs> {
        &self.temp_file_factory
    }

    /// Main loop for the scheduler. This should be run on a task of its own. There should be
    /// exactly one of these in a broker process. It will return when all senders associated with
    /// the receiver are closed, which will happen when the listener and all outstanding worker and
    /// client socket tasks terminate.
    ///
    /// This function ignores any errors it encounters sending a message to an
    /// [tokio_mpsc::UnboundedSender]. The rationale is that this indicates that the socket
    /// connection has closed, and there are no more worker tasks to handle that connection. This
    /// means that a disconnected message is on its way to notify the scheduler. It is best to just
    /// ignore the error in that case. Besides, the [scheduler::SchedulerDeps] interface doesn't
    /// give us a way to return an error, for precisely this reason.
    pub async fn run(mut self) {
        sync::channel_reader(self.receiver, |msg| {
            self.scheduler.receive_message(&mut PassThroughDeps, msg)
        })
        .await
        .unwrap();
    }
}
