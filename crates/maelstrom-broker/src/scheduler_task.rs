mod scheduler;

pub use maelstrom_util::cache::CacheDir;

use maelstrom_base::{
    manifest::{ManifestEntryData, ManifestFileData},
    proto::{BrokerToClient, BrokerToMonitor, BrokerToWorker},
    JobId, Sha256Digest,
};
use maelstrom_util::{
    async_fs,
    cache::{self, Cache, TempFileFactory},
    config::common::CacheSize,
    manifest::AsyncManifestReader,
    root::RootBuf,
    sync,
};
use ref_cast::RefCast;
use scheduler::{Message, Scheduler, SchedulerDeps};
use slog::Logger;
use std::{path::PathBuf, sync::mpsc as std_mpsc};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::task::JoinSet;

#[derive(Debug)]
pub struct PassThroughDeps {
    manifest_reader_sender: tokio_mpsc::UnboundedSender<(Sha256Digest, JobId)>,
}

impl PassThroughDeps {
    fn new(manifest_reader_sender: tokio_mpsc::UnboundedSender<(Sha256Digest, JobId)>) -> Self {
        Self {
            manifest_reader_sender,
        }
    }
}

/// The production implementation of [SchedulerDeps]. This implementation just hands the
/// message to the provided sender.
impl SchedulerDeps for PassThroughDeps {
    type ClientSender = tokio_mpsc::UnboundedSender<BrokerToClient>;
    type WorkerSender = tokio_mpsc::UnboundedSender<BrokerToWorker>;
    type MonitorSender = tokio_mpsc::UnboundedSender<BrokerToMonitor>;
    type WorkerArtifactFetcherSender = std_mpsc::Sender<Option<(PathBuf, u64)>>;

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

    fn read_manifest(&mut self, manifest_digest: Sha256Digest, job_id: JobId) {
        self.manifest_reader_sender
            .send((manifest_digest, job_id))
            .ok();
    }
}

#[derive(Debug)]
struct CacheManifestReader {
    tasks: JoinSet<()>,
    receiver: tokio_mpsc::UnboundedReceiver<(Sha256Digest, JobId)>,
    sender: SchedulerSender,
}

async fn read_manifest_from_path(
    sender: SchedulerSender,
    manifest_path: PathBuf,
    job_id: JobId,
) -> anyhow::Result<()> {
    let fs = async_fs::Fs::new();
    let mut reader = AsyncManifestReader::new(fs.open_file(manifest_path).await?).await?;
    while let Some(entry) = reader.next().await? {
        if let ManifestEntryData::File(ManifestFileData::Digest(digest)) = entry.data {
            sender.send(Message::GotManifestEntry(digest, job_id)).ok();
        }
    }
    Ok(())
}

impl CacheManifestReader {
    fn new(
        receiver: tokio_mpsc::UnboundedReceiver<(Sha256Digest, JobId)>,
        sender: SchedulerSender,
    ) -> Self {
        Self {
            tasks: JoinSet::new(),
            receiver,
            sender,
        }
    }

    fn kick_off_manifest_readings(&mut self, cache: &TaskCache) {
        while let Ok((manifest_digest, job_id)) = self.receiver.try_recv() {
            let sender = self.sender.clone();
            let manifest_path = cache
                .cache_path(scheduler::BrokerKey::ref_cast(&manifest_digest))
                .into_path_buf();
            self.tasks.spawn(async move {
                let result = read_manifest_from_path(sender.clone(), manifest_path, job_id).await;
                sender
                    .send(Message::FinishedReadingManifest(
                        manifest_digest,
                        job_id,
                        result,
                    ))
                    .ok();
            });
        }
    }
}

/// The production scheduler message type. Some [Message] arms contain a
/// [SchedulerDeps], so it's defined as a generic type. But in this module, we only use
/// one implementation of [SchedulerDeps].
pub type SchedulerMessage = Message<PassThroughDeps, cache::fs::std::TempFile>;

/// This type is used often enough to warrant an alias.
pub type SchedulerSender = tokio_mpsc::UnboundedSender<SchedulerMessage>;

type TaskCache = Cache<cache::fs::std::Fs, scheduler::BrokerKey, scheduler::BrokerGetStrategy>;

pub struct SchedulerTask {
    scheduler: Scheduler<TaskCache, PassThroughDeps>,
    sender: SchedulerSender,
    receiver: tokio_mpsc::UnboundedReceiver<SchedulerMessage>,
    temp_file_factory: TempFileFactory<cache::fs::std::Fs>,
    manifest_reader: CacheManifestReader,
    deps: PassThroughDeps,
}

impl SchedulerTask {
    pub fn new(cache_root: RootBuf<CacheDir>, cache_size: CacheSize, log: Logger) -> Self {
        let (sender, receiver) = tokio_mpsc::unbounded_channel();
        let (cache, temp_file_factory) =
            Cache::new(cache::fs::std::Fs, cache_root, cache_size, log, true).unwrap();

        let (manifest_reader_sender, manifest_reader_receiver) = tokio_mpsc::unbounded_channel();
        let manifest_reader = CacheManifestReader::new(manifest_reader_receiver, sender.clone());
        let deps = PassThroughDeps::new(manifest_reader_sender);

        SchedulerTask {
            scheduler: Scheduler::new(cache),
            sender,
            receiver,
            temp_file_factory,
            manifest_reader,
            deps,
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
            self.scheduler.receive_message(&mut self.deps, msg);
            self.manifest_reader
                .kick_off_manifest_readings(&self.scheduler.cache);
        })
        .await
        .unwrap();
    }
}
