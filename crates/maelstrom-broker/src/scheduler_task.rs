mod cache;
mod scheduler;

pub use maelstrom_util::cache::CacheDir;

use maelstrom_base::{
    manifest::{ManifestEntryData, ManifestFileData},
    proto::{BrokerToClient, BrokerToMonitor, BrokerToWorker},
    JobId, Sha256Digest,
};
use maelstrom_util::{
    cache::{fs as cache_fs, Cache, TempFileFactory},
    config::common::CacheSize,
    manifest::AsyncManifestReader,
    root::RootBuf,
    sync,
};
use scheduler::{Message, MessageM, Scheduler, SchedulerDeps};
use slog::Logger;
use std::{path::PathBuf, sync::mpsc as std_mpsc};
use tokio::io::AsyncRead;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::task::JoinSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestReadRequest<ArtifactStreamT> {
    manifest_stream: ArtifactStreamT,
    size: u64,
    digest: Sha256Digest,
    job_id: JobId,
}

#[derive(Debug)]
pub struct PassThroughDeps;

/// The production implementation of [SchedulerDeps]. This implementation just hands the
/// message to the provided sender.
impl SchedulerDeps for PassThroughDeps {
    type ClientSender = tokio_mpsc::UnboundedSender<BrokerToClient>;
    type WorkerSender = tokio_mpsc::UnboundedSender<BrokerToWorker>;
    type MonitorSender = tokio_mpsc::UnboundedSender<BrokerToMonitor>;
    type WorkerArtifactFetcherSender = std_mpsc::Sender<Option<(PathBuf, u64)>>;
    type ArtifactStream = <SchedulerCache as cache::SchedulerCache>::ArtifactStream;
    type ManifestReaderSender =
        tokio_mpsc::UnboundedSender<ManifestReadRequest<Self::ArtifactStream>>;

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

    fn read_manifest(
        &mut self,
        sender: &mut Self::ManifestReaderSender,
        req: ManifestReadRequest<Self::ArtifactStream>,
    ) {
        sender.send(req).ok();
    }
}

#[derive(Debug)]
struct CacheManifestReader<ArtifactStreamT> {
    tasks: JoinSet<()>,
    receiver: tokio_mpsc::UnboundedReceiver<ManifestReadRequest<ArtifactStreamT>>,
    sender: SchedulerSender,
}

async fn read_manifest<ArtifactStreamT: AsyncRead + Unpin>(
    sender: SchedulerSender,
    stream: ArtifactStreamT,
    size: u64,
    job_id: JobId,
) -> anyhow::Result<()> {
    let mut reader = AsyncManifestReader::new_with_size(stream, size).await?;
    while let Some(entry) = reader.next().await? {
        if let ManifestEntryData::File(ManifestFileData::Digest(digest)) = entry.data {
            sender.send(Message::GotManifestEntry(digest, job_id)).ok();
        }
    }
    Ok(())
}

impl<ArtifactStreamT: AsyncRead + Unpin + Send + Sync + 'static>
    CacheManifestReader<ArtifactStreamT>
{
    fn new(
        receiver: tokio_mpsc::UnboundedReceiver<ManifestReadRequest<ArtifactStreamT>>,
        sender: SchedulerSender,
    ) -> Self {
        Self {
            tasks: JoinSet::new(),
            receiver,
            sender,
        }
    }

    async fn run(&mut self) {
        while let Some(req) = self.receiver.recv().await {
            let sender = self.sender.clone();
            self.tasks.spawn(async move {
                let result =
                    read_manifest(sender.clone(), req.manifest_stream, req.size, req.job_id).await;
                sender
                    .send(Message::FinishedReadingManifest(
                        req.digest, req.job_id, result,
                    ))
                    .ok();
            });
        }
    }
}

/// The production scheduler message type. Some [Message] arms contain a
/// [SchedulerDeps], so it's defined as a generic type. But in this module, we only use
/// one implementation of [SchedulerDeps].
pub type SchedulerMessage = MessageM<PassThroughDeps, cache_fs::std::TempFile>;

/// This type is used often enough to warrant an alias.
pub type SchedulerSender = tokio_mpsc::UnboundedSender<SchedulerMessage>;

type SchedulerCache = Cache<cache_fs::std::Fs, cache::BrokerKey, cache::BrokerGetStrategy>;

pub struct SchedulerTask {
    scheduler: Scheduler<SchedulerCache, PassThroughDeps>,
    sender: SchedulerSender,
    receiver: tokio_mpsc::UnboundedReceiver<SchedulerMessage>,
    temp_file_factory: TempFileFactory<cache_fs::std::Fs>,
}

impl SchedulerTask {
    pub fn new(cache_root: RootBuf<CacheDir>, cache_size: CacheSize, log: Logger) -> Self {
        let (sender, receiver) = tokio_mpsc::unbounded_channel();
        let (cache, temp_file_factory) =
            Cache::new(cache_fs::std::Fs, cache_root, cache_size, log, true).unwrap();

        let (manifest_reader_sender, manifest_reader_receiver) = tokio_mpsc::unbounded_channel();
        let mut manifest_reader =
            CacheManifestReader::new(manifest_reader_receiver, sender.clone());
        tokio::task::spawn(async move { manifest_reader.run().await });

        SchedulerTask {
            scheduler: Scheduler::new(cache, manifest_reader_sender),
            sender,
            receiver,
            temp_file_factory,
        }
    }

    pub fn scheduler_sender(&self) -> &SchedulerSender {
        &self.sender
    }

    pub fn temp_file_factory(&self) -> &TempFileFactory<cache_fs::std::Fs> {
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
            self.scheduler.receive_message(&mut PassThroughDeps, msg);
        })
        .await
        .unwrap();
    }
}
