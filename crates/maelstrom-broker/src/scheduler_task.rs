mod scheduler;

use crate::cache::SchedulerCache;
use maelstrom_base::{
    manifest::{ManifestEntryData, ManifestFileData},
    proto::{BrokerToClient, BrokerToMonitor, BrokerToWorker},
    JobId, Sha256Digest,
};
use maelstrom_util::{manifest::AsyncManifestReader, sync};
use scheduler::{Message, Scheduler, SchedulerDeps};
use std::marker::PhantomData;
use std::{path::PathBuf, sync::mpsc as std_mpsc};
use tokio::io::AsyncRead;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::task::JoinSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestReadRequest<ArtifactStreamT> {
    manifest_stream: ArtifactStreamT,
    digest: Sha256Digest,
    job_id: JobId,
}

#[derive(Debug)]
pub struct PassThroughDeps<ArtifactStreamT>(PhantomData<ArtifactStreamT>);

/// The production implementation of [SchedulerDeps]. This implementation just hands the
/// message to the provided sender.
impl<ArtifactStreamT> SchedulerDeps for PassThroughDeps<ArtifactStreamT> {
    type ClientSender = tokio_mpsc::UnboundedSender<BrokerToClient>;
    type WorkerSender = tokio_mpsc::UnboundedSender<BrokerToWorker>;
    type MonitorSender = tokio_mpsc::UnboundedSender<BrokerToMonitor>;
    type WorkerArtifactFetcherSender = std_mpsc::Sender<Option<(PathBuf, u64)>>;
    type ArtifactStream = ArtifactStreamT;
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
struct CacheManifestReader<ArtifactStreamT, TempFileT> {
    tasks: JoinSet<()>,
    receiver: tokio_mpsc::UnboundedReceiver<ManifestReadRequest<ArtifactStreamT>>,
    sender: SchedulerSender<TempFileT>,
}

async fn read_manifest<ArtifactStreamT: AsyncRead + Unpin, TempFileT>(
    sender: SchedulerSender<TempFileT>,
    stream: ArtifactStreamT,
    job_id: JobId,
) -> anyhow::Result<()> {
    let mut reader = AsyncManifestReader::new(stream).await?;
    while let Some(entry) = reader.next().await? {
        if let ManifestEntryData::File(ManifestFileData::Digest(digest)) = entry.data {
            sender.send(Message::GotManifestEntry(digest, job_id)).ok();
        }
    }
    Ok(())
}

impl<ArtifactStreamT: AsyncRead + Unpin + Send + 'static, TempFileT>
    CacheManifestReader<ArtifactStreamT, TempFileT>
where
    TempFileT: Send + 'static,
{
    fn new(
        receiver: tokio_mpsc::UnboundedReceiver<ManifestReadRequest<ArtifactStreamT>>,
        sender: SchedulerSender<TempFileT>,
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
                let result = read_manifest(sender.clone(), req.manifest_stream, req.job_id).await;
                sender
                    .send(Message::FinishedReadingManifest(
                        req.digest, req.job_id, result,
                    ))
                    .ok();
            });
        }
    }
}

/// The production scheduler message type.
pub type SchedulerMessage<TempFileT> = Message<
    tokio_mpsc::UnboundedSender<BrokerToClient>,
    tokio_mpsc::UnboundedSender<BrokerToWorker>,
    tokio_mpsc::UnboundedSender<BrokerToMonitor>,
    std_mpsc::Sender<Option<(PathBuf, u64)>>,
    TempFileT,
>;

/// This type is used often enough to warrant an alias.
pub type SchedulerSender<TempFileT> = tokio_mpsc::UnboundedSender<SchedulerMessage<TempFileT>>;

pub struct SchedulerTask<CacheT: SchedulerCache> {
    scheduler: Scheduler<CacheT, PassThroughDeps<CacheT::ArtifactStream>>,
    sender: SchedulerSender<CacheT::TempFile>,
    receiver: tokio_mpsc::UnboundedReceiver<SchedulerMessage<CacheT::TempFile>>,
}

impl<CacheT: SchedulerCache> SchedulerTask<CacheT>
where
    CacheT::ArtifactStream: tokio::io::AsyncRead + Unpin + Send + 'static,
    CacheT::TempFile: Send + Sync + 'static,
{
    pub fn new(cache: CacheT) -> Self {
        let (sender, receiver) = tokio_mpsc::unbounded_channel();

        let (manifest_reader_sender, manifest_reader_receiver) = tokio_mpsc::unbounded_channel();
        let mut manifest_reader =
            CacheManifestReader::new(manifest_reader_receiver, sender.clone());
        tokio::task::spawn(async move { manifest_reader.run().await });

        SchedulerTask {
            scheduler: Scheduler::new(cache, manifest_reader_sender),
            sender,
            receiver,
        }
    }

    pub fn scheduler_sender(&self) -> &SchedulerSender<CacheT::TempFile> {
        &self.sender
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
        let mut deps = PassThroughDeps(PhantomData);
        sync::channel_reader(self.receiver, |msg| {
            self.scheduler.receive_message(&mut deps, msg);
        })
        .await
        .unwrap();
    }
}
