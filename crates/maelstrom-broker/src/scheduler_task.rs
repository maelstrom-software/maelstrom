mod artifact_gatherer;
mod scheduler;

use crate::cache::SchedulerCache;
use artifact_gatherer::{ArtifactGatherer, Deps as ArtifactGathererDeps};
use maelstrom_base::{
    manifest::{ManifestEntryData, ManifestFileData},
    proto::{BrokerToClient, BrokerToMonitor, BrokerToWorker},
    ArtifactType, ClientId, JobId, NonEmpty, Sha256Digest,
};
use maelstrom_util::{manifest::AsyncManifestReader, sync};
use scheduler::{Message, Scheduler, SchedulerDeps};
use std::{path::PathBuf, sync::mpsc as std_mpsc};
use tokio::{io::AsyncRead, sync::mpsc as tokio_mpsc, task::JoinSet};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ManifestReadRequest<ArtifactStreamT> {
    manifest_stream: ArtifactStreamT,
    digest: Sha256Digest,
    jid: JobId,
}

#[derive(Debug)]
pub struct PassThroughSchedulerDeps;

/// The production implementation of [SchedulerDeps]. This implementation just hands the
/// message to the provided sender.
impl SchedulerDeps for PassThroughSchedulerDeps {
    type ClientSender = tokio_mpsc::UnboundedSender<BrokerToClient>;
    type WorkerSender = tokio_mpsc::UnboundedSender<BrokerToWorker>;
    type MonitorSender = tokio_mpsc::UnboundedSender<BrokerToMonitor>;

    fn send_job_response_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        cjid: maelstrom_base::ClientJobId,
        result: maelstrom_base::JobOutcomeResult,
    ) {
        let _ = sender.send(BrokerToClient::JobResponse(cjid, result));
    }

    fn send_job_status_update_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        cjid: maelstrom_base::ClientJobId,
        status: maelstrom_base::JobBrokerStatus,
    ) {
        let _ = sender.send(BrokerToClient::JobStatusUpdate(cjid, status));
    }

    fn send_message_to_worker(&mut self, sender: &mut Self::WorkerSender, message: BrokerToWorker) {
        let _ = sender.send(message);
    }

    fn send_message_to_monitor(
        &mut self,
        sender: &mut Self::MonitorSender,
        message: BrokerToMonitor,
    ) {
        let _ = sender.send(message);
    }
}

#[derive(Debug)]
struct PassThroughArtifactGathererDeps<TempFileT, ArtifactStreamT> {
    task_sender: SchedulerSender<TempFileT>,
    manifest_reader_sender: tokio_mpsc::UnboundedSender<ManifestReadRequest<ArtifactStreamT>>,
}

impl<TempFileT, ArtifactStreamT> PassThroughArtifactGathererDeps<TempFileT, ArtifactStreamT> {
    fn new(
        task_sender: SchedulerSender<TempFileT>,
        manifest_reader_sender: tokio_mpsc::UnboundedSender<ManifestReadRequest<ArtifactStreamT>>,
    ) -> Self {
        Self {
            task_sender,
            manifest_reader_sender,
        }
    }
}

impl<TempFileT, ArtifactStreamT> ArtifactGathererDeps
    for PassThroughArtifactGathererDeps<TempFileT, ArtifactStreamT>
{
    type ArtifactStream = ArtifactStreamT;
    type WorkerArtifactFetcherSender = std_mpsc::Sender<Option<(PathBuf, u64)>>;
    type ClientSender = tokio_mpsc::UnboundedSender<BrokerToClient>;

    fn send_message_to_manifest_reader(&mut self, req: ManifestReadRequest<Self::ArtifactStream>) {
        let _ = self.manifest_reader_sender.send(req);
    }

    fn send_message_to_worker_artifact_fetcher(
        &mut self,
        sender: &mut Self::WorkerArtifactFetcherSender,
        message: Option<(PathBuf, u64)>,
    ) {
        let _ = sender.send(message);
    }

    fn send_transfer_artifact_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        digest: Sha256Digest,
    ) {
        let _ = sender.send(BrokerToClient::TransferArtifact(digest));
    }

    fn send_artifact_transferred_response_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        digest: Sha256Digest,
        result: Result<(), String>,
    ) {
        let _ = sender.send(BrokerToClient::ArtifactTransferredResponse(digest, result));
    }

    fn send_jobs_ready_to_scheduler(&mut self, jids: NonEmpty<JobId>) {
        let _ = self
            .task_sender
            .send(Message::JobsReadyFromArtifactGatherer(jids));
    }

    fn send_job_failure_to_scheduler(&mut self, jid: JobId, err: String) {
        let _ = self
            .task_sender
            .send(Message::JobFailureFromArtifactGatherer(jid, err));
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
                let result = read_manifest(sender.clone(), req.manifest_stream, req.jid).await;
                sender
                    .send(Message::FinishedReadingManifest(
                        req.digest, req.jid, result,
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

type ArtifactGathererForCache<CacheT> = ArtifactGatherer<
    CacheT,
    PassThroughArtifactGathererDeps<
        <CacheT as SchedulerCache>::TempFile,
        <CacheT as SchedulerCache>::ArtifactStream,
    >,
>;

impl<CacheT, DepsT> scheduler::ArtifactGatherer
    for artifact_gatherer::ArtifactGatherer<CacheT, DepsT>
where
    CacheT: SchedulerCache,
    DepsT: artifact_gatherer::Deps<ArtifactStream = CacheT::ArtifactStream>,
{
    type ClientSender = DepsT::ClientSender;

    fn receive_client_connected(&mut self, cid: ClientId, sender: Self::ClientSender) {
        self.receive_client_connected(cid, sender)
    }

    fn receive_client_disconnected(&mut self, cid: ClientId) {
        self.receive_client_disconnected(cid)
    }

    fn start_job(
        &mut self,
        jid: JobId,
        layers: NonEmpty<(Sha256Digest, ArtifactType)>,
    ) -> artifact_gatherer::StartJob {
        self.start_job(jid, layers)
    }

    fn receive_job_completed(&mut self, jid: JobId) {
        self.receive_job_completed(jid)
    }

    fn get_waiting_for_artifacts_count(&self, cid: ClientId) -> u64 {
        self.get_waiting_for_artifacts_count(cid)
    }
}

pub struct SchedulerTask<CacheT: SchedulerCache> {
    artifact_gatherer: ArtifactGathererForCache<CacheT>,
    scheduler: Scheduler<ArtifactGathererForCache<CacheT>, PassThroughSchedulerDeps>,
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
            artifact_gatherer: ArtifactGatherer::new(
                cache,
                PassThroughArtifactGathererDeps::new(sender.clone(), manifest_reader_sender),
            ),
            scheduler: Scheduler::new(PassThroughSchedulerDeps),
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
        sync::channel_reader(self.receiver, |msg| match msg {
            Message::ClientConnected(id, sender) => {
                self.scheduler
                    .receive_client_connected(&mut self.artifact_gatherer, id, sender)
            }
            Message::ClientDisconnected(id) => self
                .scheduler
                .receive_client_disconnected(&mut self.artifact_gatherer, id),
            Message::JobRequestFromClient(cid, cjid, spec) => self
                .scheduler
                .receive_job_request_from_client(&mut self.artifact_gatherer, cid, cjid, spec),
            Message::ArtifactTransferredFromClient(cid, digest, location) => self
                .artifact_gatherer
                .receive_artifact_transferred(cid, digest, location),
            Message::WorkerConnected(id, slots, sender) => {
                self.scheduler.receive_worker_connected(id, slots, sender)
            }
            Message::WorkerDisconnected(id) => self.scheduler.receive_worker_disconnected(id),
            Message::JobResponseFromWorker(wid, jid, result) => self
                .scheduler
                .receive_job_response_from_worker(&mut self.artifact_gatherer, wid, jid, result),
            Message::JobStatusUpdateFromWorker(wid, jid, status) => self
                .scheduler
                .receive_job_status_update_from_worker(wid, jid, status),
            Message::MonitorConnected(id, sender) => {
                self.scheduler.receive_monitor_connected(id, sender)
            }
            Message::MonitorDisconnected(id) => self.scheduler.receive_monitor_disconnected(id),
            Message::StatisticsRequestFromMonitor(mid) => {
                self.scheduler.receive_statistics_request_from_monitor(mid)
            }
            Message::GotArtifact(digest, file) => {
                self.artifact_gatherer.receive_got_artifact(digest, file)
            }
            Message::GetArtifactForWorker(digest, sender) => self
                .artifact_gatherer
                .receive_get_artifact_for_worker(digest, sender),
            Message::DecrementRefcount(digest) => self
                .artifact_gatherer
                .receive_decrement_refcount_from_worker(digest),
            Message::StatisticsHeartbeat => self
                .scheduler
                .receive_statistics_heartbeat(&mut self.artifact_gatherer),
            Message::GotManifestEntry(digest, jid) => {
                self.artifact_gatherer.receive_manifest_entry(digest, jid)
            }
            Message::FinishedReadingManifest(digest, jid, result) => self
                .artifact_gatherer
                .receive_finished_reading_manifest(digest, jid, result),
            Message::JobsReadyFromArtifactGatherer(ready) => {
                self.scheduler
                    .receive_jobs_ready_from_artifact_gatherer(ready);
            }
            Message::JobFailureFromArtifactGatherer(jid, err) => {
                self.scheduler
                    .receive_job_failure_from_artifact_gatherer(jid, err);
            }
        })
        .await
        .unwrap();
    }
}
