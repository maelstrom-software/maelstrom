mod artifact_gatherer;
mod scheduler;

use crate::cache::SchedulerCache;
use artifact_gatherer::{ArtifactGatherer, StartJob};
use maelstrom_base::{
    manifest::{ManifestEntryData, ManifestFileData},
    proto::{BrokerToClient, BrokerToMonitor, BrokerToWorker},
    stats::BrokerStatistics,
    ArtifactType, ArtifactUploadLocation, ClientId, ClientJobId, JobBrokerStatus, JobId,
    JobOutcomeResult, JobSpec, JobWorkerStatus, MonitorId, NonEmpty, Sha256Digest, WorkerId,
};
use maelstrom_util::{manifest::AsyncManifestReader, sync};
use scheduler::Scheduler;
use std::{path::PathBuf, sync::mpsc::Sender as SyncSender};
use tokio::{
    io::AsyncRead,
    sync::mpsc::{self as tokio_mpsc, UnboundedReceiver, UnboundedSender},
    task::{self, JoinSet},
};

/*  ____       _              _       _
 * / ___|  ___| |__   ___  __| |_   _| | ___ _ __
 * \___ \ / __| '_ \ / _ \/ _` | | | | |/ _ \ '__|
 *  ___) | (__| | | |  __/ (_| | |_| | |  __/ |
 * |____/ \___|_| |_|\___|\__,_|\__,_|_|\___|_|
 *  ____                            _                 _
 * |  _ \  ___ _ __   ___ _ __   __| | ___ _ __   ___(_) ___  ___
 * | | | |/ _ \ '_ \ / _ \ '_ \ / _` |/ _ \ '_ \ / __| |/ _ \/ __|
 * | |_| |  __/ |_) |  __/ | | | (_| |  __/ | | | (__| |  __/\__ \
 * |____/ \___| .__/ \___|_| |_|\__,_|\___|_| |_|\___|_|\___||___/
 *            |_|
 *  FIGLET: Scheduler Dependencies
 */

#[derive(Debug)]
pub struct PassThroughSchedulerDeps;

/// The production implementation of [SchedulerDeps]. This implementation just hands the
/// message to the provided sender.
impl scheduler::Deps for PassThroughSchedulerDeps {
    type ClientSender = UnboundedSender<BrokerToClient>;
    type WorkerSender = UnboundedSender<BrokerToWorker>;
    type MonitorSender = UnboundedSender<BrokerToMonitor>;

    fn send_job_response_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        cjid: ClientJobId,
        result: JobOutcomeResult,
    ) {
        let _ = sender.send(BrokerToClient::JobResponse(cjid, result));
    }

    fn send_job_status_update_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        cjid: ClientJobId,
        status: JobBrokerStatus,
    ) {
        let _ = sender.send(BrokerToClient::JobStatusUpdate(cjid, status));
    }

    fn send_enqueue_job_to_worker(
        &mut self,
        sender: &mut Self::WorkerSender,
        jid: JobId,
        spec: JobSpec,
    ) {
        let _ = sender.send(BrokerToWorker::EnqueueJob(jid, spec));
    }

    fn send_cancel_job_to_worker(&mut self, sender: &mut Self::WorkerSender, jid: JobId) {
        let _ = sender.send(BrokerToWorker::CancelJob(jid));
    }

    fn send_statistics_response_to_monitor(
        &mut self,
        sender: &mut Self::MonitorSender,
        statistics: BrokerStatistics,
    ) {
        let _ = sender.send(BrokerToMonitor::StatisticsResponse(statistics));
    }
}

impl<CacheT, DepsT> scheduler::ArtifactGatherer for ArtifactGatherer<CacheT, DepsT>
where
    CacheT: SchedulerCache,
    DepsT: artifact_gatherer::Deps<ArtifactStream = CacheT::ArtifactStream>,
{
    type ClientSender = DepsT::ClientSender;

    fn client_connected(&mut self, cid: ClientId, sender: Self::ClientSender) {
        self.client_connected(cid, sender)
    }

    fn client_disconnected(&mut self, cid: ClientId) {
        self.client_disconnected(cid)
    }

    fn start_job(
        &mut self,
        jid: JobId,
        layers: NonEmpty<(Sha256Digest, ArtifactType)>,
    ) -> StartJob {
        self.start_job(jid, layers)
    }

    fn job_completed(&mut self, jid: JobId) {
        self.job_completed(jid)
    }
}

/*     _         _ _   _  __            _    ____       _   _
 *    / \   _ __(_) |_(_)/ _| __ _  ___| |_ / ___| __ _| |_| |__   ___ _ __ ___ _ __
 *   / _ \ | '__| | __| | |_ / _` |/ __| __| |  _ / _` | __| '_ \ / _ \ '__/ _ \ '__|
 *  / ___ \| |  | | |_| |  _| (_| | (__| |_| |_| | (_| | |_| | | |  __/ | |  __/ |
 * /_/   \_\_|  |_|\__|_|_|  \__,_|\___|\__|\____|\__,_|\__|_| |_|\___|_|  \___|_|
 *  ____                            _                 _
 * |  _ \  ___ _ __   ___ _ __   __| | ___ _ __   ___(_) ___  ___
 * | | | |/ _ \ '_ \ / _ \ '_ \ / _` |/ _ \ '_ \ / __| |/ _ \/ __|
 * | |_| |  __/ |_) |  __/ | | | (_| |  __/ | | | (__| |  __/\__ \
 * |____/ \___| .__/ \___|_| |_|\__,_|\___|_| |_|\___|_|\___||___/
 *            |_|
 *  FIGLET: AritifactGatherer Dependencies
 */

#[derive(Debug)]
struct PassThroughArtifactGathererDeps<TempFileT, ArtifactStreamT> {
    task_sender: Sender<TempFileT>,
    manifest_reader_sender: UnboundedSender<ManifestReadRequest<ArtifactStreamT>>,
}

impl<TempFileT, ArtifactStreamT> PassThroughArtifactGathererDeps<TempFileT, ArtifactStreamT> {
    fn new(
        task_sender: Sender<TempFileT>,
        manifest_reader_sender: UnboundedSender<ManifestReadRequest<ArtifactStreamT>>,
    ) -> Self {
        Self {
            task_sender,
            manifest_reader_sender,
        }
    }
}

impl<TempFileT, ArtifactStreamT> artifact_gatherer::Deps
    for PassThroughArtifactGathererDeps<TempFileT, ArtifactStreamT>
{
    type ArtifactStream = ArtifactStreamT;
    type WorkerArtifactFetcherSender = SyncSender<Option<(PathBuf, u64)>>;
    type ClientSender = UnboundedSender<BrokerToClient>;

    fn send_read_request_to_manifest_reader(
        &mut self,
        manifest_stream: ArtifactStreamT,
        manifest_digest: Sha256Digest,
    ) {
        let _ = self.manifest_reader_sender.send(ManifestReadRequest {
            manifest_stream,
            digest: manifest_digest,
        });
    }

    fn send_response_to_worker_artifact_fetcher(
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

    fn send_general_error_to_client(&mut self, sender: &mut Self::ClientSender, error: String) {
        let _ = sender.send(BrokerToClient::GeneralError(error));
    }

    fn send_jobs_ready_to_scheduler(&mut self, jids: NonEmpty<JobId>) {
        let _ = self
            .task_sender
            .send(Message::JobsReadyFromArtifactGatherer(jids));
    }

    fn send_jobs_failed_to_scheduler(&mut self, jobs: NonEmpty<JobId>, err: String) {
        let _ = self
            .task_sender
            .send(Message::JobsFailedFromArtifactGatherer(jobs, err));
    }
}

/*   ____           _          __  __             _  __           _
 *  / ___|__ _  ___| |__   ___|  \/  | __ _ _ __ (_)/ _| ___  ___| |_
 * | |   / _` |/ __| '_ \ / _ \ |\/| |/ _` | '_ \| | |_ / _ \/ __| __|
 * | |__| (_| | (__| | | |  __/ |  | | (_| | | | | |  _|  __/\__ \ |_
 *  \____\__,_|\___|_| |_|\___|_|  |_|\__,_|_| |_|_|_|  \___||___/\__|
 *  ____                _
 * |  _ \ ___  __ _  __| | ___ _ __
 * | |_) / _ \/ _` |/ _` |/ _ \ '__|
 * |  _ <  __/ (_| | (_| |  __/ |
 * |_| \_\___|\__,_|\__,_|\___|_|
 *  FIGLET: CacheManifestReader
 */

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ManifestReadRequest<ArtifactStreamT> {
    manifest_stream: ArtifactStreamT,
    digest: Sha256Digest,
}

#[derive(Debug)]
struct CacheManifestReader<ArtifactStreamT, TempFileT> {
    tasks: JoinSet<()>,
    receiver: UnboundedReceiver<ManifestReadRequest<ArtifactStreamT>>,
    sender: Sender<TempFileT>,
}

async fn read_manifest<ArtifactStreamT: AsyncRead + Unpin, TempFileT>(
    sender: Sender<TempFileT>,
    stream: ArtifactStreamT,
    manifest: Sha256Digest,
) -> anyhow::Result<()> {
    let mut reader = AsyncManifestReader::new(stream).await?;
    while let Some(entry) = reader.next().await? {
        if let ManifestEntryData::File(ManifestFileData::Digest(entry_digest)) = entry.data {
            sender
                .send(Message::GotManifestEntry {
                    manifest_digest: manifest.clone(),
                    entry_digest,
                })
                .ok();
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
        receiver: UnboundedReceiver<ManifestReadRequest<ArtifactStreamT>>,
        sender: Sender<TempFileT>,
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
                    read_manifest(sender.clone(), req.manifest_stream, req.digest.clone()).await;
                sender
                    .send(Message::FinishedReadingManifest(req.digest, result))
                    .ok();
            });
        }
    }
}

/*  __  __
 * |  \/  | ___  ___ ___  __ _  __ _  ___
 * | |\/| |/ _ \/ __/ __|/ _` |/ _` |/ _ \
 * | |  | |  __/\__ \__ \ (_| | (_| |  __/
 * |_|  |_|\___||___/___/\__,_|\__, |\___|
 *                             |___/
 *  FIGLET: Message
 */

/// The incoming messages, or events, for [`Scheduler`].
///
/// If [`Scheduler`] weren't implement as an async state machine, these would be its methods.
#[derive(Debug)]
pub enum Message<
    TempFileT,
    ClientSenderT = UnboundedSender<BrokerToClient>,
    WorkerSenderT = UnboundedSender<BrokerToWorker>,
    MonitorSenderT = UnboundedSender<BrokerToMonitor>,
    WorkerArtifactFetcherSenderT = SyncSender<Option<(PathBuf, u64)>>,
> {
    /// The given client connected, and messages can be sent to it on the given sender.
    ClientConnected(ClientId, ClientSenderT),

    /// The given client disconnected.
    ClientDisconnected(ClientId),

    /// The given client has sent us the given message.
    JobRequestFromClient(ClientId, ClientJobId, JobSpec),
    ArtifactTransferredFromClient(ClientId, Sha256Digest, ArtifactUploadLocation),

    /// The given worker connected. It has the given number of slots and messages can be sent to it
    /// on the given sender.
    WorkerConnected(WorkerId, usize, WorkerSenderT),

    /// The given worker disconnected.
    WorkerDisconnected(WorkerId),

    /// The given worker has sent us the given message.
    JobResponseFromWorker(WorkerId, JobId, JobOutcomeResult),
    JobStatusUpdateFromWorker(WorkerId, JobId, JobWorkerStatus),

    /// The given monitor connected, and messages can be sent to it on the given sender.
    MonitorConnected(MonitorId, MonitorSenderT),

    /// The given monitor disconnected.
    MonitorDisconnected(MonitorId),

    /// The given client has sent us the given message.
    StatisticsRequestFromMonitor(MonitorId),

    /// An artifact has been pushed to us. The artifact has the given digest and length. It is
    /// temporarily stored at the given path.
    GotArtifact(Sha256Digest, TempFileT),

    /// A worker has requested the given artifact be sent to it over the given sender. After the
    /// contents are sent to the worker, the refcount needs to be decremented with a
    /// [`Message::DecrementRefcount`] message.
    GetArtifactForWorker(Sha256Digest, WorkerArtifactFetcherSenderT),

    /// A worker has been sent an artifact, and we can now release the refcount that was keeping
    /// the artifact from being removed while being transferred.
    DecrementRefcount(Sha256Digest),

    /// The stats heartbeat task has decided it's time to take another statistics sample.
    StatisticsHeartbeat,

    /// Read a manifest and found the given digest as a dependency.
    GotManifestEntry {
        manifest_digest: Sha256Digest,
        entry_digest: Sha256Digest,
    },

    /// Finished reading the manifest either due to reaching EOF or an error and no more dependent
    /// digests messages will be sent.
    FinishedReadingManifest(Sha256Digest, anyhow::Result<()>),

    /// The ArtifactGatherer has determined that it has everything necessary to start the given
    /// job. This isn't used for every job, as there is a "fast path" in the situation where the
    /// ArtifactGatherer has everything it needs when it first receives the JobRequest.
    JobsReadyFromArtifactGatherer(NonEmpty<JobId>),

    /// The ArtifactGatherer has encountered an error gatherering artifacts for the given job.
    JobsFailedFromArtifactGatherer(NonEmpty<JobId>, String),
}

/// This type is used often enough to warrant an alias.
pub type Sender<TempFileT> = UnboundedSender<Message<TempFileT>>;

/*  ____       _              _       _          _____         _
 * / ___|  ___| |__   ___  __| |_   _| | ___ _ _|_   _|_ _ ___| | __
 * \___ \ / __| '_ \ / _ \/ _` | | | | |/ _ \ '__|| |/ _` / __| |/ /
 *  ___) | (__| | | |  __/ (_| | |_| | |  __/ |   | | (_| \__ \   <
 * |____/ \___|_| |_|\___|\__,_|\__,_|_|\___|_|   |_|\__,_|___/_|\_\
 *  FIGLET: SchedulerTask
 */

type ArtifactGathererForCache<CacheT> = ArtifactGatherer<
    CacheT,
    PassThroughArtifactGathererDeps<
        <CacheT as SchedulerCache>::TempFile,
        <CacheT as SchedulerCache>::ArtifactStream,
    >,
>;

pub struct SchedulerTask<CacheT: SchedulerCache> {
    artifact_gatherer: ArtifactGathererForCache<CacheT>,
    scheduler: Scheduler<PassThroughSchedulerDeps>,
    sender: Sender<CacheT::TempFile>,
    receiver: UnboundedReceiver<Message<CacheT::TempFile>>,
}

impl<CacheT: SchedulerCache> SchedulerTask<CacheT>
where
    CacheT::ArtifactStream: AsyncRead + Unpin + Send + 'static,
    CacheT::TempFile: Send + Sync + 'static,
{
    pub fn new(cache: CacheT) -> Self {
        let (sender, receiver) = tokio_mpsc::unbounded_channel();

        let (manifest_reader_sender, manifest_reader_receiver) = tokio_mpsc::unbounded_channel();
        let mut manifest_reader =
            CacheManifestReader::new(manifest_reader_receiver, sender.clone());
        task::spawn(async move { manifest_reader.run().await });

        SchedulerTask {
            artifact_gatherer: ArtifactGatherer::new(
                cache,
                PassThroughArtifactGathererDeps::new(sender.clone(), manifest_reader_sender),
                10_000_000,
                16.try_into().unwrap(),
            ),
            scheduler: Scheduler::new(PassThroughSchedulerDeps),
            sender,
            receiver,
        }
    }

    pub fn scheduler_task_sender(&self) -> &Sender<CacheT::TempFile> {
        &self.sender
    }

    /// Main loop for the scheduler. This should be run on a task of its own. There should be
    /// exactly one of these in a broker process. It will return when all senders associated with
    /// the receiver are closed, which will happen when the listener and all outstanding worker and
    /// client socket tasks terminate.
    ///
    /// This function ignores any errors it encounters sending a message to an [`UnboundedSender`].
    /// The rationale is that this indicates that the socket connection has closed, and there are
    /// no more worker tasks to handle that connection. This means that a disconnected message is
    /// on its way to notify the scheduler. It is best to just ignore the error in that case.
    /// Besides, the [`scheduler::Deps`] interface doesn't give us a way to return an error, for
    /// precisely this reason.
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
            Message::StatisticsHeartbeat => self.scheduler.receive_statistics_heartbeat(),
            Message::GotManifestEntry {
                manifest_digest,
                entry_digest,
            } => {
                self.artifact_gatherer
                    .receive_manifest_entry(manifest_digest, entry_digest);
            }
            Message::FinishedReadingManifest(digest, result) => self
                .artifact_gatherer
                .receive_finished_reading_manifest(digest, result),
            Message::JobsReadyFromArtifactGatherer(jobs) => {
                self.scheduler
                    .receive_jobs_ready_from_artifact_gatherer(jobs);
            }
            Message::JobsFailedFromArtifactGatherer(jobs, err) => {
                self.scheduler
                    .receive_jobs_failed_from_artifact_gatherer(jobs, err);
            }
        })
        .await
        .unwrap();
    }
}
