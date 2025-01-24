//! Central processing module for the broker. Receives and sends messages to and from clients and
//! workers.

use crate::scheduler_task::artifact_gatherer::StartJob;
use enum_map::enum_map;
use maelstrom_base::{
    proto::{ArtifactUploadLocation, BrokerToClient, BrokerToMonitor, BrokerToWorker},
    stats::{
        BrokerStatistics, JobState, JobStateCounts, JobStatisticsSample, JobStatisticsTimeSeries,
        WorkerStatistics,
    },
    ArtifactType, ClientId, ClientJobId, JobBrokerStatus, JobId, JobOutcomeResult, JobSpec,
    JobWorkerStatus, MonitorId, NonEmpty, Sha256Digest, WorkerId,
};
use maelstrom_util::{
    duration,
    ext::{BoolExt as _, OptionExt as _},
    heap::{Heap, HeapDeps, HeapIndex},
};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    time::Duration,
};

/*              _     _ _
 *  _ __  _   _| |__ | (_) ___
 * | '_ \| | | | '_ \| | |/ __|
 * | |_) | |_| | |_) | | | (__
 * | .__/ \__,_|_.__/|_|_|\___|
 * |_|
 *  FIGLET: public
 */

pub trait ArtifactGatherer {
    type TempFile;
    type ClientSender;
    fn client_connected(&mut self, cid: ClientId, sender: Self::ClientSender);
    fn client_disconnected(&mut self, cid: ClientId);
    fn start_job(&mut self, jid: JobId, layers: NonEmpty<(Sha256Digest, ArtifactType)>)
        -> StartJob;
    fn artifact_transferred(
        &mut self,
        cid: ClientId,
        digest: Sha256Digest,
        file: Option<Self::TempFile>,
    ) -> HashSet<JobId>;
    fn manifest_read_for_job_entry(&mut self, digest: &Sha256Digest, jid: JobId);
    #[must_use]
    fn manifest_read_for_job_complete(
        &mut self,
        digest: Sha256Digest,
        jid: JobId,
        result: anyhow::Result<()>,
    ) -> bool;
    fn complete_job(&mut self, jid: JobId);
    fn get_waiting_for_artifacts_count(&self, cid: ClientId) -> u64;
}

/// The external dependencies for [`Scheduler`]. All of these methods must be asynchronous: they
/// must not block the current thread.
pub trait SchedulerDeps {
    type ClientSender: Clone;
    type WorkerSender;
    type MonitorSender;
    fn send_message_to_client(&mut self, sender: &mut Self::ClientSender, message: BrokerToClient);
    fn send_message_to_worker(&mut self, sender: &mut Self::WorkerSender, message: BrokerToWorker);
    fn send_message_to_monitor(
        &mut self,
        sender: &mut Self::MonitorSender,
        message: BrokerToMonitor,
    );
}

/// The incoming messages, or events, for [`Scheduler`].
///
/// If [`Scheduler`] weren't implement as an async state machine, these would be its methods.
#[derive(Debug)]
pub enum Message<
    ClientSenderT,
    WorkerSenderT,
    MonitorSenderT,
    WorkerArtifactFetcherSenderT,
    TempFileT,
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

    /// Read a manifest and found the given digest as a dependency. The job which is responsible
    /// for the reading is included.
    GotManifestEntry(Sha256Digest, JobId),

    /// Finished reading the manifest either due to reaching EOF or an error and no more dependent
    /// digests messages will be sent.
    FinishedReadingManifest(Sha256Digest, JobId, anyhow::Result<()>),
}

impl<ArtifactGathererT: ArtifactGatherer, DepsT: SchedulerDeps>
    Scheduler<ArtifactGathererT, DepsT>
{
    /// Create a new scheduler with the given [`ArtifactGatherer`]. Note that [`SchedulerDeps`] are
    /// passed in to `Self::receive_message`.
    pub fn new(deps: DepsT) -> Self {
        Scheduler {
            deps,
            clients: ClientMap(HashMap::default()),
            workers: WorkerMap(HashMap::default()),
            monitors: HashMap::default(),
            queued_jobs: BinaryHeap::default(),
            worker_heap: Heap::default(),
            job_statistics: JobStatisticsTimeSeries::default(),
            tcp_upload_landing_pad: Default::default(),
        }
    }
}

/*             _            _
 *  _ __  _ __(_)_   ____ _| |_ ___
 * | '_ \| '__| \ \ / / _` | __/ _ \
 * | |_) | |  | |\ V / (_| | ||  __/
 * | .__/|_|  |_| \_/ \__,_|\__\___|
 * |_|
 *  FIGLET: private
 */

struct Job {
    spec: JobSpec,
}

impl Job {
    fn new(spec: JobSpec) -> Self {
        Job { spec }
    }
}

struct Client<DepsT: SchedulerDeps> {
    sender: DepsT::ClientSender,
    jobs: HashMap<ClientJobId, Job>,
    num_completed_jobs: u64,
}

impl<DepsT: SchedulerDeps> Client<DepsT> {
    fn new(sender: DepsT::ClientSender) -> Self {
        Client {
            sender,
            jobs: HashMap::default(),
            num_completed_jobs: 0,
        }
    }
}

struct ClientMap<DepsT: SchedulerDeps>(HashMap<ClientId, Client<DepsT>>);

impl<DepsT: SchedulerDeps> ClientMap<DepsT> {
    fn job_from_jid(&self, jid: JobId) -> &Job {
        self.0.get(&jid.cid).unwrap().jobs.get(&jid.cjid).unwrap()
    }
}

struct Worker<DepsT: SchedulerDeps> {
    slots: usize,
    pending: HashSet<JobId>,
    heap_index: HeapIndex,
    sender: DepsT::WorkerSender,
}

impl<DepsT: SchedulerDeps> Worker<DepsT> {
    fn new(slots: usize, sender: DepsT::WorkerSender) -> Self {
        Worker {
            slots,
            sender,
            pending: HashSet::default(),
            heap_index: HeapIndex::default(),
        }
    }
}

struct WorkerMap<DepsT: SchedulerDeps>(HashMap<WorkerId, Worker<DepsT>>);

impl<DepsT: SchedulerDeps> HeapDeps for WorkerMap<DepsT> {
    type Element = WorkerId;

    fn is_element_less_than(&self, lhs_id: &WorkerId, rhs_id: &WorkerId) -> bool {
        let lhs_worker = self.0.get(lhs_id).unwrap();
        let rhs_worker = self.0.get(rhs_id).unwrap();
        let lhs = (lhs_worker.pending.len() * rhs_worker.slots, *lhs_id);
        let rhs = (rhs_worker.pending.len() * lhs_worker.slots, *rhs_id);
        lhs.cmp(&rhs) == Ordering::Less
    }

    fn update_index(&mut self, elem: &WorkerId, idx: HeapIndex) {
        self.0.get_mut(elem).unwrap().heap_index = idx;
    }
}

struct QueuedJob {
    jid: JobId,
    priority: i8,
    estimated_duration: Option<Duration>,
}

impl QueuedJob {
    fn new(jid: JobId, priority: i8, estimated_duration: Option<Duration>) -> Self {
        Self {
            jid,
            priority,
            estimated_duration,
        }
    }
}

impl PartialEq for QueuedJob {
    fn eq(&self, other: &Self) -> bool {
        self.priority.eq(&other.priority) && self.estimated_duration.eq(&other.estimated_duration)
    }
}

impl PartialOrd for QueuedJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for QueuedJob {}

impl Ord for QueuedJob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| duration::cmp(&self.estimated_duration, &other.estimated_duration))
    }
}

pub struct Scheduler<ArtifactGathererT: ArtifactGatherer, DepsT: SchedulerDeps> {
    deps: DepsT,
    clients: ClientMap<DepsT>,
    workers: WorkerMap<DepsT>,
    monitors: HashMap<MonitorId, DepsT::MonitorSender>,
    queued_jobs: BinaryHeap<QueuedJob>,
    worker_heap: Heap<WorkerMap<DepsT>>,
    job_statistics: JobStatisticsTimeSeries,
    tcp_upload_landing_pad: HashMap<Sha256Digest, ArtifactGathererT::TempFile>,
}

impl<ArtifactGathererT, DepsT> Scheduler<ArtifactGathererT, DepsT>
where
    DepsT: SchedulerDeps,
    ArtifactGathererT: ArtifactGatherer<ClientSender = DepsT::ClientSender>,
{
    fn possibly_start_jobs(&mut self, mut just_enqueued: HashSet<JobId>) {
        while !self.queued_jobs.is_empty() && !self.workers.0.is_empty() {
            let wid = self.worker_heap.peek().unwrap();
            let worker = self.workers.0.get_mut(wid).unwrap();

            if worker.pending.len() == 2 * worker.slots {
                break;
            }

            let jid = self.queued_jobs.pop().unwrap().jid;
            let job = self.clients.job_from_jid(jid);
            self.deps.send_message_to_worker(
                &mut worker.sender,
                BrokerToWorker::EnqueueJob(jid, job.spec.clone()),
            );
            just_enqueued.remove(&jid);

            worker.pending.insert(jid).assert_is_true();
            let heap_index = worker.heap_index;
            self.worker_heap.sift_down(&mut self.workers, heap_index);
        }
        for jid in just_enqueued {
            let client = self.clients.0.get_mut(&jid.cid).unwrap();
            self.deps.send_message_to_client(
                &mut client.sender,
                BrokerToClient::JobStatusUpdate(jid.cjid, JobBrokerStatus::WaitingForWorker),
            );
        }
    }

    pub fn receive_client_connected(
        &mut self,
        artifact_gatherer: &mut ArtifactGathererT,
        cid: ClientId,
        sender: DepsT::ClientSender,
    ) {
        artifact_gatherer.client_connected(cid, sender.clone());
        self.clients
            .0
            .insert(cid, Client::new(sender))
            .assert_is_none();
    }

    pub fn receive_client_disconnected(
        &mut self,
        artifact_gatherer: &mut ArtifactGathererT,
        cid: ClientId,
    ) {
        artifact_gatherer.client_disconnected(cid);
        self.clients.0.remove(&cid).assert_is_some();

        self.queued_jobs.retain(|qj| qj.jid.cid != cid);
        for worker in self.workers.0.values_mut() {
            worker.pending.retain(|jid| {
                let retain = jid.cid != cid;
                if !retain {
                    self.deps.send_message_to_worker(
                        &mut worker.sender,
                        BrokerToWorker::CancelJob(*jid),
                    );
                }
                retain
            });
        }
        self.worker_heap.rebuild(&mut self.workers);
        self.possibly_start_jobs(HashSet::default());
    }

    pub fn receive_job_request_from_client(
        &mut self,
        artifact_gatherer: &mut ArtifactGathererT,
        cid: ClientId,
        cjid: ClientJobId,
        spec: JobSpec,
    ) {
        let layers = spec.layers.clone();
        let priority = spec.priority;
        let estimated_duration = spec.estimated_duration;

        let client = self.clients.0.get_mut(&cid).unwrap();
        let jid = JobId { cid, cjid };
        client.jobs.insert(cjid, Job::new(spec)).assert_is_none();

        match artifact_gatherer.start_job(jid, layers) {
            StartJob::Ready => {
                self.queued_jobs
                    .push(QueuedJob::new(jid, priority, estimated_duration));
                self.possibly_start_jobs(HashSet::from_iter([jid]));
            }
            StartJob::NotReady => {
                self.deps.send_message_to_client(
                    &mut client.sender,
                    BrokerToClient::JobStatusUpdate(jid.cjid, JobBrokerStatus::WaitingForLayers),
                );
            }
        }
    }

    pub fn receive_worker_connected(
        &mut self,
        id: WorkerId,
        slots: usize,
        sender: DepsT::WorkerSender,
    ) {
        self.workers
            .0
            .insert(id, Worker::new(slots, sender))
            .assert_is_none();
        self.worker_heap.push(&mut self.workers, id);
        self.possibly_start_jobs(HashSet::default());
    }

    pub fn receive_worker_disconnected(&mut self, id: WorkerId) {
        let mut worker = self.workers.0.remove(&id).unwrap();
        self.worker_heap
            .remove(&mut self.workers, worker.heap_index);

        let mut just_enqueued = HashSet::new();
        for jid in worker.pending.drain() {
            let job = self.clients.job_from_jid(jid);
            self.queued_jobs.push(QueuedJob::new(
                jid,
                job.spec.priority,
                job.spec.estimated_duration,
            ));
            just_enqueued.insert(jid);
        }

        self.possibly_start_jobs(just_enqueued);
    }

    pub fn receive_job_response_from_worker(
        &mut self,
        artifact_gatherer: &mut ArtifactGathererT,
        wid: WorkerId,
        jid: JobId,
        result: JobOutcomeResult,
    ) {
        let worker = self.workers.0.get_mut(&wid).unwrap();

        if !worker.pending.remove(&jid) {
            // This indicates that the client isn't around anymore. Just ignore this response from
            // the worker. When the client disconnected, we canceled all of the outstanding
            // requests and updated our version of the worker's pending requests.
            return;
        }

        let client = self.clients.0.get_mut(&jid.cid).unwrap();
        self.deps.send_message_to_client(
            &mut client.sender,
            BrokerToClient::JobResponse(jid.cjid, result),
        );
        client.jobs.remove(&jid.cjid).assert_is_some();
        artifact_gatherer.complete_job(jid);
        client.num_completed_jobs += 1;

        if let Some(QueuedJob { jid, .. }) = self.queued_jobs.pop() {
            // If there are any queued_requests, we can just pop one off of the front of
            // the queue and not have to update the worker's used slot count or position in the
            // workers list.
            let job = self.clients.job_from_jid(jid);
            self.deps.send_message_to_worker(
                &mut worker.sender,
                BrokerToWorker::EnqueueJob(jid, job.spec.clone()),
            );
            worker.pending.insert(jid);
        } else {
            // Since there are no queued_requests, we're going to have to update the
            // worker's position in the workers list.
            let heap_index = worker.heap_index;
            self.worker_heap.sift_up(&mut self.workers, heap_index);
        }
    }

    pub fn receive_job_status_update_from_worker(
        &mut self,
        wid: WorkerId,
        jid: JobId,
        status: JobWorkerStatus,
    ) {
        if !self.workers.0.get(&wid).unwrap().pending.contains(&jid) {
            // This indicates that the client isn't around anymore. Just ignore this status update.
            return;
        }
        let client = self.clients.0.get_mut(&jid.cid).unwrap();
        self.deps.send_message_to_client(
            &mut client.sender,
            BrokerToClient::JobStatusUpdate(jid.cjid, JobBrokerStatus::AtWorker(wid, status)),
        );
    }

    pub fn receive_monitor_connected(&mut self, id: MonitorId, sender: DepsT::MonitorSender) {
        self.monitors.insert(id, sender).assert_is_none();
    }

    pub fn receive_monitor_disconnected(&mut self, id: MonitorId) {
        self.monitors.remove(&id).unwrap();
    }

    pub fn receive_statistics_request_from_monitor(&mut self, mid: MonitorId) {
        let worker_iter = self.workers.0.iter();
        let resp = BrokerToMonitor::StatisticsResponse(BrokerStatistics {
            worker_statistics: worker_iter
                .map(|(id, w)| (*id, WorkerStatistics { slots: w.slots }))
                .collect(),
            job_statistics: self.job_statistics.clone(),
        });
        self.deps
            .send_message_to_monitor(self.monitors.get_mut(&mid).unwrap(), resp);
    }

    pub fn receive_got_artifact(
        &mut self,
        digest: Sha256Digest,
        file: ArtifactGathererT::TempFile,
    ) {
        self.tcp_upload_landing_pad.insert(digest, file);
    }

    pub fn receive_artifact_transferred_from_client(
        &mut self,
        artifact_gatherer: &mut ArtifactGathererT,
        cid: ClientId,
        digest: Sha256Digest,
        location: ArtifactUploadLocation,
    ) {
        let file = (location == ArtifactUploadLocation::TcpUpload)
            .then(|| self.tcp_upload_landing_pad.remove(&digest))
            .flatten();

        let ready = artifact_gatherer.artifact_transferred(cid, digest, file);
        for jid in &ready {
            let job = self.clients.job_from_jid(*jid);
            self.queued_jobs.push(QueuedJob::new(
                *jid,
                job.spec.priority,
                job.spec.estimated_duration,
            ));
        }
        self.possibly_start_jobs(ready);
    }

    pub fn receive_manifest_entry(
        &mut self,
        artifact_gatherer: &mut ArtifactGathererT,
        digest: Sha256Digest,
        jid: JobId,
    ) {
        artifact_gatherer.manifest_read_for_job_entry(&digest, jid);
    }

    pub fn receive_finished_reading_manifest(
        &mut self,
        artifact_gatherer: &mut ArtifactGathererT,
        digest: Sha256Digest,
        jid: JobId,
        result: anyhow::Result<()>,
    ) {
        let job_ready = artifact_gatherer.manifest_read_for_job_complete(digest, jid, result);
        if job_ready {
            let job = self.clients.job_from_jid(jid);
            self.queued_jobs.push(QueuedJob::new(
                jid,
                job.spec.priority,
                job.spec.estimated_duration,
            ));
            self.possibly_start_jobs(HashSet::from_iter([jid]));
        }
    }

    fn sample_job_statistics_for_client(
        &self,
        artifact_gatherer: &mut ArtifactGathererT,
        cid: ClientId,
    ) -> JobStateCounts {
        enum_map! {
            JobState::WaitingForArtifacts => {
                artifact_gatherer.get_waiting_for_artifacts_count(cid)
            }
            JobState::Pending => {
                self
                    .queued_jobs
                    .iter()
                    .filter(|QueuedJob { jid, .. }| jid.cid == cid)
                    .count() as u64
            }
            JobState::Running => {
                self
                    .workers
                    .0
                    .values()
                    .flat_map(|w| w.pending.iter())
                    .filter(|jid| jid.cid == cid)
                    .count() as u64
            }
            JobState::Complete => self.clients.0.get(&cid).unwrap().num_completed_jobs,
        }
    }

    pub fn receive_statistics_heartbeat(&mut self, artifact_gatherer: &mut ArtifactGathererT) {
        let sample = JobStatisticsSample {
            client_to_stats: self
                .clients
                .0
                .keys()
                .map(|&cid| {
                    (
                        cid,
                        self.sample_job_statistics_for_client(artifact_gatherer, cid),
                    )
                })
                .collect(),
        };
        self.job_statistics.insert(sample);
    }
}

/*  _            _
 * | |_ ___  ___| |_ ___
 * | __/ _ \/ __| __/ __|
 * | ||  __/\__ \ |_\__ \
 *  \__\___||___/\__|___/
 *  FIGLET: tests
 */

#[cfg(test)]
mod tests {
    use super::{Message::*, *};
    use crate::{
        cache::SchedulerCache,
        scheduler_task::{artifact_gatherer::Deps as ArtifactGathererDeps, ManifestReadRequest},
    };
    use anyhow::Result;
    use enum_map::enum_map;
    use itertools::Itertools;
    use maelstrom_base::{
        digest, job_spec,
        proto::BrokerToWorker::{self, *},
        tar_digest,
    };
    use maelstrom_test::*;
    use maelstrom_util::cache::{fs::test, GetArtifact};
    use maplit::hashmap;
    use std::{cell::RefCell, path::PathBuf, rc::Rc};

    type MessageM<DepsT, TempFileT> = Message<
        <DepsT as SchedulerDeps>::ClientSender,
        <DepsT as SchedulerDeps>::WorkerSender,
        <DepsT as SchedulerDeps>::MonitorSender,
        <DepsT as ArtifactGathererDeps>::WorkerArtifactFetcherSender,
        TempFileT,
    >;

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        ToClient(ClientId, BrokerToClient),
        ToWorker(WorkerId, BrokerToWorker),
        ToMonitor(MonitorId, BrokerToMonitor),
        ToWorkerArtifactFetcher(u32, Option<(PathBuf, u64)>),
        CacheGetArtifact(JobId, Sha256Digest),
        CacheGotArtifact(Sha256Digest, Option<test::TempFile>),
        CacheDecrementRefcount(Sha256Digest),
        CacheClientDisconnected(ClientId),
        CacheGetArtifactForWorker(Sha256Digest),
        CacheReadArtifact(Sha256Digest),
        ReadManifest(ManifestReadRequest<TestArtifactStream>),
    }

    use TestMessage::*;

    #[derive(Clone)]
    struct TestClientSender(ClientId);
    struct TestWorkerSender(WorkerId);
    struct TestMonitorSender(MonitorId);
    struct TestWorkerArtifactFetcherSender(u32);

    #[derive(Default)]
    struct TestState {
        messages: Vec<TestMessage>,
        get_artifact_returns: HashMap<(JobId, Sha256Digest), Vec<GetArtifact>>,
        got_artifact_returns: HashMap<Sha256Digest, Vec<Result<Vec<JobId>>>>,
        #[allow(clippy::type_complexity)]
        get_artifact_for_worker_returns: HashMap<Sha256Digest, Vec<Option<(PathBuf, u64)>>>,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct TestArtifactStream(Sha256Digest);

    impl SchedulerCache for Rc<RefCell<TestState>> {
        type TempFile = test::TempFile;
        type ArtifactStream = TestArtifactStream;

        fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact {
            self.borrow_mut()
                .messages
                .push(CacheGetArtifact(jid, digest.clone()));
            self.borrow_mut()
                .get_artifact_returns
                .get_mut(&(jid, digest))
                .unwrap()
                .remove(0)
        }

        fn got_artifact(
            &mut self,
            digest: &Sha256Digest,
            file: Option<Self::TempFile>,
        ) -> Result<Vec<JobId>> {
            self.borrow_mut()
                .messages
                .push(CacheGotArtifact(digest.clone(), file));
            self.borrow_mut()
                .got_artifact_returns
                .get_mut(digest)
                .unwrap()
                .remove(0)
        }

        fn decrement_refcount(&mut self, digest: &Sha256Digest) {
            self.borrow_mut()
                .messages
                .push(CacheDecrementRefcount(digest.clone()));
        }

        fn client_disconnected(&mut self, cid: ClientId) {
            self.borrow_mut()
                .messages
                .push(CacheClientDisconnected(cid));
        }

        fn get_artifact_for_worker(&mut self, digest: &Sha256Digest) -> Option<(PathBuf, u64)> {
            self.borrow_mut()
                .messages
                .push(CacheGetArtifactForWorker(digest.clone()));
            self.borrow_mut()
                .get_artifact_for_worker_returns
                .get_mut(digest)
                .unwrap()
                .remove(0)
        }

        fn read_artifact(&mut self, digest: &Sha256Digest) -> TestArtifactStream {
            self.borrow_mut()
                .messages
                .push(CacheReadArtifact(digest.clone()));
            TestArtifactStream(digest.clone())
        }
    }

    impl SchedulerDeps for Rc<RefCell<TestState>> {
        type ClientSender = TestClientSender;
        type WorkerSender = TestWorkerSender;
        type MonitorSender = TestMonitorSender;

        fn send_message_to_client(
            &mut self,
            sender: &mut TestClientSender,
            message: BrokerToClient,
        ) {
            self.borrow_mut().messages.push(ToClient(sender.0, message));
        }

        fn send_message_to_worker(
            &mut self,
            sender: &mut TestWorkerSender,
            message: BrokerToWorker,
        ) {
            self.borrow_mut().messages.push(ToWorker(sender.0, message));
        }

        fn send_message_to_monitor(
            &mut self,
            sender: &mut TestMonitorSender,
            message: BrokerToMonitor,
        ) {
            self.borrow_mut()
                .messages
                .push(ToMonitor(sender.0, message));
        }
    }

    impl ArtifactGathererDeps for Rc<RefCell<TestState>> {
        type ArtifactStream = TestArtifactStream;
        type WorkerArtifactFetcherSender = TestWorkerArtifactFetcherSender;
        type ClientSender = TestClientSender;

        fn send_message_to_manifest_reader(
            &mut self,
            req: ManifestReadRequest<TestArtifactStream>,
        ) {
            self.borrow_mut().messages.push(ReadManifest(req));
        }

        fn send_message_to_worker_artifact_fetcher(
            &mut self,
            sender: &mut TestWorkerArtifactFetcherSender,
            message: Option<(PathBuf, u64)>,
        ) {
            self.borrow_mut()
                .messages
                .push(ToWorkerArtifactFetcher(sender.0, message));
        }

        fn send_message_to_client(
            &mut self,
            sender: &mut TestClientSender,
            message: BrokerToClient,
        ) {
            self.borrow_mut().messages.push(ToClient(sender.0, message));
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        artifact_gatherer: super::super::artifact_gatherer::ArtifactGatherer<
            Rc<RefCell<TestState>>,
            Rc<RefCell<TestState>>,
        >,
        scheduler: Scheduler<
            super::super::artifact_gatherer::ArtifactGatherer<
                Rc<RefCell<TestState>>,
                Rc<RefCell<TestState>>,
            >,
            Rc<RefCell<TestState>>,
        >,
    }

    impl Default for Fixture {
        fn default() -> Self {
            let test_state = Rc::new(RefCell::new(TestState::default()));
            Fixture {
                test_state: test_state.clone(),
                artifact_gatherer: super::super::artifact_gatherer::ArtifactGatherer::new(
                    test_state.clone(),
                    test_state.clone(),
                ),
                scheduler: Scheduler::new(test_state.clone()),
            }
        }
    }

    impl Fixture {
        #[allow(clippy::type_complexity)]
        fn new<const L: usize, const M: usize, const N: usize>(
            get_artifact_returns: [((JobId, Sha256Digest), Vec<GetArtifact>); L],
            got_artifact_returns: [(Sha256Digest, Vec<Result<Vec<JobId>>>); M],
            get_artifact_for_worker_returns: [(Sha256Digest, Vec<Option<(PathBuf, u64)>>); N],
        ) -> Self {
            let result = Self::default();
            result.test_state.borrow_mut().get_artifact_returns =
                HashMap::from(get_artifact_returns);
            result.test_state.borrow_mut().got_artifact_returns =
                HashMap::from(got_artifact_returns);
            result
                .test_state
                .borrow_mut()
                .get_artifact_for_worker_returns = HashMap::from(get_artifact_for_worker_returns);
            result
        }

        fn expect_messages_in_any_order(&mut self, expected: Vec<TestMessage>) {
            let messages = &mut self.test_state.borrow_mut().messages;
            for perm in expected.clone().into_iter().permutations(expected.len()) {
                if perm == *messages {
                    messages.clear();
                    return;
                }
            }
            panic!(
                "Expected messages didn't match actual messages in any order.\n{}",
                colored_diff::PrettyDifference {
                    expected: &format!("{:#?}", expected),
                    actual: &format!("{:#?}", messages)
                }
            );
        }

        fn receive_message(&mut self, msg: MessageM<Rc<RefCell<TestState>>, test::TempFile>) {
            match msg {
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
                Message::ArtifactTransferredFromClient(cid, digest, location) => {
                    self.scheduler.receive_artifact_transferred_from_client(
                        &mut self.artifact_gatherer,
                        cid,
                        digest,
                        location,
                    )
                }
                Message::WorkerConnected(id, slots, sender) => {
                    self.scheduler.receive_worker_connected(id, slots, sender)
                }
                Message::WorkerDisconnected(id) => self.scheduler.receive_worker_disconnected(id),
                Message::JobResponseFromWorker(wid, jid, result) => {
                    self.scheduler.receive_job_response_from_worker(
                        &mut self.artifact_gatherer,
                        wid,
                        jid,
                        result,
                    )
                }
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
                    self.scheduler.receive_got_artifact(digest, file)
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
                Message::GotManifestEntry(entry_digest, jid) => self
                    .scheduler
                    .receive_manifest_entry(&mut self.artifact_gatherer, entry_digest, jid),
                Message::FinishedReadingManifest(digest, jid, result) => {
                    self.scheduler.receive_finished_reading_manifest(
                        &mut self.artifact_gatherer,
                        digest,
                        jid,
                        result,
                    )
                }
            }
        }
    }

    macro_rules! client_sender {
        [$n:expr] => { TestClientSender(cid![$n]) };
    }

    macro_rules! worker_sender {
        [$n:expr] => { TestWorkerSender(wid![$n]) };
    }

    macro_rules! monitor_sender {
        [$n:expr] => { TestMonitorSender(mid![$n]) };
    }

    macro_rules! worker_artifact_fetcher_sender {
        [$n:expr] => { TestWorkerArtifactFetcherSender($n) };
    }

    macro_rules! script_test {
        ($test_name:ident, $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?) => {
            script_test!($test_name, Fixture::default(), $($in_msg => { $($out_msg,)* };)+);
        };
        ($test_name:ident, $fixture_expr:expr, $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?) => {
            #[test]
            fn $test_name() {
                let mut fixture = $fixture_expr;
                $(
                    fixture.receive_message($in_msg);
                    fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
                )+
            }
        };
    }

    script_test! {
        response_from_known_worker_for_unknown_job_ignored,
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        JobResponseFromWorker(wid![1], jid![1], Ok(outcome![1])) => {};
    }

    script_test! {
        response_from_worker_for_disconnected_client_ignored,
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        JobResponseFromWorker(wid![1], jid![1], Ok(outcome![1])) => {};
    }

    script_test! {
        requests_go_to_workers_based_on_subscription_percentage,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![2]), vec![GetArtifact::Success]),
                ((jid![1, 3], digest![3]), vec![GetArtifact::Success]),
                ((jid![1, 4], digest![4]), vec![GetArtifact::Success]),
                ((jid![1, 5], digest![5]), vec![GetArtifact::Success]),
                ((jid![1, 6], digest![6]), vec![GetArtifact::Success]),
                ((jid![1, 7], digest![7]), vec![GetArtifact::Success]),
                ((jid![1, 8], digest![8]), vec![GetArtifact::Success]),
                ((jid![1, 9], digest![9]), vec![GetArtifact::Success]),
                ((jid![1, 10], digest![10]), vec![GetArtifact::Success]),
            ], [], [])
        },
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        WorkerConnected(wid![2], 2, worker_sender![2]) => {};
        WorkerConnected(wid![3], 3, worker_sender![3]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        // 0/2 0/2 0/3
        JobRequestFromClient(cid![1], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec!(1))),
        };

        // 1/2 0/2 0/3
        JobRequestFromClient(cid![1], cjid![2], spec!(2)) => {
            CacheGetArtifact(jid![1, 2], digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec!(2))),
        };

        // 1/2 1/2 0/3
        JobRequestFromClient(cid![1], cjid![3], spec!(3)) => {
            CacheGetArtifact(jid![1, 3], digest![3]),
            ToWorker(wid![3], EnqueueJob(jid![1, 3], spec!(3))),
        };

        // 1/2 1/2 1/3
        JobRequestFromClient(cid![1], cjid![4], spec!(4)) => {
            CacheGetArtifact(jid![1, 4], digest![4]),
            ToWorker(wid![3], EnqueueJob(jid![1, 4], spec!(4))),
        };

        // 1/2 1/2 2/3
        JobRequestFromClient(cid![1], cjid![5], spec!(5)) => {
            CacheGetArtifact(jid![1, 5], digest![5]),
            ToWorker(wid![1], EnqueueJob(jid![1, 5], spec!(5))),
        };

        // 2/2 1/2 2/3
        JobRequestFromClient(cid![1], cjid![6], spec!(6)) => {
            CacheGetArtifact(jid![1, 6], digest![6]),
            ToWorker(wid![2], EnqueueJob(jid![1, 6], spec!(6))),
        };

        // 2/2 2/2 2/3
        JobRequestFromClient(cid![1], cjid![7], spec!(7)) => {
            CacheGetArtifact(jid![1, 7], digest![7]),
            ToWorker(wid![3], EnqueueJob(jid![1, 7], spec!(7))),
        };

        JobResponseFromWorker(wid![1], jid![1, 1], Ok(outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
        };
        JobRequestFromClient(cid![1], cjid![8], spec!(8)) => {
            CacheGetArtifact(jid![1, 8], digest![8]),
            ToWorker(wid![1], EnqueueJob(jid![1, 8], spec!(8))),
        };

        JobResponseFromWorker(wid![2], jid![1, 2], Ok(outcome![2])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], Ok(outcome![2]))),
            CacheDecrementRefcount(digest![2]),
        };
        JobRequestFromClient(cid![1], cjid![9], spec!(9)) => {
            CacheGetArtifact(jid![1, 9], digest![9]),
            ToWorker(wid![2], EnqueueJob(jid![1, 9], spec!(9))),
        };

        JobResponseFromWorker(wid![3], jid![1, 3], Ok(outcome![3])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![3], Ok(outcome![3]))),
            CacheDecrementRefcount(digest![3]),
        };
        JobRequestFromClient(cid![1], cjid![10], spec!(10)) => {
            CacheGetArtifact(jid![1, 10], digest![10]),
            ToWorker(wid![3], EnqueueJob(jid![1, 10], spec!(10))),
        };
    }

    script_test! {
        requests_start_queuing_at_2x_workers_slot_count,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![2]), vec![GetArtifact::Success]),
                ((jid![1, 3], digest![3]), vec![GetArtifact::Success]),
                ((jid![1, 4], digest![4]), vec![GetArtifact::Success]),
                ((jid![1, 5], digest![5]), vec![GetArtifact::Success]),
                ((jid![1, 6], digest![6]), vec![GetArtifact::Success]),
            ], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        // 0/1 0/1
        JobRequestFromClient(cid![1], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec!(1))),
        };

        // 1/1 0/1
        JobRequestFromClient(cid![1], cjid![2], spec!(2)) => {
            CacheGetArtifact(jid![1, 2], digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec!(2))),
        };

        // 1/1 1/1
        JobRequestFromClient(cid![1], cjid![3], spec!(3)) => {
            CacheGetArtifact(jid![1, 3], digest![3]),
            ToWorker(wid![1], EnqueueJob(jid![1, 3], spec!(3))),
        };

        // 2/1 1/1
        JobRequestFromClient(cid![1], cjid![4], spec!(4)) => {
            CacheGetArtifact(jid![1, 4], digest![4]),
            ToWorker(wid![2], EnqueueJob(jid![1, 4], spec!(4))),
        };

        // 2/1 2/1
        JobRequestFromClient(cid![1], cjid![5], spec!(5)) => {
            CacheGetArtifact(jid![1, 5], digest![5]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![5], JobBrokerStatus::WaitingForWorker)),
        };
        JobRequestFromClient(cid![1], cjid![6], spec!(6)) => {
            CacheGetArtifact(jid![1, 6], digest![6]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![6], JobBrokerStatus::WaitingForWorker)),
        };

        // 2/2 1/2
        JobResponseFromWorker(wid![2], jid![1, 2], Ok(outcome![2])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], Ok(outcome![2]))),
            CacheDecrementRefcount(digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 5], spec!(5))),
        };

        // 1/2 2/2
        JobResponseFromWorker(wid![1], jid![1, 1], Ok(outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 6], spec!(6))),
        };
    }

    script_test! {
        requests_get_removed_from_workers_pending_map,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![2]), vec![GetArtifact::Success]),
            ], [], [])
        },

        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        JobRequestFromClient(cid![1], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec!(1))),
        };

        JobRequestFromClient(cid![1], cjid![2], spec!(2)) => {
            CacheGetArtifact(jid![1, 2], digest![2]),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec!(2))),
        };

        JobResponseFromWorker(wid![1], jid![1, 1], Ok(outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
        };

        JobResponseFromWorker(wid![1], jid![1, 2], Ok(outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![2]),
        };

        WorkerDisconnected(wid![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
    }

    script_test! {
        client_disconnects_with_outstanding_work_1,
        {
            Fixture::new([
                ((jid!(1, 1), digest![1]), vec![GetArtifact::Success]),
            ], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        JobRequestFromClient(cid![1], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid!(1, 1), digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec!(1))),
        };

        ClientDisconnected(cid![1]) => {
            ToWorker(wid![1], CancelJob(jid![1, 1])),
            CacheDecrementRefcount(digest![1]),
            CacheClientDisconnected(cid![1]),
        };
    }

    script_test! {
        client_disconnects_with_outstanding_work_2,
        {
            Fixture::new([
                ((jid!(1, 1), digest![1]), vec![GetArtifact::Success]),
                ((jid!(2, 1), digest![2]), vec![GetArtifact::Success]),
                ((jid!(1, 2), digest![3]), vec![GetArtifact::Success]),
            ], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};
        ClientConnected(cid![2], client_sender![2]) => {};

        JobRequestFromClient(cid![1], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid!(1, 1), digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec!(1))),
        };

        JobRequestFromClient(cid![2], cjid![1], spec!(2)) => {
            CacheGetArtifact(jid!(2, 1), digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![2, 1], spec!(2))),
        };

        ClientDisconnected(cid![2]) => {
            ToWorker(wid![2], CancelJob(jid![2, 1])),
            CacheDecrementRefcount(digest![2]),
            CacheClientDisconnected(cid![2]),
        };

        JobRequestFromClient(cid![1], cjid![2], spec!(3)) => {
            CacheGetArtifact(jid!(1, 2), digest![3]),
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec!(3))),
        };
    }

    script_test! {
        client_disconnects_with_outstanding_work_3,
        {
            Fixture::new([
                ((jid!(1, 1), digest![1]), vec![GetArtifact::Success]),
                ((jid!(1, 2), digest![2]), vec![GetArtifact::Success]),
                ((jid!(1, 3), digest![3]), vec![GetArtifact::Success]),
                ((jid!(2, 1), digest![1]), vec![GetArtifact::Success]),
            ], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        JobRequestFromClient(cid![1], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid!(1, 1), digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec!(1))),
        };

        JobRequestFromClient(cid![1], cjid![2], spec!(2)) => {
            CacheGetArtifact(jid!(1, 2), digest![2]),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec!(2))),
        };

        ClientConnected(cid![2], client_sender![2]) => {};
        JobRequestFromClient(cid![2], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid!(2, 1), digest![1]),
            ToClient(cid![2], BrokerToClient::JobStatusUpdate(cjid![1], JobBrokerStatus::WaitingForWorker)),
        };
        JobRequestFromClient(cid![1], cjid![3], spec!(3)) => {
            CacheGetArtifact(jid!(1, 3), digest![3]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![3], JobBrokerStatus::WaitingForWorker)),
        };

        ClientDisconnected(cid![2]) => {
            CacheDecrementRefcount(digest![1]),
            CacheClientDisconnected(cid![2]),
        };

        JobResponseFromWorker(wid![1], jid![1, 1], Ok(outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 3], spec!(3))),
        };
    }

    script_test! {
        client_disconnects_with_outstanding_work_4,
        {
            Fixture::new([
                ((jid!(1, 1), digest![1]), vec![GetArtifact::Success]),
                ((jid!(1, 2), digest![2]), vec![GetArtifact::Success]),
                ((jid!(2, 1), digest![1]), vec![GetArtifact::Success]),
                ((jid!(2, 2), digest![2]), vec![GetArtifact::Success]),
                ((jid!(2, 3), digest![3]), vec![GetArtifact::Success]),
                ((jid!(2, 4), digest![4]), vec![GetArtifact::Success]),
            ], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};
        ClientConnected(cid![2], client_sender![2]) => {};

        JobRequestFromClient(cid![1], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid!(1, 1), digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec!(1))),
        };

        JobRequestFromClient(cid![2], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid!(2, 1), digest![1]),
            ToWorker(wid![2], EnqueueJob(jid![2, 1], spec!(1))),
        };

        JobRequestFromClient(cid![2], cjid![2], spec!(2)) => {
            CacheGetArtifact(jid!(2, 2), digest![2]),
            ToWorker(wid![1], EnqueueJob(jid![2, 2], spec!(2))),
        };

        JobRequestFromClient(cid![2], cjid![3], spec!(3)) => {
            CacheGetArtifact(jid!(2, 3), digest![3]),
            ToWorker(wid![2], EnqueueJob(jid![2, 3], spec!(3))),
        };

        JobRequestFromClient(cid![2], cjid![4], spec!(4)) => {
            CacheGetArtifact(jid!(2, 4), digest![4]),
            ToClient(cid![2], BrokerToClient::JobStatusUpdate(cjid![4], JobBrokerStatus::WaitingForWorker)),
        };
        JobRequestFromClient(cid![1], cjid![2], spec!(2)) => {
            CacheGetArtifact(jid!(1, 2), digest![2]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![2], JobBrokerStatus::WaitingForWorker)),
        };

        ClientDisconnected(cid![2]) => {
            CacheDecrementRefcount(digest![1]),
            CacheDecrementRefcount(digest![2]),
            CacheDecrementRefcount(digest![3]),
            CacheDecrementRefcount(digest![4]),

            ToWorker(wid![2], CancelJob(jid![2, 1])),
            ToWorker(wid![1], CancelJob(jid![2, 2])),
            ToWorker(wid![2], CancelJob(jid![2, 3])),

            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec!(2))),

            CacheClientDisconnected(cid![2]),
        };
    }

    script_test! {
        request_with_layers,
        {
            Fixture::new([
                ((jid![1, 2], digest![42]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![43]), vec![GetArtifact::Wait]),
                ((jid![1, 2], digest![44]), vec![GetArtifact::Get]),
            ], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        JobRequestFromClient(cid![1], cjid![2], job_spec!("1", [tar_digest!(42), tar_digest!(43), tar_digest!(44)])) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            CacheGetArtifact(jid![1, 2], digest![43]),
            CacheGetArtifact(jid![1, 2], digest![44]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![44])),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![2], JobBrokerStatus::WaitingForLayers)),
        };

        ClientDisconnected(cid![1]) => {
            CacheClientDisconnected(cid![1]),
            CacheDecrementRefcount(digest![42]),
        }
    }

    script_test! {
        get_artifact_for_worker_tar,
        {
            Fixture::new([], [], [
                (
                    digest![42],
                    vec![Some(("/a/good/path".into(), 42))],
                ),
            ])
        },
        GetArtifactForWorker(digest![42], worker_artifact_fetcher_sender![1]) => {
            CacheGetArtifactForWorker(digest![42]),
            ToWorkerArtifactFetcher(1, Some(("/a/good/path".into(), 42))),
        }
    }

    script_test! {
        get_artifact_for_worker_manifest,
        {
            Fixture::new([], [], [
                (
                    digest![42],
                    vec![Some(("/a/good/path".into(), 42))]
                ),
            ])
        },
        GetArtifactForWorker(digest![42], worker_artifact_fetcher_sender![1]) => {
            CacheGetArtifactForWorker(digest![42]),
            ToWorkerArtifactFetcher(1, Some(("/a/good/path".into(), 42)))
        }
    }

    script_test! {
        decrement_refcount,
        DecrementRefcount(digest![42]) => {
            CacheDecrementRefcount(digest![42]),
        }
    }

    script_test! {
        job_statistics_waiting_for_artifacts,
        {
            Fixture::new([
                ((jid![1, 1], digest![42]), vec![GetArtifact::Wait]),
            ], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        MonitorConnected(mid![1], monitor_sender![1]) => {};
        JobRequestFromClient(cid![1], cjid![1], job_spec!("1", [tar_digest!(42)])) => {
            CacheGetArtifact(jid![1, 1], digest![42]),
            ToClient(
                cid![1],
                BrokerToClient::JobStatusUpdate(cjid![1], JobBrokerStatus::WaitingForLayers)
            ),
        };
        StatisticsHeartbeat => {};
        StatisticsRequestFromMonitor(mid![1]) => {
            ToMonitor(mid![1], BrokerToMonitor::StatisticsResponse(BrokerStatistics {
                worker_statistics: hashmap! {
                    wid![1] => WorkerStatistics { slots: 2 }
                },
                job_statistics: [JobStatisticsSample {
                    client_to_stats: hashmap! {
                        cid![1] => enum_map! {
                            JobState::WaitingForArtifacts => 1,
                            JobState::Pending => 0,
                            JobState::Running => 0,
                            JobState::Complete => 0,
                        }
                    }
                }].into_iter().collect()
            }))
        }
    }

    script_test! {
        job_statistics_pending,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
            ], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        MonitorConnected(mid![1], monitor_sender![1]) => {};
        JobRequestFromClient(cid![1], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![1], JobBrokerStatus::WaitingForWorker)),
        };
        StatisticsHeartbeat => {};
        StatisticsRequestFromMonitor(mid![1]) => {
            ToMonitor(mid![1], BrokerToMonitor::StatisticsResponse(BrokerStatistics {
                worker_statistics: hashmap!{},
                job_statistics: [JobStatisticsSample {
                    client_to_stats: hashmap! {
                        cid![1] => enum_map! {
                            JobState::WaitingForArtifacts => 0,
                            JobState::Pending => 1,
                            JobState::Running => 0,
                            JobState::Complete => 0,
                        }
                    }
                }].into_iter().collect()
            }))
        }
    }

    script_test! {
        job_statistics_running,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
            ], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        MonitorConnected(mid![1], monitor_sender![1]) => {};
        JobRequestFromClient(cid![1], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec!(1))),
        };
        StatisticsHeartbeat => {};
        StatisticsRequestFromMonitor(mid![1]) => {
            ToMonitor(mid![1], BrokerToMonitor::StatisticsResponse(BrokerStatistics {
                worker_statistics: hashmap! {
                    wid![1] => WorkerStatistics { slots: 2 }
                },
                job_statistics: [JobStatisticsSample {
                    client_to_stats: hashmap! {
                        cid![1] => enum_map! {
                            JobState::WaitingForArtifacts => 0,
                            JobState::Pending => 0,
                            JobState::Running => 1,
                            JobState::Complete => 0,
                        }
                    }
                }].into_iter().collect()
            }))
        }
    }

    script_test! {
        job_statistics_completed,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
            ], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        MonitorConnected(mid![1], monitor_sender![1]) => {};
        JobRequestFromClient(cid![1], cjid![1], spec!(1)) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec!(1))),
        };
        JobResponseFromWorker(wid![1], jid![1, 1], Ok(outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
        };
        StatisticsHeartbeat => {};
        StatisticsRequestFromMonitor(mid![1]) => {
            ToMonitor(mid![1], BrokerToMonitor::StatisticsResponse(BrokerStatistics {
                worker_statistics: hashmap! {
                    wid![1] => WorkerStatistics { slots: 2 }
                },
                job_statistics: [JobStatisticsSample {
                    client_to_stats: hashmap! {
                        cid![1] => enum_map! {
                            JobState::WaitingForArtifacts => 0,
                            JobState::Pending => 0,
                            JobState::Running => 0,
                            JobState::Complete => 1,
                        }
                    }
                }].into_iter().collect()
            }))
        }
    }

    script_test! {
        forward_job_status_update,
        {
            Fixture::new([
                ((jid![1, 3], digest![1]), vec![GetArtifact::Success]),
            ], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        JobRequestFromClient(cid![1], cjid![3], spec!(1)) => {
            CacheGetArtifact(jid![1, 3], digest![1]),
            ToWorker(wid![2], EnqueueJob(jid![1, 3], spec!(1))),
        };
        JobStatusUpdateFromWorker(wid![2], jid![1, 3], JobWorkerStatus::WaitingForLayers) => {
            ToClient(
                cid![1],
                BrokerToClient::JobStatusUpdate(
                    cjid![3],
                    JobBrokerStatus::AtWorker(wid![2], JobWorkerStatus::WaitingForLayers)
                )
            )
        };
        JobStatusUpdateFromWorker(wid![2], jid![1, 3], JobWorkerStatus::WaitingToExecute) => {
            ToClient(
                cid![1],
                BrokerToClient::JobStatusUpdate(
                    cjid![3],
                    JobBrokerStatus::AtWorker(wid![2], JobWorkerStatus::WaitingToExecute)
                )
            )
        };
        JobStatusUpdateFromWorker(wid![2], jid![1, 3], JobWorkerStatus::Executing) => {
            ToClient(
                cid![1],
                BrokerToClient::JobStatusUpdate(
                    cjid![3],
                    JobBrokerStatus::AtWorker(wid![2], JobWorkerStatus::Executing)
                )
            )
        };
    }

    script_test! {
        drop_unknown_job_status_update,
        {
            Fixture::new([], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        JobStatusUpdateFromWorker(wid![1], jid![2, 3], JobWorkerStatus::WaitingForLayers) => {};
        JobStatusUpdateFromWorker(wid![1], jid![2, 3], JobWorkerStatus::WaitingToExecute) => {};
        JobStatusUpdateFromWorker(wid![1], jid![2, 3], JobWorkerStatus::Executing) => {};
    }
}

#[cfg(test)]
mod tests2 {
    use super::*;
    use maelstrom_base::{job_spec, nonempty, tar_digest};
    use maelstrom_test::{cid, cjid, jid, mid, outcome, wid};
    use std::{
        cell::RefCell,
        ops::{Deref, DerefMut},
        rc::Rc,
    };

    #[derive(Clone, Debug)]
    struct TestClientSender(ClientId);

    #[derive(Debug)]
    struct TestWorkerSender(WorkerId);

    #[derive(Debug)]
    struct TestMonitorSender(MonitorId);

    macro_rules! client_sender {
        [$n:expr] => { TestClientSender(cid![$n]) };
    }

    macro_rules! worker_sender {
        [$n:expr] => { TestWorkerSender(wid![$n]) };
    }

    macro_rules! monitor_sender {
        [$n:expr] => { TestMonitorSender(mid![$n]) };
    }

    #[derive(Default)]
    struct Mock {
        client_connected: Vec<ClientId>,
        client_disconnected: Vec<ClientId>,
        start_job: Vec<(JobId, NonEmpty<(Sha256Digest, ArtifactType)>, StartJob)>,
        complete_job: Vec<JobId>,
        send_message_to_client: Vec<(ClientId, BrokerToClient)>,
        send_message_to_worker: Vec<(WorkerId, BrokerToWorker)>,
        send_message_to_monitor: Vec<(MonitorId, BrokerToMonitor)>,
    }

    impl ArtifactGatherer for Rc<RefCell<Mock>> {
        type TempFile = String;
        type ClientSender = TestClientSender;
        fn client_connected(&mut self, cid: ClientId, sender: TestClientSender) {
            assert_eq!(sender.0, cid);
            let client_connected = &mut self.borrow_mut().client_connected;
            let index = client_connected
                .iter()
                .position(|e| *e == cid)
                .expect(&format!(
                    "sending unexpected client_connected to artifact gatherer for client {cid}"
                ));
            client_connected.remove(index);
        }
        fn client_disconnected(&mut self, cid: ClientId) {
            let client_disconnected = &mut self.borrow_mut().client_disconnected;
            let index = client_disconnected
                .iter()
                .position(|e| *e == cid)
                .expect(&format!(
                    "sending unexpected client_disconnected to artifact gatherer for client {cid}"
                ));
            client_disconnected.remove(index);
        }
        fn start_job(
            &mut self,
            jid: JobId,
            layers: NonEmpty<(Sha256Digest, ArtifactType)>,
        ) -> StartJob {
            let start_job = &mut self.borrow_mut().start_job;
            let index = start_job
                .iter()
                .position(|e| e.0 == jid && e.1 == layers)
                .expect(&format!(
                    "sending unexpected start_job to artifact gatherer for job {jid}: {layers:#?}"
                ));
            start_job.remove(index).2
        }
        fn artifact_transferred(
            &mut self,
            cid: ClientId,
            digest: Sha256Digest,
            file: Option<Self::TempFile>,
        ) -> HashSet<JobId> {
            todo!("{cid} {digest:?} {file:?}");
        }
        fn manifest_read_for_job_entry(&mut self, digest: &Sha256Digest, jid: JobId) {
            todo!("{digest:?} {jid:?}");
        }
        fn manifest_read_for_job_complete(
            &mut self,
            digest: Sha256Digest,
            jid: JobId,
            result: anyhow::Result<()>,
        ) -> bool {
            todo!("{digest:?} {jid:?} {result:?}");
        }
        fn complete_job(&mut self, jid: JobId) {
            let complete_job = &mut self.borrow_mut().complete_job;
            let index = complete_job.iter().position(|e| *e == jid).expect(&format!(
                "sending unexpected complete_job to artifact gatherer for job {jid}"
            ));
            complete_job.remove(index);
        }
        fn get_waiting_for_artifacts_count(&self, cid: ClientId) -> u64 {
            todo!("{cid:?}");
        }
    }

    impl SchedulerDeps for Rc<RefCell<Mock>> {
        type ClientSender = TestClientSender;
        type WorkerSender = TestWorkerSender;
        type MonitorSender = TestMonitorSender;

        fn send_message_to_client(
            &mut self,
            sender: &mut Self::ClientSender,
            message: BrokerToClient,
        ) {
            let send_message_to_client = &mut self.borrow_mut().send_message_to_client;
            let index = send_message_to_client
                .iter()
                .position(|e| e.0 == sender.0 && e.1 == message)
                .expect(&format!(
                    "sending unexpected message to client {sender:?}: {message:#?}"
                ));
            send_message_to_client.remove(index);
        }

        fn send_message_to_worker(
            &mut self,
            sender: &mut Self::WorkerSender,
            message: BrokerToWorker,
        ) {
            let send_message_to_worker = &mut self.borrow_mut().send_message_to_worker;
            let index = send_message_to_worker
                .iter()
                .position(|e| e.0 == sender.0 && e.1 == message)
                .expect(&format!(
                    "sending unexpected message to worker {sender:?}: {message:#?}"
                ));
            send_message_to_worker.remove(index);
        }

        fn send_message_to_monitor(
            &mut self,
            sender: &mut Self::MonitorSender,
            message: BrokerToMonitor,
        ) {
            let send_message_to_monitor = &mut self.borrow_mut().send_message_to_monitor;
            let index = send_message_to_monitor
                .iter()
                .position(|e| e.0 == sender.0 && e.1 == message)
                .expect(&format!(
                    "sending unexpected message to monitor {sender:?}: {message:#?}"
                ));
            send_message_to_monitor.remove(index);
        }
    }

    struct Fixture {
        mock: Rc<RefCell<Mock>>,
        sut: Scheduler<Rc<RefCell<Mock>>, Rc<RefCell<Mock>>>,
    }

    struct Expect<'a> {
        fixture: &'a mut Fixture,
    }

    impl<'a> Expect<'a> {
        fn client_connected(self, cid: ClientId) -> Self {
            self.fixture.mock.borrow_mut().client_connected.push(cid);
            self
        }

        /*
        fn client_disconnected(self, cid: ClientId) -> Self {
            self.fixture.mock.borrow_mut().client_disconnected.push(cid);
            self
        }
        */

        fn start_job(
            self,
            jid: JobId,
            layers: NonEmpty<(Sha256Digest, ArtifactType)>,
            result: StartJob,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .start_job
                .push((jid, layers, result));
            self
        }

        /*
        fn artifact_transferred(
            self,
            digest: Sha256Digest,
            file: Option<String>,
            result: Result<HashSet<JobId>>,
        ) -> Self {
            todo!();
        }
        */

        /*
        fn manifest_read_for_job_entry(
            self,
            digest: Sha256Digest,
            jid: JobId,
            job_ready: bool,
        ) -> Self {
            todo!();
        }

        fn manifest_read_for_job_complete(
            self,
            digest: Sha256Digest,
            jid: JobId,
            result: anyhow::Result<()>,
            job_ready: bool,
        ) -> Self {
            todo!();
        }
        */

        fn complete_job(self, jid: JobId) -> Self {
            self.fixture.mock.borrow_mut().complete_job.push(jid);
            self
        }

        /*
        fn get_artifact_for_worker(
            self,
            digest: Sha256Digest,
            result: Option<(PathBuf, u64)>,
        ) -> Self {
            todo!();
        }

        fn receive_decrement_refcount(self, digest: Sha256Digest) -> Self {
            todo!();
        }

        fn get_waiting_for_artifacts_count(&self, cid: ClientId, count: u64) -> Self {
            todo!();
        }
        */

        fn send_message_to_client(self, sender: TestClientSender, message: BrokerToClient) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_message_to_client
                .push((sender.0, message));
            self
        }

        fn send_message_to_worker(self, sender: TestWorkerSender, message: BrokerToWorker) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_message_to_worker
                .push((sender.0, message));
            self
        }

        /*
        fn send_message_to_monitor(
            self,
            sender: TestMonitorSender,
            message: BrokerToMonitor,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_message_to_monitor
                .push((sender.0, message));
            self
        }
        */

        /*
        fn send_message_to_worker_artifact_fetcher(
            self,
            sender: TestWorkerArtifactFetcherSender,
            message: Option<(PathBuf, u64)>,
        ) -> Self {
            todo!();
        }
        */

        fn when(self) -> When<'a> {
            let Expect { fixture } = self;
            When { fixture }
        }
    }

    struct When<'a> {
        fixture: &'a mut Fixture,
    }

    impl<'a> Deref for When<'a> {
        type Target = Fixture;

        fn deref(&self) -> &Self::Target {
            &self.fixture
        }
    }

    impl<'a> DerefMut for When<'a> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.fixture
        }
    }

    impl<'a> Drop for When<'a> {
        fn drop(&mut self) {
            let mock = self.fixture.mock.borrow();
            assert!(mock.client_connected.is_empty());
            assert!(mock.client_disconnected.is_empty());
            assert!(mock.start_job.is_empty());
            assert!(mock.complete_job.is_empty());
            assert!(mock.send_message_to_client.is_empty());
            assert!(mock.send_message_to_worker.is_empty());
            assert!(mock.send_message_to_monitor.is_empty());
        }
    }

    impl Fixture {
        fn new() -> Self {
            let mock = Rc::new(RefCell::new(Default::default()));
            let sut = Scheduler::new(mock.clone());
            Self { mock, sut }
        }

        fn expect(&mut self) -> Expect {
            Expect { fixture: self }
        }

        fn with_client(mut self, cid: ClientId) -> Self {
            self.expect()
                .client_connected(cid)
                .when()
                .receive_client_connected(cid, TestClientSender(cid));
            self
        }

        fn with_worker(mut self, wid: WorkerId, slots: usize) -> Self {
            self.receive_worker_connected(wid, slots, TestWorkerSender(wid));
            self
        }

        fn receive_client_connected(&mut self, cid: ClientId, sender: TestClientSender) {
            self.sut
                .receive_client_connected(&mut self.mock, cid, sender);
        }

        fn receive_client_disconnected(&mut self, cid: ClientId) {
            self.sut.receive_client_disconnected(&mut self.mock, cid);
        }

        fn receive_job_request_from_client(
            &mut self,
            cid: ClientId,
            cjid: ClientJobId,
            spec: JobSpec,
        ) {
            self.sut
                .receive_job_request_from_client(&mut self.mock, cid, cjid, spec);
        }

        fn receive_worker_connected(
            &mut self,
            wid: WorkerId,
            slots: usize,
            sender: TestWorkerSender,
        ) {
            self.sut.receive_worker_connected(wid, slots, sender);
        }

        fn receive_worker_disconnected(&mut self, wid: WorkerId) {
            self.sut.receive_worker_disconnected(wid);
        }

        fn receive_job_response_from_worker(
            &mut self,
            wid: WorkerId,
            jid: JobId,
            result: JobOutcomeResult,
        ) {
            self.sut
                .receive_job_response_from_worker(&mut self.mock, wid, jid, result);
        }

        fn receive_job_status_update_from_worker(
            &mut self,
            wid: WorkerId,
            jid: JobId,
            status: JobWorkerStatus,
        ) {
            self.sut
                .receive_job_status_update_from_worker(wid, jid, status);
        }

        fn receive_monitor_connected(&mut self, mid: MonitorId, sender: TestMonitorSender) {
            self.sut.receive_monitor_connected(mid, sender);
        }

        fn receive_monitor_disconnected(&mut self, mid: MonitorId) {
            self.sut.receive_monitor_disconnected(mid);
        }

        fn receive_statistics_request_from_monitor(&mut self, mid: MonitorId) {
            self.sut.receive_statistics_request_from_monitor(mid);
        }
    }

    #[test]
    #[should_panic]
    fn message_from_unknown_client_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_job_request_from_client(
            cid!(1),
            cjid!(1),
            job_spec!("test", [tar_digest!(1)]),
        );
    }

    #[test]
    #[should_panic]
    fn disconnect_from_unknown_client_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_client_disconnected(cid!(1));
    }

    #[test]
    #[should_panic]
    fn connect_from_duplicate_client_panics() {
        Fixture::new().with_client(cid!(1)).with_client(cid!(1));
    }

    #[test]
    #[should_panic]
    fn message_from_unknown_worker_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_job_status_update_from_worker(wid!(1), jid!(1), JobWorkerStatus::Executing);
    }

    #[test]
    #[should_panic]
    fn disconnect_from_unknown_worker_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_worker_disconnected(wid!(1));
    }

    #[test]
    #[should_panic]
    fn connect_from_duplicate_worker_panics() {
        Fixture::new()
            .with_worker(wid!(1), 2)
            .with_worker(wid!(1), 2);
    }

    #[test]
    #[should_panic]
    fn message_from_unknown_monitor_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_statistics_request_from_monitor(mid!(1));
    }

    #[test]
    #[should_panic]
    fn disconnect_from_unknown_monitor_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_monitor_disconnected(mid!(1));
    }

    #[test]
    #[should_panic]
    fn connect_from_duplicate_monitor_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_monitor_connected(mid!(1), monitor_sender!(1));
        fixture.receive_monitor_connected(mid!(1), monitor_sender!(1));
    }

    #[test]
    fn job_request_ready_worker_available() {
        let mut fixture = Fixture::new().with_client(cid!(1)).with_worker(wid!(1), 2);

        let job_spec = job_spec!("test", [tar_digest!(1), tar_digest!(2)]);

        fixture
            .expect()
            .start_job(
                jid!(1),
                nonempty![tar_digest!(1), tar_digest!(2)],
                StartJob::Ready,
            )
            .send_message_to_worker(
                worker_sender!(1),
                BrokerToWorker::EnqueueJob(jid!(1), job_spec.clone()),
            )
            .when()
            .receive_job_request_from_client(cid!(1), cjid!(1), job_spec);
    }

    #[test]
    fn job_request_ready_no_worker_available() {
        let mut fixture = Fixture::new().with_client(cid!(1));
        let job_spec = job_spec!("test", [tar_digest!(1), tar_digest!(2)]);

        fixture
            .expect()
            .start_job(
                jid!(1),
                nonempty![tar_digest!(1), tar_digest!(2)],
                StartJob::Ready,
            )
            .send_message_to_client(
                client_sender!(1),
                BrokerToClient::JobStatusUpdate(cjid!(1), JobBrokerStatus::WaitingForWorker),
            )
            .when()
            .receive_job_request_from_client(cid!(1), cjid!(1), job_spec.clone());

        fixture
            .expect()
            .send_message_to_worker(
                worker_sender!(1),
                BrokerToWorker::EnqueueJob(jid!(1), job_spec),
            )
            .when()
            .receive_worker_connected(wid!(1), 2, TestWorkerSender(wid!(1)));
    }

    #[test]
    fn job_request_not_ready_start_fetching_some() {
        let mut fixture = Fixture::new().with_client(cid!(1)).with_worker(wid!(1), 2);

        fixture
            .expect()
            .start_job(
                jid!(1),
                nonempty![tar_digest!(1), tar_digest!(2)],
                StartJob::NotReady,
            )
            .send_message_to_client(
                client_sender!(1),
                BrokerToClient::JobStatusUpdate(cjid!(1), JobBrokerStatus::WaitingForLayers),
            )
            .when()
            .receive_job_request_from_client(
                cid!(1),
                cjid!(1),
                job_spec!("test", [tar_digest!(1), tar_digest!(2)]),
            );
    }

    #[test]
    fn job_request_not_ready_start_fetching_none() {
        let mut fixture = Fixture::new().with_client(cid!(1)).with_worker(wid!(1), 2);

        fixture
            .expect()
            .start_job(
                jid!(1),
                nonempty![tar_digest!(1), tar_digest!(2)],
                StartJob::NotReady,
            )
            .send_message_to_client(
                client_sender!(1),
                BrokerToClient::JobStatusUpdate(cjid!(1), JobBrokerStatus::WaitingForLayers),
            )
            .when()
            .receive_job_request_from_client(
                cid!(1),
                cjid!(1),
                job_spec!("test", [tar_digest!(1), tar_digest!(2)]),
            );
    }

    #[test]
    fn worker_disconnected_with_outstanding_jobs_no_other_available_workers() {
        let mut fixture = Fixture::new().with_client(cid!(1)).with_worker(wid!(1), 2);
        let job_spec = job_spec!("test", [tar_digest!(1), tar_digest!(2)]);

        fixture
            .expect()
            .start_job(
                jid!(1),
                nonempty![tar_digest!(1), tar_digest!(2)],
                StartJob::Ready,
            )
            .send_message_to_worker(
                worker_sender!(1),
                BrokerToWorker::EnqueueJob(jid!(1), job_spec.clone()),
            )
            .when()
            .receive_job_request_from_client(cid!(1), cjid!(1), job_spec.clone());

        fixture
            .expect()
            .send_message_to_client(
                client_sender!(1),
                BrokerToClient::JobStatusUpdate(cjid!(1), JobBrokerStatus::WaitingForWorker),
            )
            .when()
            .receive_worker_disconnected(wid!(1));

        fixture
            .expect()
            .send_message_to_worker(
                worker_sender!(2),
                BrokerToWorker::EnqueueJob(jid!(1), job_spec),
            )
            .when()
            .receive_worker_connected(wid!(2), 2, TestWorkerSender(wid!(2)));
    }

    #[test]
    fn worker_disconnected_with_outstanding_jobs_with_other_available_workers() {
        let mut fixture = Fixture::new()
            .with_client(cid!(1))
            .with_worker(wid!(1), 2)
            .with_worker(wid!(2), 2);
        let job_spec = job_spec!("test", [tar_digest!(1), tar_digest!(2)]);

        fixture
            .expect()
            .start_job(
                jid!(1),
                nonempty![tar_digest!(1), tar_digest!(2)],
                StartJob::Ready,
            )
            .send_message_to_worker(
                worker_sender!(1),
                BrokerToWorker::EnqueueJob(jid!(1), job_spec.clone()),
            )
            .when()
            .receive_job_request_from_client(cid!(1), cjid!(1), job_spec.clone());

        fixture
            .expect()
            .send_message_to_worker(
                worker_sender!(2),
                BrokerToWorker::EnqueueJob(jid!(1), job_spec),
            )
            .when()
            .receive_worker_disconnected(wid!(1));
    }

    #[test]
    fn worker_response_with_no_other_available_jobs() {
        let mut fixture = Fixture::new().with_client(cid!(1)).with_worker(wid!(1), 2);
        let job_spec = job_spec!("test", [tar_digest!(1), tar_digest!(2)]);
        let result = Ok(outcome!(1));

        fixture
            .expect()
            .start_job(
                jid!(1),
                nonempty![tar_digest!(1), tar_digest!(2)],
                StartJob::Ready,
            )
            .send_message_to_worker(
                worker_sender!(1),
                BrokerToWorker::EnqueueJob(jid!(1), job_spec.clone()),
            )
            .when()
            .receive_job_request_from_client(cid!(1), cjid!(1), job_spec.clone());

        fixture
            .expect()
            .complete_job(jid!(1))
            .send_message_to_client(
                client_sender!(1),
                BrokerToClient::JobResponse(cjid!(1), result.clone()),
            )
            .when()
            .receive_job_response_from_worker(wid!(1), jid!(1), result.clone());
    }

    #[test]
    fn worker_response_with_other_available_jobs() {
        let mut fixture = Fixture::new().with_client(cid!(1)).with_worker(wid!(1), 1);
        let job_spec_1 = job_spec!("test_1", [tar_digest!(1), tar_digest!(2)]);
        let job_spec_2 = job_spec!("test_2", [tar_digest!(1)]);
        let job_spec_3 = job_spec!("test_3", [tar_digest!(2)]);
        let result_1 = Ok(outcome!(1));

        fixture
            .expect()
            .start_job(
                jid!(1, 1),
                nonempty![tar_digest!(1), tar_digest!(2)],
                StartJob::Ready,
            )
            .send_message_to_worker(
                worker_sender!(1),
                BrokerToWorker::EnqueueJob(jid!(1, 1), job_spec_1.clone()),
            )
            .when()
            .receive_job_request_from_client(cid!(1), cjid!(1), job_spec_1);

        fixture
            .expect()
            .start_job(jid!(1, 2), nonempty![tar_digest!(1)], StartJob::Ready)
            .send_message_to_worker(
                worker_sender!(1),
                BrokerToWorker::EnqueueJob(jid!(1, 2), job_spec_2.clone()),
            )
            .when()
            .receive_job_request_from_client(cid!(1), cjid!(2), job_spec_2);

        fixture
            .expect()
            .start_job(jid!(1, 3), nonempty![tar_digest!(2)], StartJob::Ready)
            .send_message_to_client(
                client_sender!(1),
                BrokerToClient::JobStatusUpdate(cjid!(3), JobBrokerStatus::WaitingForWorker),
            )
            .when()
            .receive_job_request_from_client(cid!(1), cjid!(3), job_spec_3.clone());

        fixture
            .expect()
            .complete_job(jid!(1, 1))
            .send_message_to_client(
                client_sender!(1),
                BrokerToClient::JobResponse(cjid!(1), result_1.clone()),
            )
            .send_message_to_worker(
                worker_sender!(1),
                BrokerToWorker::EnqueueJob(jid!(1, 3), job_spec_3),
            )
            .when()
            .receive_job_response_from_worker(wid!(1), jid!(1), result_1);
    }
}
