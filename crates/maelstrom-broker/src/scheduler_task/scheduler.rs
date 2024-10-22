//! Central processing module for the broker. Receives and sends messages to and from clients and
//! workers.

use anyhow::Result;
use maelstrom_base::{
    manifest::{ManifestEntry, ManifestEntryData, ManifestFileData},
    proto::{
        BrokerToClient, BrokerToMonitor, BrokerToWorker, ClientToBroker, MonitorToBroker,
        WorkerToBroker,
    },
    stats::{
        BrokerStatistics, JobState, JobStateCounts, JobStatisticsSample, JobStatisticsTimeSeries,
        WorkerStatistics,
    },
    ArtifactType, ClientId, ClientJobId, JobBrokerStatus, JobId, JobOutcomeResult, JobSpec,
    JobWorkerStatus, MonitorId, Sha256Digest, WorkerId,
};
use maelstrom_util::{
    cache::{
        fs::{Fs, TempFile},
        Cache, GetArtifact, GetStrategy, GotArtifact, KeyKind,
    },
    duration,
    ext::{BoolExt as _, OptionExt as _},
    heap::{Heap, HeapDeps, HeapIndex},
};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    error,
    fmt::{self, Debug, Formatter},
    path::{Path, PathBuf},
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

/// The external dependencies for [`Scheduler`]. All of these methods must be asynchronous: they
/// must not block the current thread.
pub trait SchedulerDeps {
    type ClientSender;
    type WorkerSender;
    type MonitorSender;
    type WorkerArtifactFetcherSender;
    type ManifestError: error::Error + Send + Sync + 'static;
    type ManifestIterator: Iterator<Item = Result<ManifestEntry, Self::ManifestError>>;
    fn send_message_to_client(&mut self, sender: &mut Self::ClientSender, message: BrokerToClient);
    fn send_message_to_worker(&mut self, sender: &mut Self::WorkerSender, message: BrokerToWorker);
    fn send_message_to_monitor(
        &mut self,
        sender: &mut Self::MonitorSender,
        message: BrokerToMonitor,
    );
    fn send_message_to_worker_artifact_fetcher(
        &mut self,
        sender: &mut Self::WorkerArtifactFetcherSender,
        message: Option<(PathBuf, u64)>,
    );
    fn read_manifest(&mut self, path: &Path)
        -> Result<Self::ManifestIterator, Self::ManifestError>;
}

#[derive(Clone, Copy, Debug, strum::Display, Eq, Hash, PartialEq, strum::EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum BrokerKeyKind {
    Blob,
}

impl KeyKind for BrokerKeyKind {
    type Iterator = <Self as strum::IntoEnumIterator>::Iterator;

    fn iter() -> Self::Iterator {
        <Self as strum::IntoEnumIterator>::iter()
    }
}

pub enum BrokerGetStrategy {}

impl GetStrategy for BrokerGetStrategy {
    type Getter = ClientId;
    fn getter_from_job_id(jid: JobId) -> Self::Getter {
        jid.cid
    }
}

/// The required interface for the cache that is provided to the [`Scheduler`]. This mirrors the
/// API for [`Cache`]. We keep them separate so that we can test the [`Scheduler`] more easily.
///
/// Unlike with [`SchedulerDeps`], all of these functions are immediate. In production, the
/// [`Cache`] is owned by the [`Scheduler`] and the two live on the same task. So these methods
/// sometimes return actual values which can be handled immediately, unlike [`SchedulerDeps`].
pub trait SchedulerCache {
    type TempFile: TempFile;

    /// See [`Cache::get_artifact`].
    fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact;

    /// See [`Cache::got_artifact`].
    fn got_artifact(&mut self, digest: Sha256Digest, file: Self::TempFile) -> Vec<JobId>;

    /// See [`Cache::decrement_refcount`].
    fn decrement_refcount(&mut self, digest: Sha256Digest);

    /// See [`Cache::client_disconnected`].
    fn client_disconnected(&mut self, cid: ClientId);

    /// See [`Cache::get_artifact_for_worker`].
    fn get_artifact_for_worker(&mut self, digest: &Sha256Digest) -> Option<(PathBuf, u64)>;

    /// See [`Cache::cache_path`].
    fn cache_path(&self, digest: &Sha256Digest) -> PathBuf;
}

impl<FsT: Fs> SchedulerCache for Cache<FsT, BrokerKeyKind, BrokerGetStrategy> {
    type TempFile = FsT::TempFile;

    fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact {
        self.get_artifact(BrokerKeyKind::Blob, digest, jid)
    }

    fn got_artifact(&mut self, digest: Sha256Digest, file: FsT::TempFile) -> Vec<JobId> {
        self.got_artifact_success(BrokerKeyKind::Blob, &digest, GotArtifact::file(file))
            .map_err(|(err, _)| err)
            .unwrap()
    }

    fn decrement_refcount(&mut self, digest: Sha256Digest) {
        self.decrement_ref_count(BrokerKeyKind::Blob, &digest)
    }

    fn client_disconnected(&mut self, cid: ClientId) {
        self.getter_disconnected(cid)
    }

    fn get_artifact_for_worker(&mut self, digest: &Sha256Digest) -> Option<(PathBuf, u64)> {
        let cache_path = self.cache_path(BrokerKeyKind::Blob, digest).into_path_buf();
        self.try_increment_ref_count(BrokerKeyKind::Blob, digest)
            .map(|size| (cache_path, size))
    }

    fn cache_path(&self, digest: &Sha256Digest) -> PathBuf {
        self.cache_path(BrokerKeyKind::Blob, digest).into_path_buf()
    }
}

/// The incoming messages, or events, for [`Scheduler`].
///
/// If [`Scheduler`] weren't implement as an async state machine, these would be its methods.
pub enum Message<DepsT: SchedulerDeps, TempFileT: TempFile> {
    /// The given client connected, and messages can be sent to it on the given sender.
    ClientConnected(ClientId, DepsT::ClientSender),

    /// The given client disconnected.
    ClientDisconnected(ClientId),

    /// The given client has sent us the given message.
    FromClient(ClientId, ClientToBroker),

    /// The given worker connected. It has the given number of slots and messages can be sent to it
    /// on the given sender.
    WorkerConnected(WorkerId, usize, DepsT::WorkerSender),

    /// The given worker disconnected.
    WorkerDisconnected(WorkerId),

    /// The given worker has sent us the given message.
    FromWorker(WorkerId, WorkerToBroker),

    /// The given monitor connected, and messages can be sent to it on the given sender.
    MonitorConnected(MonitorId, DepsT::MonitorSender),

    /// The given monitor disconnected.
    MonitorDisconnected(MonitorId),

    /// The given client has sent us the given message.
    FromMonitor(MonitorId, MonitorToBroker),

    /// An artifact has been pushed to us. The artifact has the given digest and length. It is
    /// temporarily stored at the given path.
    GotArtifact(Sha256Digest, TempFileT),

    /// A worker has requested the given artifact be sent to it over the given sender. After the
    /// contents are sent to the worker, the refcount needs to be decremented with a
    /// [`Message::DecrementRefcount`] message.
    GetArtifactForWorker(Sha256Digest, DepsT::WorkerArtifactFetcherSender),

    /// A worker has been sent an artifact, and we can now release the refcount that was keeping
    /// the artifact from being removed while being transferred.
    DecrementRefcount(Sha256Digest),

    /// The stats heartbeat task has decided it's time to take another statistics sample.
    StatisticsHeartbeat,
}

impl<DepsT: SchedulerDeps, TempFileT: TempFile> Debug for Message<DepsT, TempFileT> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Message::ClientConnected(cid, _sender) => {
                f.debug_tuple("ClientConnected").field(cid).finish()
            }
            Message::ClientDisconnected(cid) => {
                f.debug_tuple("ClientDisconnected").field(cid).finish()
            }
            Message::FromClient(cid, msg) => {
                f.debug_tuple("FromClient").field(cid).field(msg).finish()
            }
            Message::WorkerConnected(wid, slots, _sender) => f
                .debug_tuple("WorkerConnected")
                .field(wid)
                .field(slots)
                .finish(),
            Message::WorkerDisconnected(wid) => {
                f.debug_tuple("WorkerDisconnected").field(wid).finish()
            }
            Message::FromWorker(wid, msg) => {
                f.debug_tuple("FromWorker").field(wid).field(msg).finish()
            }
            Message::MonitorConnected(mid, _sender) => {
                f.debug_tuple("MonitorConnected").field(mid).finish()
            }
            Message::MonitorDisconnected(mid) => {
                f.debug_tuple("MonitorDisconnected").field(mid).finish()
            }
            Message::FromMonitor(mid, msg) => {
                f.debug_tuple("FromMonitor").field(mid).field(msg).finish()
            }
            Message::GotArtifact(digest, file) => f
                .debug_tuple("GotArtifact")
                .field(digest)
                .field(file)
                .finish(),
            Message::GetArtifactForWorker(digest, _sender) => {
                f.debug_tuple("GetArtifactForWorker").field(digest).finish()
            }
            Message::DecrementRefcount(digest) => {
                f.debug_tuple("DecrementRefcount").field(digest).finish()
            }
            Message::StatisticsHeartbeat => f.debug_tuple("StatisticsHeartbeat").finish(),
        }
    }
}

impl<CacheT: SchedulerCache, DepsT: SchedulerDeps> Scheduler<CacheT, DepsT> {
    /// Create a new scheduler with the given [`SchedulerCache`]. Note that [`SchedulerDeps`] are
    /// passed in to `Self::receive_message`.
    pub fn new(cache: CacheT) -> Self {
        Scheduler {
            cache,
            clients: ClientMap(HashMap::default()),
            workers: WorkerMap(HashMap::default()),
            monitors: HashMap::default(),
            queued_jobs: BinaryHeap::default(),
            worker_heap: Heap::default(),
            job_statistics: JobStatisticsTimeSeries::default(),
        }
    }

    /// Process an individual [`Message`]. This function doesn't block as the scheduler is
    /// implemented as an async state machine.
    pub fn receive_message(&mut self, deps: &mut DepsT, msg: Message<DepsT, CacheT::TempFile>) {
        match msg {
            Message::ClientConnected(id, sender) => self.receive_client_connected(id, sender),
            Message::ClientDisconnected(id) => self.receive_client_disconnected(deps, id),
            Message::FromClient(cid, ClientToBroker::JobRequest(cjid, spec)) => {
                self.receive_client_job_request(deps, cid, cjid, spec)
            }
            Message::WorkerConnected(id, slots, sender) => {
                self.receive_worker_connected(deps, id, slots, sender)
            }
            Message::WorkerDisconnected(id) => self.receive_worker_disconnected(deps, id),
            Message::FromWorker(wid, WorkerToBroker::JobResponse(jid, result)) => {
                self.receive_worker_response(deps, wid, jid, result)
            }
            Message::FromWorker(wid, WorkerToBroker::JobStatusUpdate(jid, status)) => {
                self.receive_worker_job_status_update(deps, wid, jid, status)
            }
            Message::MonitorConnected(id, sender) => self.receive_monitor_connected(id, sender),
            Message::MonitorDisconnected(id) => self.receive_monitor_disconnected(id),
            Message::FromMonitor(mid, MonitorToBroker::StatisticsRequest) => {
                self.receive_monitor_statistics_request(deps, mid)
            }
            Message::GotArtifact(digest, file) => self.receive_got_artifact(deps, digest, file),
            Message::GetArtifactForWorker(digest, sender) => {
                self.receive_get_artifact_for_worker(deps, digest, sender)
            }
            Message::DecrementRefcount(digest) => self.receive_decrement_refcount(digest),
            Message::StatisticsHeartbeat => self.receive_statistics_heartbeat(),
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum IsManifest {
    Manifest,
    NotManifest,
}

impl From<bool> for IsManifest {
    fn from(is_manifest: bool) -> Self {
        if is_manifest {
            Self::Manifest
        } else {
            Self::NotManifest
        }
    }
}

impl IsManifest {
    fn is_manifest(&self) -> bool {
        self == &Self::Manifest
    }
}

struct Job {
    spec: JobSpec,
    acquired_artifacts: HashSet<Sha256Digest>,
    missing_artifacts: HashMap<Sha256Digest, IsManifest>,
}

impl Job {
    fn new(spec: JobSpec) -> Self {
        Job {
            spec,
            acquired_artifacts: Default::default(),
            missing_artifacts: Default::default(),
        }
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

pub struct Scheduler<CacheT, DepsT: SchedulerDeps> {
    cache: CacheT,
    clients: ClientMap<DepsT>,
    workers: WorkerMap<DepsT>,
    monitors: HashMap<MonitorId, DepsT::MonitorSender>,
    queued_jobs: BinaryHeap<QueuedJob>,
    worker_heap: Heap<WorkerMap<DepsT>>,
    job_statistics: JobStatisticsTimeSeries,
}

impl<CacheT: SchedulerCache, DepsT: SchedulerDeps> Scheduler<CacheT, DepsT> {
    fn possibly_start_jobs(&mut self, deps: &mut DepsT, mut just_enqueued: HashSet<JobId>) {
        while !self.queued_jobs.is_empty() && !self.workers.0.is_empty() {
            let wid = self.worker_heap.peek().unwrap();
            let worker = self.workers.0.get_mut(wid).unwrap();

            if worker.pending.len() == 2 * worker.slots {
                break;
            }

            let jid = self.queued_jobs.pop().unwrap().jid;
            let job = self.clients.job_from_jid(jid);
            deps.send_message_to_worker(
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
            deps.send_message_to_client(
                &mut client.sender,
                BrokerToClient::JobStatusUpdate(jid.cjid, JobBrokerStatus::WaitingForWorker),
            );
        }
    }

    fn receive_client_connected(&mut self, id: ClientId, sender: DepsT::ClientSender) {
        self.clients
            .0
            .insert(id, Client::new(sender))
            .assert_is_none();
    }

    fn receive_client_disconnected(&mut self, deps: &mut DepsT, id: ClientId) {
        self.cache.client_disconnected(id);

        let client = self.clients.0.remove(&id).unwrap();
        for job in client.jobs.into_values() {
            for artifact in job.acquired_artifacts {
                self.cache.decrement_refcount(artifact);
            }
        }

        self.queued_jobs.retain(|qj| qj.jid.cid != id);
        for worker in self.workers.0.values_mut() {
            worker.pending.retain(|jid| {
                jid.cid != id || {
                    deps.send_message_to_worker(
                        &mut worker.sender,
                        BrokerToWorker::CancelJob(*jid),
                    );
                    false
                }
            });
        }
        self.worker_heap.rebuild(&mut self.workers);
        self.possibly_start_jobs(deps, HashSet::default());
    }

    fn ensure_artifact_for_job(
        &mut self,
        deps: &mut DepsT,
        digest: Sha256Digest,
        jid: JobId,
        is_manifest: IsManifest,
    ) {
        let client = self.clients.0.get_mut(&jid.cid).unwrap();
        let job = client.jobs.get_mut(&jid.cjid).unwrap();
        if job.acquired_artifacts.contains(&digest) || job.missing_artifacts.contains_key(&digest) {
            return;
        }
        match self.cache.get_artifact(jid, digest.clone()) {
            GetArtifact::Success => {
                job.acquired_artifacts
                    .insert(digest.clone())
                    .assert_is_true();
                if is_manifest.is_manifest() {
                    self.ensure_manifest_artifacts_for_job(deps, jid, digest)
                        .unwrap();
                }
            }
            GetArtifact::Wait => {
                job.missing_artifacts
                    .insert(digest, is_manifest)
                    .assert_is_none();
            }
            GetArtifact::Get => {
                job.missing_artifacts
                    .insert(digest.clone(), is_manifest)
                    .assert_is_none();
                deps.send_message_to_client(
                    &mut client.sender,
                    BrokerToClient::TransferArtifact(digest),
                );
            }
        }
    }

    fn receive_client_job_request(
        &mut self,
        deps: &mut DepsT,
        cid: ClientId,
        cjid: ClientJobId,
        spec: JobSpec,
    ) {
        let jid = JobId { cid, cjid };
        let client = self.clients.0.get_mut(&cid).unwrap();
        let layers = spec.layers.clone();
        let priority = spec.priority;
        let estimated_duration = spec.estimated_duration;
        client.jobs.insert(cjid, Job::new(spec)).assert_is_none();

        for (digest, type_) in layers {
            let is_manifest = IsManifest::from(type_ == ArtifactType::Manifest);
            self.ensure_artifact_for_job(deps, digest, jid, is_manifest);
        }

        let client = self.clients.0.get_mut(&jid.cid).unwrap();
        let job = client.jobs.get(&jid.cjid).unwrap();
        let have_all_artifacts = job.missing_artifacts.is_empty();
        if have_all_artifacts {
            self.queued_jobs
                .push(QueuedJob::new(jid, priority, estimated_duration));
            self.possibly_start_jobs(deps, HashSet::from_iter([jid]));
        } else {
            deps.send_message_to_client(
                &mut client.sender,
                BrokerToClient::JobStatusUpdate(jid.cjid, JobBrokerStatus::WaitingForLayers),
            );
        }
    }

    fn receive_worker_connected(
        &mut self,
        deps: &mut DepsT,
        id: WorkerId,
        slots: usize,
        sender: DepsT::WorkerSender,
    ) {
        self.workers
            .0
            .insert(id, Worker::new(slots, sender))
            .assert_is_none();
        self.worker_heap.push(&mut self.workers, id);
        self.possibly_start_jobs(deps, HashSet::default());
    }

    fn receive_worker_disconnected(&mut self, deps: &mut DepsT, id: WorkerId) {
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

        self.possibly_start_jobs(deps, just_enqueued);
    }

    fn receive_worker_response(
        &mut self,
        deps: &mut DepsT,
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
        deps.send_message_to_client(
            &mut client.sender,
            BrokerToClient::JobResponse(jid.cjid, result),
        );
        let job = client.jobs.remove(&jid.cjid).unwrap();
        for artifact in job.acquired_artifacts {
            self.cache.decrement_refcount(artifact);
        }
        client.num_completed_jobs += 1;

        if let Some(QueuedJob { jid, .. }) = self.queued_jobs.pop() {
            // If there are any queued_requests, we can just pop one off of the front of
            // the queue and not have to update the worker's used slot count or position in the
            // workers list.
            let job = self.clients.job_from_jid(jid);
            deps.send_message_to_worker(
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

    fn receive_worker_job_status_update(
        &mut self,
        deps: &mut DepsT,
        wid: WorkerId,
        jid: JobId,
        status: JobWorkerStatus,
    ) {
        if !self.workers.0.get(&wid).unwrap().pending.contains(&jid) {
            // This indicates that the client isn't around anymore. Just ignore this status update.
            return;
        }
        let client = self.clients.0.get_mut(&jid.cid).unwrap();
        deps.send_message_to_client(
            &mut client.sender,
            BrokerToClient::JobStatusUpdate(jid.cjid, JobBrokerStatus::AtWorker(wid, status)),
        );
    }

    fn receive_monitor_connected(&mut self, id: MonitorId, sender: DepsT::MonitorSender) {
        self.monitors.insert(id, sender).assert_is_none();
    }

    fn receive_monitor_disconnected(&mut self, id: MonitorId) {
        self.monitors.remove(&id).unwrap();
    }

    fn receive_monitor_statistics_request(&mut self, deps: &mut DepsT, mid: MonitorId) {
        let worker_iter = self.workers.0.iter();
        let resp = BrokerToMonitor::StatisticsResponse(BrokerStatistics {
            worker_statistics: worker_iter
                .map(|(id, w)| (*id, WorkerStatistics { slots: w.slots }))
                .collect(),
            job_statistics: self.job_statistics.clone(),
        });
        deps.send_message_to_monitor(self.monitors.get_mut(&mid).unwrap(), resp);
    }

    fn ensure_manifest_artifacts_for_job(
        &mut self,
        deps: &mut DepsT,
        jid: JobId,
        digest: Sha256Digest,
    ) -> Result<()> {
        for entry in deps.read_manifest(&self.cache.cache_path(&digest))? {
            let entry = entry?;
            if let ManifestEntryData::File(ManifestFileData::Digest(digest)) = entry.data {
                self.ensure_artifact_for_job(deps, digest, jid, IsManifest::NotManifest);
            }
        }
        Ok(())
    }

    fn receive_got_artifact(
        &mut self,
        deps: &mut DepsT,
        digest: Sha256Digest,
        file: CacheT::TempFile,
    ) {
        let mut just_enqueued = HashSet::default();
        for jid in self.cache.got_artifact(digest.clone(), file) {
            let client = self.clients.0.get_mut(&jid.cid).unwrap();
            let job = client.jobs.get_mut(&jid.cjid).unwrap();
            job.acquired_artifacts
                .insert(digest.clone())
                .assert_is_true();
            let is_manifest = job.missing_artifacts.remove(&digest.clone()).unwrap();

            if is_manifest.is_manifest() {
                self.ensure_manifest_artifacts_for_job(deps, jid, digest.clone())
                    .unwrap();
            }

            let job = self.clients.job_from_jid(jid);
            if job.missing_artifacts.is_empty() {
                self.queued_jobs.push(QueuedJob::new(
                    jid,
                    job.spec.priority,
                    job.spec.estimated_duration,
                ));
                just_enqueued.insert(jid);
            }
        }
        self.possibly_start_jobs(deps, just_enqueued);
    }

    fn receive_get_artifact_for_worker(
        &mut self,
        deps: &mut DepsT,
        digest: Sha256Digest,
        mut sender: DepsT::WorkerArtifactFetcherSender,
    ) {
        deps.send_message_to_worker_artifact_fetcher(
            &mut sender,
            self.cache.get_artifact_for_worker(&digest),
        );
    }

    fn receive_decrement_refcount(&mut self, digest: Sha256Digest) {
        self.cache.decrement_refcount(digest);
    }

    fn sample_job_statistics_for_client(&self, cid: ClientId) -> JobStateCounts {
        let client = self.clients.0.get(&cid).unwrap();
        let jobs = &client.jobs;

        let mut counts = JobStateCounts::default();
        counts[JobState::WaitingForArtifacts] = jobs
            .values()
            .filter(|job| !job.missing_artifacts.is_empty())
            .count() as u64;

        counts[JobState::Pending] = self
            .queued_jobs
            .iter()
            .filter(|QueuedJob { jid, .. }| jid.cid == cid)
            .count() as u64;

        counts[JobState::Running] = self
            .workers
            .0
            .values()
            .flat_map(|w| w.pending.iter())
            .filter(|jid| jid.cid == cid)
            .count() as u64;

        counts[JobState::Complete] = client.num_completed_jobs;

        counts
    }

    fn receive_statistics_heartbeat(&mut self) {
        let sample = JobStatisticsSample {
            client_to_stats: self
                .clients
                .0
                .keys()
                .map(|&cid| (cid, self.sample_job_statistics_for_client(cid)))
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
    use enum_map::enum_map;
    use itertools::Itertools;
    use maelstrom_base::{
        manifest::{ManifestEntry, ManifestEntryMetadata, Mode, UnixTimestamp},
        proto::BrokerToWorker::{self, *},
    };
    use maelstrom_test::*;
    use maelstrom_util::cache::fs::test;
    use maplit::hashmap;
    use std::{cell::RefCell, error, rc::Rc, str::FromStr as _};
    use strum::Display;

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        ToClient(ClientId, BrokerToClient),
        ToWorker(WorkerId, BrokerToWorker),
        ToMonitor(MonitorId, BrokerToMonitor),
        ToWorkerArtifactFetcher(u32, Option<(PathBuf, u64)>),
        CacheGetArtifact(JobId, Sha256Digest),
        CacheGotArtifact(Sha256Digest, test::TempFile),
        CacheDecrementRefcount(Sha256Digest),
        CacheClientDisconnected(ClientId),
        CacheGetArtifactForWorker(Sha256Digest),
    }

    use TestMessage::*;

    struct TestClientSender(ClientId);
    struct TestWorkerSender(WorkerId);
    struct TestMonitorSender(MonitorId);
    struct TestWorkerArtifactFetcherSender(u32);

    #[derive(Default)]
    struct TestState {
        messages: Vec<TestMessage>,
        get_artifact_returns: HashMap<(JobId, Sha256Digest), Vec<GetArtifact>>,
        got_artifact_returns: HashMap<Sha256Digest, Vec<Vec<JobId>>>,
        #[allow(clippy::type_complexity)]
        get_artifact_for_worker_returns: HashMap<Sha256Digest, Vec<Option<(PathBuf, u64)>>>,
        read_manifest_returns: HashMap<Sha256Digest, Vec<ManifestEntry>>,
    }

    impl SchedulerCache for Rc<RefCell<TestState>> {
        type TempFile = test::TempFile;

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

        fn got_artifact(&mut self, digest: Sha256Digest, file: Self::TempFile) -> Vec<JobId> {
            self.borrow_mut()
                .messages
                .push(CacheGotArtifact(digest.clone(), file));
            self.borrow_mut()
                .got_artifact_returns
                .get_mut(&digest)
                .unwrap()
                .remove(0)
        }

        fn decrement_refcount(&mut self, digest: Sha256Digest) {
            self.borrow_mut()
                .messages
                .push(CacheDecrementRefcount(digest));
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

        fn cache_path(&self, digest: &Sha256Digest) -> PathBuf {
            Path::new("/").join(digest.to_string())
        }
    }

    #[derive(Debug, Display)]
    enum NoError {}

    impl error::Error for NoError {}

    impl SchedulerDeps for Rc<RefCell<TestState>> {
        type ClientSender = TestClientSender;
        type WorkerSender = TestWorkerSender;
        type MonitorSender = TestMonitorSender;
        type WorkerArtifactFetcherSender = TestWorkerArtifactFetcherSender;
        type ManifestError = NoError;
        type ManifestIterator = <Vec<Result<ManifestEntry, NoError>> as IntoIterator>::IntoIter;

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

        fn send_message_to_worker_artifact_fetcher(
            &mut self,
            sender: &mut TestWorkerArtifactFetcherSender,
            message: Option<(PathBuf, u64)>,
        ) {
            self.borrow_mut()
                .messages
                .push(ToWorkerArtifactFetcher(sender.0, message));
        }

        fn read_manifest(&mut self, path: &Path) -> Result<Self::ManifestIterator, NoError> {
            let digest =
                Sha256Digest::from_str(path.file_name().unwrap().to_str().unwrap()).unwrap();
            Ok(self
                .borrow_mut()
                .read_manifest_returns
                .get(&digest)
                .unwrap()
                .iter()
                .map(|e| Ok(e.clone()))
                .collect_vec()
                .into_iter())
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        scheduler: Scheduler<Rc<RefCell<TestState>>, Rc<RefCell<TestState>>>,
    }

    impl Default for Fixture {
        fn default() -> Self {
            let test_state = Rc::new(RefCell::new(TestState::default()));
            Fixture {
                test_state: test_state.clone(),
                scheduler: Scheduler::new(test_state),
            }
        }
    }

    impl Fixture {
        #[allow(clippy::type_complexity)]
        fn new<const L: usize, const M: usize, const N: usize, const O: usize>(
            get_artifact_returns: [((JobId, Sha256Digest), Vec<GetArtifact>); L],
            got_artifact_returns: [(Sha256Digest, Vec<Vec<JobId>>); M],
            get_artifact_for_worker_returns: [(Sha256Digest, Vec<Option<(PathBuf, u64)>>); N],
            read_manifest_returns: [(Sha256Digest, Vec<ManifestEntry>); O],
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
            result.test_state.borrow_mut().read_manifest_returns =
                HashMap::from(read_manifest_returns);
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

        fn receive_message(&mut self, msg: Message<Rc<RefCell<TestState>>, test::TempFile>) {
            self.scheduler.receive_message(&mut self.test_state, msg);
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
        message_from_known_client_ok,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
            ], [], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![1], JobBrokerStatus::WaitingForWorker)),
        };
    }

    #[test]
    #[should_panic]
    fn request_from_unknown_client_panics() {
        let mut fixture = Fixture::default();
        fixture.receive_message(FromClient(
            cid![1],
            ClientToBroker::JobRequest(cjid![1], spec![1, Tar]),
        ));
    }

    #[test]
    #[should_panic]
    fn disconnect_from_unknown_client_panics() {
        let mut fixture = Fixture::default();
        fixture.receive_message(ClientDisconnected(cid![1]));
    }

    #[test]
    #[should_panic]
    fn connect_from_duplicate_client_panics() {
        let mut fixture = Fixture::default();
        fixture.receive_message(ClientDisconnected(cid![1]));
        fixture.receive_message(ClientDisconnected(cid![1]));
    }

    #[test]
    #[should_panic]
    fn response_from_unknown_worker_panics() {
        let mut fixture = Fixture::default();
        // The response will be ignored unless we use a valid ClientId.
        fixture.receive_message(ClientConnected(cid![1], client_sender![1]));

        fixture.receive_message(FromWorker(
            wid![1],
            WorkerToBroker::JobResponse(jid![1], Ok(outcome![1])),
        ));
    }

    #[test]
    #[should_panic]
    fn disconnect_from_unknown_worker_panics() {
        let mut fixture = Fixture::default();
        fixture.receive_message(WorkerDisconnected(wid![1]));
    }

    #[test]
    #[should_panic]
    fn connect_from_duplicate_worker_panics() {
        let mut fixture = Fixture::default();
        fixture.receive_message(WorkerConnected(wid![1], 2, worker_sender![1]));
        fixture.receive_message(WorkerConnected(wid![1], 2, worker_sender![1]));
    }

    script_test! {
        response_from_known_worker_for_unknown_job_ignored,
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        FromWorker(wid![1], WorkerToBroker::JobResponse(jid![1], Ok(outcome![1]))) => {};
    }

    script_test! {
        one_client_one_worker,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
            ], [], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1], spec![1, Tar])),
        };
        FromWorker(wid![1], WorkerToBroker::JobResponse(jid![1], Ok(outcome![1]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
        };
    }

    script_test! {
        response_from_worker_for_disconnected_client_ignored,
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        FromWorker(wid![1], WorkerToBroker::JobResponse(jid![1], Ok(outcome![1]))) => {};
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
            ], [], [], [])
        },
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        WorkerConnected(wid![2], 2, worker_sender![2]) => {};
        WorkerConnected(wid![3], 3, worker_sender![3]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        // 0/2 0/2 0/3
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };

        // 1/2 0/2 0/3
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar])) => {
            CacheGetArtifact(jid![1, 2], digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec![2, Tar])),
        };

        // 1/2 1/2 0/3
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar])) => {
            CacheGetArtifact(jid![1, 3], digest![3]),
            ToWorker(wid![3], EnqueueJob(jid![1, 3], spec![3, Tar])),
        };

        // 1/2 1/2 1/3
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![4], spec![4, Tar])) => {
            CacheGetArtifact(jid![1, 4], digest![4]),
            ToWorker(wid![3], EnqueueJob(jid![1, 4], spec![4, Tar])),
        };

        // 1/2 1/2 2/3
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![5], spec![5, Tar])) => {
            CacheGetArtifact(jid![1, 5], digest![5]),
            ToWorker(wid![1], EnqueueJob(jid![1, 5], spec![5, Tar])),
        };

        // 2/2 1/2 2/3
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![6], spec![6, Tar])) => {
            CacheGetArtifact(jid![1, 6], digest![6]),
            ToWorker(wid![2], EnqueueJob(jid![1, 6], spec![6, Tar])),
        };

        // 2/2 2/2 2/3
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![7], spec![7, Tar])) => {
            CacheGetArtifact(jid![1, 7], digest![7]),
            ToWorker(wid![3], EnqueueJob(jid![1, 7], spec![7, Tar])),
        };

        FromWorker(wid![1], WorkerToBroker::JobResponse(jid![1, 1], Ok(outcome![1]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![8], spec![8, Tar])) => {
            CacheGetArtifact(jid![1, 8], digest![8]),
            ToWorker(wid![1], EnqueueJob(jid![1, 8], spec![8, Tar])),
        };

        FromWorker(wid![2], WorkerToBroker::JobResponse(jid![1, 2], Ok(outcome![2]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], Ok(outcome![2]))),
            CacheDecrementRefcount(digest![2]),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![9], spec![9, Tar])) => {
            CacheGetArtifact(jid![1, 9], digest![9]),
            ToWorker(wid![2], EnqueueJob(jid![1, 9], spec![9, Tar])),
        };

        FromWorker(wid![3], WorkerToBroker::JobResponse(jid![1, 3], Ok(outcome![3]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![3], Ok(outcome![3]))),
            CacheDecrementRefcount(digest![3]),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![10], spec![10, Tar])) => {
            CacheGetArtifact(jid![1, 10], digest![10]),
            ToWorker(wid![3], EnqueueJob(jid![1, 10], spec![10, Tar])),
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
            ], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        // 0/1 0/1
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };

        // 1/1 0/1
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar])) => {
            CacheGetArtifact(jid![1, 2], digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec![2, Tar])),
        };

        // 1/1 1/1
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar])) => {
            CacheGetArtifact(jid![1, 3], digest![3]),
            ToWorker(wid![1], EnqueueJob(jid![1, 3], spec![3, Tar])),
        };

        // 2/1 1/1
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![4], spec![4, Tar])) => {
            CacheGetArtifact(jid![1, 4], digest![4]),
            ToWorker(wid![2], EnqueueJob(jid![1, 4], spec![4, Tar])),
        };

        // 2/1 2/1
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![5], spec![5, Tar])) => {
            CacheGetArtifact(jid![1, 5], digest![5]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![5], JobBrokerStatus::WaitingForWorker)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![6], spec![6, Tar])) => {
            CacheGetArtifact(jid![1, 6], digest![6]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![6], JobBrokerStatus::WaitingForWorker)),
        };

        // 2/2 1/2
        FromWorker(wid![2], WorkerToBroker::JobResponse(jid![1, 2], Ok(outcome![2]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], Ok(outcome![2]))),
            CacheDecrementRefcount(digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 5], spec![5, Tar])),
        };

        // 1/2 2/2
        FromWorker(wid![1], WorkerToBroker::JobResponse(jid![1, 1], Ok(outcome![1]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 6], spec![6, Tar])),
        };
    }

    script_test! {
        queued_requests_go_to_workers_on_connect,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![2]), vec![GetArtifact::Success]),
                ((jid![1, 3], digest![3]), vec![GetArtifact::Success]),
                ((jid![1, 4], digest![4]), vec![GetArtifact::Success]),
                ((jid![1, 5], digest![5]), vec![GetArtifact::Success]),
                ((jid![1, 6], digest![6]), vec![GetArtifact::Success]),
            ], [], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest!(1)),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![1], JobBrokerStatus::WaitingForWorker)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar].priority(-1).estimated_duration(Some(millis!(6))))) => {
            CacheGetArtifact(jid![1, 2], digest!(2)),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![2], JobBrokerStatus::WaitingForWorker)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar].estimated_duration(Some(millis!(5))))) => {
            CacheGetArtifact(jid![1, 3], digest!(3)),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![3], JobBrokerStatus::WaitingForWorker)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![4], spec![4, Tar].estimated_duration(Some(millis!(4))))) => {
            CacheGetArtifact(jid![1, 4], digest!(4)),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![4], JobBrokerStatus::WaitingForWorker)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![5], spec![5, Tar].estimated_duration(Some(millis!(3))))) => {
            CacheGetArtifact(jid![1, 5], digest!(5)),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![5], JobBrokerStatus::WaitingForWorker)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![6], spec![6, Tar].priority(1).estimated_duration(Some(millis!(2))))) => {
            CacheGetArtifact(jid![1, 6], digest!(6)),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![6], JobBrokerStatus::WaitingForWorker)),
        };

        WorkerConnected(wid![1], 2, worker_sender![1]) => {
            ToWorker(wid![1], EnqueueJob(jid![1, 6], spec![6, Tar].priority(1).estimated_duration(Some(millis!(2))))),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
            ToWorker(wid![1], EnqueueJob(jid![1, 3], spec![3, Tar].estimated_duration(Some(millis!(5))))),
            ToWorker(wid![1], EnqueueJob(jid![1, 4], spec![4, Tar].estimated_duration(Some(millis!(4))))),
        };

        WorkerConnected(wid![2], 2, worker_sender![2]) => {
            ToWorker(wid![2], EnqueueJob(jid![1, 5], spec![5, Tar].estimated_duration(Some(millis!(3))))),
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec![2, Tar].priority(-1).estimated_duration(Some(millis!(6))))),
        };
    }

    script_test! {
        requests_outstanding_on_disconnected_worker_get_sent_to_new_workers,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![2]), vec![GetArtifact::Success]),
                ((jid![1, 3], digest![3]), vec![GetArtifact::Success]),
                ((jid![1, 4], digest![4]), vec![GetArtifact::Success]),
                ((jid![1, 5], digest![5]), vec![GetArtifact::Success]),
            ], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        WorkerConnected(wid![3], 1, worker_sender![3]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar].priority(1).estimated_duration(Some(millis!(50))))) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar].priority(1).estimated_duration(Some(millis!(50))))),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar])) => {
            CacheGetArtifact(jid![1, 2], digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec![2, Tar])),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar])) => {
            CacheGetArtifact(jid![1, 3], digest![3]),
            ToWorker(wid![3], EnqueueJob(jid![1, 3], spec![3, Tar])),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![4], spec![4, Tar].estimated_duration(Some(millis!(40))))) => {
            CacheGetArtifact(jid![1, 4], digest![4]),
            ToWorker(wid![1], EnqueueJob(jid![1, 4], spec![4, Tar].estimated_duration(Some(millis!(40))))),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![5], spec![5, Tar])) => {
            CacheGetArtifact(jid![1, 5], digest![5]),
            ToWorker(wid![2], EnqueueJob(jid![1, 5], spec![5, Tar])),
        };

        WorkerDisconnected(wid![1]) => {
            ToWorker(wid![3], EnqueueJob(jid![1, 1], spec![1, Tar].priority(1).estimated_duration(Some(millis!(50))))),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![4], JobBrokerStatus::WaitingForWorker)),
        };

        FromWorker(wid![2], WorkerToBroker::JobResponse(jid![1, 2], Ok(outcome![2]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], Ok(outcome![2]))),
            CacheDecrementRefcount(digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 4], spec![4, Tar].estimated_duration(Some(millis!(40))))),
        };
    }

    script_test! {
        requests_outstanding_on_disconnected_worker_get_sent_to_new_workers_2,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![2]), vec![GetArtifact::Success]),
                ((jid![1, 3], digest![3]), vec![GetArtifact::Success]),
                ((jid![1, 4], digest![4]), vec![GetArtifact::Success]),
            ], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar])) => {
            CacheGetArtifact(jid![1, 2], digest![2]),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![2, Tar])),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar].estimated_duration(Some(millis!(300))))) => {
            CacheGetArtifact(jid![1, 3], digest![3]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![3], JobBrokerStatus::WaitingForWorker)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![4], spec![4, Tar].estimated_duration(Some(millis!(40))))) => {
            CacheGetArtifact(jid![1, 4], digest![4]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![4], JobBrokerStatus::WaitingForWorker)),
        };

        FromWorker(wid![1], WorkerToBroker::JobResponse(jid![1, 1], Ok(outcome![1]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 3], spec![3, Tar].estimated_duration(Some(millis!(300))))),
        };

        WorkerConnected(wid![2], 1, worker_sender![2]) => {
            ToWorker(wid![2], EnqueueJob(jid![1, 4], spec![4, Tar].estimated_duration(Some(millis!(40))))),
        };

        WorkerDisconnected(wid![1]) => {
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec![2, Tar])),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![3], JobBrokerStatus::WaitingForWorker)),
        };

        FromWorker(wid![2], WorkerToBroker::JobResponse(jid![1, 2], Ok(outcome![2]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], Ok(outcome![2]))),
            CacheDecrementRefcount(digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 3], spec![3, Tar].estimated_duration(Some(millis!(300))))),
        };
    }

    script_test! {
        requests_outstanding_on_disconnected_worker_get_reenqueued_for_other_workers,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![2]), vec![GetArtifact::Success]),
                ((jid![1, 3], digest![3]), vec![GetArtifact::Success]),
                ((jid![1, 4], digest![4]), vec![GetArtifact::Success]),
            ], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar].estimated_duration(Some(millis!(10))))) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar].estimated_duration(Some(millis!(10))))),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar].estimated_duration(Some(millis!(20))))) => {
            CacheGetArtifact(jid![1, 2], digest![2]),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![2, Tar].estimated_duration(Some(millis!(20))))),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar].priority(1).estimated_duration(Some(millis!(1))))) => {
            CacheGetArtifact(jid![1, 3], digest![3]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![3], JobBrokerStatus::WaitingForWorker)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![4], spec![4, Tar].estimated_duration(Some(millis!(40))))) => {
            CacheGetArtifact(jid![1, 4], digest![4]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![4], JobBrokerStatus::WaitingForWorker)),
        };

        WorkerDisconnected(wid![1]) => {
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![1], JobBrokerStatus::WaitingForWorker)),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![2], JobBrokerStatus::WaitingForWorker)),
        };

        WorkerConnected(wid![2], 1, worker_sender![2]) => {
            ToWorker(wid![2], EnqueueJob(jid![1, 3], spec![3, Tar].priority(1).estimated_duration(Some(millis!(1))))),
            ToWorker(wid![2], EnqueueJob(jid![1, 4], spec![4, Tar].estimated_duration(Some(millis!(40))))),
        };
    }

    script_test! {
        requests_get_removed_from_workers_pending_map,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![2]), vec![GetArtifact::Success]),
            ], [], [], [])
        },

        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar])) => {
            CacheGetArtifact(jid![1, 2], digest![2]),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![2, Tar])),
        };

        FromWorker(wid![1], WorkerToBroker::JobResponse(jid![1, 1], Ok(outcome![1]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
        };

        FromWorker(wid![1], WorkerToBroker::JobResponse(jid![1, 2], Ok(outcome![1]))) => {
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
            ], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid!(1, 1), digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
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
            ], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};
        ClientConnected(cid![2], client_sender![2]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid!(1, 1), digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };

        FromClient(cid![2], ClientToBroker::JobRequest(cjid![1], spec![2, Tar])) => {
            CacheGetArtifact(jid!(2, 1), digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![2, 1], spec![2, Tar])),
        };

        ClientDisconnected(cid![2]) => {
            ToWorker(wid![2], CancelJob(jid![2, 1])),
            CacheDecrementRefcount(digest![2]),
            CacheClientDisconnected(cid![2]),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![3, Tar])) => {
            CacheGetArtifact(jid!(1, 2), digest![3]),
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec![3, Tar])),
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
            ], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid!(1, 1), digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar])) => {
            CacheGetArtifact(jid!(1, 2), digest![2]),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![2, Tar])),
        };

        ClientConnected(cid![2], client_sender![2]) => {};
        FromClient(cid![2], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid!(2, 1), digest![1]),
            ToClient(cid![2], BrokerToClient::JobStatusUpdate(cjid![1], JobBrokerStatus::WaitingForWorker)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar])) => {
            CacheGetArtifact(jid!(1, 3), digest![3]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![3], JobBrokerStatus::WaitingForWorker)),
        };

        ClientDisconnected(cid![2]) => {
            CacheDecrementRefcount(digest![1]),
            CacheClientDisconnected(cid![2]),
        };

        FromWorker(wid![1], WorkerToBroker::JobResponse(jid![1, 1], Ok(outcome![1]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 3], spec![3, Tar])),
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
            ], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};
        ClientConnected(cid![2], client_sender![2]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid!(1, 1), digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };

        FromClient(cid![2], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid!(2, 1), digest![1]),
            ToWorker(wid![2], EnqueueJob(jid![2, 1], spec![1, Tar])),
        };

        FromClient(cid![2], ClientToBroker::JobRequest(cjid![2], spec![2, Tar])) => {
            CacheGetArtifact(jid!(2, 2), digest![2]),
            ToWorker(wid![1], EnqueueJob(jid![2, 2], spec![2, Tar])),
        };

        FromClient(cid![2], ClientToBroker::JobRequest(cjid![3], spec![3, Tar])) => {
            CacheGetArtifact(jid!(2, 3), digest![3]),
            ToWorker(wid![2], EnqueueJob(jid![2, 3], spec![3, Tar])),
        };

        FromClient(cid![2], ClientToBroker::JobRequest(cjid![4], spec![4, Tar])) => {
            CacheGetArtifact(jid!(2, 4), digest![4]),
            ToClient(cid![2], BrokerToClient::JobStatusUpdate(cjid![4], JobBrokerStatus::WaitingForWorker)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar])) => {
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

            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec![2, Tar])),

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
            ], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(
            cid![1],
            ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Tar), (43, Tar), (44, Tar)]])
        ) => {
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
        request_with_layers_2,
        {
            Fixture::new([
                ((jid![1, 2], digest![42]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![43]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![44]), vec![GetArtifact::Success]),
            ], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(
            cid![1],
            ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Tar), (43, Tar), (44, Tar)]])
        ) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            CacheGetArtifact(jid![1, 2], digest![43]),
            CacheGetArtifact(jid![1, 2], digest![44]),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![1, [(42, Tar), (43, Tar), (44, Tar)]])),
        };

        FromWorker(wid![1], WorkerToBroker::JobResponse(jid![1, 2], Ok(outcome![1]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![42]),
            CacheDecrementRefcount(digest![43]),
            CacheDecrementRefcount(digest![44]),
        };
    }

    script_test! {
        request_with_layers_3,
        {
            Fixture::new([
                ((jid![1, 2], digest![42]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![43]), vec![GetArtifact::Wait]),
                ((jid![1, 2], digest![44]), vec![GetArtifact::Get]),
            ], [
                (digest![43], vec![vec![jid![1, 2]]]),
                (digest![44], vec![vec![jid![1, 2]]]),
            ], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(
            cid![1],
            ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Tar), (43, Tar), (44, Tar)]])
        ) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            CacheGetArtifact(jid![1, 2], digest![43]),
            CacheGetArtifact(jid![1, 2], digest![44]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![44])),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![2], JobBrokerStatus::WaitingForLayers)),
        };

        GotArtifact(digest![43], "/z/tmp/foo".into()) => {
            CacheGotArtifact(digest![43], "/z/tmp/foo".into()),
        };
        GotArtifact(digest![44], "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![44], "/z/tmp/bar".into()),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![1, [(42, Tar), (43, Tar), (44, Tar)]])),
        };

        ClientDisconnected(cid![1]) => {
            ToWorker(wid![1], CancelJob(jid![1, 2])),
            CacheClientDisconnected(cid![1]),
            CacheDecrementRefcount(digest![42]),
            CacheDecrementRefcount(digest![43]),
            CacheDecrementRefcount(digest![44]),
        }
    }

    script_test! {
        request_with_duplicate_layers_in_cache,
        {
            Fixture::new([
                ((jid![1, 2], digest![42]), vec![GetArtifact::Success]),
            ], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(
            cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Tar), (42, Tar)]])
        ) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![1, [(42, Tar), (42, Tar)]])),
        };

        ClientDisconnected(cid![1]) => {
            ToWorker(wid![1], CancelJob(jid![1, 2])),
            CacheClientDisconnected(cid![1]),
            CacheDecrementRefcount(digest![42]),
        }
    }

    script_test! {
        request_with_duplicate_layers_not_in_cache,
        {
            Fixture::new([
                ((jid![1, 2], digest![42]), vec![GetArtifact::Get]),
            ], [
                (digest![42], vec![vec![jid![1, 2]]]),
            ], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(
            cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Tar), (42, Tar)]])
        ) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![42])),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![2], JobBrokerStatus::WaitingForLayers)),
        };

        GotArtifact(digest![42], "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![42], "/z/tmp/bar".into()),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![1, [(42, Tar), (42, Tar)]])),
        };

        ClientDisconnected(cid![1]) => {
            ToWorker(wid![1], CancelJob(jid![1, 2])),
            CacheClientDisconnected(cid![1]),
            CacheDecrementRefcount(digest![42]),
        }
    }

    script_test! {
        request_with_manifest_nothing_in_cache,
        {
            Fixture::new([
                ((jid![1, 2], digest![42]), vec![GetArtifact::Get]),
                ((jid![1, 2], digest![43]), vec![GetArtifact::Get]),
            ], [
                (digest![42], vec![vec![jid![1, 2]]]),
                (digest![43], vec![vec![jid![1, 2]]]),
            ], [], [
                (digest![42], vec![ManifestEntry {
                    path: "foobar.txt".into(),
                    metadata: ManifestEntryMetadata {
                        size: 11,
                        mode: Mode(0o0555),
                        mtime: UnixTimestamp(1705538554),
                    },
                    data: ManifestEntryData::File(ManifestFileData::Digest(digest![43])),
                }])
            ])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Manifest)]])) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![42])),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![2], JobBrokerStatus::WaitingForLayers)),
        };

        GotArtifact(digest![42], "/z/tmp/foo".into()) => {
            CacheGotArtifact(digest![42], "/z/tmp/foo".into()),
            CacheGetArtifact(jid![1, 2], digest![43]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![43])),
        };

        GotArtifact(digest![43], "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![43], "/z/tmp/bar".into()),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![1, [(42, Manifest)]])),
        };

        ClientDisconnected(cid![1]) => {
            ToWorker(wid![1], CancelJob(jid![1, 2])),
            CacheClientDisconnected(cid![1]),
            CacheDecrementRefcount(digest![42]),
            CacheDecrementRefcount(digest![43]),
        }
    }

    script_test! {
        request_with_manifest_with_duplicate_digests_with_nothing_in_cache,
        {
            Fixture::new([
                ((jid![1, 2], digest![42]), vec![GetArtifact::Get]),
                ((jid![1, 2], digest![43]), vec![GetArtifact::Get]),
            ], [
                (digest![42], vec![vec![jid![1, 2]]]),
                (digest![43], vec![vec![jid![1, 2]]]),
            ], [], [
                (digest![42], vec![
                    ManifestEntry {
                        path: "foo.txt".into(),
                        metadata: ManifestEntryMetadata {
                            size: 11,
                            mode: Mode(0o0555),
                            mtime: UnixTimestamp(1705538554),
                        },
                        data: ManifestEntryData::File(ManifestFileData::Digest(digest![43])),
                    },
                    ManifestEntry {
                        path: "bar.txt".into(),
                        metadata: ManifestEntryMetadata {
                            size: 11,
                            mode: Mode(0o0555),
                            mtime: UnixTimestamp(1705538554),
                        },
                        data: ManifestEntryData::File(ManifestFileData::Digest(digest![43])),
                    }
                ])
            ])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Manifest)]])) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![42])),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![2], JobBrokerStatus::WaitingForLayers)),
        };

        GotArtifact(digest![42], "/z/tmp/foo".into()) => {
            CacheGotArtifact(digest![42], "/z/tmp/foo".into()),
            CacheGetArtifact(jid![1, 2], digest![43]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![43])),
        };

        GotArtifact(digest![43], "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![43], "/z/tmp/bar".into()),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![1, [(42, Manifest)]])),
        };

        ClientDisconnected(cid![1]) => {
            ToWorker(wid![1], CancelJob(jid![1, 2])),
            CacheClientDisconnected(cid![1]),
            CacheDecrementRefcount(digest![42]),
            CacheDecrementRefcount(digest![43]),
        }
    }

    script_test! {
        request_with_manifest_already_in_cache,
        {
            Fixture::new([
                ((jid![1, 2], digest![42]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![43]), vec![GetArtifact::Get]),
            ], [
                (digest![42], vec![vec![jid![1, 2]]]),
                (digest![43], vec![vec![jid![1, 2]]]),
            ], [], [
                (digest![42], vec![ManifestEntry {
                    path: "foobar.txt".into(),
                    metadata: ManifestEntryMetadata {
                        size: 11,
                        mode: Mode(0o0555),
                        mtime: UnixTimestamp(1705538554),
                    },
                    data: ManifestEntryData::File(ManifestFileData::Digest(digest![43])),
                }])
            ])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Manifest)]])) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            CacheGetArtifact(jid![1, 2], digest![43]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![43])),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![2], JobBrokerStatus::WaitingForLayers)),
        };

        GotArtifact(digest![43], "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![43], "/z/tmp/bar".into()),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![1, [(42, Manifest)]])),
        };

        ClientDisconnected(cid![1]) => {
            ToWorker(wid![1], CancelJob(jid![1, 2])),
            CacheClientDisconnected(cid![1]),
            CacheDecrementRefcount(digest![42]),
            CacheDecrementRefcount(digest![43]),
        }
    }

    script_test! {
        request_with_manifest_with_manifest_dependency_not_in_cache,
        {
            Fixture::new([
                ((jid![1, 2], digest![42]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![43]), vec![GetArtifact::Get]),
            ], [
                (digest![42], vec![vec![jid![1, 2]]]),
                (digest![43], vec![vec![jid![1, 2]]]),
            ], [], [
                (digest![42], vec![ManifestEntry {
                    path: "foobar.txt".into(),
                    metadata: ManifestEntryMetadata {
                        size: 11,
                        mode: Mode(0o0555),
                        mtime: UnixTimestamp(1705538554),
                    },
                    data: ManifestEntryData::File(ManifestFileData::Digest(digest![43])),
                }])
            ])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Manifest)]])) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            CacheGetArtifact(jid![1, 2], digest![43]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![43])),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![2], JobBrokerStatus::WaitingForLayers)),
        };

        GotArtifact(digest![43], "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![43], "/z/tmp/bar".into()),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![1, [(42, Manifest)]])),
        };

        ClientDisconnected(cid![1]) => {
            ToWorker(wid![1], CancelJob(jid![1, 2])),
            CacheClientDisconnected(cid![1]),
            CacheDecrementRefcount(digest![42]),
            CacheDecrementRefcount(digest![43]),
        }
    }

    script_test! {
        request_with_manifest_with_manifest_dependency_in_cache,
        {
            Fixture::new([
                ((jid![1, 2], digest![42]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![43]), vec![GetArtifact::Success]),
            ], [
                (digest![42], vec![vec![jid![1, 2]]]),
                (digest![43], vec![vec![jid![1, 2]]]),
            ], [], [
                (digest![42], vec![ManifestEntry {
                    path: "foobar.txt".into(),
                    metadata: ManifestEntryMetadata {
                        size: 11,
                        mode: Mode(0o0555),
                        mtime: UnixTimestamp(1705538554),
                    },
                    data: ManifestEntryData::File(ManifestFileData::Digest(digest![43])),
                }])
            ])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Manifest)]])) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            CacheGetArtifact(jid![1, 2], digest![43]),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![1, [(42, Manifest)]])),
        };

        ClientDisconnected(cid![1]) => {
            ToWorker(wid![1], CancelJob(jid![1, 2])),
            CacheClientDisconnected(cid![1]),
            CacheDecrementRefcount(digest![42]),
            CacheDecrementRefcount(digest![43]),
        }
    }

    script_test! {
        request_with_manifest_with_duplicate_digests_already_in_cache,
        {
            Fixture::new([
                ((jid![1, 2], digest![42]), vec![GetArtifact::Success]),
                ((jid![1, 2], digest![43]), vec![GetArtifact::Success]),
            ], [
                (digest![42], vec![vec![jid![1, 2]]]),
                (digest![43], vec![vec![jid![1, 2]]]),
            ], [], [
                (digest![42], vec![
                    ManifestEntry {
                        path: "foo.txt".into(),
                        metadata: ManifestEntryMetadata {
                            size: 11,
                            mode: Mode(0o0555),
                            mtime: UnixTimestamp(1705538554),
                        },
                        data: ManifestEntryData::File(ManifestFileData::Digest(digest![43])),
                    },
                    ManifestEntry {
                        path: "bar.txt".into(),
                        metadata: ManifestEntryMetadata {
                            size: 11,
                            mode: Mode(0o0555),
                            mtime: UnixTimestamp(1705538554),
                        },
                        data: ManifestEntryData::File(ManifestFileData::Digest(digest![43])),
                    }
                ])
            ])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Manifest)]])) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            CacheGetArtifact(jid![1, 2], digest![43]),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![1, [(42, Manifest)]])),
        };

        ClientDisconnected(cid![1]) => {
            ToWorker(wid![1], CancelJob(jid![1, 2])),
            CacheClientDisconnected(cid![1]),
            CacheDecrementRefcount(digest![42]),
            CacheDecrementRefcount(digest![43]),
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
            ], [])
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
            ], [])
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
            ], [], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        MonitorConnected(mid![1], monitor_sender![1]) => {};
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, [(42, Tar)]])) => {
            CacheGetArtifact(jid![1, 1], digest![42]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![1], JobBrokerStatus::WaitingForLayers)),
        };
        StatisticsHeartbeat => {};
        FromMonitor(mid![1], MonitorToBroker::StatisticsRequest) => {
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
            ], [], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        MonitorConnected(mid![1], monitor_sender![1]) => {};
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToClient(cid![1], BrokerToClient::JobStatusUpdate(cjid![1], JobBrokerStatus::WaitingForWorker)),
        };
        StatisticsHeartbeat => {};
        FromMonitor(mid![1], MonitorToBroker::StatisticsRequest) => {
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
            ], [], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        MonitorConnected(mid![1], monitor_sender![1]) => {};
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };
        StatisticsHeartbeat => {};
        FromMonitor(mid![1], MonitorToBroker::StatisticsRequest) => {
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
            ], [], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        MonitorConnected(mid![1], monitor_sender![1]) => {};
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };
        FromWorker(wid![1], WorkerToBroker::JobResponse(jid![1, 1], Ok(outcome![1]))) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], Ok(outcome![1]))),
            CacheDecrementRefcount(digest![1]),
        };
        StatisticsHeartbeat => {};
        FromMonitor(mid![1], MonitorToBroker::StatisticsRequest) => {
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
            ], [], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        WorkerConnected(wid![2], 1, worker_sender![2]) => {};
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 3], digest![1]),
            ToWorker(wid![2], EnqueueJob(jid![1, 3], spec![1, Tar])),
        };
        FromWorker(
            wid![2],
            WorkerToBroker::JobStatusUpdate(jid![1, 3], JobWorkerStatus::WaitingForLayers)
        ) => {
            ToClient(
                cid![1],
                BrokerToClient::JobStatusUpdate(
                    cjid![3],
                    JobBrokerStatus::AtWorker(wid![2], JobWorkerStatus::WaitingForLayers)
                )
            )
        };
        FromWorker(
            wid![2],
            WorkerToBroker::JobStatusUpdate(jid![1, 3], JobWorkerStatus::WaitingToExecute)
        ) => {
            ToClient(
                cid![1],
                BrokerToClient::JobStatusUpdate(
                    cjid![3],
                    JobBrokerStatus::AtWorker(wid![2], JobWorkerStatus::WaitingToExecute)
                )
            )
        };
        FromWorker(
            wid![2],
            WorkerToBroker::JobStatusUpdate(jid![1, 3], JobWorkerStatus::Executing)
        ) => {
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
            Fixture::new([], [], [], [])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        FromWorker(
            wid![1],
            WorkerToBroker::JobStatusUpdate(jid![2, 3], JobWorkerStatus::WaitingForLayers)
        ) => {};
        FromWorker(
            wid![1],
            WorkerToBroker::JobStatusUpdate(jid![2, 3], JobWorkerStatus::WaitingToExecute)
        ) => {};
        FromWorker(
            wid![1],
            WorkerToBroker::JobStatusUpdate(jid![2, 3], JobWorkerStatus::Executing)
        ) => {};
    }
}
