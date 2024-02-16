//! Central processing module for the broker. Receives and sends messages to and from clients and
//! workers.

use crate::scheduler_task::cache::{Cache, CacheFs, GetArtifact, GetArtifactForWorkerError};
use anyhow::Result;
use maelstrom_base::{
    manifest::{ManifestEntryData, ManifestReader},
    proto::{BrokerToClient, BrokerToWorker, ClientToBroker, WorkerToBroker},
    stats::{
        BrokerStatistics, JobState, JobStateCounts, JobStatisticsSample, JobStatisticsTimeSeries,
        WorkerStatistics,
    },
    ArtifactType, ClientId, ClientJobId, JobId, JobOutcomeResult, JobSpec, Sha256Digest, WorkerId,
};
use maelstrom_util::{
    ext::{BoolExt as _, OptionExt as _},
    heap::{Heap, HeapDeps, HeapIndex},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::{self, Debug, Formatter},
    io,
    path::{Path, PathBuf},
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
    type WorkerArtifactFetcherSender;
    fn send_message_to_client(&mut self, sender: &mut Self::ClientSender, message: BrokerToClient);
    fn send_message_to_worker(&mut self, sender: &mut Self::WorkerSender, message: BrokerToWorker);
    fn send_message_to_worker_artifact_fetcher(
        &mut self,
        sender: &mut Self::WorkerArtifactFetcherSender,
        message: Result<(PathBuf, u64), GetArtifactForWorkerError>,
    );
}

/// The required interface for the cache that is provided to the [`Scheduler`]. This mirrors the API
/// for [`super::cache::Cache`]. We keep them separate so that we can test the [`Scheduler`] more
/// easily.
///
/// Unlike with [`SchedulerDeps`], all of these functions are immediate. In production, the
/// [`super::cache::Cache`] is owned by the [`Scheduler`] and the two live on the same task. So
/// these methods sometimes return actual values which can be handled immediately, unlike
/// [`SchedulerDeps`].
pub trait SchedulerCache {
    /// See [`super::cache::Cache::get_artifact`].
    fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact;

    /// See [`super::cache::Cache::got_artifact`].
    fn got_artifact(&mut self, digest: Sha256Digest, size: u64, path: &Path) -> Vec<JobId>;

    /// See [`super::cache::Cache::read_manifest`].
    fn read_manifest(
        &mut self,
        digest: Sha256Digest,
    ) -> Result<ManifestReader<impl io::Read + io::Seek + 'static>>;

    /// See [`super::cache::Cache::decrement_refcount`].
    fn decrement_refcount(&mut self, digest: Sha256Digest);

    /// See [`super::cache::Cache::client_disconnected`].
    fn client_disconnected(&mut self, cid: ClientId);

    /// See [`super::cache::Cache::get_artifact_for_worker`].
    fn get_artifact_for_worker(
        &mut self,
        digest: &Sha256Digest,
    ) -> Result<(PathBuf, u64), GetArtifactForWorkerError>;
}

impl<FsT: CacheFs> SchedulerCache for Cache<FsT> {
    fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact {
        self.get_artifact(jid, digest)
    }

    fn got_artifact(&mut self, digest: Sha256Digest, size: u64, path: &Path) -> Vec<JobId> {
        self.got_artifact(digest, size, path)
    }

    fn read_manifest(
        &mut self,
        digest: Sha256Digest,
    ) -> Result<ManifestReader<impl io::Read + io::Seek + 'static>> {
        self.read_manifest(digest)
    }

    fn decrement_refcount(&mut self, digest: Sha256Digest) {
        self.decrement_refcount(digest)
    }

    fn client_disconnected(&mut self, cid: ClientId) {
        self.client_disconnected(cid)
    }

    fn get_artifact_for_worker(
        &mut self,
        digest: &Sha256Digest,
    ) -> Result<(PathBuf, u64), GetArtifactForWorkerError> {
        self.get_artifact_for_worker(digest)
    }
}

/// The incoming messages, or events, for [`Scheduler`].
///
/// If [`Scheduler`] weren't implement as an async state machine, these would be its methods.
pub enum Message<DepsT: SchedulerDeps> {
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

    /// An artifact has been pushed to us. The artifact has the given digest and length. It is
    /// temporarily stored at the given path.
    GotArtifact(Sha256Digest, u64, PathBuf),

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

impl<DepsT: SchedulerDeps> Debug for Message<DepsT> {
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
            Message::GotArtifact(digest, size, path) => f
                .debug_tuple("GotArtifact")
                .field(digest)
                .field(size)
                .field(path)
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
            clients: HashMap::default(),
            workers: WorkerMap(HashMap::default()),
            queued_requests: VecDeque::default(),
            worker_heap: Heap::default(),
            job_statistics: JobStatisticsTimeSeries::default(),
        }
    }

    /// Process an individual [`Message`]. This function doesn't block as the scheduler is
    /// implemented as an async state machine.
    pub fn receive_message(&mut self, deps: &mut DepsT, msg: Message<DepsT>) {
        match msg {
            Message::ClientConnected(id, sender) => self.receive_client_connected(id, sender),
            Message::ClientDisconnected(id) => self.receive_client_disconnected(deps, id),
            Message::FromClient(cid, ClientToBroker::JobRequest(cjid, spec)) => {
                self.receive_client_job_request(deps, cid, cjid, spec)
            }
            Message::FromClient(cid, ClientToBroker::StatisticsRequest) => {
                self.receive_client_statistics_request(deps, cid)
            }
            Message::FromClient(cid, ClientToBroker::JobStateCountsRequest) => {
                self.receive_client_job_state_counts(deps, cid)
            }
            Message::WorkerConnected(id, slots, sender) => {
                self.receive_worker_connected(deps, id, slots, sender)
            }
            Message::WorkerDisconnected(id) => self.receive_worker_disconnected(deps, id),
            Message::FromWorker(wid, WorkerToBroker(jid, result)) => {
                self.receive_worker_response(deps, wid, jid, result)
            }
            Message::GotArtifact(digest, size, path) => {
                self.receive_got_artifact(deps, digest, size, path)
            }
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
        lhs.cmp(&rhs) == std::cmp::Ordering::Less
    }

    fn update_index(&mut self, elem: &WorkerId, idx: HeapIndex) {
        self.0.get_mut(elem).unwrap().heap_index = idx;
    }
}

pub struct Scheduler<CacheT, DepsT: SchedulerDeps> {
    cache: CacheT,
    clients: HashMap<ClientId, Client<DepsT>>,
    workers: WorkerMap<DepsT>,
    queued_requests: VecDeque<JobId>,
    worker_heap: Heap<WorkerMap<DepsT>>,
    job_statistics: JobStatisticsTimeSeries,
}

impl<CacheT: SchedulerCache, DepsT: SchedulerDeps> Scheduler<CacheT, DepsT> {
    fn possibly_start_jobs(&mut self, deps: &mut DepsT) {
        while !self.queued_requests.is_empty() && !self.workers.0.is_empty() {
            let wid = self.worker_heap.peek().unwrap();
            let worker = self.workers.0.get_mut(wid).unwrap();

            if worker.pending.len() == 2 * worker.slots {
                break;
            }

            let jid = self.queued_requests.pop_front().unwrap();
            let spec = &self
                .clients
                .get(&jid.cid)
                .unwrap()
                .jobs
                .get(&jid.cjid)
                .unwrap()
                .spec;
            deps.send_message_to_worker(
                &mut worker.sender,
                BrokerToWorker::EnqueueJob(jid, spec.clone()),
            );

            worker.pending.insert(jid).assert_is_true();
            let heap_index = worker.heap_index;
            self.worker_heap.sift_down(&mut self.workers, heap_index);
        }
    }

    fn receive_client_connected(&mut self, id: ClientId, sender: DepsT::ClientSender) {
        self.clients
            .insert(id, Client::new(sender))
            .assert_is_none();
    }

    fn receive_client_disconnected(&mut self, deps: &mut DepsT, id: ClientId) {
        self.cache.client_disconnected(id);

        let client = self.clients.remove(&id).unwrap();
        for job in client.jobs.into_values() {
            for artifact in job.acquired_artifacts {
                self.cache.decrement_refcount(artifact);
            }
        }

        self.queued_requests.retain(|JobId { cid, .. }| *cid != id);
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
        self.possibly_start_jobs(deps);
    }

    fn ensure_artifact_for_job(
        &mut self,
        deps: &mut DepsT,
        digest: Sha256Digest,
        jid: JobId,
        is_manifest: IsManifest,
    ) {
        let client = self.clients.get_mut(&jid.cid).unwrap();
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
        let client = self.clients.get_mut(&cid).unwrap();
        let layers = spec.layers.clone();
        client.jobs.insert(cjid, Job::new(spec)).assert_is_none();

        for (digest, type_) in layers {
            let is_manifest = IsManifest::from(type_ == ArtifactType::Manifest);
            self.ensure_artifact_for_job(deps, digest, jid, is_manifest);
        }

        let client = self.clients.get_mut(&cid).unwrap();
        let job = client.jobs.get(&jid.cjid).unwrap();
        let have_all_artifacts = job.missing_artifacts.is_empty();
        if have_all_artifacts {
            self.queued_requests.push_back(jid);
            self.possibly_start_jobs(deps);
        }
    }

    fn receive_client_statistics_request(&mut self, deps: &mut DepsT, cid: ClientId) {
        let worker_iter = self.workers.0.iter();
        let resp = BrokerToClient::StatisticsResponse(BrokerStatistics {
            worker_statistics: worker_iter
                .map(|(id, w)| (*id, WorkerStatistics { slots: w.slots }))
                .collect(),
            job_statistics: self.job_statistics.clone(),
        });
        deps.send_message_to_client(&mut self.clients.get_mut(&cid).unwrap().sender, resp);
    }

    fn receive_client_job_state_counts(&mut self, deps: &mut DepsT, cid: ClientId) {
        let resp =
            BrokerToClient::JobStateCountsResponse(self.sample_job_statistics_for_client(cid));
        deps.send_message_to_client(&mut self.clients.get_mut(&cid).unwrap().sender, resp);
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
        self.possibly_start_jobs(deps);
    }

    fn receive_worker_disconnected(&mut self, deps: &mut DepsT, id: WorkerId) {
        let mut worker = self.workers.0.remove(&id).unwrap();
        self.worker_heap
            .remove(&mut self.workers, worker.heap_index);

        // We sort the requests to keep our tests deterministic.
        let mut vec: Vec<_> = worker.pending.drain().collect();
        vec.sort();
        for jid in vec.into_iter().rev() {
            self.queued_requests.push_front(jid);
        }

        self.possibly_start_jobs(deps);
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

        let client = self.clients.get_mut(&jid.cid).unwrap();
        deps.send_message_to_client(
            &mut client.sender,
            BrokerToClient::JobResponse(jid.cjid, result),
        );
        let job = client.jobs.remove(&jid.cjid).unwrap();
        for artifact in job.acquired_artifacts {
            self.cache.decrement_refcount(artifact);
        }
        client.num_completed_jobs += 1;

        if let Some(jid) = self.queued_requests.pop_front() {
            let spec = &self
                .clients
                .get(&jid.cid)
                .unwrap()
                .jobs
                .get(&jid.cjid)
                .unwrap()
                .spec;
            // If there are any queued_requests, we can just pop one off of the front of
            // the queue and not have to update the worker's used slot count or position in the
            // workers list.
            deps.send_message_to_worker(
                &mut worker.sender,
                BrokerToWorker::EnqueueJob(jid, spec.clone()),
            );
            worker.pending.insert(jid);
        } else {
            // Since there are no queued_requests, we're going to have to update the
            // worker's position in the workers list.
            let heap_index = worker.heap_index;
            self.worker_heap.sift_up(&mut self.workers, heap_index);
        }
    }

    fn ensure_manifest_artifacts_for_job(
        &mut self,
        deps: &mut DepsT,
        jid: JobId,
        digest: Sha256Digest,
    ) -> Result<()> {
        for entry in self.cache.read_manifest(digest)? {
            let entry = entry?;
            if let ManifestEntryData::File(Some(digest)) = entry.data {
                self.ensure_artifact_for_job(deps, digest, jid, IsManifest::NotManifest);
            }
        }
        Ok(())
    }

    fn receive_got_artifact(
        &mut self,
        deps: &mut DepsT,
        digest: Sha256Digest,
        size: u64,
        path: PathBuf,
    ) {
        for jid in self.cache.got_artifact(digest.clone(), size, &path) {
            let client = self.clients.get_mut(&jid.cid).unwrap();
            let job = client.jobs.get_mut(&jid.cjid).unwrap();
            job.acquired_artifacts
                .insert(digest.clone())
                .assert_is_true();
            let is_manifest = job.missing_artifacts.remove(&digest.clone()).unwrap();

            if is_manifest.is_manifest() {
                self.ensure_manifest_artifacts_for_job(deps, jid, digest.clone())
                    .unwrap();
            }

            let client = self.clients.get_mut(&jid.cid).unwrap();
            let job = client.jobs.get_mut(&jid.cjid).unwrap();
            if job.missing_artifacts.is_empty() {
                self.queued_requests.push_back(jid);
            }
        }
        self.possibly_start_jobs(deps);
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
        let client = self.clients.get(&cid).unwrap();
        let jobs = &client.jobs;

        let mut counts = JobStateCounts::default();
        counts[JobState::WaitingForArtifacts] = jobs
            .values()
            .filter(|job| !job.missing_artifacts.is_empty())
            .count() as u64;

        counts[JobState::Pending] = self
            .queued_requests
            .iter()
            .filter(|jid| jid.cid == cid)
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
        manifest::{ManifestEntry, ManifestEntryMetadata, ManifestWriter, Mode, UnixTimestamp},
        proto::BrokerToWorker::{self, *},
    };
    use maelstrom_test::*;
    use maplit::hashmap;
    use std::{cell::RefCell, rc::Rc};

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        ToClient(ClientId, BrokerToClient),
        ToWorker(WorkerId, BrokerToWorker),
        ToWorkerArtifactFetcher(u32, Result<(PathBuf, u64), GetArtifactForWorkerError>),
        CacheGetArtifact(JobId, Sha256Digest),
        CacheGotArtifact(Sha256Digest, u64, PathBuf),
        CacheDecrementRefcount(Sha256Digest),
        CacheClientDisconnected(ClientId),
        CacheGetArtifactForWorker(Sha256Digest),
    }

    use TestMessage::*;

    struct TestClientSender(ClientId);
    struct TestWorkerSender(WorkerId);
    struct TestWorkerArtifactFetcherSender(u32);

    #[derive(Default)]
    struct TestState {
        messages: Vec<TestMessage>,
        get_artifact_returns: HashMap<(JobId, Sha256Digest), Vec<GetArtifact>>,
        got_artifact_returns: HashMap<Sha256Digest, Vec<Vec<JobId>>>,
        #[allow(clippy::type_complexity)]
        get_artifact_for_worker_returns:
            HashMap<Sha256Digest, Vec<Result<(PathBuf, u64), GetArtifactForWorkerError>>>,
        read_manifest_returns: HashMap<Sha256Digest, Vec<ManifestEntry>>,
    }

    impl SchedulerCache for Rc<RefCell<TestState>> {
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
        fn got_artifact(&mut self, digest: Sha256Digest, size: u64, path: &Path) -> Vec<JobId> {
            self.borrow_mut().messages.push(CacheGotArtifact(
                digest.clone(),
                size,
                path.to_owned(),
            ));
            self.borrow_mut()
                .got_artifact_returns
                .get_mut(&digest)
                .unwrap()
                .remove(0)
        }
        fn read_manifest(
            &mut self,
            digest: Sha256Digest,
        ) -> Result<ManifestReader<impl io::Read + io::Seek + 'static>> {
            let mut manifest_data = vec![];
            let mut writer = ManifestWriter::new(&mut manifest_data).unwrap();
            writer
                .write_entries(
                    self.borrow_mut()
                        .read_manifest_returns
                        .get(&digest)
                        .unwrap(),
                )
                .unwrap();
            Ok(ManifestReader::new(io::Cursor::new(manifest_data))?)
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
        fn get_artifact_for_worker(
            &mut self,
            digest: &Sha256Digest,
        ) -> Result<(PathBuf, u64), GetArtifactForWorkerError> {
            self.borrow_mut()
                .messages
                .push(CacheGetArtifactForWorker(digest.clone()));
            self.borrow_mut()
                .get_artifact_for_worker_returns
                .get_mut(digest)
                .unwrap()
                .remove(0)
        }
    }

    impl SchedulerDeps for Rc<RefCell<TestState>> {
        type ClientSender = TestClientSender;
        type WorkerSender = TestWorkerSender;
        type WorkerArtifactFetcherSender = TestWorkerArtifactFetcherSender;

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

        fn send_message_to_worker_artifact_fetcher(
            &mut self,
            sender: &mut TestWorkerArtifactFetcherSender,
            message: Result<(PathBuf, u64), GetArtifactForWorkerError>,
        ) {
            self.borrow_mut()
                .messages
                .push(ToWorkerArtifactFetcher(sender.0, message));
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
            get_artifact_for_worker_returns: [(
                Sha256Digest,
                Vec<Result<(PathBuf, u64), GetArtifactForWorkerError>>,
            ); N],
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

        fn receive_message(&mut self, msg: Message<Rc<RefCell<TestState>>>) {
            self.scheduler.receive_message(&mut self.test_state, msg);
        }
    }

    macro_rules! client_sender {
        [$n:expr] => { TestClientSender(cid![$n]) };
    }

    macro_rules! worker_sender {
        [$n:expr] => { TestWorkerSender(wid![$n]) };
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

        fixture.receive_message(FromWorker(wid![1], WorkerToBroker(jid![1], outcome![1])));
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
        FromWorker(wid![1], WorkerToBroker(jid![1], outcome![1])) => {};
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
        FromWorker(wid![1], WorkerToBroker(jid![1], outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], outcome![1])),
            CacheDecrementRefcount(digest![1]),
        };
    }

    script_test! {
        response_from_worker_for_disconnected_client_ignored,
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        FromWorker(wid![1], WorkerToBroker(jid![1], outcome![1])) => {};
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

        FromWorker(wid![1], WorkerToBroker(jid![1, 1], outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], outcome![1])),
            CacheDecrementRefcount(digest![1]),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![8], spec![8, Tar])) => {
            CacheGetArtifact(jid![1, 8], digest![8]),
            ToWorker(wid![1], EnqueueJob(jid![1, 8], spec![8, Tar])),
        };

        FromWorker(wid![2], WorkerToBroker(jid![1, 2], outcome![2])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], outcome![2])),
            CacheDecrementRefcount(digest![2]),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![9], spec![9, Tar])) => {
            CacheGetArtifact(jid![1, 9], digest![9]),
            ToWorker(wid![2], EnqueueJob(jid![1, 9], spec![9, Tar])),
        };

        FromWorker(wid![3], WorkerToBroker(jid![1, 3], outcome![3])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![3], outcome![3])),
            CacheDecrementRefcount(digest![3]),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![10], spec![10, Tar])) => {
            CacheGetArtifact(jid![1, 10], digest![10]),
            ToWorker(wid![3], EnqueueJob(jid![1, 10], spec![10, Tar])),
        };
    }

    script_test! {
        requests_start_queueing_at_2x_workers_slot_count,
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
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![6], spec![6, Tar])) => {
            CacheGetArtifact(jid![1, 6], digest![6]),
        };

        // 2/2 1/2
        FromWorker(wid![2], WorkerToBroker(jid![1, 2], outcome![2])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], outcome![2])),
            CacheDecrementRefcount(digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 5], spec![5, Tar])),
        };

        // 1/2 2/2
        FromWorker(wid![1], WorkerToBroker(jid![1, 1], outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], outcome![1])),
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
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar])) => {
            CacheGetArtifact(jid![1, 2], digest!(2)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar])) => {
            CacheGetArtifact(jid![1, 3], digest!(3)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![4], spec![4, Tar])) => {
            CacheGetArtifact(jid![1, 4], digest!(4)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![5], spec![5, Tar])) => {
            CacheGetArtifact(jid![1, 5], digest!(5)),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![6], spec![6, Tar])) => {
            CacheGetArtifact(jid![1, 6], digest!(6)),
        };

        WorkerConnected(wid![1], 2, worker_sender![1]) => {
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
            ToWorker(wid![1], EnqueueJob(jid![1, 2], spec![2, Tar])),
            ToWorker(wid![1], EnqueueJob(jid![1, 3], spec![3, Tar])),
            ToWorker(wid![1], EnqueueJob(jid![1, 4], spec![4, Tar])),
        };

        WorkerConnected(wid![2], 2, worker_sender![2]) => {
            ToWorker(wid![2], EnqueueJob(jid![1, 5], spec![5, Tar])),
            ToWorker(wid![2], EnqueueJob(jid![1, 6], spec![6, Tar])),
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

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar])) => {
            CacheGetArtifact(jid![1, 2], digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec![2, Tar])),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar])) => {
            CacheGetArtifact(jid![1, 3], digest![3]),
            ToWorker(wid![3], EnqueueJob(jid![1, 3], spec![3, Tar])),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![4], spec![4, Tar])) => {
            CacheGetArtifact(jid![1, 4], digest![4]),
            ToWorker(wid![1], EnqueueJob(jid![1, 4], spec![4, Tar])),
        };

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![5], spec![5, Tar])) => {
            CacheGetArtifact(jid![1, 5], digest![5]),
            ToWorker(wid![2], EnqueueJob(jid![1, 5], spec![5, Tar])),
        };

        WorkerDisconnected(wid![1]) => {
            ToWorker(wid![3], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };

        FromWorker(wid![2], WorkerToBroker(jid![1, 2], outcome![2])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], outcome![2])),
            CacheDecrementRefcount(digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 4], spec![4, Tar])),
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

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar])) => {
            CacheGetArtifact(jid![1, 3], digest![3]),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![4], spec![4, Tar])) => {
            CacheGetArtifact(jid![1, 4], digest![4]),
        };

        FromWorker(wid![1], WorkerToBroker(jid![1, 1], outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], outcome![1])),
            CacheDecrementRefcount(digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 3], spec![3, Tar])),
        };

        WorkerConnected(wid![2], 1, worker_sender![2]) => {
            ToWorker(wid![2], EnqueueJob(jid![1, 4], spec![4, Tar])),
        };

        WorkerDisconnected(wid![1]) => {
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec![2, Tar])),
        };

        FromWorker(wid![2], WorkerToBroker(jid![1, 2], outcome![2])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], outcome![2])),
            CacheDecrementRefcount(digest![2]),
            ToWorker(wid![2], EnqueueJob(jid![1, 3], spec![3, Tar])),
        };
    }

    script_test! {
        requests_outstanding_on_disconnected_worker_go_to_head_of_queue_for_other_workers,
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

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar])) => {
            CacheGetArtifact(jid![1, 3], digest![3]),
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![4], spec![4, Tar])) => {
            CacheGetArtifact(jid![1, 4], digest![4]),
        };

        WorkerDisconnected(wid![1]) => {};

        WorkerConnected(wid![2], 1, worker_sender![2]) => {
            ToWorker(wid![2], EnqueueJob(jid![1, 1], spec![1, Tar])),
            ToWorker(wid![2], EnqueueJob(jid![1, 2], spec![2, Tar])),
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

        FromWorker(wid![1], WorkerToBroker(jid![1, 1], outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], outcome![1])),
            CacheDecrementRefcount(digest![1]),
        };

        FromWorker(wid![1], WorkerToBroker(jid![1, 2], outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], outcome![1])),
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
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![3], spec![3, Tar])) => {
            CacheGetArtifact(jid!(1, 3), digest![3]),
        };

        ClientDisconnected(cid![2]) => {
            CacheDecrementRefcount(digest![1]),
            CacheClientDisconnected(cid![2]),
        };

        FromWorker(wid![1], WorkerToBroker(jid![1, 1], outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], outcome![1])),
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
        };
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![2, Tar])) => {
            CacheGetArtifact(jid!(1, 2), digest![2]),
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

        FromWorker(wid![1], WorkerToBroker(jid![1, 2], outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![2], outcome![1])),
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
        };

        GotArtifact(digest![43], 100, "/z/tmp/foo".into()) => {
            CacheGotArtifact(digest![43], 100, "/z/tmp/foo".into()),
        };
        GotArtifact(digest![44], 100, "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![44], 100, "/z/tmp/bar".into()),
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
        };

        GotArtifact(digest![42], 100, "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![42], 100, "/z/tmp/bar".into()),
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
                    data: ManifestEntryData::File(Some(digest![43])),
                }])
            ])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Manifest)]])) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![42])),
        };

        GotArtifact(digest![42], 100, "/z/tmp/foo".into()) => {
            CacheGotArtifact(digest![42], 100, "/z/tmp/foo".into()),
            CacheGetArtifact(jid![1, 2], digest![43]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![43])),
        };

        GotArtifact(digest![43], 100, "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![43], 100, "/z/tmp/bar".into()),
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
                        data: ManifestEntryData::File(Some(digest![43])),
                    },
                    ManifestEntry {
                        path: "bar.txt".into(),
                        metadata: ManifestEntryMetadata {
                            size: 11,
                            mode: Mode(0o0555),
                            mtime: UnixTimestamp(1705538554),
                        },
                        data: ManifestEntryData::File(Some(digest![43])),
                    }
                ])
            ])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Manifest)]])) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![42])),
        };

        GotArtifact(digest![42], 100, "/z/tmp/foo".into()) => {
            CacheGotArtifact(digest![42], 100, "/z/tmp/foo".into()),
            CacheGetArtifact(jid![1, 2], digest![43]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![43])),
        };

        GotArtifact(digest![43], 100, "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![43], 100, "/z/tmp/bar".into()),
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
                    data: ManifestEntryData::File(Some(digest![43])),
                }])
            ])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Manifest)]])) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            CacheGetArtifact(jid![1, 2], digest![43]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![43])),
        };

        GotArtifact(digest![43], 100, "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![43], 100, "/z/tmp/bar".into()),
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
                    data: ManifestEntryData::File(Some(digest![43])),
                }])
            ])
        },
        WorkerConnected(wid![1], 1, worker_sender![1]) => {};
        ClientConnected(cid![1], client_sender![1]) => {};

        FromClient(cid![1], ClientToBroker::JobRequest(cjid![2], spec![1, [(42, Manifest)]])) => {
            CacheGetArtifact(jid![1, 2], digest![42]),
            CacheGetArtifact(jid![1, 2], digest![43]),
            ToClient(cid![1], BrokerToClient::TransferArtifact(digest![43])),
        };

        GotArtifact(digest![43], 100, "/z/tmp/bar".into()) => {
            CacheGotArtifact(digest![43], 100, "/z/tmp/bar".into()),
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
                    data: ManifestEntryData::File(Some(digest![43])),
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
                        data: ManifestEntryData::File(Some(digest![43])),
                    },
                    ManifestEntry {
                        path: "bar.txt".into(),
                        metadata: ManifestEntryMetadata {
                            size: 11,
                            mode: Mode(0o0555),
                            mtime: UnixTimestamp(1705538554),
                        },
                        data: ManifestEntryData::File(Some(digest![43])),
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
                    vec![Ok(("/a/good/path".into(), 42))],
                ),
            ], [])
        },
        GetArtifactForWorker(digest![42], worker_artifact_fetcher_sender![1]) => {
            CacheGetArtifactForWorker(digest![42]),
            ToWorkerArtifactFetcher(1, Ok(("/a/good/path".into(), 42))),
        }
    }

    script_test! {
        get_artifact_for_worker_manifest,
        {
            Fixture::new([], [], [
                (
                    digest![42],
                    vec![Ok(("/a/good/path".into(), 42))]
                ),
            ], [])
        },
        GetArtifactForWorker(digest![42], worker_artifact_fetcher_sender![1]) => {
            CacheGetArtifactForWorker(digest![42]),
            ToWorkerArtifactFetcher(1, Ok(("/a/good/path".into(), 42)))
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
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, [(42, Tar)]])) => {
            CacheGetArtifact(jid![1, 1], digest![42]),
        };
        StatisticsHeartbeat => {};
        FromClient(cid![1], ClientToBroker::StatisticsRequest) => {
            ToClient(cid![1], BrokerToClient::StatisticsResponse(BrokerStatistics {
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
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
        };
        StatisticsHeartbeat => {};
        FromClient(cid![1], ClientToBroker::StatisticsRequest) => {
            ToClient(cid![1], BrokerToClient::StatisticsResponse(BrokerStatistics {
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
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };
        StatisticsHeartbeat => {};
        FromClient(cid![1], ClientToBroker::StatisticsRequest) => {
            ToClient(cid![1], BrokerToClient::StatisticsResponse(BrokerStatistics {
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
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };
        FromWorker(wid![1], WorkerToBroker(jid![1, 1], outcome![1])) => {
            ToClient(cid![1], BrokerToClient::JobResponse(cjid![1], outcome![1])),
            CacheDecrementRefcount(digest![1]),
        };
        StatisticsHeartbeat => {};
        FromClient(cid![1], ClientToBroker::StatisticsRequest) => {
            ToClient(cid![1], BrokerToClient::StatisticsResponse(BrokerStatistics {
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
        job_state_counts,
        {
            Fixture::new([
                ((jid![1, 1], digest![1]), vec![GetArtifact::Success]),
            ], [], [], [])
        },
        ClientConnected(cid![1], client_sender![1]) => {};
        WorkerConnected(wid![1], 2, worker_sender![1]) => {};
        FromClient(cid![1], ClientToBroker::JobRequest(cjid![1], spec![1, Tar])) => {
            CacheGetArtifact(jid![1, 1], digest![1]),
            ToWorker(wid![1], EnqueueJob(jid![1, 1], spec![1, Tar])),
        };
        FromClient(cid![1], ClientToBroker::JobStateCountsRequest) => {
            ToClient(cid![1], BrokerToClient::JobStateCountsResponse(
                enum_map! {
                    JobState::WaitingForArtifacts => 0,
                    JobState::Pending => 0,
                    JobState::Running => 1,
                    JobState::Complete => 0,
                }
            ))
        }
    }
}
