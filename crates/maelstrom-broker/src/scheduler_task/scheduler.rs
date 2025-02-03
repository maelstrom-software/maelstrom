//! Central processing module for the broker. Receives and sends messages to and from clients and
//! workers.

use crate::scheduler_task::artifact_gatherer::StartJob;
use enum_map::enum_map;
use maelstrom_base::{
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
    marker::PhantomData,
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

/// The dependencies for [`Scheduler`] on the [`ArtifactGatherer`]. These should be identical to
/// the methods on [`crate::scheduler_task::artifact_gatherer::ArtifactGatherer`]. They are split
/// out from [`Deps`] so they can be implemented separately.
pub trait ArtifactGatherer {
    type ClientSender;
    fn client_connected(&mut self, cid: ClientId, sender: Self::ClientSender);
    fn client_disconnected(&mut self, cid: ClientId);
    fn start_job(&mut self, jid: JobId, layers: NonEmpty<(Sha256Digest, ArtifactType)>)
        -> StartJob;
    fn job_completed(&mut self, jid: JobId);
    fn get_waiting_for_artifacts_count(&self, cid: ClientId) -> u64;
}

/// The external dependencies for [`Scheduler`] that aren't the [`ArtifactGatherer`]. All of these
/// methods must be asynchronous: they must not block the current thread.
pub trait Deps {
    type ClientSender: Clone;
    type WorkerSender;
    type MonitorSender;
    fn send_job_response_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        cjid: ClientJobId,
        result: JobOutcomeResult,
    );
    fn send_job_status_update_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        cjid: ClientJobId,
        status: JobBrokerStatus,
    );
    fn send_enqueue_job_to_worker(
        &mut self,
        sender: &mut Self::WorkerSender,
        jid: JobId,
        spec: JobSpec,
    );
    fn send_cancel_job_to_worker(&mut self, sender: &mut Self::WorkerSender, jid: JobId);
    fn send_statistics_response_to_monitor(
        &mut self,
        sender: &mut Self::MonitorSender,
        statistics: BrokerStatistics,
    );
}

impl<ArtifactGathererT: ArtifactGatherer, DepsT: Deps> Scheduler<ArtifactGathererT, DepsT> {
    /// Create a new scheduler with the given [`ArtifactGatherer`]. Note that [`SchedulerDeps`] are
    /// passed in to `Self::receive_message`.
    pub fn new(deps: DepsT) -> Self {
        Scheduler {
            deps,
            clients: ClientMap(Default::default()),
            workers: WorkerMap(Default::default()),
            monitors: Default::default(),
            queued_jobs: Default::default(),
            worker_heap: Default::default(),
            job_statistics: Default::default(),
            _artifact_gatherer: Default::default(),
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

struct Client<DepsT: Deps> {
    sender: DepsT::ClientSender,
    jobs: HashMap<ClientJobId, Job>,
    num_completed_jobs: u64,
}

impl<DepsT: Deps> Client<DepsT> {
    fn new(sender: DepsT::ClientSender) -> Self {
        Client {
            sender,
            jobs: HashMap::default(),
            num_completed_jobs: 0,
        }
    }
}

struct ClientMap<DepsT: Deps>(HashMap<ClientId, Client<DepsT>>);

impl<DepsT: Deps> ClientMap<DepsT> {
    fn job_from_jid(&self, jid: JobId) -> &Job {
        self.0.get(&jid.cid).unwrap().jobs.get(&jid.cjid).unwrap()
    }
}

struct Worker<DepsT: Deps> {
    slots: usize,
    pending: HashSet<JobId>,
    heap_index: HeapIndex,
    sender: DepsT::WorkerSender,
}

impl<DepsT: Deps> Worker<DepsT> {
    fn new(slots: usize, sender: DepsT::WorkerSender) -> Self {
        Worker {
            slots,
            sender,
            pending: HashSet::default(),
            heap_index: HeapIndex::default(),
        }
    }
}

struct WorkerMap<DepsT: Deps>(HashMap<WorkerId, Worker<DepsT>>);

impl<DepsT: Deps> HeapDeps for WorkerMap<DepsT> {
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

pub struct Scheduler<ArtifactGathererT, DepsT: Deps> {
    deps: DepsT,
    clients: ClientMap<DepsT>,
    workers: WorkerMap<DepsT>,
    monitors: HashMap<MonitorId, DepsT::MonitorSender>,
    queued_jobs: BinaryHeap<QueuedJob>,
    worker_heap: Heap<WorkerMap<DepsT>>,
    job_statistics: JobStatisticsTimeSeries,
    _artifact_gatherer: PhantomData<ArtifactGathererT>,
}

impl<ArtifactGathererT, DepsT> Scheduler<ArtifactGathererT, DepsT>
where
    DepsT: Deps,
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
            self.deps
                .send_enqueue_job_to_worker(&mut worker.sender, jid, job.spec.clone());
            just_enqueued.remove(&jid);

            worker.pending.insert(jid).assert_is_true();
            let heap_index = worker.heap_index;
            self.worker_heap.sift_down(&mut self.workers, heap_index);
        }
        for jid in just_enqueued {
            let client = self.clients.0.get_mut(&jid.cid).unwrap();
            self.deps.send_job_status_update_to_client(
                &mut client.sender,
                jid.cjid,
                JobBrokerStatus::WaitingForWorker,
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
                    self.deps
                        .send_cancel_job_to_worker(&mut worker.sender, *jid);
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
                self.deps.send_job_status_update_to_client(
                    &mut client.sender,
                    jid.cjid,
                    JobBrokerStatus::WaitingForLayers,
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
        self.deps
            .send_job_response_to_client(&mut client.sender, jid.cjid, result);
        client.jobs.remove(&jid.cjid).assert_is_some();
        artifact_gatherer.job_completed(jid);
        client.num_completed_jobs += 1;

        if let Some(QueuedJob { jid, .. }) = self.queued_jobs.pop() {
            // If there are any queued_requests, we can just pop one off of the front of
            // the queue and not have to update the worker's used slot count or position in the
            // workers list.
            let job = self.clients.job_from_jid(jid);
            self.deps
                .send_enqueue_job_to_worker(&mut worker.sender, jid, job.spec.clone());
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
        self.deps.send_job_status_update_to_client(
            &mut client.sender,
            jid.cjid,
            JobBrokerStatus::AtWorker(wid, status),
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
        let resp = BrokerStatistics {
            worker_statistics: worker_iter
                .map(|(id, w)| (*id, WorkerStatistics { slots: w.slots }))
                .collect(),
            job_statistics: self.job_statistics.clone(),
        };
        self.deps
            .send_statistics_response_to_monitor(self.monitors.get_mut(&mid).unwrap(), resp);
    }

    pub fn receive_jobs_ready_from_artifact_gatherer(&mut self, ready: NonEmpty<JobId>) {
        let just_enqueued = ready
            .into_iter()
            .filter(|jid| match self.clients.0.get_mut(&jid.cid) {
                None => false,
                Some(client) => {
                    let job = client.jobs.get(&jid.cjid).unwrap();
                    self.queued_jobs.push(QueuedJob::new(
                        *jid,
                        job.spec.priority,
                        job.spec.estimated_duration,
                    ));
                    true
                }
            })
            .collect();
        self.possibly_start_jobs(just_enqueued);
    }

    pub fn receive_jobs_failed_from_artifact_gatherer(
        &mut self,
        jobs: NonEmpty<JobId>,
        err: String,
    ) {
        todo!("{jobs:?} {err}");
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
    use super::*;
    use crate::{
        cache::SchedulerCache,
        scheduler_task::{
            artifact_gatherer::Deps as ArtifactGathererDeps,
            ManifestReadRequest,
            Message::{self, *},
        },
    };
    use anyhow::{Error, Result};
    use enum_map::enum_map;
    use itertools::Itertools;
    use maelstrom_base::{
        digest, job_spec,
        proto::{
            BrokerToClient, BrokerToMonitor,
            BrokerToWorker::{self, *},
        },
        tar_digest,
    };
    use maelstrom_test::*;
    use maelstrom_util::cache::{fs::test, GetArtifact};
    use maplit::hashmap;
    use std::{cell::RefCell, path::PathBuf, rc::Rc};

    type MessageM<DepsT, TempFileT> = Message<
        <DepsT as Deps>::ClientSender,
        <DepsT as Deps>::WorkerSender,
        <DepsT as Deps>::MonitorSender,
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
        got_artifact_returns: HashMap<Sha256Digest, Vec<Result<Vec<JobId>, (Error, Vec<JobId>)>>>,
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
        ) -> Result<Vec<JobId>, (Error, Vec<JobId>)> {
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

    impl Deps for Rc<RefCell<TestState>> {
        type ClientSender = TestClientSender;
        type WorkerSender = TestWorkerSender;
        type MonitorSender = TestMonitorSender;

        fn send_job_response_to_client(
            &mut self,
            sender: &mut Self::ClientSender,
            cjid: ClientJobId,
            result: JobOutcomeResult,
        ) {
            self.borrow_mut().messages.push(ToClient(
                sender.0,
                BrokerToClient::JobResponse(cjid, result),
            ));
        }

        fn send_job_status_update_to_client(
            &mut self,
            sender: &mut Self::ClientSender,
            cjid: ClientJobId,
            status: JobBrokerStatus,
        ) {
            self.borrow_mut().messages.push(ToClient(
                sender.0,
                BrokerToClient::JobStatusUpdate(cjid, status),
            ));
        }

        fn send_enqueue_job_to_worker(
            &mut self,
            sender: &mut TestWorkerSender,
            jid: JobId,
            spec: JobSpec,
        ) {
            self.borrow_mut()
                .messages
                .push(ToWorker(sender.0, BrokerToWorker::EnqueueJob(jid, spec)));
        }

        fn send_cancel_job_to_worker(&mut self, sender: &mut TestWorkerSender, jid: JobId) {
            self.borrow_mut()
                .messages
                .push(ToWorker(sender.0, BrokerToWorker::CancelJob(jid)));
        }

        fn send_statistics_response_to_monitor(
            &mut self,
            sender: &mut TestMonitorSender,
            statistics: BrokerStatistics,
        ) {
            self.borrow_mut().messages.push(ToMonitor(
                sender.0,
                BrokerToMonitor::StatisticsResponse(statistics),
            ));
        }
    }

    impl ArtifactGathererDeps for Rc<RefCell<TestState>> {
        type ArtifactStream = TestArtifactStream;
        type WorkerArtifactFetcherSender = TestWorkerArtifactFetcherSender;
        type ClientSender = TestClientSender;

        fn send_read_request_to_manifest_reader(
            &mut self,
            manifest_stream: Self::ArtifactStream,
            manifest_digest: Sha256Digest,
        ) {
            self.borrow_mut()
                .messages
                .push(ReadManifest(ManifestReadRequest {
                    manifest_stream,
                    digest: manifest_digest,
                }));
        }

        fn send_response_to_worker_artifact_fetcher(
            &mut self,
            sender: &mut TestWorkerArtifactFetcherSender,
            message: Option<(PathBuf, u64)>,
        ) {
            self.borrow_mut()
                .messages
                .push(ToWorkerArtifactFetcher(sender.0, message));
        }

        fn send_transfer_artifact_to_client(
            &mut self,
            sender: &mut TestClientSender,
            digest: Sha256Digest,
        ) {
            self.borrow_mut()
                .messages
                .push(ToClient(sender.0, BrokerToClient::TransferArtifact(digest)));
        }

        fn send_general_error_to_client(&mut self, sender: &mut TestClientSender, error: String) {
            self.borrow_mut()
                .messages
                .push(ToClient(sender.0, BrokerToClient::GeneralError(error)));
        }

        fn send_jobs_ready_to_scheduler(&mut self, jobs: NonEmpty<JobId>) {
            todo!("{jobs:?}");
        }

        fn send_jobs_failed_to_scheduler(&mut self, jobs: NonEmpty<JobId>, err: String) {
            todo!("{jobs:?} {err}");
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
                    1_000_000,
                    10.try_into().unwrap(),
                ),
                scheduler: Scheduler::new(test_state.clone()),
            }
        }
    }

    impl Fixture {
        #[allow(clippy::type_complexity)]
        fn new<const L: usize, const M: usize, const N: usize>(
            get_artifact_returns: [((JobId, Sha256Digest), Vec<GetArtifact>); L],
            got_artifact_returns: [(Sha256Digest, Vec<Result<Vec<JobId>, (Error, Vec<JobId>)>>); M],
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
                Message::ArtifactTransferredFromClient(cid, digest, location) => self
                    .artifact_gatherer
                    .receive_artifact_transferred(cid, digest, location),
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
                Message::JobsReadyFromArtifactGatherer(ready) => {
                    self.scheduler
                        .receive_jobs_ready_from_artifact_gatherer(ready);
                }
                Message::JobsFailedFromArtifactGatherer(jid, err) => {
                    self.scheduler
                        .receive_jobs_failed_from_artifact_gatherer(jid, err);
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
    use maelstrom_base::{job_spec, tar_digest};
    use maelstrom_test::outcome;
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

    #[derive(Default)]
    struct Mock {
        // ArtifactGatherer
        client_connected: HashSet<ClientId>,
        client_disconnected: HashSet<ClientId>,
        start_job: Vec<(JobId, NonEmpty<(Sha256Digest, ArtifactType)>, StartJob)>,
        complete_job: HashSet<JobId>,
        // Deps
        send_job_response_to_client: Vec<(ClientId, ClientJobId, JobOutcomeResult)>,
        send_job_status_update_to_client: Vec<(ClientId, ClientJobId, JobBrokerStatus)>,
        send_enqueue_job_to_worker: Vec<(WorkerId, JobId, JobSpec)>,
        send_cancel_job_to_worker: HashSet<(WorkerId, JobId)>,
        send_statistics_response_to_monitor: Vec<(MonitorId, BrokerStatistics)>,
    }

    impl Mock {
        fn assert_is_empty(&self) {
            assert!(
                self.client_connected.is_empty(),
                "unused mock entries for ArtifactGatherer::client_connected: {:?}",
                self.client_connected,
            );
            assert!(
                self.client_disconnected.is_empty(),
                "unused mock entries for ArtifactGatherer::client_disconnected: {:?}",
                self.client_disconnected,
            );
            assert!(
                self.start_job.is_empty(),
                "unused mock entries for ArtifactGatherer::start_job: {:?}",
                self.start_job,
            );
            assert!(
                self.complete_job.is_empty(),
                "unused mock entries for ArtifactGatherer::complete_job: {:?}",
                self.complete_job,
            );
            assert!(
                self.send_job_response_to_client.is_empty(),
                "unused mock entries for ArtifactGatherer::send_job_response_to_client: {:?}",
                self.send_job_response_to_client,
            );
            assert!(
                self.send_job_status_update_to_client.is_empty(),
                "unused mock entries for Deps::send_job_status_update_to_client: {:?}",
                self.send_job_status_update_to_client,
            );
            assert!(
                self.send_enqueue_job_to_worker.is_empty(),
                "unused mock entries for Deps::send_enqueue_job_to_worker: {:?}",
                self.send_enqueue_job_to_worker,
            );
            assert!(
                self.send_cancel_job_to_worker.is_empty(),
                "unused mock entries for Deps::send_cancel_job_to_worker: {:?}",
                self.send_cancel_job_to_worker,
            );
            assert!(
                self.send_statistics_response_to_monitor.is_empty(),
                "unused mock entries for Deps::send_statistics_response_to_monitor: {:?}",
                self.send_statistics_response_to_monitor,
            );
        }
    }

    impl ArtifactGatherer for Rc<RefCell<Mock>> {
        type ClientSender = TestClientSender;

        fn client_connected(&mut self, cid: ClientId, sender: TestClientSender) {
            assert_eq!(sender.0, cid);
            assert!(
                self.borrow_mut().client_connected.remove(&cid),
                "sending unexpected client_connected to artifact gatherer for client {cid}",
            );
        }

        fn client_disconnected(&mut self, cid: ClientId) {
            assert!(
                self.borrow_mut().client_disconnected.remove(&cid),
                "sending unexpected client_disconnected to artifact gatherer for client {cid}",
            );
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

        fn job_completed(&mut self, jid: JobId) {
            assert!(
                self.borrow_mut().complete_job.remove(&jid),
                "sending unexpected complete_job to artifact gatherer for job {jid}"
            );
        }

        fn get_waiting_for_artifacts_count(&self, cid: ClientId) -> u64 {
            todo!("{cid:?}");
        }
    }

    impl Deps for Rc<RefCell<Mock>> {
        type ClientSender = TestClientSender;
        type WorkerSender = TestWorkerSender;
        type MonitorSender = TestMonitorSender;

        fn send_job_response_to_client(
            &mut self,
            sender: &mut Self::ClientSender,
            cjid: ClientJobId,
            result: JobOutcomeResult,
        ) {
            let send_job_response_to_client = &mut self.borrow_mut().send_job_response_to_client;
            let index = send_job_response_to_client
                .iter()
                .position(|e| e.0 == sender.0 && e.1 == cjid && e.2 == result)
                .expect(&format!(
                    "sending unexpected job_response to client {sender:?}: {cjid} {result:?}"
                ));
            let _ = send_job_response_to_client.remove(index);
        }

        fn send_job_status_update_to_client(
            &mut self,
            sender: &mut Self::ClientSender,
            cjid: ClientJobId,
            status: JobBrokerStatus,
        ) {
            let send_job_status_update_to_client =
                &mut self.borrow_mut().send_job_status_update_to_client;
            let index = send_job_status_update_to_client
                .iter()
                .position(|e| e.0 == sender.0 && e.1 == cjid && e.2 == status)
                .expect(&format!(
                    "sending unexpected job_status_update to client {sender:?}: {cjid} {status:?}"
                ));
            send_job_status_update_to_client.remove(index);
        }

        fn send_enqueue_job_to_worker(
            &mut self,
            sender: &mut Self::WorkerSender,
            jid: JobId,
            spec: JobSpec,
        ) {
            let vec = &mut self.borrow_mut().send_enqueue_job_to_worker;
            let wid = sender.0;
            let spec_clone = spec.clone();
            let index = vec
                .iter()
                .position(move |e| e == &(wid, jid, spec_clone.clone()))
                .expect(&format!(
                    "sending unexpected enqueue_job to worker {wid}: {jid} {spec:?}"
                ));
            vec.remove(index);
        }

        fn send_cancel_job_to_worker(&mut self, sender: &mut Self::WorkerSender, jid: JobId) {
            let wid = sender.0;
            assert!(
                self.borrow_mut()
                    .send_cancel_job_to_worker
                    .remove(&(wid, jid)),
                "sending unexpected cancel_job to worker {wid}: {jid}",
            );
        }

        fn send_statistics_response_to_monitor(
            &mut self,
            sender: &mut Self::MonitorSender,
            statistics: BrokerStatistics,
        ) {
            let vec = &mut self.borrow_mut().send_statistics_response_to_monitor;
            let mid = sender.0;
            let statistics_clone = statistics.clone();
            let index = vec
                .iter()
                .position(move |e| e == &(mid, statistics_clone.clone()))
                .expect(&format!(
                    "sending unexpected message to monitor {sender:?}: {statistics:#?}"
                ));
            vec.remove(index);
        }
    }

    struct Fixture {
        mock: Rc<RefCell<Mock>>,
        sut: Scheduler<Rc<RefCell<Mock>>, Rc<RefCell<Mock>>>,
    }

    impl Fixture {
        fn new() -> Self {
            let mock = Rc::new(RefCell::new(Default::default()));
            let sut = Scheduler::new(mock.clone());
            Self { mock, sut }
        }

        fn with_client(mut self, cid: impl Into<ClientId>) -> Self {
            let cid = cid.into();
            self.expect()
                .client_connected(cid)
                .when()
                .receive_client_connected(cid);
            self
        }

        fn with_worker(mut self, wid: impl Into<WorkerId>, slots: usize) -> Self {
            let wid = wid.into();
            self.receive_worker_connected(wid, slots);
            self
        }

        fn expect(&mut self) -> Expect {
            Expect { fixture: self }
        }

        fn receive_client_connected(&mut self, cid: impl Into<ClientId>) {
            let cid = cid.into();
            self.sut
                .receive_client_connected(&mut self.mock, cid, TestClientSender(cid));
        }

        fn receive_client_disconnected(&mut self, cid: impl Into<ClientId>) {
            self.sut
                .receive_client_disconnected(&mut self.mock, cid.into());
        }

        fn receive_job_request_from_client(
            &mut self,
            cid: impl Into<ClientId>,
            cjid: impl Into<ClientJobId>,
            spec: impl Into<JobSpec>,
        ) {
            self.sut.receive_job_request_from_client(
                &mut self.mock,
                cid.into(),
                cjid.into(),
                spec.into(),
            );
        }

        fn receive_worker_connected(&mut self, wid: impl Into<WorkerId>, slots: usize) {
            let wid = wid.into();
            self.sut
                .receive_worker_connected(wid, slots, TestWorkerSender(wid));
        }

        fn receive_worker_disconnected(&mut self, wid: impl Into<WorkerId>) {
            self.sut.receive_worker_disconnected(wid.into());
        }

        fn receive_job_response_from_worker(
            &mut self,
            wid: impl Into<WorkerId>,
            jid: impl Into<JobId>,
            result: impl Into<JobOutcomeResult>,
        ) {
            self.sut.receive_job_response_from_worker(
                &mut self.mock,
                wid.into(),
                jid.into(),
                result.into(),
            );
        }

        fn receive_job_status_update_from_worker(
            &mut self,
            wid: impl Into<WorkerId>,
            jid: impl Into<JobId>,
            status: impl Into<JobWorkerStatus>,
        ) {
            self.sut
                .receive_job_status_update_from_worker(wid.into(), jid.into(), status.into());
        }

        fn receive_monitor_connected(&mut self, mid: impl Into<MonitorId>) {
            let mid = mid.into();
            self.sut
                .receive_monitor_connected(mid, TestMonitorSender(mid));
        }

        fn receive_monitor_disconnected(&mut self, mid: impl Into<MonitorId>) {
            self.sut.receive_monitor_disconnected(mid.into());
        }

        fn receive_statistics_request_from_monitor(&mut self, mid: impl Into<MonitorId>) {
            self.sut.receive_statistics_request_from_monitor(mid.into());
        }
    }

    struct Expect<'a> {
        fixture: &'a mut Fixture,
    }

    impl<'a> Drop for Expect<'a> {
        fn drop(&mut self) {
            self.fixture.mock.borrow().assert_is_empty();
        }
    }

    impl<'a> Expect<'a> {
        fn when(self) -> When<'a> {
            When { expect: self }
        }

        fn client_connected(self, cid: impl Into<ClientId>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .client_connected
                .insert(cid.into())
                .assert_is_true();
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
            jid: impl Into<JobId>,
            layers: impl IntoIterator<Item = (impl Into<Sha256Digest>, impl Into<ArtifactType>)>,
            result: impl Into<StartJob>,
        ) -> Self {
            let layers = NonEmpty::collect(
                layers
                    .into_iter()
                    .map(|(digest, type_)| (digest.into(), type_.into())),
            )
            .unwrap();
            self.fixture
                .mock
                .borrow_mut()
                .start_job
                .push((jid.into(), layers, result.into()));
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

        fn complete_job(self, jid: impl Into<JobId>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .complete_job
                .insert(jid.into())
                .assert_is_true();
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

        fn send_job_response_to_client(
            self,
            cid: impl Into<ClientId>,
            cjid: impl Into<ClientJobId>,
            result: impl Into<JobOutcomeResult>,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_job_response_to_client
                .push((cid.into(), cjid.into(), result.into()));
            self
        }

        fn send_job_status_update_to_client(
            self,
            cid: impl Into<ClientId>,
            cjid: impl Into<ClientJobId>,
            status: impl Into<JobBrokerStatus>,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_job_status_update_to_client
                .push((cid.into(), cjid.into(), status.into()));
            self
        }

        fn send_enqueue_job_to_worker(
            self,
            wid: impl Into<WorkerId>,
            jid: impl Into<JobId>,
            spec: impl Into<JobSpec>,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_enqueue_job_to_worker
                .push((wid.into(), jid.into(), spec.into()));
            self
        }

        /*
        fn send_cancel_job_to_worker(
            self,
            wid: impl Into<WorkerId>,
            jid: impl Into<JobId>,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_cancel_job_to_worker
                .insert((wid.into(), jid.into()))
                .assert_is_true();
            self
        }
        */

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
    }

    struct When<'a> {
        expect: Expect<'a>,
    }

    impl<'a> Deref for When<'a> {
        type Target = Fixture;

        fn deref(&self) -> &Self::Target {
            &self.expect.fixture
        }
    }

    impl<'a> DerefMut for When<'a> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.expect.fixture
        }
    }

    #[test]
    #[should_panic]
    fn message_from_unknown_client_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_job_request_from_client(1, 1, job_spec!("test", [tar_digest!(1)]));
    }

    #[test]
    #[should_panic]
    fn disconnect_from_unknown_client_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_client_disconnected(1);
    }

    #[test]
    #[should_panic]
    fn connect_from_duplicate_client_panics() {
        Fixture::new().with_client(1).with_client(1);
    }

    #[test]
    #[should_panic]
    fn message_from_unknown_worker_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_job_status_update_from_worker(1, (1, 1), JobWorkerStatus::Executing);
    }

    #[test]
    #[should_panic]
    fn disconnect_from_unknown_worker_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_worker_disconnected(1);
    }

    #[test]
    #[should_panic]
    fn connect_from_duplicate_worker_panics() {
        Fixture::new().with_worker(1, 2).with_worker(1, 2);
    }

    #[test]
    #[should_panic]
    fn message_from_unknown_monitor_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_statistics_request_from_monitor(1);
    }

    #[test]
    #[should_panic]
    fn disconnect_from_unknown_monitor_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_monitor_disconnected(1);
    }

    #[test]
    #[should_panic]
    fn connect_from_duplicate_monitor_panics() {
        let mut fixture = Fixture::new();
        fixture.receive_monitor_connected(1);
        fixture.receive_monitor_connected(1);
    }

    #[test]
    fn job_request_ready_worker_available() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 2);

        let job_spec = job_spec!("test", [tar_digest!(1), tar_digest!(2)]);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1), tar_digest!(2)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), job_spec.clone())
            .when()
            .receive_job_request_from_client(1, 1, job_spec);
    }

    #[test]
    fn job_request_ready_no_worker_available() {
        let mut fixture = Fixture::new().with_client(1);
        let job_spec = job_spec!("test", [tar_digest!(1), tar_digest!(2)]);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1), tar_digest!(2)], StartJob::Ready)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 1, job_spec.clone());

        fixture
            .expect()
            .send_enqueue_job_to_worker(1, (1, 1), job_spec)
            .when()
            .receive_worker_connected(1, 2);
    }

    #[test]
    fn job_request_not_ready_start_fetching_some() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 2);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1), tar_digest!(2)], StartJob::NotReady)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForLayers)
            .when()
            .receive_job_request_from_client(
                1,
                1,
                job_spec!("test", [tar_digest!(1), tar_digest!(2)]),
            );
    }

    #[test]
    fn job_request_not_ready_start_fetching_none() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 2);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1), tar_digest!(2)], StartJob::NotReady)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForLayers)
            .when()
            .receive_job_request_from_client(
                1,
                1,
                job_spec!("test", [tar_digest!(1), tar_digest!(2)]),
            );
    }

    #[test]
    fn worker_disconnected_with_outstanding_jobs_no_other_available_workers() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 2);
        let job_spec = job_spec!("test", [tar_digest!(1), tar_digest!(2)]);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1), tar_digest!(2)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), job_spec.clone())
            .when()
            .receive_job_request_from_client(1, 1, job_spec.clone());

        fixture
            .expect()
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_worker_disconnected(1);

        fixture
            .expect()
            .send_enqueue_job_to_worker(2, (1, 1), job_spec)
            .when()
            .receive_worker_connected(2, 2);
    }

    #[test]
    fn worker_disconnected_with_outstanding_jobs_with_other_available_workers() {
        let mut fixture = Fixture::new()
            .with_client(1)
            .with_worker(1, 2)
            .with_worker(2, 2);
        let job_spec = job_spec!("test", [tar_digest!(1), tar_digest!(2)]);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1), tar_digest!(2)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), job_spec.clone())
            .when()
            .receive_job_request_from_client(1, 1, job_spec.clone());

        fixture
            .expect()
            .send_enqueue_job_to_worker(2, (1, 1), job_spec)
            .when()
            .receive_worker_disconnected(1);
    }

    #[test]
    fn worker_response_with_no_other_available_jobs() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 2);
        let job_spec = job_spec!("test", [tar_digest!(1), tar_digest!(2)]);
        let result = Ok(outcome!(1));

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1), tar_digest!(2)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), job_spec.clone())
            .when()
            .receive_job_request_from_client(1, 1, job_spec.clone());

        fixture
            .expect()
            .complete_job((1, 1))
            .send_job_response_to_client(1, 1, result.clone())
            .when()
            .receive_job_response_from_worker(1, (1, 1), result.clone());
    }

    #[test]
    fn worker_response_with_other_available_jobs() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 1);
        let job_spec_1 = job_spec!("test_1", [tar_digest!(1), tar_digest!(2)]);
        let job_spec_2 = job_spec!("test_2", [tar_digest!(1)]);
        let job_spec_3 = job_spec!("test_3", [tar_digest!(2)]);
        let result_1 = Ok(outcome!(1));

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1), tar_digest!(2)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), job_spec_1.clone())
            .when()
            .receive_job_request_from_client(1, 1, job_spec_1);

        fixture
            .expect()
            .start_job((1, 2), [tar_digest!(1)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 2), job_spec_2.clone())
            .when()
            .receive_job_request_from_client(1, 2, job_spec_2);

        fixture
            .expect()
            .start_job((1, 3), [tar_digest!(2)], StartJob::Ready)
            .send_job_status_update_to_client(1, 3, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 3, job_spec_3.clone());

        fixture
            .expect()
            .complete_job((1, 1))
            .send_job_response_to_client(1, 1, result_1.clone())
            .send_enqueue_job_to_worker(1, (1, 3), job_spec_3)
            .when()
            .receive_job_response_from_worker(1, (1, 1), result_1);
    }
}
