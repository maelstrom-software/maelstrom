//! Central processing module for the broker. Receives and sends messages to and from clients and
//! workers.

use crate::scheduler_task::artifact_gatherer::StartJob;
use derivative::Derivative;
use derive_more::{Constructor, Deref, DerefMut};
use maelstrom_base::{
    nonempty::NonEmpty,
    stats::{
        BrokerStatistics, JobState, JobStateCounts, JobStatisticsSample, JobStatisticsTimeSeries,
        WorkerStatistics,
    },
    ArtifactType, ClientId, ClientJobId, JobBrokerStatus, JobError, JobId, JobOutcomeResult,
    JobSpec, JobWorkerStatus, MonitorId, Sha256Digest, WorkerId,
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

/// The dependencies for [`Scheduler`] on the
/// [`crate::scheduler_task::artifact_gatherer::ArtifactGatherer`]. This trait's methods should be
/// idential to those on the underlying struct.
///
/// These dependencies are split out from [`Deps`] so they can be implemented separately, and also
/// so that the underlying `ArtifactGatherer` can be accessed independently by the scheduler task.
pub trait ArtifactGatherer {
    type ClientSender;
    fn client_connected(&mut self, cid: ClientId, sender: Self::ClientSender);
    fn client_disconnected(&mut self, cid: ClientId);
    fn start_job(&mut self, jid: JobId, layers: NonEmpty<(Sha256Digest, ArtifactType)>)
        -> StartJob;
    fn job_completed(&mut self, jid: JobId);
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

struct Client<DepsT: Deps> {
    sender: DepsT::ClientSender,
    jobs: HashMap<ClientJobId, JobSpec>,
    counts: JobStateCounts,
}

impl<DepsT: Deps> Client<DepsT> {
    fn new(sender: DepsT::ClientSender) -> Self {
        Client {
            sender,
            jobs: Default::default(),
            counts: Default::default(),
        }
    }
}

type ClientMap<DepsT> = HashMap<ClientId, Client<DepsT>>;

struct Worker<DepsT: Deps> {
    slots: usize,
    sender: DepsT::WorkerSender,
    pending: HashSet<JobId>,
    heap_index: HeapIndex,
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

#[derive(Deref, DerefMut)]
struct WorkerMap<DepsT: Deps>(HashMap<WorkerId, Worker<DepsT>>);

impl<DepsT: Deps> HeapDeps for WorkerMap<DepsT> {
    type Element = WorkerId;

    fn is_element_less_than(&self, lhs_id: &WorkerId, rhs_id: &WorkerId) -> bool {
        let lhs_worker = self.get(lhs_id).unwrap();
        let rhs_worker = self.get(rhs_id).unwrap();
        let lhs = (lhs_worker.pending.len() * rhs_worker.slots, *lhs_id);
        let rhs = (rhs_worker.pending.len() * lhs_worker.slots, *rhs_id);
        lhs.cmp(&rhs) == Ordering::Less
    }

    fn update_index(&mut self, elem: &WorkerId, idx: HeapIndex) {
        self.get_mut(elem).unwrap().heap_index = idx;
    }
}

#[derive(Constructor, Debug, Derivative, Eq)]
#[derivative(Ord, PartialEq, PartialOrd)]
struct QueuedJob {
    #[derivative(Ord = "ignore", PartialEq = "ignore", PartialOrd = "ignore")]
    jid: JobId,
    priority: i8,
    #[derivative(Ord(compare_with = "duration::cmp"))]
    #[derivative(PartialOrd(compare_with = "duration::partial_cmp"))]
    estimated_duration: Option<Duration>,
}

pub struct Scheduler<DepsT: Deps> {
    deps: DepsT,
    clients: ClientMap<DepsT>,
    workers: WorkerMap<DepsT>,
    worker_heap: Heap<WorkerMap<DepsT>>,
    monitors: HashMap<MonitorId, DepsT::MonitorSender>,
    queued_jobs: BinaryHeap<QueuedJob>,
    job_statistics: JobStatisticsTimeSeries,
}

impl<DepsT: Deps> Scheduler<DepsT> {
    /// Create a new scheduler with the given [`Deps`]. Note that [`ArtifactGatherer`] is passed
    /// directly into some methods.
    pub fn new(deps: DepsT) -> Self {
        Scheduler {
            deps,
            clients: Default::default(),
            workers: WorkerMap(Default::default()),
            worker_heap: Default::default(),
            monitors: Default::default(),
            queued_jobs: Default::default(),
            job_statistics: Default::default(),
        }
    }

    fn start_job(
        clients: &mut ClientMap<DepsT>,
        deps: &mut DepsT,
        jid: JobId,
        worker: &mut Worker<DepsT>,
    ) {
        let client = clients.get_mut(&jid.cid).unwrap();
        let spec = client.jobs.get(&jid.cjid).unwrap();
        client.counts[JobState::Pending] -= 1;
        client.counts[JobState::Running] += 1;
        worker.pending.insert(jid).assert_is_true();
        deps.send_enqueue_job_to_worker(&mut worker.sender, jid, spec.clone());
    }

    fn possibly_start_jobs(&mut self, mut just_enqueued: HashSet<JobId>) {
        while !self.queued_jobs.is_empty() && !self.workers.is_empty() {
            let wid = self.worker_heap.peek().unwrap();
            let worker = self.workers.get_mut(wid).unwrap();

            if worker.pending.len() == 2 * worker.slots {
                break;
            }

            let QueuedJob { jid, .. } = self.queued_jobs.pop().unwrap();
            Self::start_job(&mut self.clients, &mut self.deps, jid, worker);
            let heap_index = worker.heap_index;
            self.worker_heap.sift_down(&mut self.workers, heap_index);
            just_enqueued.remove(&jid);
        }

        for jid in just_enqueued {
            let client = self.clients.get_mut(&jid.cid).unwrap();
            self.deps.send_job_status_update_to_client(
                &mut client.sender,
                jid.cjid,
                JobBrokerStatus::WaitingForWorker,
            );
        }
    }

    pub fn receive_client_connected(
        &mut self,
        artifact_gatherer: &mut impl ArtifactGatherer<ClientSender = DepsT::ClientSender>,
        cid: ClientId,
        sender: DepsT::ClientSender,
    ) {
        artifact_gatherer.client_connected(cid, sender.clone());
        self.clients
            .insert(cid, Client::new(sender))
            .expect_is_none(|_| {
                format!("received client_connected message for duplicate client: {cid}")
            });
    }

    pub fn receive_client_disconnected(
        &mut self,
        artifact_gatherer: &mut impl ArtifactGatherer,
        cid: ClientId,
    ) {
        artifact_gatherer.client_disconnected(cid);
        self.clients.remove(&cid).expect_is_some(|| {
            format!("received client_disconnected message for unknown client: {cid}")
        });

        self.queued_jobs.retain(|qj| qj.jid.cid != cid);
        for worker in self.workers.values_mut() {
            worker.pending.retain(|jid| {
                let cancel = jid.cid == cid;
                if cancel {
                    self.deps
                        .send_cancel_job_to_worker(&mut worker.sender, *jid);
                }
                !cancel
            });
        }
        self.worker_heap.rebuild(&mut self.workers);
        self.possibly_start_jobs(HashSet::default());
    }

    pub fn receive_job_request_from_client(
        &mut self,
        artifact_gatherer: &mut impl ArtifactGatherer,
        cid: ClientId,
        cjid: ClientJobId,
        spec: JobSpec,
    ) {
        let jid = JobId { cid, cjid };
        let layers = spec.layers.clone();
        let priority = spec.priority;
        let estimated_duration = spec.estimated_duration;

        let client = self
            .clients
            .get_mut(&cid)
            .expect_is_some(|| format!("received job_request from unknown client: {cid}"));
        client
            .jobs
            .insert(cjid, spec)
            .expect_is_none(|_| format!("received job_request for duplicate job ID: {jid}"));

        match artifact_gatherer.start_job(jid, layers) {
            StartJob::Ready => {
                self.queued_jobs
                    .push(QueuedJob::new(jid, priority, estimated_duration));
                client.counts[JobState::Pending] += 1;
                self.possibly_start_jobs(HashSet::from_iter([jid]));
            }
            StartJob::NotReady => {
                client.counts[JobState::WaitingForArtifacts] += 1;
                self.deps.send_job_status_update_to_client(
                    &mut client.sender,
                    jid.cjid,
                    JobBrokerStatus::WaitingForLayers,
                );
            }
        }
    }

    pub fn receive_jobs_ready_from_artifact_gatherer(&mut self, ready: NonEmpty<JobId>) {
        let just_enqueued = ready
            .into_iter()
            .filter(|jid| match self.clients.get_mut(&jid.cid) {
                None => false,
                Some(client) => {
                    let spec = client.jobs.get(&jid.cjid).unwrap();
                    self.queued_jobs.push(QueuedJob::new(
                        *jid,
                        spec.priority,
                        spec.estimated_duration,
                    ));
                    client.counts[JobState::WaitingForArtifacts] -= 1;
                    client.counts[JobState::Pending] += 1;
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
        for jid in jobs {
            let Some(client) = self.clients.get_mut(&jid.cid) else {
                continue;
            };
            self.deps.send_job_response_to_client(
                &mut client.sender,
                jid.cjid,
                Err(JobError::System(err.clone())),
            );
            client.jobs.remove(&jid.cjid).assert_is_some();
            client.counts[JobState::WaitingForArtifacts] -= 1;
            client.counts[JobState::Complete] += 1;
        }
    }

    pub fn receive_worker_connected(
        &mut self,
        wid: WorkerId,
        slots: usize,
        sender: DepsT::WorkerSender,
    ) {
        self.workers
            .insert(wid, Worker::new(slots, sender))
            .expect_is_none(|_| {
                format!("received worker_connected message for duplicate worker: {wid}")
            });
        self.worker_heap.push(&mut self.workers, wid);
        self.possibly_start_jobs(HashSet::default());
    }

    pub fn receive_worker_disconnected(&mut self, wid: WorkerId) {
        let worker = self.workers.remove(&wid).expect_is_some(|| {
            format!("received worker_disconnected message for unknown worker: {wid}")
        });
        self.worker_heap
            .remove(&mut self.workers, worker.heap_index);

        for jid in &worker.pending {
            let client = self.clients.get_mut(&jid.cid).unwrap();
            let spec = client.jobs.get(&jid.cjid).unwrap();
            self.queued_jobs
                .push(QueuedJob::new(*jid, spec.priority, spec.estimated_duration));
            client.counts[JobState::Running] -= 1;
            client.counts[JobState::Pending] += 1;
        }
        self.possibly_start_jobs(worker.pending);
    }

    pub fn receive_job_response_from_worker(
        &mut self,
        artifact_gatherer: &mut impl ArtifactGatherer,
        wid: WorkerId,
        jid: JobId,
        result: JobOutcomeResult,
    ) {
        let worker = self
            .workers
            .get_mut(&wid)
            .expect_is_some(|| format!("received job_response message from unknown worker: {wid}"));
        if !worker.pending.remove(&jid) {
            // This indicates that the client isn't around anymore. Just ignore this response from
            // the worker. When the client disconnected, we canceled all of the outstanding
            // requests and updated our version of the worker's pending requests.
            return;
        }

        artifact_gatherer.job_completed(jid);
        let client = self.clients.get_mut(&jid.cid).unwrap();
        self.deps
            .send_job_response_to_client(&mut client.sender, jid.cjid, result);
        client.jobs.remove(&jid.cjid).assert_is_some();
        client.counts[JobState::Running] -= 1;
        client.counts[JobState::Complete] += 1;

        if let Some(QueuedJob { jid, .. }) = self.queued_jobs.pop() {
            // If there are any queued_requests, we can just pop one off of the front of
            // the queue and not have to update the worker's used slot count or position in the
            // workers list.
            Self::start_job(&mut self.clients, &mut self.deps, jid, worker);
        } else {
            // Since there are no queued_jobs, we're going to have to update the worker's position
            // in the workers list.
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
        let worker = self.workers.get_mut(&wid).expect_is_some(|| {
            format!("received job_status_update message from unknown worker: {wid}")
        });
        if !worker.pending.contains(&jid) {
            // This indicates that the client isn't around anymore. Just ignore this status update.
            return;
        }

        let client = self.clients.get_mut(&jid.cid).unwrap();
        self.deps.send_job_status_update_to_client(
            &mut client.sender,
            jid.cjid,
            JobBrokerStatus::AtWorker(wid, status),
        );
    }

    pub fn receive_monitor_connected(&mut self, mid: MonitorId, sender: DepsT::MonitorSender) {
        self.monitors.insert(mid, sender).expect_is_none(|_| {
            format!("received monitor_connected message for duplicate monitor: {mid}")
        });
    }

    pub fn receive_monitor_disconnected(&mut self, mid: MonitorId) {
        self.monitors.remove(&mid).expect_is_some(|| {
            format!("received monitor_disconnected message for unknown monitor: {mid}")
        });
    }

    pub fn receive_statistics_request_from_monitor(&mut self, mid: MonitorId) {
        let sender = self.monitors.get_mut(&mid).expect_is_some(|| {
            format!("received statistics_request message from unknown monitor: {mid}")
        });
        self.deps.send_statistics_response_to_monitor(
            sender,
            BrokerStatistics {
                worker_statistics: self
                    .workers
                    .iter()
                    .map(|(wid, Worker { slots, .. })| (*wid, WorkerStatistics { slots: *slots }))
                    .collect(),
                job_statistics: self.job_statistics.clone(),
            },
        );
    }

    pub fn receive_statistics_heartbeat(&mut self) {
        self.job_statistics.insert(JobStatisticsSample {
            client_to_stats: self
                .clients
                .iter()
                .map(|(&cid, client)| (cid, client.counts))
                .collect(),
        });
    }

    #[cfg(test)]
    fn get_job_state_counts_for_client(&self, cid: ClientId) -> JobStateCounts {
        self.clients.get(&cid).unwrap().counts.clone()
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
    use enum_map::enum_map;
    use maelstrom_base::tar_digest;
    use maelstrom_test::{outcome, spec};
    use maplit::hashmap;
    use rstest::rstest;
    use std::{
        cell::RefCell,
        ops::{Deref, DerefMut},
        rc::Rc,
    };

    #[test]
    fn queued_job_ordering() {
        let jid_1 = (1, 1).into();
        let jid_2 = (1, 2).into();
        let duration_none = None;
        let duration_1 = Some(Duration::from_secs(1));
        let duration_2 = Some(Duration::from_secs(2));
        let priority_1 = 1;
        let priority_2 = 2;
        assert!(
            QueuedJob::new(jid_1, priority_2, duration_1)
                > QueuedJob::new(jid_2, priority_1, duration_2)
        );
        assert!(
            QueuedJob::new(jid_1, priority_1, duration_2)
                > QueuedJob::new(jid_2, priority_1, duration_1)
        );
        assert!(
            QueuedJob::new(jid_1, priority_1, duration_none)
                > QueuedJob::new(jid_2, priority_1, duration_2)
        );
        assert!(
            !(QueuedJob::new(jid_1, priority_1, duration_none)
                > QueuedJob::new(jid_2, priority_1, duration_none))
        );
        assert!(
            !(QueuedJob::new(jid_1, priority_1, duration_none)
                < QueuedJob::new(jid_2, priority_1, duration_none))
        );
        assert_eq!(
            QueuedJob::new(jid_1, priority_1, duration_none),
            QueuedJob::new(jid_2, priority_1, duration_none),
        );
        assert_ne!(
            QueuedJob::new(jid_1, priority_1, duration_none),
            QueuedJob::new(jid_1, priority_2, duration_none),
        );
        assert_ne!(
            QueuedJob::new(jid_1, priority_1, duration_1),
            QueuedJob::new(jid_1, priority_1, duration_2),
        );
    }

    #[derive(Derivative)]
    #[derivative(Default)]
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
        // Drops
        #[derivative(Default(value = "true"))]
        check_drops: bool,
        client_sender_clone: HashSet<ClientId>,
        client_sender_drop: HashSet<ClientId>,
        worker_sender_drop: HashSet<WorkerId>,
        monitor_sender_drop: HashSet<MonitorId>,
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
            assert!(
                self.client_sender_clone.is_empty(),
                "unused mock entries for ClientSender::clone: {:?}",
                self.client_sender_clone,
            );
            assert!(
                self.client_sender_drop.is_empty(),
                "unused mock entries for ClientSender::drop: {:?}",
                self.client_sender_drop,
            );
            assert!(
                self.worker_sender_drop.is_empty(),
                "unused mock entries for WorkerSender::drop: {:?}",
                self.worker_sender_drop,
            );
            assert!(
                self.monitor_sender_drop.is_empty(),
                "unused mock entries for MonitorSender::drop: {:?}",
                self.monitor_sender_drop,
            );
        }
    }

    impl ArtifactGatherer for Rc<RefCell<Mock>> {
        type ClientSender = TestClientSender;

        fn client_connected(&mut self, cid: ClientId, sender: TestClientSender) {
            assert_eq!(sender.cid, cid);
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
                .position(|e| e.0 == sender.cid && e.1 == cjid && e.2 == result)
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
                .position(|e| e.0 == sender.cid && e.1 == cjid && e.2 == status)
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
            let wid = sender.wid;
            let vec = &mut self.borrow_mut().send_enqueue_job_to_worker;
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
            let wid = sender.wid;
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
            let mid = sender.mid;
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

    #[derive(derive_more::Debug)]
    struct TestClientSender {
        cid: ClientId,
        #[debug(skip)]
        mock: Rc<RefCell<Mock>>,
        #[debug(skip)]
        is_clone: bool,
    }

    impl Clone for TestClientSender {
        fn clone(&self) -> Self {
            let cid = self.cid;
            let mock = self.mock.clone();
            assert!(
                mock.borrow_mut().client_sender_clone.remove(&cid),
                "unexpected clone of client sender for: {cid}",
            );
            Self {
                cid,
                mock,
                is_clone: true,
            }
        }
    }

    impl Drop for TestClientSender {
        fn drop(&mut self) {
            let cid = self.cid;
            let mut mock = self.mock.borrow_mut();
            if !self.is_clone && mock.check_drops {
                assert!(
                    mock.client_sender_drop.remove(&cid),
                    "unexpected drop of client sender for: {cid}",
                );
            }
        }
    }

    impl TestClientSender {
        fn new(cid: ClientId, mock: Rc<RefCell<Mock>>) -> Self {
            Self {
                cid,
                mock,
                is_clone: false,
            }
        }
    }

    #[derive(Constructor, derive_more::Debug)]
    struct TestWorkerSender {
        wid: WorkerId,
        #[debug(skip)]
        mock: Rc<RefCell<Mock>>,
    }

    impl Drop for TestWorkerSender {
        fn drop(&mut self) {
            let wid = self.wid;
            let mut mock = self.mock.borrow_mut();
            if mock.check_drops {
                assert!(
                    mock.worker_sender_drop.remove(&wid),
                    "unexpected drop of worker sender for: {wid}",
                );
            }
        }
    }

    #[derive(Constructor, derive_more::Debug)]
    struct TestMonitorSender {
        mid: MonitorId,
        #[debug(skip)]
        mock: Rc<RefCell<Mock>>,
    }

    impl Drop for TestMonitorSender {
        fn drop(&mut self) {
            let mid = self.mid;
            let mut mock = self.mock.borrow_mut();
            if mock.check_drops {
                assert!(
                    mock.monitor_sender_drop.remove(&mid),
                    "unexpected drop of monitor sender for: {mid}",
                );
            }
        }
    }

    struct Fixture {
        mock: Rc<RefCell<Mock>>,
        sut: Scheduler<Rc<RefCell<Mock>>>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.mock.borrow_mut().check_drops = false;
        }
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
                .client_sender_clone(cid)
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

        #[track_caller]
        fn assert_job_state_counts_for_client(
            &self,
            cid: impl Into<ClientId>,
            expected: JobStateCounts,
        ) {
            let actual = self.sut.get_job_state_counts_for_client(cid.into());
            assert_eq!(actual, expected);
        }

        fn expect(&mut self) -> Expect {
            Expect { fixture: self }
        }

        fn receive_client_connected(&mut self, cid: impl Into<ClientId>) {
            let cid = cid.into();
            let mock = self.mock.clone();
            self.sut.receive_client_connected(
                &mut self.mock,
                cid,
                TestClientSender::new(cid, mock),
            );
        }

        fn receive_client_disconnected(&mut self, cid: impl Into<ClientId>) {
            self.sut
                .receive_client_disconnected(&mut self.mock, cid.into());
        }

        fn receive_job_request_from_client(
            &mut self,
            cid: impl Into<ClientId>,
            cjid: impl Into<ClientJobId>,
            spec: JobSpec,
        ) {
            self.sut
                .receive_job_request_from_client(&mut self.mock, cid.into(), cjid.into(), spec);
        }

        fn receive_jobs_ready_from_artifact_gatherer(
            &mut self,
            ready: impl IntoIterator<Item = impl Into<JobId>>,
        ) {
            self.sut.receive_jobs_ready_from_artifact_gatherer(
                NonEmpty::try_from_iter(ready.into_iter().map(Into::into)).unwrap(),
            );
        }

        fn receive_jobs_failed_from_artifact_gatherer(
            &mut self,
            jobs: impl IntoIterator<Item = impl Into<JobId>>,
            err: impl Into<String>,
        ) {
            self.sut.receive_jobs_failed_from_artifact_gatherer(
                NonEmpty::try_from_iter(jobs.into_iter().map(Into::into)).unwrap(),
                err.into(),
            );
        }

        fn receive_worker_connected(&mut self, wid: impl Into<WorkerId>, slots: usize) {
            let wid = wid.into();
            self.sut.receive_worker_connected(
                wid,
                slots,
                TestWorkerSender::new(wid, self.mock.clone()),
            );
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
                .receive_monitor_connected(mid, TestMonitorSender::new(mid, self.mock.clone()));
        }

        fn receive_monitor_disconnected(&mut self, mid: impl Into<MonitorId>) {
            self.sut.receive_monitor_disconnected(mid.into());
        }

        fn receive_statistics_request_from_monitor(&mut self, mid: impl Into<MonitorId>) {
            self.sut.receive_statistics_request_from_monitor(mid.into());
        }

        pub fn receive_statistics_heartbeat(&mut self) {
            self.sut.receive_statistics_heartbeat();
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

        fn client_disconnected(self, cid: impl Into<ClientId>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .client_disconnected
                .insert(cid.into())
                .assert_is_true();
            self
        }

        fn start_job(
            self,
            jid: impl Into<JobId>,
            layers: impl IntoIterator<Item = (impl Into<Sha256Digest>, impl Into<ArtifactType>)>,
            result: impl Into<StartJob>,
        ) -> Self {
            let layers = NonEmpty::try_from_iter(
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

        fn complete_job(self, jid: impl Into<JobId>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .complete_job
                .insert(jid.into())
                .assert_is_true();
            self
        }

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
            spec: JobSpec,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_enqueue_job_to_worker
                .push((wid.into(), jid.into(), spec));
            self
        }

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

        fn client_sender_clone(self, cid: impl Into<ClientId>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .client_sender_clone
                .insert(cid.into())
                .assert_is_true();
            self
        }

        fn client_sender_drop(self, cid: impl Into<ClientId>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .client_sender_drop
                .insert(cid.into())
                .assert_is_true();
            self
        }

        fn worker_sender_drop(self, wid: impl Into<WorkerId>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .worker_sender_drop
                .insert(wid.into())
                .assert_is_true();
            self
        }

        fn monitor_sender_drop(self, mid: impl Into<MonitorId>) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .monitor_sender_drop
                .insert(mid.into())
                .assert_is_true();
            self
        }

        fn send_statistics_response_to_monitor(
            self,
            mid: impl Into<MonitorId>,
            statistics: BrokerStatistics,
        ) -> Self {
            self.fixture
                .mock
                .borrow_mut()
                .send_statistics_response_to_monitor
                .push((mid.into(), statistics));
            self
        }
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
    #[should_panic(expected = "received client_connected message for duplicate client: 1")]
    fn receive_client_connected_for_duplicate_client() {
        let fixture = Fixture::new().with_client(1);
        fixture.mock.borrow_mut().check_drops = false;
        fixture.with_client(1);
    }

    #[test]
    fn receive_client_connected_forwards_to_artifact_gatherer() {
        let mut fixture = Fixture::new();
        fixture
            .expect()
            .client_sender_clone(1)
            .client_connected(1)
            .when()
            .receive_client_connected(1);
    }

    #[test]
    #[should_panic(expected = "received client_disconnected message for unknown client: 1")]
    fn receive_client_disconnected_for_unknown_client() {
        let mut fixture = Fixture::new();
        fixture
            .expect()
            .client_disconnected(1)
            .when()
            .receive_client_disconnected(1);
    }

    #[test]
    fn receive_client_disconnected_forwards_to_artifact_gatherer_and_drops_sender() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .client_disconnected(1)
            .client_sender_drop(1)
            .when()
            .receive_client_disconnected(1);
    }

    #[test]
    fn receive_client_disconnected_clears_queued_jobs() {
        let mut fixture = Fixture::new().with_client(1).with_client(2);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture
            .expect()
            .start_job((2, 1), [tar_digest!(2)], StartJob::Ready)
            .send_job_status_update_to_client(2, 1, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(2, 1, spec!(2));
        fixture
            .expect()
            .start_job((2, 2), [tar_digest!(3)], StartJob::Ready)
            .send_job_status_update_to_client(2, 2, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(2, 2, spec!(3));

        fixture
            .expect()
            .client_disconnected(1)
            .client_sender_drop(1)
            .when()
            .receive_client_disconnected(1);

        fixture
            .expect()
            .send_enqueue_job_to_worker(1, (2, 1), spec!(2))
            .send_enqueue_job_to_worker(1, (2, 2), spec!(3))
            .when()
            .receive_worker_connected(1, 1);
    }

    #[test]
    fn receive_client_disconnected_reorders_workers() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 1);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), spec!(1))
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture
            .expect()
            .start_job((1, 2), [tar_digest!(2)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 2), spec!(2))
            .when()
            .receive_job_request_from_client(1, 2, spec!(2));

        let mut fixture = fixture.with_client(2).with_worker(2, 1);

        fixture
            .expect()
            .start_job((2, 1), [tar_digest!(3)], StartJob::Ready)
            .send_enqueue_job_to_worker(2, (2, 1), spec!(3))
            .when()
            .receive_job_request_from_client(2, 1, spec!(3));
        fixture
            .expect()
            .start_job((2, 2), [tar_digest!(4)], StartJob::Ready)
            .send_enqueue_job_to_worker(2, (2, 2), spec!(4))
            .when()
            .receive_job_request_from_client(2, 2, spec!(4));

        fixture
            .expect()
            .start_job((2, 3), [tar_digest!(5)], StartJob::Ready)
            .send_job_status_update_to_client(2, 3, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(2, 3, spec!(5));
        fixture
            .expect()
            .start_job((2, 4), [tar_digest!(6)], StartJob::Ready)
            .send_job_status_update_to_client(2, 4, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(2, 4, spec!(6));

        fixture
            .expect()
            .client_disconnected(1)
            .client_sender_drop(1)
            .send_cancel_job_to_worker(1, (1, 1))
            .send_cancel_job_to_worker(1, (1, 2))
            .send_enqueue_job_to_worker(1, (2, 3), spec!(5))
            .send_enqueue_job_to_worker(1, (2, 4), spec!(6))
            .when()
            .receive_client_disconnected(1);
    }

    #[test]
    #[should_panic(expected = "received job_request from unknown client: 1")]
    fn receive_job_request_from_client_from_unknown_client() {
        let mut fixture = Fixture::new();
        fixture.receive_job_request_from_client(1, 1, spec!(1));
    }

    #[test]
    #[should_panic(expected = "received job_request for duplicate job ID: 1.1")]
    fn receive_job_request_for_duplicate_jid() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture.receive_job_request_from_client(1, 1, spec!(1));
    }

    #[test]
    fn receive_job_request_not_ready_from_artifact_gatherer() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 1);
        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::NotReady)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForLayers)
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::WaitingForArtifacts => 1,
                _ => 0,
            },
        );
    }

    #[test]
    fn receive_job_request_worker_available() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 1);
        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), spec!(1))
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 1,
                _ => 0,
            },
        );
    }

    #[test]
    fn receive_job_request_no_worker_available() {
        let mut fixture = Fixture::new().with_client(1);
        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Pending => 1,
                _ => 0,
            },
        );
    }

    #[test]
    fn receive_jobs_ready_from_artifact_gatherer_ignores_disconnected_clients() {
        let mut fixture = Fixture::new();
        fixture.receive_jobs_ready_from_artifact_gatherer([(1, 2), (2, 1)]);
    }

    #[test]
    fn receive_jobs_ready_from_artifact_gatherer_batches() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 1);
        let job_spec_1 = spec!(1, priority: 1);
        let job_spec_2 = spec!(2, priority: 2);
        let job_spec_3 = spec!(3, priority: 3);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::NotReady)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForLayers)
            .when()
            .receive_job_request_from_client(1, 1, job_spec_1.clone());
        fixture
            .expect()
            .start_job((1, 2), [tar_digest!(2)], StartJob::NotReady)
            .send_job_status_update_to_client(1, 2, JobBrokerStatus::WaitingForLayers)
            .when()
            .receive_job_request_from_client(1, 2, job_spec_2.clone());
        fixture
            .expect()
            .start_job((1, 3), [tar_digest!(3)], StartJob::NotReady)
            .send_job_status_update_to_client(1, 3, JobBrokerStatus::WaitingForLayers)
            .when()
            .receive_job_request_from_client(1, 3, job_spec_3.clone());
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::WaitingForArtifacts => 3,
                _ => 0,
            },
        );

        fixture
            .expect()
            .send_enqueue_job_to_worker(1, (1, 3), job_spec_3)
            .send_enqueue_job_to_worker(1, (1, 2), job_spec_2)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_jobs_ready_from_artifact_gatherer([(1, 1), (1, 2), (1, 3)]);
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 2,
                JobState::Pending => 1,
                _ => 0,
            },
        );
    }

    #[test]
    fn receive_jobs_failed_from_artifact_gatherer_ignores_disconnected_clients() {
        let mut fixture = Fixture::new();
        fixture.receive_jobs_failed_from_artifact_gatherer([(2, 1), (1, 1)], "foo");
    }

    #[test]
    fn receive_jobs_failed_from_artifact_gatherer() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 1);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::NotReady)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForLayers)
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture
            .expect()
            .start_job((1, 2), [tar_digest!(2)], StartJob::NotReady)
            .send_job_status_update_to_client(1, 2, JobBrokerStatus::WaitingForLayers)
            .when()
            .receive_job_request_from_client(1, 2, spec!(2));
        fixture
            .expect()
            .start_job((1, 3), [tar_digest!(3)], StartJob::NotReady)
            .send_job_status_update_to_client(1, 3, JobBrokerStatus::WaitingForLayers)
            .when()
            .receive_job_request_from_client(1, 3, spec!(3));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::WaitingForArtifacts => 3,
                _ => 0,
            },
        );

        fixture
            .expect()
            .send_job_response_to_client(1, 1, Err(JobError::System("error".into())))
            .send_job_response_to_client(1, 2, Err(JobError::System("error".into())))
            .send_job_response_to_client(1, 3, Err(JobError::System("error".into())))
            .when()
            .receive_jobs_failed_from_artifact_gatherer([(1, 1), (1, 2), (1, 3)], "error");
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Complete => 3,
                _ => 0,
            },
        );
    }

    #[test]
    #[should_panic(expected = "received worker_connected message for duplicate worker: 1")]
    fn receive_worker_connected_for_duplicate_worker() {
        let mut fixture = Fixture::new().with_worker(1, 1);
        fixture.mock.borrow_mut().check_drops = false;
        fixture.receive_worker_connected(1, 1);
    }

    #[test]
    fn receive_worker_connected() {
        let mut fixture = Fixture::new().with_client(1);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture
            .expect()
            .start_job((1, 2), [tar_digest!(2)], StartJob::Ready)
            .send_job_status_update_to_client(1, 2, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 2, spec!(2));
        fixture
            .expect()
            .start_job((1, 3), [tar_digest!(3)], StartJob::Ready)
            .send_job_status_update_to_client(1, 3, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 3, spec!(3));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Pending => 3,
                _ => 0,
            },
        );

        fixture
            .expect()
            .send_enqueue_job_to_worker(1, (1, 1), spec!(1))
            .send_enqueue_job_to_worker(1, (1, 2), spec!(2))
            .send_enqueue_job_to_worker(1, (1, 3), spec!(3))
            .when()
            .receive_worker_connected(1, 2);
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 3,
                _ => 0,
            },
        );
    }

    #[test]
    #[should_panic(expected = "received worker_disconnected message for unknown worker: 1")]
    fn receive_worker_disconnected_for_unknown_worker() {
        let mut fixture = Fixture::new();
        fixture.receive_worker_disconnected(1);
    }

    #[test]
    fn receive_worker_disconnected_with_outstanding_jobs_no_other_available_workers() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 2);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), spec!(1))
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 1,
                _ => 0,
            },
        );

        fixture
            .expect()
            .worker_sender_drop(1)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_worker_disconnected(1);
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Pending => 1,
                _ => 0,
            },
        );
    }

    #[test]
    fn receive_worker_disconnected_with_outstanding_jobs_with_other_available_workers() {
        let mut fixture = Fixture::new()
            .with_client(1)
            .with_worker(1, 2)
            .with_worker(2, 2);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), spec!(1))
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 1,
                _ => 0,
            },
        );

        fixture
            .expect()
            .send_enqueue_job_to_worker(2, (1, 1), spec!(1))
            .worker_sender_drop(1)
            .when()
            .receive_worker_disconnected(1);
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 1,
                _ => 0,
            },
        );
    }

    #[test]
    #[should_panic(expected = "received job_response message from unknown worker: 1")]
    fn receive_job_response_from_worker_for_unknown_worker() {
        let mut fixture = Fixture::new();
        fixture.receive_job_response_from_worker(1, (1, 1), Ok(outcome!(1)));
    }

    #[test]
    fn receive_job_response_from_worker_for_unknown_job() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 1);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), spec!(1))
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture
            .expect()
            .start_job((1, 2), [tar_digest!(2)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 2), spec!(2))
            .when()
            .receive_job_request_from_client(1, 2, spec!(2));
        fixture
            .expect()
            .start_job((1, 3), [tar_digest!(3)], StartJob::Ready)
            .send_job_status_update_to_client(1, 3, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 3, spec!(3));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 2,
                JobState::Pending => 1,
                _ => 0,
            },
        );

        fixture.receive_job_response_from_worker(1, (2, 1), Ok(outcome!(1)));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 2,
                JobState::Pending => 1,
                _ => 0,
            },
        );
    }

    #[test]
    fn receive_job_response_from_worker() {
        let mut fixture = Fixture::new().with_client(1).with_worker(1, 1);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), spec!(1))
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture
            .expect()
            .start_job((1, 2), [tar_digest!(2)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 2), spec!(2))
            .when()
            .receive_job_request_from_client(1, 2, spec!(2));
        fixture
            .expect()
            .start_job((1, 3), [tar_digest!(3)], StartJob::Ready)
            .send_job_status_update_to_client(1, 3, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 3, spec!(3));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 2,
                JobState::Pending => 1,
                _ => 0,
            },
        );

        fixture
            .expect()
            .complete_job((1, 1))
            .send_job_response_to_client(1, 1, Ok(outcome!(1)))
            .send_enqueue_job_to_worker(1, (1, 3), spec!(3))
            .when()
            .receive_job_response_from_worker(1, (1, 1), Ok(outcome!(1)));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 2,
                JobState::Complete => 1,
                _ => 0,
            },
        );

        fixture
            .expect()
            .complete_job((1, 2))
            .send_job_response_to_client(1, 2, Ok(outcome!(1)))
            .when()
            .receive_job_response_from_worker(1, (1, 2), Ok(outcome!(1)));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 1,
                JobState::Complete => 2,
                _ => 0,
            },
        );

        fixture
            .expect()
            .complete_job((1, 3))
            .send_job_response_to_client(1, 3, Ok(outcome!(1)))
            .when()
            .receive_job_response_from_worker(1, (1, 3), Ok(outcome!(1)));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Complete => 3,
                _ => 0,
            },
        );
    }

    #[rstest]
    #[case("1")]
    #[case("2")]
    fn receive_job_response_from_worker_reprioritizes_worker(#[case] wid: u32) {
        let mut fixture = Fixture::new()
            .with_client(1)
            .with_worker(1, 1)
            .with_worker(2, 1);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), spec!(1))
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture
            .expect()
            .start_job((1, 2), [tar_digest!(2)], StartJob::Ready)
            .send_enqueue_job_to_worker(2, (1, 2), spec!(2))
            .when()
            .receive_job_request_from_client(1, 2, spec!(2));
        fixture
            .expect()
            .start_job((1, 3), [tar_digest!(3)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 3), spec!(3))
            .when()
            .receive_job_request_from_client(1, 3, spec!(3));
        fixture
            .expect()
            .start_job((1, 4), [tar_digest!(4)], StartJob::Ready)
            .send_enqueue_job_to_worker(2, (1, 4), spec!(4))
            .when()
            .receive_job_request_from_client(1, 4, spec!(4));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Running => 4,
                _ => 0,
            },
        );

        let cjid = wid;
        fixture
            .expect()
            .complete_job((1, cjid))
            .send_job_response_to_client(1, cjid, Ok(outcome!(1)))
            .when()
            .receive_job_response_from_worker(wid, (1, cjid), Ok(outcome!(1)));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Complete => 1,
                JobState::Running => 3,
                _ => 0,
            },
        );

        fixture
            .expect()
            .start_job((1, 5), [tar_digest!(5)], StartJob::Ready)
            .send_enqueue_job_to_worker(wid, (1, 5), spec!(5))
            .when()
            .receive_job_request_from_client(1, 5, spec!(5));
        fixture.assert_job_state_counts_for_client(
            1,
            enum_map! {
                JobState::Complete => 1,
                JobState::Running => 4,
                _ => 0,
            },
        );
    }

    #[test]
    #[should_panic(expected = "received job_status_update message from unknown worker: 1")]
    fn receive_job_status_update_from_worker_for_unknown_worker() {
        let mut fixture = Fixture::new();
        fixture.receive_job_status_update_from_worker(1, (1, 1), JobWorkerStatus::Executing);
    }

    #[test]
    fn receive_job_status_update_from_worker_for_unknown_job() {
        let mut fixture = Fixture::new().with_worker(1, 1);
        fixture.receive_job_status_update_from_worker(1, (1, 1), JobWorkerStatus::WaitingToExecute);
    }

    #[test]
    fn receive_job_status_update_from_worker() {
        let mut fixture = Fixture::new().with_client(1).with_worker(2, 1);
        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_enqueue_job_to_worker(2, (1, 1), spec!(1))
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture
            .expect()
            .send_job_status_update_to_client(
                1,
                1,
                JobBrokerStatus::AtWorker(2.into(), JobWorkerStatus::WaitingToExecute),
            )
            .when()
            .receive_job_status_update_from_worker(2, (1, 1), JobWorkerStatus::WaitingToExecute);
    }

    #[test]
    #[should_panic(expected = "received monitor_connected message for duplicate monitor: 1")]
    fn receive_monitor_connected_for_duplicate_monitor() {
        let mut fixture = Fixture::new();
        fixture.receive_monitor_connected(1);
        fixture.mock.borrow_mut().check_drops = false;
        fixture.receive_monitor_connected(1);
    }

    #[test]
    #[should_panic(expected = "received monitor_disconnected message for unknown monitor: 1")]
    fn receive_monitor_disconnected_for_unknown_monitor() {
        let mut fixture = Fixture::new();
        fixture.receive_monitor_disconnected(1);
    }

    #[test]
    fn receive_monitor_disconnected_drops_sender() {
        let mut fixture = Fixture::new();
        fixture.receive_monitor_connected(1);
        fixture
            .expect()
            .monitor_sender_drop(1)
            .when()
            .receive_monitor_disconnected(1);
    }

    #[test]
    #[should_panic(expected = "received statistics_request message from unknown monitor: 1")]
    fn receive_statistics_request_from_monitor_for_unknown_monitor() {
        let mut fixture = Fixture::new();
        fixture.receive_statistics_request_from_monitor(1);
    }

    #[test]
    fn receive_statistics_request_from_monitor_after_some_heartbeats() {
        let mut fixture = Fixture::new().with_worker(10, 1).with_client(1);
        fixture.receive_monitor_connected(1);

        fixture
            .expect()
            .send_statistics_response_to_monitor(
                1,
                BrokerStatistics {
                    worker_statistics: hashmap! {
                        10.into() => WorkerStatistics { slots: 1 },
                    },
                    job_statistics: Default::default(),
                },
            )
            .when()
            .receive_statistics_request_from_monitor(1);

        fixture.receive_statistics_heartbeat();
        fixture
            .expect()
            .send_statistics_response_to_monitor(
                1,
                BrokerStatistics {
                    worker_statistics: hashmap! {
                        10.into() => WorkerStatistics { slots: 1 },
                    },
                    job_statistics: JobStatisticsTimeSeries::from_iter([JobStatisticsSample {
                        client_to_stats: hashmap! {
                            1.into() => enum_map! { _ => 0 },
                        },
                    }]),
                },
            )
            .when()
            .receive_statistics_request_from_monitor(1);

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_enqueue_job_to_worker(10, (1, 1), spec!(1))
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));
        fixture
            .expect()
            .start_job((1, 2), [tar_digest!(2)], StartJob::NotReady)
            .send_job_status_update_to_client(1, 2, JobBrokerStatus::WaitingForLayers)
            .when()
            .receive_job_request_from_client(1, 2, spec!(2));
        fixture
            .expect()
            .start_job((1, 3), [tar_digest!(3)], StartJob::Ready)
            .send_enqueue_job_to_worker(10, (1, 3), spec!(3))
            .when()
            .receive_job_request_from_client(1, 3, spec!(3));
        fixture
            .expect()
            .start_job((1, 4), [tar_digest!(4)], StartJob::Ready)
            .send_job_status_update_to_client(1, 4, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 4, spec!(4));
        fixture
            .expect()
            .start_job((1, 5), [tar_digest!(5)], StartJob::Ready)
            .send_job_status_update_to_client(1, 5, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 5, spec!(5));
        fixture
            .expect()
            .send_job_response_to_client(1, 1, Ok(outcome!(1)))
            .complete_job((1, 1))
            .send_enqueue_job_to_worker(10, (1, 4), spec!(4))
            .when()
            .receive_job_response_from_worker(10, (1, 1), Ok(outcome!(1)));

        fixture.receive_statistics_heartbeat();
        fixture
            .expect()
            .send_statistics_response_to_monitor(
                1,
                BrokerStatistics {
                    worker_statistics: hashmap! {
                        10.into() => WorkerStatistics { slots: 1 },
                    },
                    job_statistics: JobStatisticsTimeSeries::from_iter([
                        JobStatisticsSample {
                            client_to_stats: hashmap! {
                                1.into() => enum_map! { _ => 0 },
                            },
                        },
                        JobStatisticsSample {
                            client_to_stats: hashmap! {
                                1.into() => enum_map! {
                                    JobState::WaitingForArtifacts => 1,
                                    JobState::Pending => 1,
                                    JobState::Running => 2,
                                    JobState::Complete => 1,
                                },
                            },
                        },
                    ]),
                },
            )
            .when()
            .receive_statistics_request_from_monitor(1);
    }

    #[test]
    fn requests_go_to_workers_based_on_subscription_percentage() {
        let mut fixture = Fixture::new()
            .with_client(1)
            .with_worker(1, 2)
            .with_worker(2, 2)
            .with_worker(3, 3);

        // 0/2 0/2 0/3
        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 1), spec!(1))
            .when()
            .receive_job_request_from_client(1, 1, spec!(1));

        // 1/2 0/2 0/3
        fixture
            .expect()
            .start_job((1, 2), [tar_digest!(2)], StartJob::Ready)
            .send_enqueue_job_to_worker(2, (1, 2), spec!(2))
            .when()
            .receive_job_request_from_client(1, 2, spec!(2));

        // 1/2 1/2 0/3
        fixture
            .expect()
            .start_job((1, 3), [tar_digest!(3)], StartJob::Ready)
            .send_enqueue_job_to_worker(3, (1, 3), spec!(3))
            .when()
            .receive_job_request_from_client(1, 3, spec!(3));

        // 1/2 1/2 1/3
        fixture
            .expect()
            .start_job((1, 4), [tar_digest!(4)], StartJob::Ready)
            .send_enqueue_job_to_worker(3, (1, 4), spec!(4))
            .when()
            .receive_job_request_from_client(1, 4, spec!(4));

        // 1/2 1/2 2/3
        fixture
            .expect()
            .start_job((1, 5), [tar_digest!(5)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 5), spec!(5))
            .when()
            .receive_job_request_from_client(1, 5, spec!(5));

        // 2/2 1/2 2/3
        fixture
            .expect()
            .start_job((1, 6), [tar_digest!(6)], StartJob::Ready)
            .send_enqueue_job_to_worker(2, (1, 6), spec!(6))
            .when()
            .receive_job_request_from_client(1, 6, spec!(6));

        // 2/2 2/2 2/3
        fixture
            .expect()
            .start_job((1, 7), [tar_digest!(7)], StartJob::Ready)
            .send_enqueue_job_to_worker(3, (1, 7), spec!(7))
            .when()
            .receive_job_request_from_client(1, 7, spec!(7));

        fixture
            .expect()
            .send_job_response_to_client(1, 1, Ok(outcome!(1)))
            .complete_job((1, 1))
            .when()
            .receive_job_response_from_worker(1, (1, 1), Ok(outcome!(1)));
        // 1/2 2/2 3/3
        fixture
            .expect()
            .start_job((1, 8), [tar_digest!(8)], StartJob::Ready)
            .send_enqueue_job_to_worker(1, (1, 8), spec!(8))
            .when()
            .receive_job_request_from_client(1, 8, spec!(8));

        fixture
            .expect()
            .send_job_response_to_client(1, 2, Ok(outcome!(2)))
            .complete_job((1, 2))
            .when()
            .receive_job_response_from_worker(2, (1, 2), Ok(outcome!(2)));
        // 2/2 1/2 3/3
        fixture
            .expect()
            .start_job((1, 9), [tar_digest!(9)], StartJob::Ready)
            .send_enqueue_job_to_worker(2, (1, 9), spec!(9))
            .when()
            .receive_job_request_from_client(1, 9, spec!(9));

        fixture
            .expect()
            .send_job_response_to_client(1, 3, Ok(outcome!(3)))
            .complete_job((1, 3))
            .when()
            .receive_job_response_from_worker(3, (1, 3), Ok(outcome!(3)));
        // 2/2 2/2 2/3
        fixture
            .expect()
            .start_job((1, 10), [tar_digest!(10)], StartJob::Ready)
            .send_enqueue_job_to_worker(3, (1, 10), spec!(10))
            .when()
            .receive_job_request_from_client(1, 10, spec!(10));
    }

    #[test]
    fn priority_and_estimated_duration_have_priority() {
        let mut fixture = Fixture::new().with_client(1);

        let spec_0_none = spec!(1);
        let spec_0_1 = spec!(1, estimated_duration: Duration::from_secs(1));
        let spec_0_2 = spec!(1, estimated_duration: Duration::from_secs(2));
        let spec_1_none = spec!(1, priority: 1);
        let spec_1_1 = spec!(1, priority: 1, estimated_duration: Duration::from_secs(1));
        let spec_1_2 = spec!(1, priority: 1, estimated_duration: Duration::from_secs(2));
        let spec_2_none = spec!(1, priority: 2);
        let spec_2_1 = spec!(1, priority: 2, estimated_duration: Duration::from_secs(1));
        let spec_2_2 = spec!(1, priority: 2, estimated_duration: Duration::from_secs(2));

        fixture
            .expect()
            .start_job((1, 1), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 1, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 1, spec_0_none.clone());
        fixture
            .expect()
            .start_job((1, 2), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 2, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 2, spec_0_1.clone());
        fixture
            .expect()
            .start_job((1, 3), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 3, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 3, spec_0_2.clone());
        fixture
            .expect()
            .start_job((1, 4), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 4, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 4, spec_1_none.clone());
        fixture
            .expect()
            .start_job((1, 5), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 5, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 5, spec_1_1.clone());
        fixture
            .expect()
            .start_job((1, 6), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 6, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 6, spec_1_2.clone());
        fixture
            .expect()
            .start_job((1, 7), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 7, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 7, spec_2_none.clone());
        fixture
            .expect()
            .start_job((1, 8), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 8, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 8, spec_2_1.clone());
        fixture
            .expect()
            .start_job((1, 9), [tar_digest!(1)], StartJob::Ready)
            .send_job_status_update_to_client(1, 9, JobBrokerStatus::WaitingForWorker)
            .when()
            .receive_job_request_from_client(1, 9, spec_2_2.clone());

        fixture
            .expect()
            .send_enqueue_job_to_worker(1, (1, 7), spec_2_none)
            .send_enqueue_job_to_worker(1, (1, 9), spec_2_2)
            .when()
            .receive_worker_connected(1, 1);
        fixture
            .expect()
            .send_enqueue_job_to_worker(2, (1, 4), spec_1_none)
            .send_enqueue_job_to_worker(2, (1, 8), spec_2_1)
            .when()
            .receive_worker_connected(2, 1);
        fixture
            .expect()
            .send_enqueue_job_to_worker(3, (1, 5), spec_1_1)
            .send_enqueue_job_to_worker(3, (1, 6), spec_1_2)
            .when()
            .receive_worker_connected(3, 1);
        fixture
            .expect()
            .send_enqueue_job_to_worker(4, (1, 1), spec_0_none)
            .send_enqueue_job_to_worker(4, (1, 3), spec_0_2)
            .when()
            .receive_worker_connected(4, 1);
        fixture
            .expect()
            .send_enqueue_job_to_worker(5, (1, 2), spec_0_1)
            .when()
            .receive_worker_connected(5, 1);
    }
}
