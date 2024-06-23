use crate::artifact_pusher;
use anyhow::{anyhow, Error, Result};
use maelstrom_base::{
    proto::{BrokerToClient, BrokerToWorker, ClientToBroker, WorkerToBroker},
    stats::{JobState, JobStateCounts},
    ClientId, ClientJobId, JobId, JobOutcomeResult, JobSpec, Sha256Digest,
};
use maelstrom_util::{config::common::Slots, ext::OptionExt as _, fs::Fs, sync};
use maelstrom_worker::local_worker;
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
};
use tokio::{
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task::JoinSet,
};

pub trait Deps {
    type JobHandle;
    fn job_done(&self, handle: Self::JobHandle, cjid: ClientJobId, result: JobOutcomeResult);

    type JobStateCountsHandle;
    fn job_state_counts(&self, handle: Self::JobStateCountsHandle, counts: JobStateCounts);

    // Only in remote broker mode.
    fn send_job_request_to_broker(&self, cjid: ClientJobId, spec: JobSpec);
    fn send_job_state_counts_request_to_broker(&self);
    fn start_artifact_transfer_to_broker(&self, digest: Sha256Digest, path: PathBuf);

    // Only in standalone mode.
    fn send_enqueue_job_to_local_worker(&self, jid: JobId, spec: JobSpec);
    fn send_artifact_fetch_completed_to_local_worker(
        &self,
        digest: Sha256Digest,
        result: Result<u64>,
    );
    fn link_artifact_for_local_worker(&self, from: &Path, to: &Path) -> Result<u64>;
    fn shutdown_local_worker(&self, error: Error);
}

pub enum Message<DepsT: Deps> {
    // These are requests from the client.
    AddArtifact(PathBuf, Sha256Digest),
    RunJob(JobSpec, DepsT::JobHandle),
    GetJobStateCounts(DepsT::JobStateCountsHandle),

    // Only in non-standalone mode.
    Broker(BrokerToClient),

    // Only in standalone mode.
    LocalWorker(WorkerToBroker),
    LocalWorkerStartArtifactFetch(Sha256Digest, PathBuf),
    Shutdown(Error),
}

struct Router<DepsT: Deps> {
    deps: DepsT,
    standalone: bool,
    slots: Slots,
    artifacts: HashMap<Sha256Digest, PathBuf>,
    next_client_job_id: u32,
    job_handles: HashMap<ClientJobId, DepsT::JobHandle>,
    job_state_counts_handles: VecDeque<DepsT::JobStateCountsHandle>,
    counts: JobStateCounts,
}

impl<DepsT: Deps> Router<DepsT> {
    fn new(deps: DepsT, standalone: bool, slots: Slots) -> Self {
        Self {
            deps,
            standalone,
            slots,
            artifacts: Default::default(),
            next_client_job_id: Default::default(),
            job_handles: Default::default(),
            job_state_counts_handles: Default::default(),
            counts: Default::default(),
        }
    }

    fn receive_job_response(&mut self, cjid: ClientJobId, result: JobOutcomeResult) {
        let handle = self
            .job_handles
            .remove(&cjid)
            .unwrap_or_else(|| panic!("received response for unknown job {cjid}"));
        self.deps.job_done(handle, cjid, result);
    }

    fn receive_message(&mut self, message: Message<DepsT>) {
        match message {
            Message::AddArtifact(path, digest) => {
                self.artifacts.insert(digest, path);
            }
            Message::RunJob(spec, handle) => {
                let cjid = self.next_client_job_id.into();
                self.next_client_job_id = self.next_client_job_id.checked_add(1).unwrap();

                self.job_handles.insert(cjid, handle).assert_is_none();

                if self.standalone || spec.must_be_run_locally() {
                    if self.counts[JobState::Running] < *self.slots.inner() as u64 {
                        self.counts[JobState::Running] += 1;
                    } else {
                        self.counts[JobState::Pending] += 1;
                    }
                    self.deps.send_enqueue_job_to_local_worker(
                        JobId {
                            cid: ClientId::from(0),
                            cjid,
                        },
                        spec,
                    );
                } else {
                    self.deps.send_job_request_to_broker(cjid, spec);
                }
            }
            Message::GetJobStateCounts(handle) => {
                if self.standalone {
                    assert!(self.job_state_counts_handles.is_empty());
                    self.deps.job_state_counts(handle, self.counts);
                } else {
                    self.job_state_counts_handles.push_back(handle);
                    self.deps.send_job_state_counts_request_to_broker();
                }
            }
            Message::Broker(BrokerToClient::JobResponse(cjid, result)) => {
                assert!(!self.standalone);
                self.receive_job_response(cjid, result);
            }
            Message::Broker(BrokerToClient::TransferArtifact(digest)) => {
                assert!(!self.standalone);
                let path = self.artifacts.get(&digest).unwrap_or_else(|| {
                    panic!("got request for unknown artifact with digest {digest}")
                });
                self.deps
                    .start_artifact_transfer_to_broker(digest, path.to_owned());
            }
            Message::Broker(BrokerToClient::StatisticsResponse(_)) => {
                panic!("got unexpected statistics response")
            }
            Message::Broker(BrokerToClient::JobStateCountsResponse(mut counts)) => {
                assert!(!self.standalone);
                for (state, count) in &self.counts {
                    counts[state] += count;
                }
                self.deps.job_state_counts(
                    self.job_state_counts_handles
                        .pop_front()
                        .unwrap_or_else(|| {
                            panic!("got JobStateCountsResponse with no requests outstanding")
                        }),
                    counts,
                );
            }
            Message::LocalWorker(WorkerToBroker(jid, result)) => {
                if self.counts[JobState::Pending] > 0 {
                    self.counts[JobState::Pending] -= 1;
                } else {
                    self.counts[JobState::Running] -= 1;
                }
                self.counts[JobState::Complete] += 1;
                self.receive_job_response(jid.cjid, result);
            }
            Message::LocalWorkerStartArtifactFetch(digest, path) => {
                self.deps.send_artifact_fetch_completed_to_local_worker(
                    digest.clone(),
                    match self.artifacts.get(&digest) {
                        None => Err(anyhow!("no artifact found for digest {digest}")),
                        Some(stored_path) => self
                            .deps
                            .link_artifact_for_local_worker(stored_path.as_path(), path.as_path()),
                    },
                );
            }
            Message::Shutdown(error) => self.deps.shutdown_local_worker(error),
        }
    }
}

pub struct Adapter {
    broker_sender: UnboundedSender<ClientToBroker>,
    artifact_pusher_sender: artifact_pusher::Sender,
    local_worker_sender: maelstrom_worker::DispatcherSender,
    fs: Fs,
}

impl Adapter {
    fn new(
        broker_sender: UnboundedSender<ClientToBroker>,
        artifact_pusher_sender: artifact_pusher::Sender,
        local_worker_sender: maelstrom_worker::DispatcherSender,
    ) -> Self {
        Self {
            broker_sender,
            artifact_pusher_sender,
            local_worker_sender,
            fs: Fs::new(),
        }
    }
}

impl Deps for Adapter {
    type JobHandle = oneshot::Sender<(ClientJobId, JobOutcomeResult)>;

    fn job_done(&self, handle: Self::JobHandle, cjid: ClientJobId, result: JobOutcomeResult) {
        handle.send((cjid, result)).ok();
    }

    type JobStateCountsHandle = oneshot::Sender<JobStateCounts>;

    fn job_state_counts(&self, handle: Self::JobStateCountsHandle, counts: JobStateCounts) {
        handle.send(counts).ok();
    }

    fn send_job_request_to_broker(&self, cjid: ClientJobId, spec: JobSpec) {
        let _ = self
            .broker_sender
            .send(ClientToBroker::JobRequest(cjid, spec));
    }

    fn send_job_state_counts_request_to_broker(&self) {
        let _ = self
            .broker_sender
            .send(ClientToBroker::JobStateCountsRequest);
    }

    fn start_artifact_transfer_to_broker(&self, digest: Sha256Digest, path: PathBuf) {
        let _ = self
            .artifact_pusher_sender
            .send(artifact_pusher::Message { digest, path });
    }

    fn send_enqueue_job_to_local_worker(&self, jid: JobId, spec: JobSpec) {
        let _ = self.local_worker_sender.send(local_worker::Message::Broker(
            BrokerToWorker::EnqueueJob(jid, spec),
        ));
    }

    fn send_artifact_fetch_completed_to_local_worker(
        &self,
        digest: Sha256Digest,
        result: Result<u64>,
    ) {
        let _ = self
            .local_worker_sender
            .send(local_worker::Message::ArtifactFetchCompleted(
                digest, result,
            ));
    }

    fn link_artifact_for_local_worker(&self, from: &Path, to: &Path) -> Result<u64> {
        self.fs.symlink(from, to)?;
        Ok(self.fs.metadata(to)?.len())
    }

    fn shutdown_local_worker(&self, error: Error) {
        let _ = self
            .local_worker_sender
            .send(local_worker::Message::Shutdown(error));
    }
}

pub type Sender = UnboundedSender<Message<Adapter>>;
pub type Receiver = UnboundedReceiver<Message<Adapter>>;

pub fn channel() -> (Sender, Receiver) {
    mpsc::unbounded_channel()
}

pub fn start_task(
    join_set: &mut JoinSet<Result<()>>,
    standalone: bool,
    slots: Slots,
    receiver: Receiver,
    broker_sender: UnboundedSender<ClientToBroker>,
    artifact_pusher_sender: artifact_pusher::Sender,
    local_worker_sender: maelstrom_worker::DispatcherSender,
) {
    let adapter = Adapter::new(broker_sender, artifact_pusher_sender, local_worker_sender);
    let mut router = Router::new(adapter, standalone, slots);
    join_set.spawn(sync::channel_reader(receiver, move |msg| {
        router.receive_message(msg)
    }));
}

#[cfg(test)]
mod tests {
    use super::{Message::*, *};
    use enum_map::enum_map;
    use maelstrom_base::{stats::BrokerStatistics, JobNetwork};
    use maelstrom_test::*;
    use std::{cell::RefCell, rc::Rc, result};
    use BrokerToClient::*;
    use TestMessage::*;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum TestMessage {
        JobDone(ClientJobId, JobOutcomeResult),
        JobStateCountsResponse(i32, JobStateCounts),
        JobRequestToBroker(ClientJobId, JobSpec),
        JobStatesCountRequestToBroker,
        StartArtifactTransferToBroker(Sha256Digest, PathBuf),
        EnqueueJobToLocalWorker(JobId, JobSpec),
        ArtifactFetchCompletedToLocalWorker(Sha256Digest, result::Result<u64, String>),
        LinkArtifactForLocalWorker(PathBuf, PathBuf),
        ShutdownLocalWorker(String),
    }

    struct TestState {
        messages: Vec<TestMessage>,
        link_artifact_for_local_worker_returns: HashMap<(PathBuf, PathBuf), u64>,
    }

    impl Deps for Rc<RefCell<TestState>> {
        type JobHandle = ClientJobId;

        fn job_done(&self, handle: Self::JobHandle, cjid: ClientJobId, result: JobOutcomeResult) {
            assert_eq!(handle, cjid);
            self.borrow_mut()
                .messages
                .push(TestMessage::JobDone(cjid, result));
        }

        type JobStateCountsHandle = i32;
        fn job_state_counts(&self, handle: Self::JobStateCountsHandle, counts: JobStateCounts) {
            self.borrow_mut()
                .messages
                .push(TestMessage::JobStateCountsResponse(handle, counts));
        }

        fn send_job_request_to_broker(&self, cjid: ClientJobId, spec: JobSpec) {
            self.borrow_mut()
                .messages
                .push(TestMessage::JobRequestToBroker(cjid, spec));
        }

        fn send_job_state_counts_request_to_broker(&self) {
            self.borrow_mut()
                .messages
                .push(TestMessage::JobStatesCountRequestToBroker);
        }

        fn start_artifact_transfer_to_broker(&self, digest: Sha256Digest, path: PathBuf) {
            self.borrow_mut()
                .messages
                .push(TestMessage::StartArtifactTransferToBroker(digest, path));
        }

        fn send_enqueue_job_to_local_worker(&self, jid: JobId, spec: JobSpec) {
            self.borrow_mut()
                .messages
                .push(TestMessage::EnqueueJobToLocalWorker(jid, spec));
        }

        fn send_artifact_fetch_completed_to_local_worker(
            &self,
            digest: Sha256Digest,
            result: Result<u64>,
        ) {
            self.borrow_mut()
                .messages
                .push(TestMessage::ArtifactFetchCompletedToLocalWorker(
                    digest,
                    result.map_err(|e| format!("{e}")),
                ));
        }

        fn link_artifact_for_local_worker(&self, from: &Path, to: &Path) -> Result<u64> {
            let mut test_state = self.borrow_mut();
            test_state
                .messages
                .push(TestMessage::LinkArtifactForLocalWorker(
                    from.into(),
                    to.into(),
                ));
            if let Some(size) = test_state
                .link_artifact_for_local_worker_returns
                .remove(&(from.into(), to.into()))
            {
                Ok(size)
            } else {
                Err(anyhow!("link error"))
            }
        }

        fn shutdown_local_worker(&self, error: Error) {
            self.borrow_mut()
                .messages
                .push(TestMessage::ShutdownLocalWorker(error.to_string()));
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        router: Router<Rc<RefCell<TestState>>>,
    }

    impl Fixture {
        fn new<'a>(
            standalone: bool,
            slots: u16,
            link_artifact_for_local_worker_returns: impl IntoIterator<Item = (&'a str, &'a str, u64)>,
        ) -> Self {
            let test_state = Rc::new(RefCell::new(TestState {
                messages: Vec::default(),
                link_artifact_for_local_worker_returns: link_artifact_for_local_worker_returns
                    .into_iter()
                    .map(|(from, to, result)| ((from.into(), to.into()), result))
                    .collect(),
            }));
            let router = Router::new(test_state.clone(), standalone, slots.try_into().unwrap());
            Fixture { test_state, router }
        }

        fn expect_messages_in_any_order(&mut self, mut expected: Vec<TestMessage>) {
            expected.sort();
            let messages = &mut self.test_state.borrow_mut().messages;
            messages.sort();
            if expected == *messages {
                messages.clear();
                return;
            }
            panic!(
                "Expected messages didn't match actual messages in any order.\n\
                Expected: {expected:#?}\n\
                Actual: {messages:#?}\n\
                Diff: {}",
                colored_diff::PrettyDifference {
                    expected: &format!("{expected:#?}"),
                    actual: &format!("{messages:#?}")
                }
            );
        }
    }

    macro_rules! script_test {
        ($test_name:ident, $fixture:expr, $($in_msg:expr => { $($out_msg:expr),* $(,)? });* $(;)?) => {
            #[test]
            fn $test_name() {
                let mut fixture = $fixture;
                $(
                    fixture.router.receive_message($in_msg);
                    fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
                )*
            }
        };
    }

    script_test! {
        local_worker_start_artifact_fetch_unknown_standalone,
        Fixture::new(true, 1, []),
        LocalWorkerStartArtifactFetch(digest!(1), path_buf!("foo")) => {
            ArtifactFetchCompletedToLocalWorker(
                digest!(1),
                Err(string!("no artifact found for digest 0000000000000000000000000000000000000000000000000000000000000001")),
            ),
        };
    }

    script_test! {
        local_worker_start_artifact_fetch_known_link_error_standalone,
        Fixture::new(true, 1, []),
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        LocalWorkerStartArtifactFetch(digest!(1), path_buf!("foo")) => {
            LinkArtifactForLocalWorker(path_buf!("bar"), path_buf!("foo")),
            ArtifactFetchCompletedToLocalWorker(
                digest!(1),
                Err(string!("link error")),
            ),
        };
    }

    script_test! {
        local_worker_start_artifact_fetch_known_standalone,
        Fixture::new(true, 1, [("bar", "foo", 1234)]),
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        LocalWorkerStartArtifactFetch(digest!(1), path_buf!("foo")) => {
            LinkArtifactForLocalWorker(path_buf!("bar"), path_buf!("foo")),
            ArtifactFetchCompletedToLocalWorker(digest!(1), Ok(1234)),
        };
    }

    script_test! {
        local_worker_start_artifact_last_add_artifact_wins_standalone,
        Fixture::new(true, 1, [("bar", "foo", 1234)]),
        AddArtifact(path_buf!("baz"), digest!(1)) => {};
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        LocalWorkerStartArtifactFetch(digest!(1), path_buf!("foo")) => {
            LinkArtifactForLocalWorker(path_buf!("bar"), path_buf!("foo")),
            ArtifactFetchCompletedToLocalWorker(digest!(1), Ok(1234)),
        };
    }

    script_test! {
        local_worker_start_artifact_fetch_unknown_clustered,
        Fixture::new(false, 1, []),
        LocalWorkerStartArtifactFetch(digest!(1), path_buf!("foo")) => {
            ArtifactFetchCompletedToLocalWorker(
                digest!(1),
                Err(string!("no artifact found for digest 0000000000000000000000000000000000000000000000000000000000000001")),
            ),
        };
    }

    script_test! {
        local_worker_start_artifact_fetch_known_link_error_clustered,
        Fixture::new(false, 1, []),
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        LocalWorkerStartArtifactFetch(digest!(1), path_buf!("foo")) => {
            LinkArtifactForLocalWorker(path_buf!("bar"), path_buf!("foo")),
            ArtifactFetchCompletedToLocalWorker(
                digest!(1),
                Err(string!("link error")),
            ),
        };
    }

    script_test! {
        local_worker_start_artifact_fetch_known_clustered,
        Fixture::new(false, 1, [("bar", "foo", 1234)]),
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        LocalWorkerStartArtifactFetch(digest!(1), path_buf!("foo")) => {
            LinkArtifactForLocalWorker(path_buf!("bar"), path_buf!("foo")),
            ArtifactFetchCompletedToLocalWorker(digest!(1), Ok(1234)),
        };
    }

    script_test! {
        local_worker_start_artifact_last_add_artifact_wins_clustered,
        Fixture::new(false, 1, [("bar", "foo", 1234)]),
        AddArtifact(path_buf!("baz"), digest!(1)) => {};
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        LocalWorkerStartArtifactFetch(digest!(1), path_buf!("foo")) => {
            LinkArtifactForLocalWorker(path_buf!("bar"), path_buf!("foo")),
            ArtifactFetchCompletedToLocalWorker(digest!(1), Ok(1234)),
        };
    }

    #[test]
    #[should_panic(expected = "assertion failed: !self.standalone")]
    fn broker_transfer_artifact_standalone() {
        let mut fixture = Fixture::new(true, 1, []);
        fixture
            .router
            .receive_message(Broker(TransferArtifact(digest!(1))));
    }

    #[test]
    #[should_panic(
        expected = "got request for unknown artifact with digest 0000000000000000000000000000000000000000000000000000000000000001"
    )]
    fn broker_transfer_artifact_unknown_clustered() {
        let mut fixture = Fixture::new(false, 1, []);
        fixture
            .router
            .receive_message(Broker(TransferArtifact(digest!(1))));
    }

    script_test! {
        broker_transfer_artifact_known_clustered,
        Fixture::new(false, 1, []),
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        Broker(TransferArtifact(digest!(1))) => {
            StartArtifactTransferToBroker(digest!(1), path_buf!("bar")),
        };
    }

    script_test! {
        broker_transfer_artifact_last_add_artifact_wins_clustered,
        Fixture::new(false, 1, []),
        AddArtifact(path_buf!("baz"), digest!(1)) => {};
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        Broker(TransferArtifact(digest!(1))) => {
            StartArtifactTransferToBroker(digest!(1), path_buf!("bar")),
        };
    }

    script_test! {
        run_job_standalone,
        Fixture::new(true, 1, []),
        RunJob(spec!(0, Tar), cjid!(0)) => {
            EnqueueJobToLocalWorker(jid!(0, 0), spec!(0, Tar)),
        };
        RunJob(spec!(1, Tar), cjid!(1)) => {
            EnqueueJobToLocalWorker(jid!(0, 1), spec!(1, Tar)),
        };
    }

    script_test! {
        shutdown,
        Fixture::new(true, 1, []),
        RunJob(spec!(0, Tar), cjid!(0)) => {
            EnqueueJobToLocalWorker(jid!(0, 0), spec!(0, Tar)),
        };
        RunJob(spec!(1, Tar), cjid!(1)) => {
            EnqueueJobToLocalWorker(jid!(0, 1), spec!(1, Tar)),
        };
        Shutdown(anyhow!("test error")) => {
            ShutdownLocalWorker("test error".into())
        };
    }

    script_test! {
        run_job_clustered,
        Fixture::new(false, 1, []),
        RunJob(spec!(0, Tar), cjid!(0)) => {
            JobRequestToBroker(cjid!(0), spec!(0, Tar)),
        };
        RunJob(spec!(1, Tar), cjid!(1)) => {
            JobRequestToBroker(cjid!(1), spec!(1, Tar)),
        };
    }

    script_test! {
        run_job_must_be_local_clustered,
        Fixture::new(false, 1, []),
        RunJob(spec!(0, Tar), cjid!(0)) => {
            JobRequestToBroker(cjid!(0), spec!(0, Tar)),
        };
        RunJob(spec!(1, Tar).network(JobNetwork::Local), cjid!(1)) => {
            EnqueueJobToLocalWorker(jid!(0, 1), spec!(1, Tar).network(JobNetwork::Local)),
        };
    }

    #[test]
    #[should_panic(expected = "received response for unknown job 1")]
    fn job_response_from_local_worker_unknown_standalone() {
        let mut fixture = Fixture::new(true, 1, []);
        // Give it a job just so it doesn't crash subracting the job counts.
        fixture
            .router
            .receive_message(RunJob(spec!(0, Tar), cjid!(0)));
        fixture
            .router
            .receive_message(LocalWorker(WorkerToBroker(jid!(0, 1), Ok(outcome!(0)))));
    }

    script_test! {
        job_response_from_local_worker_known_standalone,
        Fixture::new(true, 1, []),
        RunJob(spec!(0, Tar), cjid!(0)) => {
            EnqueueJobToLocalWorker(jid!(0, 0), spec!(0, Tar)),
        };
        LocalWorker(WorkerToBroker(jid!(0, 0), Ok(outcome!(0)))) => {
            JobDone(cjid!(0), Ok(outcome!(0))),
        };
    }

    #[test]
    #[should_panic(expected = "received response for unknown job 1")]
    fn job_response_from_local_worker_unknown_clustered() {
        let mut fixture = Fixture::new(false, 1, []);
        // Give it a job just so it doesn't crash subracting the job counts.
        fixture
            .router
            .receive_message(RunJob(spec!(0, Tar).network(JobNetwork::Local), cjid!(0)));
        fixture
            .router
            .receive_message(LocalWorker(WorkerToBroker(jid!(0, 1), Ok(outcome!(0)))));
    }

    script_test! {
        job_response_from_local_worker_known_clustered,
        Fixture::new(false, 1, []),
        RunJob(spec!(0, Tar).network(JobNetwork::Local), cjid!(0)) => {
            EnqueueJobToLocalWorker(jid!(0, 0), spec!(0, Tar).network(JobNetwork::Local)),
        };
        LocalWorker(WorkerToBroker(jid!(0, 0), Ok(outcome!(0)))) => {
            JobDone(cjid!(0), Ok(outcome!(0))),
        };
    }

    #[test]
    #[should_panic(expected = "assertion failed: !self.standalone")]
    fn job_response_from_broker_unknown_standalone() {
        let mut fixture = Fixture::new(true, 1, []);
        fixture
            .router
            .receive_message(Broker(BrokerToClient::JobResponse(
                cjid!(0),
                Ok(outcome!(0)),
            )));
    }

    #[test]
    #[should_panic(expected = "assertion failed: !self.standalone")]
    fn job_response_from_broker_known_standalone() {
        let mut fixture = Fixture::new(true, 1, []);
        fixture
            .router
            .receive_message(RunJob(spec!(0, Tar), cjid!(0)));
        fixture
            .router
            .receive_message(Broker(BrokerToClient::JobResponse(
                cjid!(0),
                Ok(outcome!(0)),
            )));
    }

    #[test]
    #[should_panic(expected = "received response for unknown job 0")]
    fn job_response_from_broker_unknown_clustered() {
        let mut fixture = Fixture::new(false, 1, []);
        fixture
            .router
            .receive_message(Broker(BrokerToClient::JobResponse(
                cjid!(0),
                Ok(outcome!(0)),
            )));
    }

    script_test! {
        job_response_from_broker_known_clustered,
        Fixture::new(false, 1, []),
        RunJob(spec!(0, Tar), cjid!(0)) => {
            JobRequestToBroker(cjid!(0), spec!(0, Tar)),
        };
        Broker(BrokerToClient::JobResponse(cjid!(0), Ok(outcome!(0)))) => {
            JobDone(cjid!(0), Ok(outcome!(0))),
        };
    }

    script_test! {
        get_job_state_counts_standalone,
        Fixture::new(true, 1, []),
        GetJobStateCounts(0) => {
            TestMessage::JobStateCountsResponse(0, enum_map! {
                JobState::WaitingForArtifacts => 0,
                JobState::Pending => 0,
                JobState::Running => 0,
                JobState::Complete => 0,
            }),
        };
    }

    script_test! {
        get_job_state_counts_clustered,
        Fixture::new(false, 1, []),
        GetJobStateCounts(0) => {
            JobStatesCountRequestToBroker,
        }
    }

    script_test! {
        get_job_state_counts_lies_in_standalone_to_simulate_jobs_being_on_broker,
        Fixture::new(true, 2, []),
        GetJobStateCounts(0) => {
            TestMessage::JobStateCountsResponse(0, enum_map! {
                JobState::WaitingForArtifacts => 0,
                JobState::Pending => 0,
                JobState::Running => 0,
                JobState::Complete => 0,
            }),
        };

        RunJob(spec!(0, Tar), cjid!(0)) => {
            EnqueueJobToLocalWorker(jid!(0, 0), spec!(0, Tar)),
        };
        GetJobStateCounts(1) => {
            TestMessage::JobStateCountsResponse(1, enum_map! {
                JobState::WaitingForArtifacts => 0,
                JobState::Pending => 0,
                JobState::Running => 1,
                JobState::Complete => 0,
            }),
        };

        RunJob(spec!(1, Tar), cjid!(1)) => {
            EnqueueJobToLocalWorker(jid!(0, 1), spec!(1, Tar)),
        };
        GetJobStateCounts(2) => {
            TestMessage::JobStateCountsResponse(2, enum_map! {
                JobState::WaitingForArtifacts => 0,
                JobState::Pending => 0,
                JobState::Running => 2,
                JobState::Complete => 0,
            }),
        };

        RunJob(spec!(2, Tar), cjid!(2)) => {
            EnqueueJobToLocalWorker(jid!(0, 2), spec!(2, Tar)),
        };
        GetJobStateCounts(3) => {
            TestMessage::JobStateCountsResponse(3, enum_map! {
                JobState::WaitingForArtifacts => 0,
                JobState::Pending => 1,
                JobState::Running => 2,
                JobState::Complete => 0,
            }),
        };

        RunJob(spec!(3, Tar), cjid!(3)) => {
            EnqueueJobToLocalWorker(jid!(0, 3), spec!(3, Tar)),
        };
        GetJobStateCounts(4) => {
            TestMessage::JobStateCountsResponse(4, enum_map! {
                JobState::WaitingForArtifacts => 0,
                JobState::Pending => 2,
                JobState::Running => 2,
                JobState::Complete => 0,
            }),
        };

        LocalWorker(WorkerToBroker(jid!(0, 3), Ok(outcome!(3)))) => {
            JobDone(cjid!(3), Ok(outcome!(3))),
        };
        GetJobStateCounts(5) => {
            TestMessage::JobStateCountsResponse(5, enum_map! {
                JobState::WaitingForArtifacts => 0,
                JobState::Pending => 1,
                JobState::Running => 2,
                JobState::Complete => 1,
            }),
        };

        LocalWorker(WorkerToBroker(jid!(0, 2), Ok(outcome!(2)))) => {
            JobDone(cjid!(2), Ok(outcome!(2))),
        };
        GetJobStateCounts(6) => {
            TestMessage::JobStateCountsResponse(6, enum_map! {
                JobState::WaitingForArtifacts => 0,
                JobState::Pending => 0,
                JobState::Running => 2,
                JobState::Complete => 2,
            }),
        };

        LocalWorker(WorkerToBroker(jid!(0, 1), Ok(outcome!(1)))) => {
            JobDone(cjid!(1), Ok(outcome!(1))),
        };
        GetJobStateCounts(7) => {
            TestMessage::JobStateCountsResponse(7, enum_map! {
                JobState::WaitingForArtifacts => 0,
                JobState::Pending => 0,
                JobState::Running => 1,
                JobState::Complete => 3,
            }),
        };

        LocalWorker(WorkerToBroker(jid!(0, 0), Ok(outcome!(0)))) => {
            JobDone(cjid!(0), Ok(outcome!(0))),
        };
        GetJobStateCounts(8) => {
            TestMessage::JobStateCountsResponse(8, enum_map! {
                JobState::WaitingForArtifacts => 0,
                JobState::Pending => 0,
                JobState::Running => 0,
                JobState::Complete => 4,
            }),
        };
    }

    #[test]
    #[should_panic(expected = "assertion failed: !self.standalone")]
    fn job_states_response_from_broker_standalone() {
        let mut fixture = Fixture::new(true, 1, []);
        fixture
            .router
            .receive_message(Broker(BrokerToClient::JobStateCountsResponse(enum_map! {
                JobState::WaitingForArtifacts => 1,
                JobState::Pending => 2,
                JobState::Running => 3,
                JobState::Complete => 4,
            })));
    }

    #[test]
    #[should_panic(expected = "got JobStateCountsResponse with no requests outstanding")]
    fn job_states_response_from_broker_no_requests_outstanding_clustered() {
        let mut fixture = Fixture::new(false, 1, []);
        fixture
            .router
            .receive_message(Broker(BrokerToClient::JobStateCountsResponse(enum_map! {
                JobState::WaitingForArtifacts => 1,
                JobState::Pending => 2,
                JobState::Running => 3,
                JobState::Complete => 4,
            })));
    }

    script_test! {
        job_states_response_from_broker_clustered,
        Fixture::new(false, 1, []),

        GetJobStateCounts(0) => { JobStatesCountRequestToBroker };
        GetJobStateCounts(1) => { JobStatesCountRequestToBroker };
        GetJobStateCounts(2) => { JobStatesCountRequestToBroker };

        Broker(BrokerToClient::JobStateCountsResponse(enum_map! {
            JobState::WaitingForArtifacts => 1,
            JobState::Pending => 2,
            JobState::Running => 3,
            JobState::Complete => 4,
        })) => {
            TestMessage::JobStateCountsResponse(0, enum_map! {
                JobState::WaitingForArtifacts => 1,
                JobState::Pending => 2,
                JobState::Running => 3,
                JobState::Complete => 4,
            }),
        };

        RunJob(spec!(0, Tar).network(JobNetwork::Local), cjid!(0)) => {
            EnqueueJobToLocalWorker(jid!(0, 0), spec!(0, Tar).network(JobNetwork::Local)),
        };
        RunJob(spec!(1, Tar).network(JobNetwork::Local), cjid!(1)) => {
            EnqueueJobToLocalWorker(jid!(0, 1), spec!(1, Tar).network(JobNetwork::Local)),
        };
        Broker(BrokerToClient::JobStateCountsResponse(enum_map! {
            JobState::WaitingForArtifacts => 2,
            JobState::Pending => 3,
            JobState::Running => 4,
            JobState::Complete => 5,
        })) => {
            TestMessage::JobStateCountsResponse(1, enum_map! {
                JobState::WaitingForArtifacts => 2,
                JobState::Pending => 4,
                JobState::Running => 5,
                JobState::Complete => 5,
            }),
        };

        LocalWorker(WorkerToBroker(jid!(0, 0), Ok(outcome!(0)))) => {
            JobDone(cjid!(0), Ok(outcome!(0))),
        };
        Broker(BrokerToClient::JobStateCountsResponse(enum_map! {
            JobState::WaitingForArtifacts => 3,
            JobState::Pending => 4,
            JobState::Running => 5,
            JobState::Complete => 6,
        })) => {
            TestMessage::JobStateCountsResponse(2, enum_map! {
                JobState::WaitingForArtifacts => 3,
                JobState::Pending => 4,
                JobState::Running => 6,
                JobState::Complete => 7,
            }),
        };
    }

    #[test]
    #[should_panic(expected = "got unexpected statistics response")]
    fn statistic_response_panics() {
        let mut fixture = Fixture::new(false, 1, []);
        fixture
            .router
            .receive_message(Broker(StatisticsResponse(BrokerStatistics::default())));
    }
}
