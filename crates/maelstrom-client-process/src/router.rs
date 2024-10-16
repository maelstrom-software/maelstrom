use crate::artifact_pusher;
use anyhow::{anyhow, Error, Result};
use maelstrom_base::{
    proto::{BrokerToClient, BrokerToWorker, ClientToBroker, WorkerToBroker},
    ClientId, ClientJobId, JobId, JobOutcomeResult, JobSpec, Sha256Digest,
};
use maelstrom_client_base::{JobRunningStatus, JobStatus};
use maelstrom_util::{ext::OptionExt as _, sync};
use maelstrom_worker::local_worker;
use std::{collections::HashMap, path::PathBuf};
use tokio::{
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};

pub trait Deps {
    type JobHandle;
    fn job_update(&self, handle: &Self::JobHandle, status: JobStatus);

    // Only in remote-broker mode.
    fn send_job_request_to_broker(&self, cjid: ClientJobId, spec: JobSpec);
    fn start_artifact_transfer_to_broker(&self, digest: Sha256Digest, path: PathBuf);

    // For local jobs, which can happen in standalone mode or remote-broker mode.
    fn send_enqueue_job_to_local_worker(&self, jid: JobId, spec: JobSpec);
    fn send_artifact_fetch_completed_to_local_worker(
        &self,
        digest: Sha256Digest,
        result: Result<PathBuf>,
    );
    fn shutdown_local_worker(&self, error: Error);
}

pub enum Message<DepsT: Deps> {
    // These are requests from the client.
    AddArtifact(PathBuf, Sha256Digest),
    RunJob(JobSpec, DepsT::JobHandle),
    Shutdown(Error),

    // Only in remote-broker mode.
    Broker(BrokerToClient),

    // For local jobs, which can happen in standalone mode or remote-broker mode.
    LocalWorker(WorkerToBroker),
    LocalWorkerStartArtifactFetch(Sha256Digest),
}

struct JobEntry<HandleT> {
    handle: HandleT,
    status: Option<JobRunningStatus>,
}

impl<HandleT> JobEntry<HandleT> {
    fn new(handle: HandleT) -> Self {
        Self {
            handle,
            status: None,
        }
    }
}

struct Router<DepsT: Deps> {
    deps: DepsT,
    standalone: bool,
    artifacts: HashMap<Sha256Digest, PathBuf>,
    next_client_job_id: u32,
    jobs: HashMap<ClientJobId, JobEntry<DepsT::JobHandle>>,
    completed_jobs: u64,
}

impl<DepsT: Deps> Router<DepsT> {
    fn new(deps: DepsT, standalone: bool) -> Self {
        Self {
            deps,
            standalone,
            artifacts: Default::default(),
            next_client_job_id: Default::default(),
            jobs: Default::default(),
            completed_jobs: Default::default(),
        }
    }

    fn receive_job_response(&mut self, client_job_id: ClientJobId, result: JobOutcomeResult) {
        let handle = self
            .jobs
            .remove(&client_job_id)
            .unwrap_or_else(|| panic!("received response for unknown job {client_job_id}"))
            .handle;
        self.deps.job_update(
            &handle,
            JobStatus::Completed {
                client_job_id,
                result,
            },
        );
        self.completed_jobs += 1;
    }

    fn receive_message(&mut self, message: Message<DepsT>) {
        match message {
            Message::AddArtifact(path, digest) => {
                self.artifacts.insert(digest, path);
            }
            Message::RunJob(spec, handle) => {
                let cjid = self.next_client_job_id.into();
                self.next_client_job_id = self.next_client_job_id.checked_add(1).unwrap();

                self.jobs
                    .insert(cjid, JobEntry::new(handle))
                    .assert_is_none();

                if self.standalone || spec.must_be_run_locally() {
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
            Message::Broker(BrokerToClient::JobResponse(cjid, result)) => {
                assert!(!self.standalone);
                self.receive_job_response(cjid, result);
            }
            Message::Broker(BrokerToClient::JobStatusUpdate(cjid, status)) => {
                let status = JobRunningStatus::AtBroker(status);
                let job = self.jobs.get_mut(&cjid).unwrap();
                job.status = Some(status.clone());
                self.deps.job_update(&job.handle, status.into());
            }
            Message::Broker(BrokerToClient::TransferArtifact(digest)) => {
                assert!(!self.standalone);
                let path = self.artifacts.get(&digest).unwrap_or_else(|| {
                    panic!("got request for unknown artifact with digest {digest}")
                });
                self.deps
                    .start_artifact_transfer_to_broker(digest, path.to_owned());
            }
            Message::LocalWorker(WorkerToBroker::JobResponse(jid, result)) => {
                self.receive_job_response(jid.cjid, result);
            }
            Message::LocalWorker(WorkerToBroker::JobStatusUpdate(jid, status)) => {
                let status = JobRunningStatus::AtLocalWorker(status);
                let job = self.jobs.get_mut(&jid.cjid).unwrap();
                job.status = Some(status.clone());
                self.deps.job_update(&job.handle, status.into());
            }
            Message::LocalWorkerStartArtifactFetch(digest) => {
                let result = match self.artifacts.get(&digest) {
                    None => Err(anyhow!("no artifact found for digest {digest}")),
                    Some(path) => Ok(path.clone()),
                };
                self.deps
                    .send_artifact_fetch_completed_to_local_worker(digest, result);
            }
            Message::Shutdown(error) => self.deps.shutdown_local_worker(error),
        }
    }
}

pub struct Adapter {
    broker_sender: UnboundedSender<ClientToBroker>,
    artifact_pusher_sender: artifact_pusher::Sender,
    local_worker_sender: local_worker::DispatcherSender,
}

impl Adapter {
    fn new(
        broker_sender: UnboundedSender<ClientToBroker>,
        artifact_pusher_sender: artifact_pusher::Sender,
        local_worker_sender: local_worker::DispatcherSender,
    ) -> Self {
        Self {
            broker_sender,
            artifact_pusher_sender,
            local_worker_sender,
        }
    }
}

impl Deps for Adapter {
    type JobHandle = futures::channel::mpsc::UnboundedSender<JobStatus>;

    fn job_update(&self, handle: &Self::JobHandle, status: JobStatus) {
        handle.unbounded_send(status).ok();
    }

    fn send_job_request_to_broker(&self, cjid: ClientJobId, spec: JobSpec) {
        let _ = self
            .broker_sender
            .send(ClientToBroker::JobRequest(cjid, spec));
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
        result: Result<PathBuf>,
    ) {
        let _ = self
            .local_worker_sender
            .send(local_worker::Message::ArtifactFetchCompleted(
                digest,
                result.map(local_worker::GotArtifact::symlink),
            ));
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
    receiver: Receiver,
    broker_sender: UnboundedSender<ClientToBroker>,
    artifact_pusher_sender: artifact_pusher::Sender,
    local_worker_sender: local_worker::DispatcherSender,
) {
    let adapter = Adapter::new(broker_sender, artifact_pusher_sender, local_worker_sender);
    let mut router = Router::new(adapter, standalone);
    join_set.spawn(sync::channel_reader(receiver, move |msg| {
        router.receive_message(msg)
    }));
}

#[cfg(test)]
mod tests {
    use super::{Message::*, *};
    use maelstrom_base::JobNetwork;
    use maelstrom_test::*;
    use std::{cell::RefCell, rc::Rc, result};
    use BrokerToClient::*;
    use TestMessage::*;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum TestMessage {
        JobUpdate(ClientJobId, JobStatus),
        JobRequestToBroker(ClientJobId, JobSpec),
        StartArtifactTransferToBroker(Sha256Digest, PathBuf),
        EnqueueJobToLocalWorker(JobId, JobSpec),
        ArtifactFetchCompletedToLocalWorker(Sha256Digest, result::Result<PathBuf, String>),
        ShutdownLocalWorker(String),
    }

    struct TestState {
        messages: Vec<TestMessage>,
    }

    impl Deps for Rc<RefCell<TestState>> {
        type JobHandle = ClientJobId;

        fn job_update(&self, handle: &Self::JobHandle, status: JobStatus) {
            self.borrow_mut()
                .messages
                .push(TestMessage::JobUpdate(*handle, status));
        }

        fn send_job_request_to_broker(&self, cjid: ClientJobId, spec: JobSpec) {
            self.borrow_mut()
                .messages
                .push(TestMessage::JobRequestToBroker(cjid, spec));
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
            result: Result<PathBuf>,
        ) {
            self.borrow_mut()
                .messages
                .push(TestMessage::ArtifactFetchCompletedToLocalWorker(
                    digest,
                    result.map_err(|e| format!("{e}")),
                ));
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
        fn new<'a>(standalone: bool) -> Self {
            let test_state = Rc::new(RefCell::new(TestState {
                messages: Vec::default(),
            }));
            let router = Router::new(test_state.clone(), standalone);
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
        Fixture::new(true),
        LocalWorkerStartArtifactFetch(digest!(1)) => {
            ArtifactFetchCompletedToLocalWorker(
                digest!(1),
                Err(string!("no artifact found for digest 0000000000000000000000000000000000000000000000000000000000000001")),
            ),
        };
    }

    script_test! {
        local_worker_start_artifact_fetch_known_standalone,
        Fixture::new(true),
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        LocalWorkerStartArtifactFetch(digest!(1)) => {
            ArtifactFetchCompletedToLocalWorker(digest!(1), Ok(path_buf!("bar"))),
        };
    }

    script_test! {
        local_worker_start_artifact_last_add_artifact_wins_standalone,
        Fixture::new(true),
        AddArtifact(path_buf!("baz"), digest!(1)) => {};
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        LocalWorkerStartArtifactFetch(digest!(1)) => {
            ArtifactFetchCompletedToLocalWorker(digest!(1), Ok(path_buf!("bar"))),
        };
    }

    script_test! {
        local_worker_start_artifact_fetch_unknown_clustered,
        Fixture::new(false),
        LocalWorkerStartArtifactFetch(digest!(1)) => {
            ArtifactFetchCompletedToLocalWorker(
                digest!(1),
                Err(string!("no artifact found for digest 0000000000000000000000000000000000000000000000000000000000000001")),
            ),
        };
    }

    script_test! {
        local_worker_start_artifact_fetch_known_clustered,
        Fixture::new(false),
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        LocalWorkerStartArtifactFetch(digest!(1)) => {
            ArtifactFetchCompletedToLocalWorker(digest!(1), Ok(path_buf!("bar"))),
        };
    }

    script_test! {
        local_worker_start_artifact_last_add_artifact_wins_clustered,
        Fixture::new(false),
        AddArtifact(path_buf!("baz"), digest!(1)) => {};
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        LocalWorkerStartArtifactFetch(digest!(1)) => {
            ArtifactFetchCompletedToLocalWorker(digest!(1), Ok(path_buf!("bar"))),
        };
    }

    #[test]
    #[should_panic(expected = "assertion failed: !self.standalone")]
    fn broker_transfer_artifact_standalone() {
        let mut fixture = Fixture::new(true);
        fixture
            .router
            .receive_message(Broker(TransferArtifact(digest!(1))));
    }

    #[test]
    #[should_panic(
        expected = "got request for unknown artifact with digest 0000000000000000000000000000000000000000000000000000000000000001"
    )]
    fn broker_transfer_artifact_unknown_clustered() {
        let mut fixture = Fixture::new(false);
        fixture
            .router
            .receive_message(Broker(TransferArtifact(digest!(1))));
    }

    script_test! {
        broker_transfer_artifact_known_clustered,
        Fixture::new(false),
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        Broker(TransferArtifact(digest!(1))) => {
            StartArtifactTransferToBroker(digest!(1), path_buf!("bar")),
        };
    }

    script_test! {
        broker_transfer_artifact_last_add_artifact_wins_clustered,
        Fixture::new(false),
        AddArtifact(path_buf!("baz"), digest!(1)) => {};
        AddArtifact(path_buf!("bar"), digest!(1)) => {};
        Broker(TransferArtifact(digest!(1))) => {
            StartArtifactTransferToBroker(digest!(1), path_buf!("bar")),
        };
    }

    script_test! {
        run_job_standalone,
        Fixture::new(true),
        RunJob(spec!(0, Tar), cjid!(0)) => {
            EnqueueJobToLocalWorker(jid!(0, 0), spec!(0, Tar)),
        };
        RunJob(spec!(1, Tar), cjid!(1)) => {
            EnqueueJobToLocalWorker(jid!(0, 1), spec!(1, Tar)),
        };
    }

    script_test! {
        run_job_clustered,
        Fixture::new(false),
        RunJob(spec!(0, Tar), cjid!(0)) => {
            JobRequestToBroker(cjid!(0), spec!(0, Tar)),
        };
        RunJob(spec!(1, Tar), cjid!(1)) => {
            JobRequestToBroker(cjid!(1), spec!(1, Tar)),
        };
    }
    script_test! {
        run_job_must_be_local_clustered,
        Fixture::new(false),
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
        let mut fixture = Fixture::new(true);
        // Give it a job just so it doesn't crash subracting the job counts.
        fixture
            .router
            .receive_message(RunJob(spec!(0, Tar), cjid!(0)));
        fixture
            .router
            .receive_message(LocalWorker(WorkerToBroker::JobResponse(
                jid!(0, 1),
                Ok(outcome!(0)),
            )));
    }

    script_test! {
        job_response_from_local_worker_known_standalone,
        Fixture::new(true),
        RunJob(spec!(0, Tar), cjid!(0)) => {
            EnqueueJobToLocalWorker(jid!(0, 0), spec!(0, Tar)),
        };
        LocalWorker(WorkerToBroker::JobResponse(jid!(0, 0), Ok(outcome!(0)))) => {
            JobUpdate(cjid!(0), JobStatus::Completed { client_job_id: cjid!(0), result: Ok(outcome!(0)) }),
        };
    }

    #[test]
    #[should_panic(expected = "received response for unknown job 1")]
    fn job_response_from_local_worker_unknown_clustered() {
        let mut fixture = Fixture::new(false);
        // Give it a job just so it doesn't crash subracting the job counts.
        fixture
            .router
            .receive_message(RunJob(spec!(0, Tar).network(JobNetwork::Local), cjid!(0)));
        fixture
            .router
            .receive_message(LocalWorker(WorkerToBroker::JobResponse(
                jid!(0, 1),
                Ok(outcome!(0)),
            )));
    }

    script_test! {
        job_response_from_local_worker_known_clustered,
        Fixture::new(false),
        RunJob(spec!(0, Tar).network(JobNetwork::Local), cjid!(0)) => {
            EnqueueJobToLocalWorker(jid!(0, 0), spec!(0, Tar).network(JobNetwork::Local)),
        };
        LocalWorker(WorkerToBroker::JobResponse(jid!(0, 0), Ok(outcome!(0)))) => {
            JobUpdate(cjid!(0), JobStatus::Completed { client_job_id: cjid!(0), result: Ok(outcome!(0)) }),
        };
    }

    #[test]
    #[should_panic(expected = "assertion failed: !self.standalone")]
    fn job_response_from_broker_unknown_standalone() {
        let mut fixture = Fixture::new(true);
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
        let mut fixture = Fixture::new(true);
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
        let mut fixture = Fixture::new(false);
        fixture
            .router
            .receive_message(Broker(BrokerToClient::JobResponse(
                cjid!(0),
                Ok(outcome!(0)),
            )));
    }

    script_test! {
        job_response_from_broker_known_clustered,
        Fixture::new(false),
        RunJob(spec!(0, Tar), cjid!(0)) => {
            JobRequestToBroker(cjid!(0), spec!(0, Tar)),
        };
        Broker(BrokerToClient::JobResponse(cjid!(0), Ok(outcome!(0)))) => {
            JobUpdate(cjid!(0), JobStatus::Completed { client_job_id: cjid!(0), result: Ok(outcome!(0)) }),
        };
    }

    script_test! {
        shutdown_standalone,
        Fixture::new(true),
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
        shutdown_clustered,
        Fixture::new(false),
        RunJob(spec!(0, Tar), cjid!(0)) => {
            JobRequestToBroker(cjid!(0), spec!(0, Tar)),
        };
        RunJob(spec!(1, Tar), cjid!(1)) => {
            JobRequestToBroker(cjid!(1), spec!(1, Tar)),
        };
        Shutdown(anyhow!("test error")) => {
            ShutdownLocalWorker("test error".into())
        };
    }
}
