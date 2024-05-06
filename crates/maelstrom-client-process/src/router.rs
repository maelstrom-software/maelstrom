use crate::artifact_pusher;
use anyhow::{anyhow, Result};
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

    type AllJobsCompleteHandle;
    fn all_jobs_complete(&self, handle: Self::AllJobsCompleteHandle);

    // Only in remote broker mode.
    fn send_message_to_broker(&mut self, message: ClientToBroker);
    fn start_artifact_transfer_to_broker(&mut self, digest: Sha256Digest, path: &Path);

    // Only in standalone mode.
    fn send_message_to_local_worker(&mut self, message: local_worker::Message);
    fn link_artifact_for_local_worker(&mut self, from: &Path, to: &Path) -> Result<u64>;
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
}

struct Router<DepsT: Deps> {
    deps: DepsT,
    standalone: bool,
    slots: Slots,
    artifacts: HashMap<Sha256Digest, PathBuf>,
    next_client_job_id: u32,
    job_handles: HashMap<ClientJobId, DepsT::JobHandle>,
    job_state_counts_handles: VecDeque<DepsT::JobStateCountsHandle>,
    all_jobs_complete_handles: Vec<DepsT::AllJobsCompleteHandle>,
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
            all_jobs_complete_handles: Default::default(),
            counts: Default::default(),
        }
    }

    fn receive_job_response(&mut self, cjid: ClientJobId, result: JobOutcomeResult) {
        let handle = self.job_handles.remove(&cjid).unwrap();
        self.deps.job_done(handle, cjid, result);
        if self.job_handles.is_empty() {
            for handle in self.all_jobs_complete_handles.drain(..) {
                self.deps.all_jobs_complete(handle);
            }
        }
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

                if self.standalone {
                    if self.counts[JobState::Running] < *self.slots.inner() as u64 {
                        self.counts[JobState::Running] += 1;
                    } else {
                        self.counts[JobState::Pending] += 1;
                    }
                    self.deps
                        .send_message_to_local_worker(local_worker::Message::Broker(
                            BrokerToWorker::EnqueueJob(
                                JobId {
                                    cid: ClientId::from(0),
                                    cjid,
                                },
                                spec,
                            ),
                        ));
                } else {
                    self.deps
                        .send_message_to_broker(ClientToBroker::JobRequest(cjid, spec));
                }
            }
            Message::GetJobStateCounts(handle) => {
                if self.standalone {
                    assert!(self.job_state_counts_handles.is_empty());
                    self.deps.job_state_counts(handle, self.counts);
                } else {
                    self.job_state_counts_handles.push_back(handle);
                    self.deps
                        .send_message_to_broker(ClientToBroker::JobStateCountsRequest);
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
                self.deps.start_artifact_transfer_to_broker(digest, path);
            }
            Message::Broker(BrokerToClient::StatisticsResponse(_)) => {
                unimplemented!("this client doesn't send statistics requests");
            }
            Message::Broker(BrokerToClient::JobStateCountsResponse(counts)) => {
                assert!(!self.standalone);
                self.deps
                    .job_state_counts(self.job_state_counts_handles.pop_front().unwrap(), counts);
            }
            Message::LocalWorker(WorkerToBroker(jid, result)) => {
                assert!(self.standalone);
                if self.counts[JobState::Pending] > 0 {
                    self.counts[JobState::Pending] -= 1;
                } else {
                    self.counts[JobState::Running] -= 1;
                }
                self.counts[JobState::Complete] += 1;
                self.receive_job_response(jid.cjid, result);
            }
            Message::LocalWorkerStartArtifactFetch(digest, path) => {
                assert!(self.standalone);
                let response = local_worker::Message::ArtifactFetchCompleted(
                    digest.clone(),
                    match self.artifacts.get(&digest) {
                        None => Err(anyhow!("no artifact found for digest {digest}")),
                        Some(stored_path) => self
                            .deps
                            .link_artifact_for_local_worker(stored_path.as_path(), path.as_path()),
                    },
                );
                self.deps.send_message_to_local_worker(response);
            }
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

    type AllJobsCompleteHandle = oneshot::Sender<()>;
    fn all_jobs_complete(&self, handle: Self::AllJobsCompleteHandle) {
        handle.send(()).ok();
    }

    fn send_message_to_broker(&mut self, message: ClientToBroker) {
        let _ = self.broker_sender.send(message);
    }

    fn start_artifact_transfer_to_broker(&mut self, digest: Sha256Digest, path: &Path) {
        let _ = self.artifact_pusher_sender.send(artifact_pusher::Message {
            digest,
            path: path.to_owned(),
        });
    }

    fn send_message_to_local_worker(&mut self, message: local_worker::Message) {
        let _ = self.local_worker_sender.send(message);
    }

    fn link_artifact_for_local_worker(&mut self, from: &Path, to: &Path) -> Result<u64> {
        self.fs.symlink(from, to)?;
        Ok(self.fs.metadata(to)?.len())
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
