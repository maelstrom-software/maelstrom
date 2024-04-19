use crate::{
    dispatcher,
    local_broker::{Message, Receiver},
};
use anyhow::{anyhow, Result};
use maelstrom_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    stats::{JobState, JobStateCounts},
    ClientId, ClientJobId, JobId, JobOutcomeResult, Sha256Digest,
};
use maelstrom_util::{config::common::Slots, fs::Fs, sync};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use tokio::task::JoinSet;

pub trait Deps {
    fn send_job_response_to_dispatcher(&mut self, cjid: ClientJobId, result: JobOutcomeResult);
    fn send_job_state_counts_response_to_dispatcher(&mut self, counts: JobStateCounts);
    fn send_message_to_worker(&mut self, message: maelstrom_worker::dispatcher::Message);
    fn link_artifact(&mut self, from: &Path, to: &Path) -> Result<u64>;
}

pub struct LocalBroker<DepsT> {
    deps: DepsT,
    slots: Slots,
    artifacts: HashMap<Sha256Digest, PathBuf>,
    counts: JobStateCounts,
}

impl<DepsT: Deps> LocalBroker<DepsT> {
    pub fn new(deps: DepsT, slots: Slots) -> Self {
        Self {
            deps,
            slots,
            artifacts: HashMap::new(),
            counts: JobStateCounts::default(),
        }
    }

    pub fn receive_message(&mut self, message: Message) {
        match message {
            Message::AddArtifact(path, digest) => {
                self.artifacts.insert(digest, path);
            }
            Message::JobRequest(cjid, spec) => {
                if self.counts[JobState::Running] < *self.slots.inner() as u64 {
                    self.counts[JobState::Running] += 1;
                } else {
                    self.counts[JobState::Pending] += 1;
                }
                self.deps
                    .send_message_to_worker(maelstrom_worker::dispatcher::Message::Broker(
                        BrokerToWorker::EnqueueJob(
                            JobId {
                                cid: ClientId::from(0),
                                cjid,
                            },
                            spec,
                        ),
                    ));
            }
            Message::JobStateCountsRequest => {
                self.deps
                    .send_job_state_counts_response_to_dispatcher(self.counts);
            }
            Message::LocalWorker(WorkerToBroker(jid, result)) => {
                if self.counts[JobState::Pending] > 0 {
                    self.counts[JobState::Pending] -= 1;
                } else {
                    self.counts[JobState::Running] -= 1;
                }
                self.counts[JobState::Complete] += 1;
                self.deps.send_job_response_to_dispatcher(jid.cjid, result);
            }
            Message::LocalWorkerStartArtifactFetch(digest, path) => {
                let response = maelstrom_worker::dispatcher::Message::ArtifactFetchCompleted(
                    digest.clone(),
                    match self.artifacts.get(&digest) {
                        None => Err(anyhow!("no artifact found for digest {digest}")),
                        Some(stored_path) => self
                            .deps
                            .link_artifact(stored_path.as_path(), path.as_path()),
                    },
                );
                self.deps.send_message_to_worker(response);
            }
            Message::Broker(_) => {
                unimplemented!("shouldn't get this message in standalone mode");
            }
        }
    }
}

struct Adapter {
    dispatcher_sender: dispatcher::Sender,
    worker_sender: maelstrom_worker::DispatcherSender,
    fs: Fs,
}

impl Deps for Adapter {
    fn send_job_response_to_dispatcher(&mut self, cjid: ClientJobId, result: JobOutcomeResult) {
        self.dispatcher_sender
            .send(dispatcher::Message::JobResponse(cjid, result))
            .ok();
    }

    fn send_job_state_counts_response_to_dispatcher(&mut self, counts: JobStateCounts) {
        self.dispatcher_sender
            .send(dispatcher::Message::JobStateCountsResponse(counts))
            .ok();
    }

    fn send_message_to_worker(&mut self, message: maelstrom_worker::dispatcher::Message) {
        self.worker_sender.send(message).ok();
    }

    fn link_artifact(&mut self, from: &Path, to: &Path) -> Result<u64> {
        self.fs.symlink(from, to)?;
        Ok(self.fs.metadata(to)?.len())
    }
}

pub fn start_task(
    join_set: &mut JoinSet<Result<()>>,
    slots: Slots,
    receiver: Receiver,
    dispatcher_sender: dispatcher::Sender,
    worker_sender: maelstrom_worker::DispatcherSender,
) {
    let adapter = Adapter {
        dispatcher_sender,
        worker_sender,
        fs: Fs::new(),
    };
    let mut local_broker = LocalBroker::new(adapter, slots);
    join_set.spawn(sync::channel_reader(receiver, move |msg| {
        local_broker.receive_message(msg)
    }));
}
