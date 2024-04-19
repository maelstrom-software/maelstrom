use crate::{
    dispatcher,
    local_broker::{Adapter, Deps, Message, Receiver},
};
use anyhow::{anyhow, Result};
use maelstrom_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    stats::{JobState, JobStateCounts},
    ClientId, JobId, Sha256Digest,
};
use maelstrom_util::{config::common::Slots, sync};
use std::{collections::HashMap, path::PathBuf};
use tokio::{sync::mpsc, task::JoinSet};

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
                self.deps.send_message_to_local_worker(
                    maelstrom_worker::dispatcher::Message::Broker(BrokerToWorker::EnqueueJob(
                        JobId {
                            cid: ClientId::from(0),
                            cjid,
                        },
                        spec,
                    )),
                );
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
                            .link_artifact_for_local_worker(stored_path.as_path(), path.as_path()),
                    },
                );
                self.deps.send_message_to_local_worker(response);
            }
            Message::Broker(_) => {
                unimplemented!("shouldn't get this message in standalone mode");
            }
        }
    }
}

pub fn start_task(
    join_set: &mut JoinSet<Result<()>>,
    slots: Slots,
    receiver: Receiver,
    dispatcher_sender: dispatcher::Sender,
    local_worker_sender: maelstrom_worker::DispatcherSender,
) {
    // Just create black-hole broker and artifact_pusher senders. We shouldn't be sending any
    // messages to them in standalone mode.
    let (broker_sender, broker_receiver) = mpsc::unbounded_channel();
    drop(broker_receiver);
    let (artifact_pusher_sender, artifact_pusher_receiver) = mpsc::unbounded_channel();
    drop(artifact_pusher_receiver);

    let adapter = Adapter::new(
        dispatcher_sender,
        broker_sender,
        artifact_pusher_sender,
        local_worker_sender,
    );
    let mut local_broker = LocalBroker::new(adapter, slots);
    join_set.spawn(sync::channel_reader(receiver, move |msg| {
        local_broker.receive_message(msg)
    }));
}
