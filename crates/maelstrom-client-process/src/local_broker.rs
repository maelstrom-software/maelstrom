use crate::{artifact_pusher, dispatcher};
use anyhow::Result;
use maelstrom_base::{
    proto::{BrokerToClient, ClientToBroker, WorkerToBroker},
    stats::JobStateCounts,
    ClientJobId, JobOutcomeResult, JobSpec, Sha256Digest,
};
use maelstrom_util::{fs::Fs, sync};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use tokio::{
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};

pub trait Deps {
    fn send_job_response_to_dispatcher(&mut self, cjid: ClientJobId, result: JobOutcomeResult);
    fn send_job_state_counts_response_to_dispatcher(&mut self, counts: JobStateCounts);

    // Only in remote broker mode.
    fn send_message_to_broker(&mut self, message: ClientToBroker);
    fn start_artifact_transfer_to_broker(&mut self, digest: Sha256Digest, path: &Path);

    // Only in standalone mode.
    fn send_message_to_local_worker(&mut self, message: maelstrom_worker::dispatcher::Message);
    fn link_artifact_for_local_worker(&mut self, from: &Path, to: &Path) -> Result<u64>;
}

pub enum Message {
    AddArtifact(PathBuf, Sha256Digest),
    JobRequest(ClientJobId, JobSpec),
    JobStateCountsRequest,

    // Only in non-standalone mode.
    Broker(BrokerToClient),

    // Only in standalone mode.
    LocalWorker(WorkerToBroker),
    LocalWorkerStartArtifactFetch(Sha256Digest, PathBuf),
}

pub struct LocalBroker<DepsT> {
    deps: DepsT,
    artifacts: HashMap<Sha256Digest, PathBuf>,
}

impl<DepsT: Deps> LocalBroker<DepsT> {
    pub fn new(deps: DepsT) -> Self {
        Self {
            deps,
            artifacts: HashMap::new(),
        }
    }

    pub fn receive_message(&mut self, message: Message) {
        match message {
            Message::AddArtifact(path, digest) => {
                self.artifacts.insert(digest, path);
            }
            Message::JobRequest(cjid, spec) => {
                self.deps
                    .send_message_to_broker(ClientToBroker::JobRequest(cjid, spec));
            }
            Message::JobStateCountsRequest => {
                self.deps
                    .send_message_to_broker(ClientToBroker::JobStateCountsRequest);
            }
            Message::Broker(BrokerToClient::JobResponse(cjid, result)) => {
                self.deps.send_job_response_to_dispatcher(cjid, result);
            }
            Message::Broker(BrokerToClient::TransferArtifact(digest)) => {
                let path = self.artifacts.get(&digest).unwrap_or_else(|| {
                    panic!("got request for unknown artifact with digest {digest}")
                });
                self.deps.start_artifact_transfer_to_broker(digest, path);
            }
            Message::Broker(BrokerToClient::StatisticsResponse(_)) => {
                unimplemented!("this client doesn't send statistics requests");
            }
            Message::Broker(BrokerToClient::JobStateCountsResponse(counts)) => {
                self.deps
                    .send_job_state_counts_response_to_dispatcher(counts);
            }
            Message::LocalWorker(_) => {
                unimplemented!("shouldn't get this message in non-standalone mode");
            }
            Message::LocalWorkerStartArtifactFetch(_, _) => {
                unimplemented!("shouldn't get this message in non-standalone mode");
            }
        }
    }
}

pub struct Adapter {
    dispatcher_sender: dispatcher::Sender,
    broker_sender: UnboundedSender<ClientToBroker>,
    artifact_pusher_sender: artifact_pusher::Sender,
    local_worker_sender: maelstrom_worker::DispatcherSender,
    fs: Fs,
}

impl Adapter {
    pub fn new(
        dispatcher_sender: dispatcher::Sender,
        broker_sender: UnboundedSender<ClientToBroker>,
        artifact_pusher_sender: artifact_pusher::Sender,
        local_worker_sender: maelstrom_worker::DispatcherSender,
    ) -> Self {
        Self {
            dispatcher_sender,
            broker_sender,
            artifact_pusher_sender,
            local_worker_sender,
            fs: Fs::new(),
        }
    }
}

impl Deps for Adapter {
    fn send_job_response_to_dispatcher(&mut self, cjid: ClientJobId, result: JobOutcomeResult) {
        let _ = self
            .dispatcher_sender
            .send(dispatcher::Message::JobResponse(cjid, result));
    }

    fn send_job_state_counts_response_to_dispatcher(&mut self, counts: JobStateCounts) {
        let _ = self
            .dispatcher_sender
            .send(dispatcher::Message::JobStateCountsResponse(counts));
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

    fn send_message_to_local_worker(&mut self, message: maelstrom_worker::dispatcher::Message) {
        let _ = self.local_worker_sender.send(message);
    }

    fn link_artifact_for_local_worker(&mut self, from: &Path, to: &Path) -> Result<u64> {
        self.fs.symlink(from, to)?;
        Ok(self.fs.metadata(to)?.len())
    }
}

pub type Sender = UnboundedSender<Message>;
pub type Receiver = UnboundedReceiver<Message>;

pub fn channel() -> (Sender, Receiver) {
    mpsc::unbounded_channel()
}

pub fn start_task(
    join_set: &mut JoinSet<Result<()>>,
    receiver: Receiver,
    dispatcher_sender: dispatcher::Sender,
    broker_sender: UnboundedSender<ClientToBroker>,
    artifact_pusher_sender: artifact_pusher::Sender,
) {
    // Just create a black-hole local_worker sender. We shouldn't be sending any
    // messages to it in remote mode.
    let (local_worker_sender, local_worker_receiver) = mpsc::unbounded_channel();
    drop(local_worker_receiver);
    let adapter = Adapter::new(
        dispatcher_sender,
        broker_sender,
        artifact_pusher_sender,
        local_worker_sender,
    );
    let mut local_broker = LocalBroker::new(adapter);
    join_set.spawn(sync::channel_reader(receiver, move |msg| {
        local_broker.receive_message(msg)
    }));
}
