use crate::{dispatcher, ArtifactPushRequest};
use maelstrom_base::{
    proto::{BrokerToClient, ClientToBroker},
    stats::JobStateCounts,
    ClientJobId, JobOutcomeResult, JobSpec, Sha256Digest,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use tokio::sync::mpsc::UnboundedSender;

pub trait Deps {
    fn send_job_response_to_dispatcher(&mut self, cjid: ClientJobId, result: JobOutcomeResult);
    fn send_job_state_counts_response_to_dispatcher(&mut self, counts: JobStateCounts);
    fn send_message_to_broker(&mut self, message: ClientToBroker);
    fn start_artifact_transfer_to_broker(&mut self, digest: Sha256Digest, path: &Path);
}

pub enum Message {
    AddArtifact(PathBuf, Sha256Digest),
    JobRequest(ClientJobId, JobSpec),
    JobStateCountsRequest,
    Broker(BrokerToClient),
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
        }
    }
}

pub struct LocalBrokerAdapter {
    dispatcher_sender: UnboundedSender<dispatcher::Message<dispatcher::Adapter>>,
    broker_sender: UnboundedSender<ClientToBroker>,
    artifact_pusher_sender: UnboundedSender<ArtifactPushRequest>,
}

impl LocalBrokerAdapter {
    pub fn new(
        dispatcher_sender: UnboundedSender<dispatcher::Message<dispatcher::Adapter>>,
        broker_sender: UnboundedSender<ClientToBroker>,
        artifact_pusher_sender: UnboundedSender<ArtifactPushRequest>,
    ) -> Self {
        Self {
            dispatcher_sender,
            broker_sender,
            artifact_pusher_sender,
        }
    }
}

impl Deps for LocalBrokerAdapter {
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

    fn send_message_to_broker(&mut self, message: ClientToBroker) {
        self.broker_sender.send(message).ok();
    }

    fn start_artifact_transfer_to_broker(&mut self, digest: Sha256Digest, path: &Path) {
        self.artifact_pusher_sender
            .send(ArtifactPushRequest {
                digest,
                path: path.to_owned(),
            })
            .ok();
    }
}
