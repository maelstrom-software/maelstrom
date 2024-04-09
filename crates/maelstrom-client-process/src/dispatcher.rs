use anyhow::{anyhow, Result};
use maelstrom_base::{
    proto::{BrokerToClient, ClientToBroker},
    stats::JobStateCounts,
    ClientJobId, JobSpec, Sha256Digest,
};
use maelstrom_client_base::{ClientMessageKind, JobResponseHandler};
use maelstrom_util::{ext::OptionExt as _, net};
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
};
use tokio::net::tcp;
use tokio::sync::mpsc::{Receiver, Sender};

pub enum Message {
    BrokerToClient(BrokerToClient),
    AddArtifact(PathBuf, Sha256Digest),
    AddJob(JobSpec, JobResponseHandler),
    GetJobStateCounts(Sender<JobStateCounts>),
    Stop,
}

pub struct ArtifactPushRequest {
    pub path: PathBuf,
    pub digest: Sha256Digest,
}

pub struct Dispatcher {
    receiver: Receiver<Message>,
    pub stream: tcp::OwnedWriteHalf,
    artifact_pusher: Sender<ArtifactPushRequest>,
    stop_when_all_completed: bool,
    next_client_job_id: u32,
    artifacts: HashMap<Sha256Digest, PathBuf>,
    handlers: HashMap<ClientJobId, JobResponseHandler>,
    stats_reqs: VecDeque<Sender<JobStateCounts>>,
}

impl Dispatcher {
    pub fn new(
        receiver: Receiver<Message>,
        stream: tcp::OwnedWriteHalf,
        artifact_pusher: Sender<ArtifactPushRequest>,
    ) -> Self {
        Self {
            receiver,
            stream,
            artifact_pusher,
            stop_when_all_completed: false,
            next_client_job_id: 0u32,
            artifacts: Default::default(),
            handlers: Default::default(),
            stats_reqs: Default::default(),
        }
    }

    /// Processes one request. In order to drive the dispatcher, this should be called in a loop
    /// until the function return false
    pub async fn process_one(&mut self) -> Result<bool> {
        let msg = self
            .receiver
            .recv()
            .await
            .ok_or(anyhow!("dispatcher hangup"))?;
        let (cont, _) = self.handle_message(msg).await?;
        Ok(cont)
    }

    pub async fn process_one_and_tell(&mut self) -> Option<ClientMessageKind> {
        let msg = self.receiver.try_recv().ok()?;
        let (_, kind) = self.handle_message(msg).await.ok()?;
        Some(kind)
    }

    async fn handle_message(&mut self, msg: Message) -> Result<(bool, ClientMessageKind)> {
        let mut kind = ClientMessageKind::Other;
        match msg {
            Message::BrokerToClient(BrokerToClient::JobResponse(cjid, result)) => {
                self.handlers.remove(&cjid).unwrap()(cjid, result);
                if self.stop_when_all_completed && self.handlers.is_empty() {
                    return Ok((false, kind));
                }
            }
            Message::BrokerToClient(BrokerToClient::TransferArtifact(digest)) => {
                let path = self
                    .artifacts
                    .get(&digest)
                    .unwrap_or_else(|| {
                        panic!("got request for unknown artifact with digest {digest}")
                    })
                    .clone();
                self.artifact_pusher
                    .send(ArtifactPushRequest { path, digest })
                    .await?;
            }
            Message::BrokerToClient(BrokerToClient::StatisticsResponse(_)) => {
                unimplemented!("this client doesn't send statistics requests")
            }
            Message::BrokerToClient(BrokerToClient::JobStateCountsResponse(res)) => {
                self.stats_reqs.pop_front().unwrap().send(res).await.ok();
            }
            Message::AddArtifact(path, digest) => {
                self.artifacts.insert(digest, path);
            }
            Message::AddJob(spec, handler) => {
                let cjid = self.next_client_job_id.into();
                self.handlers.insert(cjid, handler).assert_is_none();
                self.next_client_job_id = self.next_client_job_id.checked_add(1).unwrap();
                net::write_message_to_async_socket(
                    &mut self.stream,
                    ClientToBroker::JobRequest(cjid, spec),
                )
                .await?;
                kind = ClientMessageKind::AddJob;
            }
            Message::Stop => {
                kind = ClientMessageKind::Stop;
                if self.handlers.is_empty() {
                    return Ok((false, kind));
                }
                self.stop_when_all_completed = true;
            }
            Message::GetJobStateCounts(sender) => {
                net::write_message_to_async_socket(
                    &mut self.stream,
                    ClientToBroker::JobStateCountsRequest,
                )
                .await?;
                self.stats_reqs.push_back(sender);
                kind = ClientMessageKind::GetJobStateCounts;
            }
        }
        Ok((true, kind))
    }
}
