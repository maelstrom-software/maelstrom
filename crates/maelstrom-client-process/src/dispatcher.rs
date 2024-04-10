use anyhow::{anyhow, Result};
use maelstrom_base::{
    proto::{BrokerToClient, ClientToBroker},
    stats::JobStateCounts,
    ClientJobId, JobOutcomeResult, JobSpec, Sha256Digest,
};
use maelstrom_client_base::ClientMessageKind;
use maelstrom_util::ext::OptionExt as _;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
};
use tokio::sync::mpsc::{Receiver, Sender};

pub trait Deps {
    type JobHandle;
    fn job_done(&self, handle: Self::JobHandle, cjid: ClientJobId, result: JobOutcomeResult);
    async fn send_message_to_broker(&mut self, message: ClientToBroker) -> Result<()>;
}

pub enum Message<DepsT: Deps> {
    BrokerToClient(BrokerToClient),
    AddArtifact(PathBuf, Sha256Digest),
    AddJob(JobSpec, DepsT::JobHandle),
    GetJobStateCounts(Sender<JobStateCounts>),
    Stop,
}

pub struct ArtifactPushRequest {
    pub path: PathBuf,
    pub digest: Sha256Digest,
}

pub struct Dispatcher<DepsT: Deps> {
    deps: DepsT,
    receiver: Receiver<Message<DepsT>>,
    artifact_pusher: Sender<ArtifactPushRequest>,
    stop_when_all_completed: bool,
    next_client_job_id: u32,
    artifacts: HashMap<Sha256Digest, PathBuf>,
    job_handles: HashMap<ClientJobId, DepsT::JobHandle>,
    stats_reqs: VecDeque<Sender<JobStateCounts>>,
}

impl<DepsT: Deps> Dispatcher<DepsT> {
    pub fn new(
        deps: DepsT,
        receiver: Receiver<Message<DepsT>>,
        artifact_pusher: Sender<ArtifactPushRequest>,
    ) -> Self {
        Self {
            deps,
            receiver,
            artifact_pusher,
            stop_when_all_completed: false,
            next_client_job_id: 0u32,
            artifacts: Default::default(),
            job_handles: Default::default(),
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

    async fn handle_message(&mut self, msg: Message<DepsT>) -> Result<(bool, ClientMessageKind)> {
        let mut kind = ClientMessageKind::Other;
        match msg {
            Message::BrokerToClient(BrokerToClient::JobResponse(cjid, result)) => {
                let handle = self.job_handles.remove(&cjid).unwrap();
                self.deps.job_done(handle, cjid, result);
                if self.stop_when_all_completed && self.job_handles.is_empty() {
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
            Message::AddJob(spec, handle) => {
                let cjid = self.next_client_job_id.into();
                self.job_handles.insert(cjid, handle).assert_is_none();
                self.next_client_job_id = self.next_client_job_id.checked_add(1).unwrap();
                self.deps
                    .send_message_to_broker(ClientToBroker::JobRequest(cjid, spec))
                    .await?;
                kind = ClientMessageKind::AddJob;
            }
            Message::Stop => {
                kind = ClientMessageKind::Stop;
                if self.job_handles.is_empty() {
                    return Ok((false, kind));
                }
                self.stop_when_all_completed = true;
            }
            Message::GetJobStateCounts(sender) => {
                self.deps
                    .send_message_to_broker(ClientToBroker::JobStateCountsRequest)
                    .await?;
                self.stats_reqs.push_back(sender);
                kind = ClientMessageKind::GetJobStateCounts;
            }
        }
        Ok((true, kind))
    }
}
