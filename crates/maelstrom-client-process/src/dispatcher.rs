use anyhow::Result;
use maelstrom_base::{
    proto::{BrokerToClient, ClientToBroker},
    stats::JobStateCounts,
    ClientJobId, JobOutcomeResult, JobSpec, Sha256Digest,
};
use maelstrom_util::ext::OptionExt as _;
use std::{
    collections::{HashMap, VecDeque},
    ops::ControlFlow,
    path::PathBuf,
};

pub trait Deps {
    type JobHandle;
    fn job_done(&self, handle: Self::JobHandle, cjid: ClientJobId, result: JobOutcomeResult);
    type JobStateCountsHandle;
    fn job_state_counts(&self, handle: Self::JobStateCountsHandle, counts: JobStateCounts);
    async fn send_message_to_broker(&mut self, message: ClientToBroker) -> Result<()>;
    async fn send_artifact_to_broker(&mut self, digest: Sha256Digest, path: PathBuf) -> Result<()>;
}

pub enum Message<DepsT: Deps> {
    Broker(BrokerToClient),
    AddArtifact(PathBuf, Sha256Digest),
    AddJob(JobSpec, DepsT::JobHandle),
    GetJobStateCounts(DepsT::JobStateCountsHandle),
    Stop,
}

pub struct Dispatcher<DepsT: Deps> {
    deps: DepsT,
    stop_when_all_completed: bool,
    next_client_job_id: u32,
    artifacts: HashMap<Sha256Digest, PathBuf>,
    job_handles: HashMap<ClientJobId, DepsT::JobHandle>,
    stats_reqs: VecDeque<DepsT::JobStateCountsHandle>,
}

impl<DepsT: Deps> Dispatcher<DepsT> {
    pub fn new(deps: DepsT) -> Self {
        Self {
            deps,
            stop_when_all_completed: false,
            next_client_job_id: 0u32,
            artifacts: Default::default(),
            job_handles: Default::default(),
            stats_reqs: Default::default(),
        }
    }

    pub async fn receive_message(&mut self, msg: Message<DepsT>) -> Result<ControlFlow<()>> {
        match msg {
            Message::Broker(BrokerToClient::JobResponse(cjid, result)) => {
                let handle = self.job_handles.remove(&cjid).unwrap();
                self.deps.job_done(handle, cjid, result);
                if self.stop_when_all_completed && self.job_handles.is_empty() {
                    return Ok(ControlFlow::Break(()));
                }
            }
            Message::Broker(BrokerToClient::TransferArtifact(digest)) => {
                let path = self.artifacts.get(&digest).unwrap_or_else(|| {
                    panic!("got request for unknown artifact with digest {digest}")
                });
                self.deps
                    .send_artifact_to_broker(digest, path.clone())
                    .await?;
            }
            Message::Broker(BrokerToClient::StatisticsResponse(_)) => {
                unimplemented!("this client doesn't send statistics requests")
            }
            Message::Broker(BrokerToClient::JobStateCountsResponse(counts)) => {
                self.deps
                    .job_state_counts(self.stats_reqs.pop_front().unwrap(), counts);
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
            }
            Message::Stop => {
                if self.job_handles.is_empty() {
                    return Ok(ControlFlow::Break(()));
                }
                self.stop_when_all_completed = true;
            }
            Message::GetJobStateCounts(handle) => {
                self.deps
                    .send_message_to_broker(ClientToBroker::JobStateCountsRequest)
                    .await?;
                self.stats_reqs.push_back(handle);
            }
        }
        Ok(ControlFlow::Continue(()))
    }
}
