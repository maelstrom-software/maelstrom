use crate::local_broker;
use maelstrom_base::{stats::JobStateCounts, ClientJobId, JobOutcomeResult, JobSpec, Sha256Digest};
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
    fn send_message_to_local_broker(&mut self, message: local_broker::Message);
}

pub enum Message<DepsT: Deps> {
    AddArtifact(PathBuf, Sha256Digest),
    AddJob(JobSpec, DepsT::JobHandle),
    GetJobStateCounts(DepsT::JobStateCountsHandle),
    JobResponse(ClientJobId, JobOutcomeResult),
    JobStateCountsResponse(JobStateCounts),
    Stop,
}

pub struct Dispatcher<DepsT: Deps> {
    deps: DepsT,
    stop_when_all_completed: bool,
    next_client_job_id: u32,
    job_handles: HashMap<ClientJobId, DepsT::JobHandle>,
    stats_reqs: VecDeque<DepsT::JobStateCountsHandle>,
}

impl<DepsT: Deps> Dispatcher<DepsT> {
    pub fn new(deps: DepsT) -> Self {
        Self {
            deps,
            stop_when_all_completed: false,
            next_client_job_id: 0u32,
            job_handles: Default::default(),
            stats_reqs: Default::default(),
        }
    }

    pub fn receive_message(&mut self, msg: Message<DepsT>) -> ControlFlow<()> {
        match msg {
            Message::JobResponse(cjid, result) => {
                let handle = self.job_handles.remove(&cjid).unwrap();
                self.deps.job_done(handle, cjid, result);
                if self.stop_when_all_completed && self.job_handles.is_empty() {
                    return ControlFlow::Break(());
                }
            }
            Message::JobStateCountsResponse(counts) => {
                self.deps
                    .job_state_counts(self.stats_reqs.pop_front().unwrap(), counts);
            }
            Message::AddArtifact(path, digest) => {
                self.deps
                    .send_message_to_local_broker(local_broker::Message::AddArtifact(path, digest));
            }
            Message::AddJob(spec, handle) => {
                let cjid = self.next_client_job_id.into();
                self.job_handles.insert(cjid, handle).assert_is_none();
                self.next_client_job_id = self.next_client_job_id.checked_add(1).unwrap();
                self.deps
                    .send_message_to_local_broker(local_broker::Message::JobRequest(cjid, spec));
            }
            Message::Stop => {
                if self.job_handles.is_empty() {
                    return ControlFlow::Break(());
                }
                self.stop_when_all_completed = true;
            }
            Message::GetJobStateCounts(handle) => {
                self.deps
                    .send_message_to_local_broker(local_broker::Message::JobStateCountsRequest);
                self.stats_reqs.push_back(handle);
            }
        }
        ControlFlow::Continue(())
    }
}
