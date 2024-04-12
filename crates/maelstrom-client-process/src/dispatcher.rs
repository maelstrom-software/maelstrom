use crate::local_broker;
use maelstrom_base::{stats::JobStateCounts, ClientJobId, JobOutcomeResult, JobSpec, Sha256Digest};
use maelstrom_util::ext::OptionExt as _;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
};
use tokio::sync::{mpsc::UnboundedSender, oneshot};

pub trait Deps {
    type JobHandle;
    fn job_done(&self, handle: Self::JobHandle, cjid: ClientJobId, result: JobOutcomeResult);

    type JobStateCountsHandle;
    fn job_state_counts(&self, handle: Self::JobStateCountsHandle, counts: JobStateCounts);

    type AllJobsCompleteHandle;
    fn all_jobs_complete(&self, handle: Self::AllJobsCompleteHandle);

    fn send_message_to_local_broker(&mut self, message: local_broker::Message);
}

pub enum Message<DepsT: Deps> {
    AddArtifact(PathBuf, Sha256Digest),
    AddJob(JobSpec, DepsT::JobHandle),
    GetJobStateCounts(DepsT::JobStateCountsHandle),
    JobResponse(ClientJobId, JobOutcomeResult),
    JobStateCountsResponse(JobStateCounts),
    NotifyWhenAllJobsComplete(DepsT::AllJobsCompleteHandle),
}

pub struct Dispatcher<DepsT: Deps> {
    deps: DepsT,
    next_client_job_id: u32,
    job_handles: HashMap<ClientJobId, DepsT::JobHandle>,
    job_state_counts_handles: VecDeque<DepsT::JobStateCountsHandle>,
    all_jobs_complete_handles: Vec<DepsT::AllJobsCompleteHandle>,
}

impl<DepsT: Deps> Dispatcher<DepsT> {
    pub fn new(deps: DepsT) -> Self {
        Self {
            deps,
            next_client_job_id: 0u32,
            job_handles: Default::default(),
            job_state_counts_handles: Default::default(),
            all_jobs_complete_handles: Default::default(),
        }
    }

    pub fn receive_message(&mut self, message: Message<DepsT>) {
        match message {
            Message::JobResponse(cjid, result) => {
                let handle = self.job_handles.remove(&cjid).unwrap();
                self.deps.job_done(handle, cjid, result);
                if self.job_handles.is_empty() {
                    for handle in self.all_jobs_complete_handles.drain(..) {
                        self.deps.all_jobs_complete(handle);
                    }
                }
            }
            Message::JobStateCountsResponse(counts) => {
                self.deps
                    .job_state_counts(self.job_state_counts_handles.pop_front().unwrap(), counts);
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
            Message::GetJobStateCounts(handle) => {
                self.deps
                    .send_message_to_local_broker(local_broker::Message::JobStateCountsRequest);
                self.job_state_counts_handles.push_back(handle);
            }
            Message::NotifyWhenAllJobsComplete(handle) => {
                if self.job_handles.is_empty() {
                    assert!(self.all_jobs_complete_handles.is_empty());
                    self.deps.all_jobs_complete(handle);
                } else {
                    self.all_jobs_complete_handles.push(handle);
                }
            }
        }
    }
}

pub struct DispatcherAdapter {
    local_broker_sender: UnboundedSender<local_broker::Message>,
}

impl DispatcherAdapter {
    pub fn new(local_broker_sender: UnboundedSender<local_broker::Message>) -> Self {
        Self {
            local_broker_sender,
        }
    }
}

impl Deps for DispatcherAdapter {
    type JobHandle = oneshot::Sender<(ClientJobId, JobOutcomeResult)>;

    fn job_done(&self, handle: Self::JobHandle, cjid: ClientJobId, result: JobOutcomeResult) {
        handle.send((cjid, result)).ok();
    }

    type JobStateCountsHandle = oneshot::Sender<JobStateCounts>;

    fn job_state_counts(&self, handle: Self::JobStateCountsHandle, counts: JobStateCounts) {
        handle.send(counts).ok();
    }

    fn send_message_to_local_broker(&mut self, message: local_broker::Message) {
        self.local_broker_sender.send(message).ok();
    }

    type AllJobsCompleteHandle = oneshot::Sender<()>;
    fn all_jobs_complete(&self, handle: Self::AllJobsCompleteHandle) {
        handle.send(()).ok();
    }
}
