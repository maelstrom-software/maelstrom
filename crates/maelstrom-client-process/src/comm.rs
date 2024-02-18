use crate::{ClientDriverMode, Layer};
use maelstrom_base::{
    stats::JobStateCounts, ArtifactType, ClientJobId, JobOutcomeResult, JobSpec, Sha256Digest,
};
use maelstrom_container::ContainerImage;
use maelstrom_util::config::BrokerAddr;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommunicationError {
    message: String,
}

impl<'a> From<&'a str> for CommunicationError {
    fn from(message: &'a str) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for CommunicationError {
    fn from(e: anyhow::Error) -> Self {
        Self {
            message: format!("{e:?}"),
        }
    }
}

impl fmt::Display for CommunicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for CommunicationError {}

pub type Result<T> = std::result::Result<T, CommunicationError>;

#[derive(Debug, Serialize, Deserialize)]
pub enum ProgressInfo {
    Length(u64),
    Inc(u64),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProgressResponse<T> {
    InProgress(ProgressInfo),
    Done(T),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Start(Result<()>),
    AddArtifact(Result<Sha256Digest>),
    AddLayer(Result<(Sha256Digest, ArtifactType)>),
    AddJob(Result<(ClientJobId, JobOutcomeResult)>),
    GetContainerImage(Result<ProgressResponse<ContainerImage>>),
    StopAccepting(Result<()>),
    WaitForOutstandingJobs(Result<()>),
    GetJobStateCounts(Result<JobStateCounts>),
    ProcessBrokerMsgSingleThreaded(Result<()>),
    ProcessClientMessagesSingleThreaded(Result<()>),
    ProcessArtifactSingleThreaded(Result<()>),
    Error(CommunicationError),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Start {
        driver_mode: ClientDriverMode,
        broker_addr: BrokerAddr,
        project_dir: PathBuf,
        cache_dir: PathBuf,
    },
    AddArtifact {
        path: PathBuf,
    },
    AddLayer {
        layer: Layer,
    },
    AddJob {
        spec: JobSpec,
    },
    GetContainerImage {
        name: String,
        tag: String,
    },
    StopAccepting,
    WaitForOutstandingJobs,
    GetJobStateCounts,
    ProcessBrokerMsgSingleThreaded {
        count: usize,
    },
    ProcessClientMessagesSingleThreaded,
    ProcessArtifactSingleThreaded,
}

#[derive(Copy, Clone, Default, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageId(u64);

impl MessageId {
    pub fn next(&self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<BodyT> {
    pub id: MessageId,
    pub finished: bool,
    pub body: BodyT,
}
