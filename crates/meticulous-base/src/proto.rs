//! Messages sent between various binaries, and helper functions related to those messages.

use crate::{BrokerStatistics, ClientJobId, JobDetails, JobId, JobResult, Sha256Digest};
use serde::{Deserialize, Serialize};

/// The first message sent by a client or worker to the broker. It identifies the client/worker and
/// gives any relevant information.
#[derive(Serialize, Deserialize, Debug)]
pub enum Hello {
    Client,
    Worker { slots: u32 },
    ClientArtifact { digest: Sha256Digest },
    WorkerArtifact { digest: Sha256Digest },
}

/// Message sent from the broker to a worker. The broker won't send a message until it has received
/// a [Hello] and determined the type of its interlocutor.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum BrokerToWorker {
    EnqueueJob(JobId, JobDetails),
    CancelJob(JobId),
}

/// Message sent from a worker to the broker. These are responses to previous
/// [BrokerToWorker::EnqueueJob] messages. After sending the initial [Hello], a worker will
/// exclusively send a stream of these messages.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkerToBroker(pub JobId, pub JobResult);

/// Message sent from a client to the broker. After sending the initial [Hello], a client will
/// exclusively send a stream of these messages.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ClientToBroker {
    JobRequest(ClientJobId, JobDetails),
    StatisticsRequest,
}

/// Message sent from the broker to a client. The broker won't send a message until it has recevied
/// a [Hello] and determined the type of its interlocutor.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum BrokerToClient {
    JobResponse(ClientJobId, JobResult),
    TransferArtifact(Sha256Digest),
    StatisticsResponse(BrokerStatistics),
}
