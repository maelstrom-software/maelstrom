//! Messages sent between various binaries, and helper functions related to those messages.

use crate::{ClientExecutionId, ExecutionDetails, ExecutionId, ExecutionResult, Sha256Digest};
use serde::{Deserialize, Serialize};

/// The first message sent by a client or worker to the broker. It identifies the client/worker and
/// gives any relevant information.
#[derive(Serialize, Deserialize, Debug)]
pub enum Hello {
    Client,
    Worker { slots: u32 },
    ClientArtifact { digest: Sha256Digest, length: u64 },
    WorkerArtifact { digest: Sha256Digest },
}

/// Message sent from the broker to a worker. The broker won't send a message until it has received
/// a [Hello] and determined the type of its interlocutor.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum BrokerToWorker {
    EnqueueExecution(ExecutionId, ExecutionDetails),
    CancelExecution(ExecutionId),
}

/// Message sent from a worker to the broker. These are responses to previous
/// [BrokerToWorker::EnqueueExecution] messages. After sending the initial [Hello], a worker will
/// exclusively send a stream of these messages.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkerToBroker(pub ExecutionId, pub ExecutionResult);

/// Message sent from a client to the broker. After sending the initial [Hello], a client will
/// exclusively send a stream of these messages.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ClientToBroker {
    ExecutionRequest(ClientExecutionId, ExecutionDetails),
    UiRequest(meticulous_ui::Request),
}

/// Message sent from the broker to a client. The broker won't send a message until it has recevied
/// a [Hello] and determined the type of its interlocutor.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum BrokerToClient {
    ExecutionResponse(ClientExecutionId, ExecutionResult),
    TransferArtifact(Sha256Digest),
    UiResponse(meticulous_ui::Response),
}
