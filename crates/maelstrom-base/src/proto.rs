//! Messages sent between various binaries.

use crate::{
    stats::BrokerStatistics, ArtifactUploadLocation, ClientJobId, JobBrokerStatus, JobId,
    JobOutcomeResult, JobSpec, JobWorkerStatus, Sha256Digest,
};
use bincode::Options;
use serde::{Deserialize, Serialize};

/// The first message sent by a connector to the broker. It identifies what the connector is, and
/// provides any relevant information.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum Hello {
    Client,
    Worker { slots: u32 },
    Monitor,
    ArtifactPusher,
    ArtifactFetcher,
}

/// Message sent from the broker to a worker. The broker won't send a message until it has received
/// a [`Hello`] and determined the type of its interlocutor.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum BrokerToWorker {
    EnqueueJob(JobId, JobSpec),
    CancelJob(JobId),
}

/// Message sent from a worker to the broker. These are responses to previous
/// [`BrokerToWorker::EnqueueJob`] messages. After sending the initial [`Hello`], a worker will
/// send a stream of these messages.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum WorkerToBroker {
    JobResponse(JobId, JobOutcomeResult),
    JobStatusUpdate(JobId, JobWorkerStatus),
}

/// Message sent from the broker to a client. The broker won't send a message until it has received
/// a [`Hello`] and determined the type of its interlocutor.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum BrokerToClient {
    JobResponse(ClientJobId, JobOutcomeResult),
    JobStatusUpdate(ClientJobId, JobBrokerStatus),
    TransferArtifact(Sha256Digest),
    GeneralError(String),
}

/// Message sent from a client to the broker. After sending the initial [`Hello`], a client will
/// send a stream of these messages.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ClientToBroker {
    JobRequest(ClientJobId, JobSpec),
    ArtifactTransferred(Sha256Digest, ArtifactUploadLocation),
}

/// Message sent from the broker to a monitor. The broker won't send a message until it has
/// recevied a [`Hello`] and determined the type of its interlocutor.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum BrokerToMonitor {
    StatisticsResponse(BrokerStatistics),
}

/// Message sent from a monitor to the broker. After sending the initial [`Hello`], a monitor will
/// send a stream of these messages.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum MonitorToBroker {
    StatisticsRequest,
}

/// Message sent from the broker to an artifact fetcher. This will be in response to an
/// [`ArtifactFetcherToBroker`] message. On failure to get the artifact, the result contains
/// details about what went wrong. After a failure, the broker will close the artifact fetcher
/// connection.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BrokerToArtifactFetcher(pub Result<u64, String>);

/// Message sent from an artifact fetcher to the broker. It will be answered with a
/// [`BrokerToArtifactFetcher`].
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ArtifactFetcherToBroker(pub Sha256Digest);

/// Message sent from the broker to an artifact pusher. This will be in response to an
/// [`ArtifactPusherToBroker`] message and the artifact's body. On success, the message contains no
/// other details, indicating that the artifact was successfully written to disk, and that the
/// digest matched. On failure, this message contains details about what went wrong. After a
/// failure, the broker will close the artifact pusher connection.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BrokerToArtifactPusher(pub Result<(), String>);

/// Message sent from an artifact pusher to the broker. It contains the digest and size of the
/// artifact. The body of the artifact will immediately follow this message. It will be answered
/// with a [`BrokerToArtifactPusher`].
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ArtifactPusherToBroker(pub Sha256Digest, pub u64);

fn bincode() -> impl Options {
    bincode::options().with_big_endian()
}

pub fn serialize<T: ?Sized + Serialize>(value: &T) -> bincode::Result<Vec<u8>> {
    bincode().serialize(value)
}

pub fn serialize_into<W: std::io::Write, T: ?Sized + Serialize>(
    writer: W,
    value: &T,
) -> bincode::Result<()> {
    bincode().serialize_into(writer, value)
}

pub fn serialized_size<T: ?Sized + Serialize>(value: &T) -> bincode::Result<u64> {
    bincode().serialized_size(value)
}

pub fn deserialize<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> bincode::Result<T> {
    bincode().deserialize(bytes)
}

fn fixint_bincode() -> impl Options {
    bincode().with_fixint_encoding()
}

pub fn fixint_serialize<T: ?Sized + Serialize>(value: &T) -> bincode::Result<Vec<u8>> {
    fixint_bincode().serialize(value)
}

pub fn fixint_serialize_into<W: std::io::Write, T: ?Sized + Serialize>(
    writer: W,
    value: &T,
) -> bincode::Result<()> {
    fixint_bincode().serialize_into(writer, value)
}

pub fn fixint_serialized_size<T: ?Sized + Serialize>(value: &T) -> bincode::Result<u64> {
    fixint_bincode().serialized_size(value)
}

pub fn fixint_deserialize<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> bincode::Result<T> {
    fixint_bincode().deserialize(bytes)
}
