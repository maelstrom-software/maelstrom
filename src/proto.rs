//! Messages sent between various binaries, and helper functions related to those messages.

use crate::{ClientExecutionId, ExecutionDetails, ExecutionId, ExecutionResult, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// The first message sent by a client or worker to the broker. It identifies the client/worker and
/// gives any relevant information.
#[derive(Serialize, Deserialize, Debug)]
pub enum Hello {
    Client { name: String },
    Worker { name: String, slots: u32 },
}

/// Message sent from the broker to a worker. The broker won't send a message until it has received
/// a [Hello] and determined the type of its interlocutor.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum WorkerRequest {
    EnqueueExecution(ExecutionId, ExecutionDetails),
    CancelExecution(ExecutionId),
}

/// Message sent from a worker to the broker. These are responses to previous
/// [WorkerRequest::EnqueueExecution] messages. After sending the initial [Hello], a worker will
/// exclusively send a stream of these messages.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkerResponse(pub ExecutionId, pub ExecutionResult);

/// Message sent from a client to the broker. After sending the initial [Hello], a client will
/// exclusively send a stream of these messages.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ClientRequest(pub ClientExecutionId, pub ExecutionDetails);

/// Message sent from the broker to a client. The broker won't send a message until it has recevied
/// a [Hello] and determined the type of its interlocutor.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ClientResponse(pub ClientExecutionId, pub ExecutionResult);

/// Write a message to a Tokio output stream. Each message is framed by sending a leading 4-byte,
/// little-endian message size.
pub async fn write_message(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    msg: impl Serialize,
) -> Result<()> {
    let msg_len = bincode::serialized_size(&msg)? as u32;

    let mut buf = Vec::<u8>::with_capacity(msg_len as usize + 4);
    std::io::Write::write(&mut buf, &msg_len.to_le_bytes())?;
    bincode::serialize_into(&mut buf, &msg)?;

    Ok(tokio::io::AsyncWriteExt::write_all(stream, &buf).await?)
}

/// Read a message from a Tokio input stream. The framing must match that of [write_message].
pub async fn read_message<T>(stream: &mut (impl tokio::io::AsyncRead + Unpin)) -> Result<T>
where
    T: DeserializeOwned,
{
    let mut msg_len: [u8; 4] = [0; 4];
    tokio::io::AsyncReadExt::read_exact(stream, &mut msg_len).await?;
    let msg_len = u32::from_le_bytes(msg_len) as usize;

    let mut buf = vec![0; msg_len];
    tokio::io::AsyncReadExt::read_exact(stream, &mut buf).await?;
    Ok(bincode::deserialize_from(&mut &buf[..])?)
}

/// Loop reading messages from a socket and writing them to an mpsc channel. If this function
/// encounters an error reading from the socket, it will return that error. On the other hand, if
/// it encounters an error writing to the sender -- which indicates that there is no longer a
/// receiver for the channel -- it will return Ok(()).
pub async fn socket_reader<T, U>(
    mut socket: (impl tokio::io::AsyncRead + Unpin),
    channel: tokio::sync::mpsc::UnboundedSender<U>,
    transform: impl Fn(T) -> U,
) -> Result<()>
where
    T: DeserializeOwned,
{
    loop {
        let msg = read_message::<T>(&mut socket).await?;
        if channel.send(transform(msg)).is_err() {
            return Ok(());
        }
    }
}

/// Loop reading messages from an mpsc channel and writing them to a socket. This will return
/// Ok(()) when all producers have closed their mpsc channel senders and there are no more messages
/// to read.
pub async fn socket_writer(
    mut channel: tokio::sync::mpsc::UnboundedReceiver<impl Serialize>,
    mut socket: (impl tokio::io::AsyncWrite + Unpin),
) -> Result<()> {
    while let Some(msg) = channel.recv().await {
        write_message(&mut socket, msg).await?;
    }
    Ok(())
}
