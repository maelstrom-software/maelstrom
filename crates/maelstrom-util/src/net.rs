//! Functions that are useful for reading/writing messages from/to sockets.

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    io::{Read, Write},
    sync::mpsc::SyncSender,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

fn write_message_to_vec(msg: impl Serialize) -> Result<Vec<u8>> {
    let msg_len = bincode::serialized_size(&msg)? as u32;
    let mut buf = Vec::<u8>::with_capacity(msg_len as usize + 4);
    Write::write_all(&mut buf, &msg_len.to_be_bytes())?;
    bincode::serialize_into(&mut buf, &msg)?;
    Ok(buf)
}

/// Write a message to a normal (threaded) writer. Each message is framed by sending a leading
/// 4-byte, little-endian message size.
pub fn write_message_to_socket(stream: &mut impl Write, msg: impl Serialize) -> Result<()> {
    Ok(stream.write_all(&write_message_to_vec(msg)?)?)
}

/// Write a message to a Tokio output stream. Each message is framed by sending a leading 4-byte,
/// little-endian message size.
pub async fn write_message_to_async_socket(
    stream: &mut (impl AsyncWrite + Unpin),
    msg: impl Serialize,
) -> Result<()> {
    Ok(stream.write_all(&write_message_to_vec(msg)?).await?)
}

/// Read a message from a normal (threaded) reader. The framing must match that of
/// [`write_message_to_socket`] and [`write_message_to_async_socket`].
pub fn read_message_from_socket<MessageT>(stream: &mut impl Read) -> Result<MessageT>
where
    MessageT: DeserializeOwned,
{
    let mut msg_len: [u8; 4] = [0; 4];
    stream.read_exact(&mut msg_len)?;
    let mut buf = vec![0; u32::from_be_bytes(msg_len) as usize];
    stream.read_exact(&mut buf)?;
    Ok(bincode::deserialize_from(&mut &buf[..])?)
}

/// Read a message from a Tokio input stream. The framing must match that of
/// [`write_message_to_socket`] and [`write_message_to_async_socket`].
pub async fn read_message_from_async_socket<MessageT>(
    stream: &mut (impl AsyncRead + Unpin),
) -> Result<MessageT>
where
    MessageT: DeserializeOwned,
{
    let mut msg_len: [u8; 4] = [0; 4];
    stream.read_exact(&mut msg_len).await?;
    let mut buf = vec![0; u32::from_be_bytes(msg_len) as usize];
    stream.read_exact(&mut buf).await?;
    Ok(bincode::deserialize_from(&mut &buf[..])?)
}

/// Loop, reading messages from a channel and writing them to a socket. The `log` parameter is used
/// to insert debug logging.
pub async fn async_socket_writer<MessageT>(
    mut channel: UnboundedReceiver<MessageT>,
    mut socket: (impl AsyncWrite + Unpin),
    mut log: impl FnMut(&MessageT),
) where
    MessageT: Serialize,
{
    while let Some(msg) = channel.recv().await {
        log(&msg);
        if write_message_to_async_socket(&mut socket, msg)
            .await
            .is_err()
        {
            break;
        }
    }
}

/// Loop, reading messages from a socket and writing them to an mpsc channel. The `transform`
/// parameter is used to log the messages and wrap them in any necessary structure for internal use
/// by the program.
pub async fn async_socket_reader<MessageT, TransformedT>(
    mut socket: (impl AsyncRead + Unpin),
    channel: UnboundedSender<TransformedT>,
    transform: impl Fn(MessageT) -> TransformedT,
) where
    MessageT: DeserializeOwned,
{
    while let Ok(msg) = read_message_from_async_socket(&mut socket).await {
        if channel.send(transform(msg)).is_err() {
            break;
        }
    }
}

/// Loop, reading messages from a socket and writing them to an mpsc channel. The `transform`
/// parameter is used to log the messages and wrap them in any necessary structure for internal use
/// by the program.
pub fn socket_reader<MessageT, TransformedT>(
    mut socket: impl Read,
    channel: SyncSender<TransformedT>,
    transform: impl Fn(MessageT) -> TransformedT,
) where
    MessageT: DeserializeOwned,
{
    while let Ok(msg) = read_message_from_socket(&mut socket) {
        if channel.send(transform(msg)).is_err() {
            break;
        }
    }
}
