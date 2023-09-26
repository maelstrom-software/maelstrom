//! Functions that are useful for reading and writing messages, to and from sockets and channels.

use crate::error::Result;
use serde::{de::DeserializeOwned, Serialize};

/// Read messages from a channel, calling an individual function on each one. Return when there are
/// no more channel senders.
pub async fn channel_reader<MessageT>(
    mut channel: tokio::sync::mpsc::UnboundedReceiver<MessageT>,
    mut processor: impl FnMut(MessageT),
) {
    while let Some(x) = channel.recv().await {
        processor(x);
    }
}

/// Write a message to a Tokio output stream. Each message is framed by sending a leading 4-byte,
/// little-endian message size.
pub async fn write_message_to_async_socket(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    msg: impl Serialize,
) -> Result<()> {
    let msg_len = bincode::serialized_size(&msg)? as u32;

    let mut buf = Vec::<u8>::with_capacity(msg_len as usize + 4);
    std::io::Write::write(&mut buf, &msg_len.to_le_bytes())?;
    bincode::serialize_into(&mut buf, &msg)?;

    Ok(tokio::io::AsyncWriteExt::write_all(stream, &buf).await?)
}

/// Read a message from a Tokio input stream. The framing must match that of
/// [write_message_to_async_socket].
pub async fn read_message_from_async_socket<MessageT>(
    stream: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<MessageT>
where
    MessageT: DeserializeOwned,
{
    let mut msg_len: [u8; 4] = [0; 4];
    tokio::io::AsyncReadExt::read_exact(stream, &mut msg_len).await?;
    let msg_len = u32::from_le_bytes(msg_len) as usize;

    let mut buf = vec![0; msg_len];
    tokio::io::AsyncReadExt::read_exact(stream, &mut buf).await?;
    Ok(bincode::deserialize_from(&mut &buf[..])?)
}

/// Loop reading messages from a socket and writing them to an mpsc channel.
pub async fn async_socket_reader<MessageT, TransformedT>(
    mut socket: (impl tokio::io::AsyncRead + Unpin),
    channel: tokio::sync::mpsc::UnboundedSender<TransformedT>,
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

/// Loop reading messages from an mpsc channel and writing them to a socket.
pub async fn async_socket_writer(
    mut channel: tokio::sync::mpsc::UnboundedReceiver<impl Serialize>,
    mut socket: (impl tokio::io::AsyncWrite + Unpin),
) {
    while let Some(msg) = channel.recv().await {
        if write_message_to_async_socket(&mut socket, msg)
            .await
            .is_err()
        {
            break;
        }
    }
}

/// Write a message to a normal (threaded) writer. Each message is framed by sending a leading
/// 4-byte, little-endian message size.
pub fn write_message_to_socket(
    stream: &mut impl std::io::Write,
    msg: impl Serialize,
) -> Result<()> {
    let msg_len = bincode::serialized_size(&msg)? as u32;

    let mut buf = Vec::<u8>::with_capacity(msg_len as usize + 4);
    std::io::Write::write_all(&mut buf, &msg_len.to_le_bytes())?;
    bincode::serialize_into(&mut buf, &msg)?;

    Ok(stream.write_all(&buf)?)
}

/// Read a message from a normal (threaded) reader. The framing must match that of
/// [write_message_to_socket].
pub fn read_message_from_socket<MessageT>(stream: &mut impl std::io::Read) -> Result<MessageT>
where
    MessageT: DeserializeOwned,
{
    let mut msg_len: [u8; 4] = [0; 4];
    stream.read_exact(&mut msg_len)?;
    let msg_len = u32::from_le_bytes(msg_len) as usize;

    let mut buf = vec![0; msg_len];
    stream.read_exact(&mut buf)?;
    Ok(bincode::deserialize_from(&mut &buf[..])?)
}

/// Loop reading messages from a socket and writing them to an mpsc channel.
pub fn socket_reader<MessageT, TransformedT>(
    mut socket: impl std::io::Read,
    channel: std::sync::mpsc::Sender<TransformedT>,
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

/// Loop reading messages from an mpsc channel and writing them to a socket.
pub fn socket_writer(
    channel: std::sync::mpsc::Receiver<impl Serialize>,
    mut socket: impl std::io::Write,
) {
    while let Ok(msg) = channel.recv() {
        if write_message_to_socket(&mut socket, msg).is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_messages() {
        let (_, rx) = tokio::sync::mpsc::unbounded_channel::<u8>();
        let mut vec = vec![];
        channel_reader(rx, |s| vec.push(s)).await;
        assert_eq!(vec, vec![]);
    }

    #[tokio::test]
    async fn one_messages() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::task::spawn(async move { tx.send(1).unwrap() });
        let mut vec = vec![];
        channel_reader(rx, |s| vec.push(s)).await;

        assert_eq!(vec, vec![1]);
    }

    #[tokio::test]
    async fn three_messages() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::task::spawn(async move {
            tx.send(1).unwrap();
            tx.send(2).unwrap();
            tx.send(3).unwrap();
        });
        let mut vec = vec![];
        channel_reader(rx, |s| vec.push(s)).await;

        assert_eq!(vec, vec![1, 2, 3]);
    }
}
