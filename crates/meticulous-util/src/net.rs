//! Functions that are useful for reading and writing messages, to and from sockets and channels.

use crate::error::Result;
use meticulous_base::Sha256Digest;
use serde::{de::DeserializeOwned, Serialize};
use std::io::{self, Chain, Read, Repeat, Take, Write};

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
    Write::write(&mut buf, &msg_len.to_le_bytes())?;
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
pub async fn async_socket_writer<MessageT>(
    mut channel: tokio::sync::mpsc::UnboundedReceiver<MessageT>,
    mut socket: (impl tokio::io::AsyncWrite + Unpin),
    mut inspect: impl FnMut(&MessageT),
) where
    MessageT: Serialize,
{
    while let Some(msg) = channel.recv().await {
        inspect(&msg);
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
pub fn write_message_to_socket(stream: &mut impl Write, msg: impl Serialize) -> Result<()> {
    let msg_len = bincode::serialized_size(&msg)? as u32;

    let mut buf = Vec::<u8>::with_capacity(msg_len as usize + 4);
    Write::write_all(&mut buf, &msg_len.to_le_bytes())?;
    bincode::serialize_into(&mut buf, &msg)?;

    Ok(stream.write_all(&buf)?)
}

/// Read a message from a normal (threaded) reader. The framing must match that of
/// [write_message_to_socket].
pub fn read_message_from_socket<MessageT>(stream: &mut impl Read) -> Result<MessageT>
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
    mut socket: impl Read,
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
pub fn socket_writer(channel: std::sync::mpsc::Receiver<impl Serialize>, mut socket: impl Write) {
    while let Ok(msg) = channel.recv() {
        if write_message_to_socket(&mut socket, msg).is_err() {
            break;
        }
    }
}

/// A reader wrapper that will always return a specific number of bytes, except on error. If the
/// inner, wrapped, reader returns EOF before the specified number of bytes have been returned,
/// this reader will pad the remaining bytes with zeros. If the inner reader returns more bytes
/// than the specified number, this reader will return EOF early, like [Read::take].
pub struct FixedSizeReader<InnerT>(Take<Chain<InnerT, Repeat>>);

impl<InnerT: Read> FixedSizeReader<InnerT> {
    pub fn new(inner: InnerT, limit: u64) -> Self {
        FixedSizeReader(inner.chain(io::repeat(0)).take(limit))
    }

    pub fn into_inner(self) -> InnerT {
        self.0.into_inner().into_inner().0
    }
}

impl<InnerT: Read> Read for FixedSizeReader<InnerT> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

pub struct Sha256Verifier<'a, InnerT> {
    hasher: Option<sha2::Sha256>,
    inner: InnerT,
    expected: &'a Sha256Digest,
}

impl<'a, InnerT> Sha256Verifier<'a, InnerT> {
    pub fn new(inner: InnerT, expected: &'a Sha256Digest) -> Self {
        use sha2::Digest;
        Sha256Verifier {
            hasher: Some(sha2::Sha256::new()),
            inner,
            expected,
        }
    }
}

impl<'a, InnerT> std::ops::Drop for Sha256Verifier<'a, InnerT> {
    fn drop(&mut self) {
        assert!(self.hasher.is_none(), "digest never verified");
    }
}

impl<'a, InnerT: Read> Read for Sha256Verifier<'a, InnerT> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        use sha2::Digest;

        // Take the hasher before we read. If there is an error reading, then we'll leave the
        // struct without a hasher, indicating that it's safe to drop.
        let hasher = self.hasher.take();
        let size = self.inner.read(buf)?;
        if size > 0 {
            self.hasher = hasher;
            match &mut self.hasher {
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Unexepcted read of non-zero bytes after read of zero bytes or error",
                    ));
                }
                Some(hasher) => {
                    hasher.update(&buf[..size]);
                }
            }
        } else {
            match hasher {
                None => {
                    // We already validated the digest.
                }
                Some(hasher) => {
                    if Sha256Digest(hasher.finalize().into()) != *self.expected {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "SHA-256 digest didn't match",
                        ));
                    }
                }
            }
        }
        Ok(size)
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
