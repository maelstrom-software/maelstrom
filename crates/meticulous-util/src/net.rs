//! Functions that are useful for reading and writing messages, to and from sockets and channels.

use anyhow::Result;
use meticulous_base::Sha256Digest;
use serde::{de::DeserializeOwned, Serialize};
use sha2::Digest;
use std::io::{self, Chain, Read, Repeat, Take, Write};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

/// Read messages from a channel, calling an individual function on each one. Return when there are
/// no more channel senders.
pub async fn channel_reader<MessageT>(
    mut channel: UnboundedReceiver<MessageT>,
    mut processor: impl FnMut(MessageT),
) {
    while let Some(x) = channel.recv().await {
        processor(x);
    }
}

fn write_message_to_vec(msg: impl Serialize) -> Result<Vec<u8>> {
    let msg_len = bincode::serialized_size(&msg)? as u32;
    let mut buf = Vec::<u8>::with_capacity(msg_len as usize + 4);
    Write::write_all(&mut buf, &msg_len.to_le_bytes())?;
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
    let mut buf = vec![0; u32::from_le_bytes(msg_len) as usize];
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
    let mut buf = vec![0; u32::from_le_bytes(msg_len) as usize];
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

pub struct Sha256Reader<InnerT> {
    inner: InnerT,
    hasher: sha2::Sha256,
}

impl<InnerT> Sha256Reader<InnerT> {
    pub fn new(inner: InnerT) -> Self {
        Sha256Reader {
            inner,
            hasher: sha2::Sha256::new(),
        }
    }

    pub fn finalize(self) -> (InnerT, Sha256Digest) {
        (self.inner, Sha256Digest::new(self.hasher.finalize().into()))
    }
}

impl<InnerT: Read> Read for Sha256Reader<InnerT> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let size = self.inner.read(buf)?;
        self.hasher.update(&buf[..size]);
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{sync::mpsc, task};

    #[tokio::test]
    async fn no_messages() {
        let (_, rx) = mpsc::unbounded_channel::<u8>();
        let mut vec = vec![];
        channel_reader(rx, |s| vec.push(s)).await;
        assert_eq!(vec, vec![]);
    }

    #[tokio::test]
    async fn one_messages() {
        let (tx, rx) = mpsc::unbounded_channel();
        task::spawn(async move { tx.send(1).unwrap() });
        let mut vec = vec![];
        channel_reader(rx, |s| vec.push(s)).await;

        assert_eq!(vec, vec![1]);
    }

    #[tokio::test]
    async fn three_messages() {
        let (tx, rx) = mpsc::unbounded_channel();
        task::spawn(async move {
            tx.send(1).unwrap();
            tx.send(2).unwrap();
            tx.send(3).unwrap();
        });
        let mut vec = vec![];
        channel_reader(rx, |s| vec.push(s)).await;

        assert_eq!(vec, vec![1, 2, 3]);
    }
}
