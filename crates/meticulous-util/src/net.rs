//! Functions that are useful for reading and writing messages, to and from sockets and channels.

use crate::error::Result;
use meticulous_base::Sha256Digest;
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

pub struct FixedSizeReader<DelegateT> {
    delegate: DelegateT,
    bytes_remaining: u64,
    delegate_eof: bool,
}

impl<DelegateT> FixedSizeReader<DelegateT> {
    pub fn new(delegate: DelegateT, bytes_remaining: u64) -> Self {
        FixedSizeReader {
            delegate,
            bytes_remaining,
            delegate_eof: false,
        }
    }

    pub fn into_inner(self) -> DelegateT {
        self.delegate
    }
}

impl<DelegateT: std::io::Read> std::io::Read for FixedSizeReader<DelegateT> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.bytes_remaining == 0 {
            Ok(0)
        } else if self.delegate_eof {
            let to_fill = buf.len().min(self.bytes_remaining as usize);
            buf[..to_fill].fill(0);
            Ok(to_fill)
        } else {
            let buf_limit = buf.len().min(self.bytes_remaining as usize);
            let count = self.delegate.read(&mut buf[..buf_limit])?;
            if count == 0 {
                self.delegate_eof = true;
                self.read(buf)
            } else {
                self.bytes_remaining -= count as u64;
                Ok(count)
            }
        }
    }
}

pub struct Sha256Verifier<'a, DelegateT> {
    hasher: Option<sha2::Sha256>,
    delegate: DelegateT,
    expected: &'a Sha256Digest,
}

impl<'a, DelegateT> Sha256Verifier<'a, DelegateT> {
    pub fn new(delegate: DelegateT, expected: &'a Sha256Digest) -> Self {
        use sha2::Digest;
        Sha256Verifier {
            hasher: Some(sha2::Sha256::new()),
            delegate,
            expected,
        }
    }
}

impl<'a, DelegateT> std::ops::Drop for Sha256Verifier<'a, DelegateT> {
    fn drop(&mut self) {
        assert!(self.hasher.is_none(), "digest never verified");
    }
}

impl<'a, DelegateT: std::io::Read> std::io::Read for Sha256Verifier<'a, DelegateT> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use sha2::Digest;

        // Take the hasher before we read. If there is an error reading, then we'll leave the
        // struct without a hasher, indicating that it's safe to drop.
        let hasher = self.hasher.take();
        let size = self.delegate.read(buf)?;
        if size > 0 {
            self.hasher = hasher;
            match &mut self.hasher {
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
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
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
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
