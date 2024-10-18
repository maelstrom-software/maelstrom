//! Functions that are useful for reading/writing messages from/to sockets.

use anyhow::Result;
use maelstrom_base::proto;
use serde::{de::DeserializeOwned, Serialize};
use slog::{debug, Logger};
use std::{
    fmt::Debug,
    io::{Read, Write},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

fn write_message_to_vec(msg: impl Serialize) -> Result<Vec<u8>> {
    let msg_len = proto::serialized_size(&msg)? as u32;
    let mut buf = Vec::<u8>::with_capacity(msg_len as usize + 4);
    Write::write_all(&mut buf, &msg_len.to_be_bytes())?;
    proto::serialize_into(&mut buf, &msg)?;
    Ok(buf)
}

/// Write a message to a normal (threaded) writer. Each message is framed by sending a leading
/// 4-byte, little-endian message size. The message is logged at the debug log level, as well as
/// any error encountered sending it.
pub fn write_message_to_socket(
    stream: &mut impl Write,
    msg: impl Debug + Serialize,
    log: &Logger,
) -> Result<()> {
    debug!(log, "sending message"; "message" => #?msg);
    (|| Ok(stream.write_all(&write_message_to_vec(msg)?)?))()
        .inspect_err(|err| debug!(log, "error sending message"; "error" => %err))
}

/// Write a message to a Tokio output stream. Each message is framed by sending a leading 4-byte,
/// little-endian message size. The message is logged at the debug log level, as well as any error
/// encountered sending it.
pub async fn write_message_to_async_socket(
    stream: &mut (impl AsyncWrite + Unpin),
    msg: impl Debug + Serialize,
    log: &Logger,
) -> Result<()> {
    debug!(log, "sending message"; "message" => #?msg);
    async { Ok(stream.write_all(&write_message_to_vec(msg)?).await?) }
        .await
        .inspect_err(|err| debug!(log, "error sending message"; "error" => %err))
}

/// Read a message from a normal (threaded) reader. The framing must match that of
/// [`write_message_to_socket`] and [`write_message_to_async_socket`]. The received message will be
/// logged at the debug log level.
pub fn read_message_from_socket<MessageT>(stream: &mut impl Read, log: &Logger) -> Result<MessageT>
where
    MessageT: Debug + DeserializeOwned,
{
    (|| {
        let mut msg_len: [u8; 4] = [0; 4];
        stream.read_exact(&mut msg_len)?;
        let mut buf = vec![0; u32::from_be_bytes(msg_len) as usize];
        stream.read_exact(&mut buf)?;
        Result::Ok(proto::deserialize_from(&mut &buf[..])?)
    })()
    .inspect(|msg| debug!(log, "received message"; "message" => #?msg))
    .inspect_err(|err| debug!(log, "error receiving message"; "error" => %err))
}

/// Read a message from a Tokio input stream. The framing must match that of
/// [`write_message_to_socket`] and [`write_message_to_async_socket`].
pub async fn read_message_from_async_socket<MessageT>(
    stream: &mut (impl AsyncRead + Unpin),
    log: &Logger,
) -> Result<MessageT>
where
    MessageT: Debug + DeserializeOwned,
{
    async {
        let mut msg_len: [u8; 4] = [0; 4];
        stream.read_exact(&mut msg_len).await?;
        let mut buf = vec![0; u32::from_be_bytes(msg_len) as usize];
        stream.read_exact(&mut buf).await?;
        Result::Ok(proto::deserialize_from(&mut &buf[..])?)
    }
    .await
    .inspect(|msg| debug!(log, "received message"; "message" => #?msg))
    .inspect_err(|err| debug!(log, "error receiving message"; "error" => %err))
}

/// Loop, reading messages from a channel and writing them to a socket. The `log` parameter is used
/// to insert debug logging. Each message is logged. Also, if there is a failure reading a message,
/// that is logged as well.
pub async fn async_socket_writer<MessageT>(
    mut channel: UnboundedReceiver<MessageT>,
    mut socket: (impl AsyncWrite + Unpin),
    log: &Logger,
) -> Result<()>
where
    MessageT: Debug + Serialize,
{
    while let Some(msg) = channel.recv().await {
        write_message_to_async_socket(&mut socket, msg, log).await?;
    }
    Ok(())
}

/// Loop, reading messages from a socket and writing them to an mpsc channel. The `transform`
/// parameter is used to log the messages and wrap them in any necessary structure for internal use
/// by the program.
pub async fn async_socket_reader<MessageT, TransformedT>(
    mut socket: (impl AsyncRead + Unpin),
    channel: UnboundedSender<TransformedT>,
    transform: impl Fn(MessageT) -> TransformedT,
    log: &Logger,
) -> Result<()>
where
    MessageT: Debug + DeserializeOwned,
{
    loop {
        let msg = read_message_from_async_socket(&mut socket, log).await?;
        if channel.send(transform(msg)).is_err() {
            return Ok(());
        }
    }
}
