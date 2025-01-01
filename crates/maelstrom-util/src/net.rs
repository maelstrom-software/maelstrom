//! Functions that are useful for reading/writing messages from/to sockets.

use anyhow::Result;
use maelstrom_base::proto;
use maelstrom_github::{GitHubReadQueue, GitHubWriteQueue};
use maelstrom_linux::{self as linux, Fd};
use serde::{de::DeserializeOwned, Serialize};
use slog::{debug, Logger};
use std::{
    fmt::Debug,
    io::{Read, Write},
    os::fd::AsRawFd,
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

pub trait AsRawFdExt: Sized {
    fn set_socket_options(self) -> Result<Self>;
}

impl<T: AsRawFd> AsRawFdExt for T {
    fn set_socket_options(self) -> Result<Self> {
        let fd = Fd::from_raw(self.as_raw_fd());
        linux::setsockopt_tcp_nodelay(&fd, true)?;
        linux::setsockopt_so_keepalive(&fd, true)?;
        linux::setsockopt_tcp_keepcnt(&fd, 3)?;
        linux::setsockopt_tcp_keepidle(&fd, 300)?;
        linux::setsockopt_tcp_keepintvl(&fd, 300)?;
        Ok(self)
    }
}

pub async fn read_message_from_github_queue<MessageT>(
    queue: &mut GitHubReadQueue,
    log: &Logger,
) -> Result<Option<MessageT>>
where
    MessageT: Debug + DeserializeOwned,
{
    async {
        queue
            .read_msg()
            .await?
            .as_ref()
            .map(|m| proto::deserialize(m))
            .transpose()
            .map_err(anyhow::Error::from)
    }
    .await
    .inspect(|msg| {
        if let Some(msg) = msg {
            debug!(log, "received message"; "message" => #?msg)
        }
    })
    .inspect_err(|err| debug!(log, "error receiving message"; "error" => %err))
}

pub async fn github_queue_reader<MessageT, TransformedT>(
    mut queue: GitHubReadQueue,
    channel: UnboundedSender<TransformedT>,
    transform: impl Fn(MessageT) -> TransformedT,
    log: &Logger,
) -> Result<()>
where
    MessageT: Debug + DeserializeOwned,
{
    while let Some(msg) = read_message_from_github_queue(&mut queue, log).await? {
        if channel.send(transform(msg)).is_err() {
            break;
        }
    }
    Ok(())
}

pub async fn write_message_to_github_queue<MessageT>(
    queue: &mut GitHubWriteQueue,
    msg: &MessageT,
    log: &Logger,
) -> Result<()>
where
    MessageT: Debug + Serialize,
{
    queue
        .write_msg(&proto::serialize(msg).unwrap())
        .await
        .inspect_err(|err| debug!(log, "error sending message"; "error" => %err))?;
    Ok(())
}

pub async fn write_many_messages_to_github_queue<MessageT>(
    queue: &mut GitHubWriteQueue,
    msgs: &[MessageT],
    log: &Logger,
) -> Result<()>
where
    MessageT: Debug + Serialize,
{
    queue
        .write_many_msgs(&Vec::from_iter(
            msgs.iter().map(|msg| proto::serialize(msg).unwrap()),
        ))
        .await
        .inspect_err(|err| debug!(log, "error sending message"; "error" => %err))?;
    Ok(())
}

pub async fn github_queue_writer<MessageT>(
    mut channel: UnboundedReceiver<MessageT>,
    mut queue: GitHubWriteQueue,
    log: &Logger,
) -> Result<()>
where
    MessageT: Debug + Serialize,
{
    let mut to_send = vec![];
    loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                if !to_send.is_empty() {
                    write_many_messages_to_github_queue(&mut queue, &to_send, log).await?;
                    to_send.clear();
                }
            },
            msg = channel.recv() => {
                if let Some(msg) = msg {
                    to_send.push(msg);
                } else {
                    break;
                }
            }
        }
    }
    queue.shut_down().await?;

    Ok(())
}
