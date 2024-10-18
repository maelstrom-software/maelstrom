use crate::fuser::ll::fuse_abi as abi;
use crate::fuser::reply::ReplySender;
use async_trait::async_trait;
use maelstrom_linux::Fd;
use maelstrom_util::io::MaybeFastWriter;
use std::os::fd::AsRawFd as _;
use std::{
    fs::File,
    io::{self, Read as _, Write as _},
    sync::Arc,
};
use tokio::io::unix::AsyncFd;
use tokio::io::Interest;
use tokio::sync::{mpsc, oneshot};
use zerocopy::AsBytes as _;

/// A raw communication channel to the FUSE kernel driver
#[derive(Debug)]
pub struct Channel {
    device: Arc<AsyncFd<File>>,
    sender: ChannelSender,
}

impl Channel {
    /// Create a new communication channel to the kernel driver by mounting the
    /// given path. The kernel driver will delegate filesystem operations of
    /// the given path to the channel.
    pub(crate) fn new(device: Arc<AsyncFd<File>>, log: slog::Logger) -> Self {
        Self {
            sender: ChannelSender::new(device.clone(), log),
            device,
        }
    }

    /// Receives data up to the capacity of the given buffer
    pub async fn receive(&self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self
                .device
                .ready(Interest::READABLE | Interest::ERROR)
                .await?;

            match guard.try_io(|inner| inner.get_ref().read(buffer)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Returns a sender object for this channel. The sender object can be
    /// used to send to the channel. Multiple sender objects can be used
    /// and they can safely be sent to other threads.
    pub fn sender(&self) -> ChannelSender {
        // Since write/writev syscalls are threadsafe, we can simply create
        // a sender by using the same file and use it in other threads.
        self.sender.clone()
    }
}

struct SpliceRequest {
    fd: Fd,
    header: abi::fuse_out_header,
    offset: u64,
    length: usize,
    reply: oneshot::Sender<io::Result<()>>,
}

fn run_splice_sender(
    fuse: Arc<AsyncFd<File>>,
    mut recv: mpsc::Receiver<SpliceRequest>,
    log: slog::Logger,
) {
    let fuse_fd = Fd::from_raw(fuse.get_ref().as_raw_fd());

    let mut writer = MaybeFastWriter::new(&log);

    while let Some(req) = recv.blocking_recv() {
        let res = (|| {
            let header_bytes = req.header.as_bytes();
            writer.write(header_bytes)?;
            writer.write_fd(req.fd, Some(req.offset), req.length)?;
            writer.copy_to_fd(fuse_fd, None)?;
            Ok(())
        })();
        req.reply.send(res).ok();
    }
}

#[derive(Clone, Debug)]
pub struct ChannelSender {
    device: Arc<AsyncFd<File>>,
    send: mpsc::Sender<SpliceRequest>,
}

impl ChannelSender {
    fn new(device: Arc<AsyncFd<File>>, log: slog::Logger) -> Self {
        let (send, recv) = mpsc::channel(1000);
        let other_file = device.clone();
        tokio::task::spawn_blocking(move || run_splice_sender(other_file, recv, log));
        Self { device, send }
    }
}

#[async_trait]
impl ReplySender for ChannelSender {
    async fn send(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<()> {
        let written = loop {
            let mut guard = self
                .device
                .ready(Interest::WRITABLE | Interest::ERROR)
                .await?;

            match guard.try_io(|inner| inner.get_ref().write_vectored(bufs)) {
                Ok(result) => break result?,
                Err(_would_block) => continue,
            }
        };

        debug_assert_eq!(bufs.iter().map(|b| b.len()).sum::<usize>(), written);
        Ok(())
    }

    async fn send_splice(
        &self,
        fd: Fd,
        header: abi::fuse_out_header,
        offset: u64,
        length: usize,
    ) -> io::Result<()> {
        let (send, recv) = oneshot::channel();
        self.send
            .send(SpliceRequest {
                fd,
                header,
                offset,
                length,
                reply: send,
            })
            .await
            .unwrap();
        recv.await.unwrap()
    }
}
