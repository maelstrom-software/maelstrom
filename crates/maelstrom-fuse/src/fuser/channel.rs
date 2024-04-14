use crate::fuser::ll::fuse_abi as abi;
use crate::fuser::reply::ReplySender;
use async_trait::async_trait;
use maelstrom_linux::{self as linux, Fd};
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
    _log: slog::Logger,
) {
    let fuse_fd = Fd::from_raw(fuse.get_ref().as_raw_fd());

    let (mut pipe_out, mut pipe_in) = linux::pipe().unwrap();
    let pipe_max_s = std::fs::read_to_string("/proc/sys/fs/pipe-max-size").unwrap();
    let pipe_max = pipe_max_s.trim().parse().unwrap();
    linux::set_pipe_size(pipe_in.as_fd(), pipe_max).unwrap();

    while let Some(req) = recv.blocking_recv() {
        let res = (|| {
            let header_bytes = req.header.as_bytes();
            assert!(req.length + header_bytes.len() <= pipe_max);
            linux::write(pipe_in.as_fd(), header_bytes)?;
            linux::splice(req.fd, Some(req.offset), pipe_in.as_fd(), None, req.length)?;
            linux::splice(
                pipe_out.as_fd(),
                None,
                fuse_fd,
                None,
                header_bytes.len() + req.length,
            )?;

            Ok(())
        })();
        if res.is_err() {
            (pipe_out, pipe_in) = linux::pipe().unwrap();
            linux::set_pipe_size(pipe_in.as_fd(), pipe_max).unwrap();
        }
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
