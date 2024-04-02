use crate::fuser::reply::ReplySender;
use async_trait::async_trait;
use std::{
    fs::File,
    io::{self, Read as _, Write as _},
    sync::Arc,
};
use tokio::io::unix::AsyncFd;
use tokio::io::Interest;

/// A raw communication channel to the FUSE kernel driver
#[derive(Debug)]
pub struct Channel(Arc<AsyncFd<File>>);

impl Channel {
    /// Create a new communication channel to the kernel driver by mounting the
    /// given path. The kernel driver will delegate filesystem operations of
    /// the given path to the channel.
    pub(crate) fn new(device: Arc<AsyncFd<File>>) -> Self {
        Self(device)
    }

    /// Receives data up to the capacity of the given buffer
    pub async fn receive(&self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.0.ready(Interest::READABLE | Interest::ERROR).await?;

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
        ChannelSender(self.0.clone())
    }
}

#[derive(Clone, Debug)]
pub struct ChannelSender(Arc<AsyncFd<File>>);

#[async_trait]
impl ReplySender for ChannelSender {
    async fn send(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<()> {
        let written = loop {
            let mut guard = self.0.ready(Interest::WRITABLE | Interest::ERROR).await?;

            match guard.try_io(|inner| inner.get_ref().write_vectored(bufs)) {
                Ok(result) => break result?,
                Err(_would_block) => continue,
            }
        };

        debug_assert_eq!(bufs.iter().map(|b| b.len()).sum::<usize>(), written);
        Ok(())
    }
}
