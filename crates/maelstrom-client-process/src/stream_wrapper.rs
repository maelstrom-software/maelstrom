use core::{
    pin::Pin,
    task::{Context, Poll},
};
use pin_project::pin_project;
use std::io::{self, IoSlice};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::oneshot,
};
use tonic::transport::server::Connected;

pub enum Never {}

#[pin_project]
pub struct StreamWrapper<T> {
    #[pin]
    inner: T,
    sender: Option<oneshot::Sender<Never>>,
}

impl<T> StreamWrapper<T> {
    pub fn new(inner: T) -> (Self, oneshot::Receiver<Never>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                inner,
                sender: Some(sender),
            },
            receiver,
        )
    }
}

impl<T: AsyncRead> AsyncRead for StreamWrapper<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let projection = self.project();
        let filled = buf.filled().len();
        let poll = projection.inner.poll_read(cx, buf);
        let send = match &poll {
            Poll::Pending => false,
            Poll::Ready(Err(_)) => true,
            Poll::Ready(Ok(())) => filled == buf.filled().len(),
        };
        if send {
            *projection.sender = None;
        }
        poll
    }
}

impl<T: AsyncWrite> AsyncWrite for StreamWrapper<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        self.project().inner.poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

impl<T: Connected> Connected for StreamWrapper<T> {
    type ConnectInfo = T::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}
