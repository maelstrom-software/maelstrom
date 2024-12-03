use core::{
    pin::Pin,
    task::{Context, Poll},
};
use maelstrom_util::sync::{self, EventReceiver, EventSender};
use pin_project::pin_project;
use std::io::{self, IoSlice};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::server::Connected;

#[pin_project]
pub struct StreamWrapper<T> {
    #[pin]
    inner: T,
    sender: Option<EventSender>,
}

impl<T> StreamWrapper<T> {
    pub fn new(inner: T) -> (Self, EventReceiver) {
        let (sender, receiver) = sync::event();
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

#[cfg(test)]
mod tests {
    use super::*;
    use maelstrom_util::io::ErrorReader;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::UnixStream,
        task,
    };

    #[tokio::test]
    async fn eof_triggers_event() {
        let (s1, mut s2) = UnixStream::pair().unwrap();
        let (mut s1, receiver) = StreamWrapper::new(s1);
        let fired = Arc::new(AtomicBool::new(false));

        let fired_clone = fired.clone();
        let receiver_task = task::spawn(async move {
            receiver.await;
            fired_clone.store(true, Ordering::SeqCst);
        });

        // We haven't read yet, so the event shouldn't have fired.
        assert!(!fired.load(Ordering::SeqCst));

        // Reading non-EOF shouldn't have triggered it either.
        s2.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        s1.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
        assert!(!fired.load(Ordering::SeqCst));

        // Dropping the writer shouldn't trigger it either, since we haven't read yet.
        drop(s2);
        assert!(!fired.load(Ordering::SeqCst));

        // Now, reading and seeing the EOF should trigger it.
        let mut vec = vec![];
        s1.read_to_end(&mut vec).await.unwrap();
        assert!(vec.is_empty());
        receiver_task.await.unwrap();
        assert!(fired.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn error_triggers_event() {
        let (mut s, receiver) = StreamWrapper::new(ErrorReader);
        let fired = Arc::new(AtomicBool::new(false));

        let fired_clone = fired.clone();
        let receiver_task = task::spawn(async move {
            receiver.await;
            fired_clone.store(true, Ordering::SeqCst);
        });

        // We haven't read yet, so the event shouldn't have fired.
        assert!(!fired.load(Ordering::SeqCst));

        // Now reading should give us an error and fire the event.
        let mut vec = vec![];
        s.read_to_end(&mut vec).await.unwrap_err();
        assert!(vec.is_empty());
        receiver_task.await.unwrap();
        assert!(fired.load(Ordering::SeqCst));
    }
}
