use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::task::{spawn as tokio_spawn, JoinHandle};

pub struct JoinHandleDropWrapper<T> {
    inner: JoinHandle<T>,
}

impl<T> Drop for JoinHandleDropWrapper<T> {
    fn drop(&mut self) {
        self.inner.abort();
    }
}

impl<T> Future for JoinHandleDropWrapper<T> {
    type Output = <JoinHandle<T> as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

pub fn spawn<T>(future: T) -> JoinHandleDropWrapper<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    JoinHandleDropWrapper {
        inner: tokio_spawn(future),
    }
}
