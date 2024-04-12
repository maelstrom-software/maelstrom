//! Functions that are useful for communicating between tasks and threads within a program.
use pin_project::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};

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

// N.B. We'd use the ! type if it were stable.
enum Never {}

/// An `EventSender` wakes an [`EventReceiver`] when it is dropped. It is okay for the associated
/// [`EventReceiver`] to be dropped before the `EventSender`.
pub struct EventSender {
    _sender: oneshot::Sender<Never>,
}

/// An `EventReceiver` waits until its associated [`EventSender`] is dropped.
#[pin_project]
pub struct EventReceiver(#[pin] oneshot::Receiver<Never>);

impl Future for EventReceiver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().0.poll(cx).map(drop)
    }
}

/// Return a connected [`EventSender`] and [`EventReceiver`] pair.
pub fn event() -> (EventSender, EventReceiver) {
    let (_sender, receiver) = oneshot::channel();
    (EventSender { _sender }, EventReceiver(receiver))
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::time::Duration;
    use tokio::{sync::mpsc, task, time};

    #[tokio::test]
    async fn no_messages() {
        let (_, rx) = mpsc::unbounded_channel::<u8>();
        let mut vec = vec![];
        channel_reader(rx, |s| vec.push(s)).await;
        assert!(vec.is_empty(), "{vec:?}");
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

    #[tokio::test]
    async fn basic_event() {
        let (event_sender, event_receiver) = event();
        task::spawn(async move {
            drop(event_sender);
        });
        event_receiver.await;
    }

    #[tokio::test]
    async fn event_sent_before_received() {
        let (event_sender, event_receiver) = event();
        let (channel_sender, channel_receiver) = oneshot::channel();
        task::spawn(async move {
            drop(event_sender);
            channel_sender.send(()).unwrap();
        });
        channel_receiver.await.unwrap();
        event_receiver.await;
    }

    #[tokio::test]
    async fn event_sent_after_received() {
        let (event_sender, event_receiver) = event();
        let (channel_sender, channel_receiver) = oneshot::channel();
        task::spawn(async move {
            channel_receiver.await.unwrap();
            drop(event_sender);
        });
        channel_sender.send(()).unwrap();
        time::sleep(Duration::from_millis(10)).await;
        event_receiver.await;
    }

    #[tokio::test]
    async fn event_sent_after_receiver_dropped() {
        let (event_sender, event_receiver) = event();
        let (channel_sender, channel_receiver) = oneshot::channel();
        let handle = task::spawn(async move {
            channel_receiver.await.unwrap();
            drop(event_sender);
        });
        drop(event_receiver);
        channel_sender.send(()).unwrap();
        handle.await.unwrap();
    }
}
