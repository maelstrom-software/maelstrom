//! Functions that are useful for communicating between tasks and threads within a program.
use anyhow::Result;
use pin_project::pin_project;
use std::{
    future::Future,
    num::NonZeroU32,
    ops::ControlFlow,
    pin::Pin,
    sync::{Condvar, Mutex},
    task::{Context, Poll},
};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};

/// Read messages from a channel, calling an individual function on each one. Return when there are
/// no more channel senders.
pub async fn channel_reader<MessageT>(
    mut channel: UnboundedReceiver<MessageT>,
    mut processor: impl FnMut(MessageT) -> ControlFlow<Result<()>>,
) -> Result<()> {
    while let Some(x) = channel.recv().await {
        if let ControlFlow::Break(result) = processor(x) {
            return result;
        }
    }
    Ok(())
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

#[derive(Default)]
pub struct Event {
    completed: std::sync::Mutex<bool>,
    condvar: std::sync::Condvar,
}

impl Event {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self) {
        *self.completed.lock().unwrap() = true;
        self.condvar.notify_all();
    }

    pub fn wait(&self) {
        let _guard = self
            .condvar
            .wait_while(self.completed.lock().unwrap(), |completed| !*completed)
            .unwrap();
    }

    pub fn wait_and_unset(&self) {
        let _guard = self
            .condvar
            .wait_while(self.completed.lock().unwrap(), |completed| {
                if *completed {
                    *completed = false;
                    false
                } else {
                    true
                }
            })
            .unwrap();
    }

    pub fn wait_timeout(&self, dur: std::time::Duration) -> std::sync::WaitTimeoutResult {
        let (_guard, result) = self
            .condvar
            .wait_timeout_while(self.completed.lock().unwrap(), dur, |completed| !*completed)
            .unwrap();
        result
    }

    pub fn is_set(&self) -> bool {
        *self.completed.lock().unwrap()
    }
}

pub struct Pool<T> {
    mutex: Mutex<PoolInner<T>>,
    condvar: Condvar,
}

struct PoolInner<T> {
    available_slots: u32,
    available_items: Vec<T>,
}

impl<T> Pool<T> {
    pub fn new(slots: NonZeroU32) -> Self {
        Self {
            mutex: Mutex::new(PoolInner {
                available_slots: slots.into(),
                available_items: Default::default(),
            }),
            condvar: Default::default(),
        }
    }

    /// Run call a closure after obtaining a slot, potentially with a pre-built item.
    ///
    /// The calling thread will block until there is an available slot. Once there is, the provided
    /// closure will be called, potentially with an item returned from a previously called closure.
    /// Upon completion, if the closure returns a new item, the item will be provided to a
    /// subsequent closure.
    pub fn call_with_item<U, E>(
        &self,
        f: impl FnOnce(Option<T>) -> Result<(T, U), E>,
    ) -> Result<U, E> {
        let guard = self.mutex.lock().unwrap();
        let mut guard = self
            .condvar
            .wait_while(guard, |inner| inner.available_slots == 0)
            .unwrap();
        guard.available_slots -= 1;
        let item = guard.available_items.pop();
        drop(guard);

        let result = f(item);

        let mut guard = self.mutex.lock().unwrap();
        guard.available_slots += 1;
        let result = result.map(|(item, ret)| {
            guard.available_items.push(item);
            ret
        });
        drop(guard);

        self.condvar.notify_one();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use core::time::Duration;
    use tokio::{sync::mpsc, task, time};

    #[tokio::test]
    async fn no_messages() {
        let (_, rx) = mpsc::unbounded_channel::<u8>();
        let mut vec = vec![];
        channel_reader(rx, |s| {
            vec.push(s);
            ControlFlow::Continue(())
        })
        .await
        .unwrap();
        assert!(vec.is_empty(), "{vec:?}");
    }

    #[tokio::test]
    async fn one_messages() {
        let (tx, rx) = mpsc::unbounded_channel();
        task::spawn(async move { tx.send(1).unwrap() });
        let mut vec = vec![];
        channel_reader(rx, |s| {
            vec.push(s);
            ControlFlow::Continue(())
        })
        .await
        .unwrap();

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
        channel_reader(rx, |s| {
            vec.push(s);
            ControlFlow::Continue(())
        })
        .await
        .unwrap();

        assert_eq!(vec, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn three_messages_with_break_after_two() {
        let (tx, rx) = mpsc::unbounded_channel();
        task::spawn(async move {
            tx.send(1).unwrap();
            tx.send(2).unwrap();
            tx.send(3).unwrap();
        });
        let mut vec = vec![];
        channel_reader(rx, |s| {
            vec.push(s);
            if s == 2 {
                ControlFlow::Break(Ok(()))
            } else {
                ControlFlow::Continue(())
            }
        })
        .await
        .unwrap();

        assert_eq!(vec, vec![1, 2]);
    }

    #[tokio::test]
    async fn three_messages_with_break_of_error_after_two() {
        let (tx, rx) = mpsc::unbounded_channel();
        task::spawn(async move {
            tx.send(1).unwrap();
            tx.send(2).unwrap();
            tx.send(3).unwrap();
        });
        let mut vec = vec![];
        let err = channel_reader(rx, |s| {
            vec.push(s);
            if s == 2 {
                ControlFlow::Break(Err(anyhow!("test error")))
            } else {
                ControlFlow::Continue(())
            }
        })
        .await
        .unwrap_err();
        assert_eq!(err.to_string(), "test error");

        assert_eq!(vec, vec![1, 2]);
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

    #[test]
    fn sync_event_wait() {
        let event = Event::new();

        std::thread::scope(|scope| {
            let t1 = scope.spawn(|| event.wait());
            let t2 = scope.spawn(|| event.wait());

            std::thread::sleep(Duration::from_millis(10));
            assert!(!t1.is_finished());
            assert!(!t2.is_finished());

            assert!(!event.is_set());
            event.set();

            t1.join().unwrap();
            t2.join().unwrap();
            assert!(event.is_set());
        });
    }

    #[test]
    fn sync_event_wait_timeout() {
        let event = Event::new();

        assert!(event.wait_timeout(Duration::from_millis(5)).timed_out());
        event.set();
        assert!(!event.wait_timeout(Duration::from_millis(5)).timed_out());
    }

    #[test]
    fn sync_event_wait_and_unset() {
        let event = Event::new();

        std::thread::scope(|scope| {
            let t1 = scope.spawn(|| event.wait_and_unset());
            let t2 = scope.spawn(|| event.wait_and_unset());

            event.set();
            while !t1.is_finished() && !t2.is_finished() {
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(!event.is_set());

            event.set();
            t1.join().unwrap();
            t2.join().unwrap();

            assert!(!event.is_set());
        });
    }
}
