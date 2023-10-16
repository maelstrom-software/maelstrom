//! Functions that are useful for communicating between tasks and threads within a program.
use tokio::sync::mpsc::UnboundedReceiver;

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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{sync::mpsc, task};

    #[tokio::test]
    async fn no_messages() {
        let (_, rx) = mpsc::unbounded_channel::<u8>();
        let mut vec = vec![];
        channel_reader(rx, |s| vec.push(s)).await;
        assert_eq!(vec, vec![]);
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
}
