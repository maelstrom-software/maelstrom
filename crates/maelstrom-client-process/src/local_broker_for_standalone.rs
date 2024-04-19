use anyhow::Result;
use crate::{
    dispatcher,
    local_broker::{Adapter, LocalBroker, Receiver},
};
use maelstrom_util::{config::common::Slots, sync};
use tokio::{sync::mpsc, task::JoinSet};

pub fn start_task(
    join_set: &mut JoinSet<Result<()>>,
    slots: Slots,
    receiver: Receiver,
    dispatcher_sender: dispatcher::Sender,
    local_worker_sender: maelstrom_worker::DispatcherSender,
) {
    // Just create black-hole broker and artifact_pusher senders. We shouldn't be sending any
    // messages to them in standalone mode.
    let (broker_sender, broker_receiver) = mpsc::unbounded_channel();
    drop(broker_receiver);
    let (artifact_pusher_sender, artifact_pusher_receiver) = mpsc::unbounded_channel();
    drop(artifact_pusher_receiver);

    let adapter = Adapter::new(
        dispatcher_sender,
        broker_sender,
        artifact_pusher_sender,
        local_worker_sender,
    );
    let mut local_broker = LocalBroker::new(true, adapter, slots);
    join_set.spawn(sync::channel_reader(receiver, move |msg| {
        local_broker.receive_message(msg)
    }));
}
