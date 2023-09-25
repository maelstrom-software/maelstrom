//! Code for the broker binary.

pub mod cache;
mod connection;
mod http;
pub mod scheduler;

use meticulous_base::proto;
use meticulous_util::{
    error::{Error, Result},
    net,
};
use std::sync::Arc;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

pub struct PassThroughDeps;

/// The production implementation of [scheduler::SchedulerDeps]. This implementation just hands the
/// message to the provided sender.
impl scheduler::SchedulerDeps for PassThroughDeps {
    type ClientSender = UnboundedSender<proto::BrokerToClient>;
    type WorkerSender = UnboundedSender<proto::BrokerToWorker>;

    fn send_response_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        response: proto::BrokerToClient,
    ) {
        sender.send(response).ok();
    }

    fn send_request_to_worker(
        &mut self,
        sender: &mut Self::WorkerSender,
        request: proto::BrokerToWorker,
    ) {
        sender.send(request).ok();
    }
}

/// The production scheduler message type. Some [scheduler::Message] arms contain a
/// [scheduler::SchedulerDeps], so it's defined as a generic type. But in this module, we only use
/// one implementation of [scheduler::SchedulerDeps].
type SchedulerMessage = scheduler::Message<PassThroughDeps>;

/// Main loop for the scheduler. This should be run on a task of its own. There should be exactly
/// one of these in a broker process. It will return when all senders associated with the
/// receiver are closed, which will happen when the listener and all outstanding worker and client
/// socket tasks terminate.
///
/// This function ignores any errors it encounters sending a message to an [UnboundedSender]. The
/// rationale is that this indicates that the socket connection has closed, and there are no more
/// worker tasks to handle that connection. This means that a disconnected message is on its way to
/// notify the scheduler. It is best to just ignore the error in that case. Besides, the
/// [scheduler::SchedulerDeps] interface doesn't give us a way to return an error, for precisely
/// this reason.
async fn scheduler_main(receiver: UnboundedReceiver<SchedulerMessage>) {
    let mut scheduler = scheduler::Scheduler::default();
    net::channel_reader(receiver, |msg| {
        scheduler.receive_message(&mut PassThroughDeps, msg)
    })
    .await;
}

/// "Main loop" for a signal handler. This function will block until it receives the indicated
/// signal, then it will return an error.
async fn signal_handler(kind: tokio::signal::unix::SignalKind) -> Result<()> {
    tokio::signal::unix::signal(kind)?.recv().await;
    Err(Error::msg(format!("received signal {kind:?}")))
}

/// The main function for the broker. This should be called on a task of its own. It will return
/// if there is an error establishing the listener socket, when a signal is received, or when the
/// listener socket returns an error at accept time.
pub async fn main(
    listener: tokio::net::TcpListener,
    http_listener: tokio::net::TcpListener,
) -> Result<()> {
    let (scheduler_sender, scheduler_receiver) = mpsc::unbounded_channel();
    let id_vendor = Arc::new(connection::IdVendor::default());

    let mut join_set = tokio::task::JoinSet::new();
    join_set.spawn(http::listener_main(
        http_listener,
        scheduler_sender.clone(),
        id_vendor.clone(),
    ));
    join_set.spawn(connection::listener_main(
        listener,
        scheduler_sender,
        id_vendor,
    ));
    join_set.spawn(async move {
        scheduler_main(scheduler_receiver).await;
        Ok(())
    });
    join_set.spawn(signal_handler(tokio::signal::unix::SignalKind::interrupt()));
    join_set.spawn(signal_handler(tokio::signal::unix::SignalKind::terminate()));

    // Loop until we get the first error return from a task, then return that.
    loop {
        join_set
            .join_next()
            .await
            .expect("join_set should not be empty")
            .expect("no task should panic or be canceled")?;
    }
}
