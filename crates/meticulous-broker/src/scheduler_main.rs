use super::scheduler;
use meticulous_base::proto;
use meticulous_util::net;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

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
pub type SchedulerMessage = scheduler::Message<PassThroughDeps>;

/// This type is used often enough to warrant an alias.
pub type SchedulerSender = UnboundedSender<SchedulerMessage>;

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
pub async fn scheduler_main(receiver: UnboundedReceiver<SchedulerMessage>) {
    let mut scheduler = scheduler::Scheduler::default();
    net::channel_reader(receiver, |msg| {
        scheduler.receive_message(&mut PassThroughDeps, msg)
    })
    .await;
}
