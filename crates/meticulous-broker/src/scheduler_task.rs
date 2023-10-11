use crate::config;
use cache::{Cache, StdCacheFs};
use meticulous_base::proto;
use meticulous_util::net;
use scheduler::{Message, Scheduler, SchedulerDeps};
use std::path::PathBuf;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

mod cache;
pub mod scheduler;

#[derive(Debug)]
pub struct PassThroughDeps;

/// The production implementation of [SchedulerDeps]. This implementation just hands the
/// message to the provided sender.
impl SchedulerDeps for PassThroughDeps {
    type ClientSender = UnboundedSender<proto::BrokerToClient>;
    type WorkerSender = UnboundedSender<proto::BrokerToWorker>;
    type WorkerArtifactFetcherSender = std::sync::mpsc::Sender<Option<(PathBuf, u64)>>;

    fn send_message_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        message: proto::BrokerToClient,
    ) {
        sender.send(message).ok();
    }

    fn send_message_to_worker(
        &mut self,
        sender: &mut Self::WorkerSender,
        message: proto::BrokerToWorker,
    ) {
        sender.send(message).ok();
    }

    fn send_message_to_worker_artifact_fetcher(
        &mut self,
        sender: &mut Self::WorkerArtifactFetcherSender,
        message: Option<(PathBuf, u64)>,
    ) {
        sender.send(message).ok();
    }
}

/// The production scheduler message type. Some [Message] arms contain a
/// [SchedulerDeps], so it's defined as a generic type. But in this module, we only use
/// one implementation of [SchedulerDeps].
pub type SchedulerMessage = Message<PassThroughDeps>;

/// This type is used often enough to warrant an alias.
pub type SchedulerSender = UnboundedSender<SchedulerMessage>;

pub struct SchedulerTask {
    scheduler: Scheduler<Cache<StdCacheFs>, PassThroughDeps>,
    sender: SchedulerSender,
    receiver: UnboundedReceiver<SchedulerMessage>,
}

impl SchedulerTask {
    pub fn new(
        cache_root: config::CacheRoot,
        cache_bytes_used_target: config::CacheBytesUsedTarget,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let cache = Cache::new(StdCacheFs, cache_root, cache_bytes_used_target);
        SchedulerTask {
            scheduler: Scheduler::new(cache),
            sender,
            receiver,
        }
    }

    pub fn scheduler_sender(&self) -> &SchedulerSender {
        &self.sender
    }

    /// Main loop for the scheduler. This should be run on a task of its own. There should be
    /// exactly one of these in a broker process. It will return when all senders associated with
    /// the receiver are closed, which will happen when the listener and all outstanding worker and
    /// client socket tasks terminate.
    ///
    /// This function ignores any errors it encounters sending a message to an [UnboundedSender].
    /// The rationale is that this indicates that the socket connection has closed, and there are
    /// no more worker tasks to handle that connection. This means that a disconnected message is
    /// on its way to notify the scheduler. It is best to just ignore the error in that case.
    /// Besides, the [scheduler::SchedulerDeps] interface doesn't give us a way to return an error,
    /// for precisely this reason.
    pub async fn run(mut self) {
        net::channel_reader(self.receiver, |msg| {
            self.scheduler.receive_message(&mut PassThroughDeps, msg)
        })
        .await;
    }
}
