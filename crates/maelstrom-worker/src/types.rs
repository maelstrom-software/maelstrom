use crate::{
    artifact_fetcher::ArtifactFetcher, deps::CacheGetStrategy, dispatcher,
    dispatcher_adapter::DispatcherAdapter,
};
use maelstrom_base::proto::{BrokerToWorker, WorkerToBroker};
use maelstrom_util::cache::{self, fs};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(
    Clone, Copy, Debug, strum::Display, PartialEq, Eq, PartialOrd, Ord, Hash, strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum CacheKeyKind {
    Blob,
    BottomFsLayer,
    UpperFsLayer,
}

impl cache::KeyKind for CacheKeyKind {
    type Iterator = <Self as strum::IntoEnumIterator>::Iterator;

    fn iter() -> Self::Iterator {
        <Self as strum::IntoEnumIterator>::iter()
    }
}

pub struct BrokerSender {
    sender: Option<BrokerSocketOutgoingSender>,
}

impl BrokerSender {
    pub fn new(broker_socket_outgoing_sender: BrokerSocketOutgoingSender) -> Self {
        Self {
            sender: Some(broker_socket_outgoing_sender),
        }
    }
}

impl dispatcher::BrokerSender for BrokerSender {
    fn send_message_to_broker(&mut self, message: WorkerToBroker) {
        if let Some(sender) = self.sender.as_ref() {
            sender.send(message).ok();
        }
    }

    fn close(&mut self) {
        self.sender = None;
    }
}

pub type CacheKey = cache::Key<CacheKeyKind>;
pub type Cache = cache::Cache<fs::std::Fs, CacheKeyKind, CacheGetStrategy>;
pub type TempFileFactory = cache::TempFileFactory<fs::std::Fs>;

pub type DispatcherReceiver = UnboundedReceiver<dispatcher::Message<fs::std::Fs>>;
pub type DispatcherSender = UnboundedSender<dispatcher::Message<fs::std::Fs>>;
pub type BrokerSocketOutgoingSender = UnboundedSender<WorkerToBroker>;
pub type BrokerSocketIncomingReceiver = UnboundedReceiver<BrokerToWorker>;
pub type DefaultDispatcher =
    dispatcher::Dispatcher<DispatcherAdapter, ArtifactFetcher, BrokerSender, Cache>;
