use crate::{deps::CacheGetStrategy, dispatcher::Message};
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

pub type CacheKey = cache::Key<CacheKeyKind>;
pub type Cache = cache::Cache<fs::std::Fs, CacheKeyKind, CacheGetStrategy>;
pub type TempFileFactory = cache::TempFileFactory<fs::std::Fs>;

pub type DispatcherReceiver = UnboundedReceiver<Message<fs::std::Fs>>;
pub type DispatcherSender = UnboundedSender<Message<fs::std::Fs>>;
pub type BrokerSocketOutgoingSender = UnboundedSender<WorkerToBroker>;
pub type BrokerSocketIncomingReceiver = UnboundedReceiver<BrokerToWorker>;
