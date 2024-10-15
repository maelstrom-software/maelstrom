use crate::{artifact_fetcher::ArtifactFetcher, dispatcher, dispatcher_adapter::DispatcherAdapter};
use maelstrom_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    JobId, Sha256Digest,
};
use maelstrom_util::cache::{self, fs::std::Fs as StdFs, GotArtifact};
use std::path::PathBuf;
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

pub enum CacheGetStrategy {}

impl cache::GetStrategy for CacheGetStrategy {
    type Getter = ();
    fn getter_from_job_id(_jid: JobId) -> Self::Getter {}
}

pub type CacheKey = cache::Key<CacheKeyKind>;
pub type Cache = cache::Cache<StdFs, CacheKeyKind, CacheGetStrategy>;
pub type TempFileFactory = cache::TempFileFactory<StdFs>;

/// The standard implementation of [`Cache`] that just calls into [`cache::Cache`].
impl dispatcher::Cache for Cache {
    type Fs = StdFs;

    fn get_artifact(
        &mut self,
        kind: CacheKeyKind,
        artifact: Sha256Digest,
        jid: JobId,
    ) -> cache::GetArtifact {
        self.get_artifact(kind, artifact, jid)
    }

    fn got_artifact_failure(&mut self, kind: CacheKeyKind, digest: &Sha256Digest) -> Vec<JobId> {
        self.got_artifact_failure(kind, digest)
    }

    fn got_artifact_success(
        &mut self,
        kind: CacheKeyKind,
        digest: &Sha256Digest,
        artifact: GotArtifact<StdFs>,
    ) -> Result<Vec<JobId>, (anyhow::Error, Vec<JobId>)> {
        self.got_artifact_success(kind, digest, artifact)
    }

    fn decrement_ref_count(&mut self, kind: CacheKeyKind, digest: &Sha256Digest) {
        self.decrement_ref_count(kind, digest)
    }

    fn cache_path(&self, kind: CacheKeyKind, digest: &Sha256Digest) -> PathBuf {
        self.cache_path(kind, digest).into_path_buf()
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

pub type DispatcherReceiver = UnboundedReceiver<dispatcher::Message<StdFs>>;
pub type DispatcherSender = UnboundedSender<dispatcher::Message<StdFs>>;
pub type BrokerSocketOutgoingSender = UnboundedSender<WorkerToBroker>;
pub type BrokerSocketIncomingReceiver = UnboundedReceiver<BrokerToWorker>;
pub type DefaultDispatcher =
    dispatcher::Dispatcher<DispatcherAdapter, ArtifactFetcher, BrokerSender, Cache>;
