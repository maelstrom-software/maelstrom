use crate::{artifact_fetcher::ArtifactFetcher, dispatcher, dispatcher_adapter::DispatcherAdapter};
use maelstrom_base::{
    proto::{BrokerToWorker, WorkerToBroker},
    JobId, Sha256Digest,
};
use maelstrom_util::cache::{self, fs::std::Fs as StdFs, GotArtifact};
use std::path::PathBuf;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CacheKeyKind {
    Blob,
    BottomFsLayer,
    UpperFsLayer,
}

impl CacheKeyKind {
    fn from_str(kind: &'static str) -> Self {
        match kind {
            "blob" => Self::Blob,
            "bottom_fs_layer" => Self::BottomFsLayer,
            "upper_fs_layer" => Self::UpperFsLayer,
            _ => {
                panic!("bad CacheKeyKind {kind}");
            }
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Blob => "blob",
            Self::BottomFsLayer => "bottom_fs_layer",
            Self::UpperFsLayer => "upper_fs_layer",
        }
    }

    fn iter() -> <[&'static str; 3] as IntoIterator>::IntoIter {
        ["blob", "bottom_fs_layer", "upper_fs_layer"].into_iter()
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CacheKey {
    kind: CacheKeyKind,
    digest: Sha256Digest,
}

impl CacheKey {
    pub fn new(kind: CacheKeyKind, digest: Sha256Digest) -> Self {
        Self { kind, digest }
    }

    pub fn blob(digest: Sha256Digest) -> Self {
        Self::new(CacheKeyKind::Blob, digest)
    }

    pub fn bottom_fs_layer(digest: Sha256Digest) -> Self {
        Self::new(CacheKeyKind::BottomFsLayer, digest)
    }

    pub fn upper_fs_layer(digest: Sha256Digest) -> Self {
        Self::new(CacheKeyKind::UpperFsLayer, digest)
    }
}

impl cache::Key for CacheKey {
    type KindIterator = <[&'static str; 3] as IntoIterator>::IntoIter;

    fn kinds() -> Self::KindIterator {
        CacheKeyKind::iter()
    }

    fn from_kind_and_digest(kind: &'static str, digest: Sha256Digest) -> Self {
        Self {
            kind: CacheKeyKind::from_str(kind),
            digest,
        }
    }

    fn kind(&self) -> &'static str {
        self.kind.as_str()
    }

    fn digest(&self) -> &Sha256Digest {
        &self.digest
    }
}

pub enum CacheGetStrategy {}

impl cache::GetStrategy for CacheGetStrategy {
    type Getter = ();
    fn getter_from_job_id(_jid: JobId) -> Self::Getter {}
}

pub type Cache = cache::Cache<StdFs, CacheKey, CacheGetStrategy>;
pub type TempFile = cache::fs::std::TempFile;
pub type TempFileFactory = cache::TempFileFactory<StdFs>;

/// The standard implementation of [`Cache`] that just calls into [`cache::Cache`].
impl dispatcher::Cache for Cache {
    type Fs = StdFs;

    fn get_artifact(&mut self, key: CacheKey, jid: JobId) -> cache::GetArtifact {
        self.get_artifact(key, jid)
    }

    fn got_artifact_failure(&mut self, key: &CacheKey) -> Vec<JobId> {
        self.got_artifact_failure(key)
    }

    fn got_artifact_success(
        &mut self,
        key: &CacheKey,
        artifact: GotArtifact<StdFs>,
    ) -> Result<Vec<JobId>, (anyhow::Error, Vec<JobId>)> {
        self.got_artifact_success(key, artifact)
    }

    fn decrement_ref_count(&mut self, key: &CacheKey) {
        self.decrement_ref_count(key)
    }

    fn cache_path(&self, key: &CacheKey) -> PathBuf {
        self.cache_path(key).into_path_buf()
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
pub type Dispatcher =
    dispatcher::Dispatcher<DispatcherAdapter, ArtifactFetcher, BrokerSender, Cache>;
