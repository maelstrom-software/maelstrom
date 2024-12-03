use crate::{
    cache::{BrokerCache, SchedulerCache, TempFileFactory},
    Config,
};
use anyhow::Result;
use maelstrom_base::{ClientId, JobId, Sha256Digest};
use maelstrom_util::cache::{fs::Fs, Cache, GetArtifact, GetStrategy, GotArtifact, Key};
use ref_cast::RefCast;
use slog::Logger;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;

#[derive(Clone, Debug, Eq, Hash, PartialEq, RefCast)]
#[repr(transparent)]
pub struct BrokerKey(pub Sha256Digest);

impl Key for BrokerKey {
    fn kinds() -> impl Iterator<Item = &'static str> {
        ["blob"].into_iter()
    }

    fn from_kind_and_digest(_kind: &'static str, digest: Sha256Digest) -> Self {
        Self(digest)
    }

    fn kind(&self) -> &'static str {
        "blob"
    }

    fn digest(&self) -> &Sha256Digest {
        &self.0
    }
}

pub enum BrokerGetStrategy {}

impl GetStrategy for BrokerGetStrategy {
    type Getter = ClientId;
    fn getter_from_job_id(jid: JobId) -> Self::Getter {
        jid.cid
    }
}

pub enum TcpUploadLocalCache {}

impl BrokerCache for TcpUploadLocalCache {
    type Cache = maelstrom_util::cache::Cache<
        maelstrom_util::cache::fs::std::Fs,
        BrokerKey,
        BrokerGetStrategy,
    >;

    type TempFileFactory =
        maelstrom_util::cache::TempFileFactory<maelstrom_util::cache::fs::std::Fs>;

    fn new(config: Config, log: Logger) -> Result<(Self::Cache, Self::TempFileFactory)> {
        maelstrom_util::cache::Cache::new(
            maelstrom_util::cache::fs::std::Fs,
            config.cache_root,
            config.cache_size,
            log.clone(),
            true,
        )
    }
}

type LazyRead<FileT> = maelstrom_util::io::LazyRead<
    Pin<Box<dyn Future<Output = io::Result<FileT>> + Send + Sync>>,
    FileT,
>;

impl<FsT: Fs> SchedulerCache for Cache<FsT, BrokerKey, BrokerGetStrategy> {
    type TempFile = FsT::TempFile;
    type ArtifactStream = LazyRead<maelstrom_util::async_fs::File>;

    fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact {
        self.get_artifact(BrokerKey(digest), jid)
    }

    fn got_artifact(&mut self, digest: &Sha256Digest, file: Option<FsT::TempFile>) -> Vec<JobId> {
        let file = file.expect("file provided");
        self.got_artifact_success(BrokerKey::ref_cast(digest), GotArtifact::file(file))
            .map_err(|(err, _)| err)
            .unwrap()
    }

    fn decrement_refcount(&mut self, digest: &Sha256Digest) {
        self.decrement_ref_count(BrokerKey::ref_cast(digest))
    }

    fn client_disconnected(&mut self, cid: ClientId) {
        self.getter_disconnected(cid)
    }

    fn get_artifact_for_worker(&mut self, digest: &Sha256Digest) -> Option<(PathBuf, u64)> {
        let cache_path = self.cache_path(BrokerKey::ref_cast(digest)).into_path_buf();
        self.try_increment_ref_count(BrokerKey::ref_cast(digest))
            .map(|size| (cache_path, size))
    }

    fn read_artifact(&mut self, digest: &Sha256Digest) -> (Self::ArtifactStream, u64) {
        let key = BrokerKey::ref_cast(digest);
        let cache_path = self.cache_path(key).into_path_buf();
        let stream = LazyRead::new(Box::pin(async move {
            let fs = maelstrom_util::async_fs::Fs::new();
            fs.open_file(cache_path)
                .await
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
        }));
        let bytes_used = self.try_get_size(key).expect("non-zero refcount");

        (stream, bytes_used)
    }
}

impl TempFileFactory
    for maelstrom_util::cache::TempFileFactory<maelstrom_util::cache::fs::std::Fs>
{
    type TempFile = <maelstrom_util::cache::fs::std::Fs as maelstrom_util::cache::fs::Fs>::TempFile;

    fn temp_file(&self) -> Result<Self::TempFile> {
        self.temp_file()
    }
}
