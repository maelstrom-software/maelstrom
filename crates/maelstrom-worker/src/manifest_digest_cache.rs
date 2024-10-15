use crate::{dispatcher::Message, types::DispatcherSender};
use anyhow::Result;
use lru::LruCache;
use maelstrom_base::{
    manifest::{ManifestEntryData, ManifestFileData},
    JobId, Sha256Digest,
};
use maelstrom_util::{async_fs, manifest::AsyncManifestReader};
use slog::debug;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    path::Path,
    path::PathBuf,
    sync::Arc,
};
use tokio::task;

async fn read_manifest(path: &Path) -> Result<HashSet<Sha256Digest>> {
    let fs = async_fs::Fs::new();
    let mut reader = AsyncManifestReader::new(fs.open_file(path).await?).await?;
    let mut digests = HashSet::new();
    while let Some(entry) = reader.next().await? {
        if let ManifestEntryData::File(ManifestFileData::Digest(digest)) = entry.data {
            digests.insert(digest);
        }
    }
    Ok(digests)
}

struct ManifestDigestCacheInner {
    pending: HashMap<PathBuf, Vec<(Sha256Digest, JobId)>>,
    cached: LruCache<PathBuf, HashSet<Sha256Digest>>,
}

impl ManifestDigestCacheInner {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            pending: HashMap::new(),
            cached: LruCache::new(capacity),
        }
    }
}

#[derive(Clone)]
pub struct ManifestDigestCache {
    sender: DispatcherSender,
    log: slog::Logger,
    cache: Arc<std::sync::Mutex<ManifestDigestCacheInner>>,
}

impl ManifestDigestCache {
    pub fn new(sender: DispatcherSender, log: slog::Logger, capacity: NonZeroUsize) -> Self {
        Self {
            sender,
            log,
            cache: Arc::new(std::sync::Mutex::new(ManifestDigestCacheInner::new(
                capacity,
            ))),
        }
    }

    pub fn get(&self, digest: Sha256Digest, path: PathBuf, jid: JobId) {
        let mut locked_cache = self.cache.lock().unwrap();
        if let Some(waiting) = locked_cache.pending.get_mut(&path) {
            waiting.push((digest, jid));
        } else if let Some(cached) = locked_cache.cached.get(&path) {
            self.sender
                .send(Message::ReadManifestDigests(
                    digest,
                    jid,
                    Ok(cached.clone()),
                ))
                .ok();
        } else {
            locked_cache
                .pending
                .insert(path.clone(), vec![(digest, jid)]);

            let self_clone = self.clone();
            task::spawn(async move {
                self_clone.fill_cache(path).await;
            });
        }
    }

    async fn fill_cache(&self, path: PathBuf) {
        debug!(self.log, "reading digests from manifest"; "manifest_path" => ?path);
        let result = read_manifest(&path).await;
        debug!(self.log, "read digests from manifest"; "result" => ?result);

        let mut locked_cache = self.cache.lock().unwrap();
        let waiting = locked_cache.pending.remove(&path).unwrap();
        for (digest, jid) in waiting {
            // This is a clippy bug <https://github.com/rust-lang/rust-clippy/issues/12357>
            #[allow(clippy::useless_asref)]
            let res = result
                .as_ref()
                .map(|v| v.clone())
                .map_err(|e| anyhow::Error::msg(e.to_string()));
            self.sender
                .send(Message::ReadManifestDigests(digest, jid, res))
                .ok();
        }
        if let Ok(digests) = result {
            locked_cache.cached.push(path, digests);
        }
    }
}
