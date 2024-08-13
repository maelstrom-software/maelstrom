use anyhow::{anyhow, Result};
use maelstrom_base::{ArtifactType, Sha256Digest};
use maelstrom_client_base::spec::Layer;
use std::collections::HashMap;
use tokio::sync::watch;

type BuiltLayer = (Sha256Digest, ArtifactType);
type StringResult<T> = std::result::Result<T, String>;

struct LayerSender(watch::Sender<Option<StringResult<BuiltLayer>>>);

impl LayerSender {
    fn new() -> Self {
        Self(watch::Sender::new(None))
    }

    fn send(&self, res: &Result<BuiltLayer>) {
        let res = res.as_ref().map_err(|e| format!("{e:?}")).cloned();
        let _ = self.0.send(Some(res));
    }

    fn subscribe(&self) -> LayerReceiver {
        LayerReceiver(self.0.subscribe())
    }
}

#[derive(Clone, Debug)]
pub struct LayerReceiver(watch::Receiver<Option<StringResult<BuiltLayer>>>);

impl LayerReceiver {
    pub async fn recv(&mut self) -> Result<BuiltLayer> {
        self.0
            .wait_for(|v| v.is_some())
            .await
            .map_err(|_| anyhow!("building layer canceled"))?;
        let sent = self.0.borrow().as_ref().unwrap().clone();
        sent.map_err(|e| anyhow!("{e}"))
    }
}

enum CacheEntry {
    Pending(LayerSender),
    Cached(BuiltLayer),
}

#[derive(Debug)]
pub enum CacheResult {
    /// The layer was already built and in the cache
    Success(BuiltLayer),
    /// The layer is currently being built. The caller can wait on the channel for it to finish.
    Wait(LayerReceiver),
    /// The layer is not in the cache and not being built, it is up to the caller to build it.
    Build(LayerReceiver),
}

#[derive(Default)]
pub struct LayerCache {
    cache: HashMap<Layer, CacheEntry>,
}

impl LayerCache {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get(&mut self, layer: &Layer) -> CacheResult {
        match self.cache.get(layer) {
            Some(CacheEntry::Pending(r)) => CacheResult::Wait(r.subscribe()),
            Some(CacheEntry::Cached(l)) => CacheResult::Success(l.clone()),
            None => {
                let s = LayerSender::new();
                let r = s.subscribe();
                self.cache.insert(layer.clone(), CacheEntry::Pending(s));
                CacheResult::Build(r)
            }
        }
    }

    pub fn fill(&mut self, layer: &Layer, res: Result<BuiltLayer>) {
        if let Some(CacheEntry::Pending(r)) = self.cache.remove(layer) {
            r.send(&res);
            if let Ok(cached) = res {
                self.cache.insert(layer.clone(), CacheEntry::Cached(cached));
            }
        } else {
            panic!("unexpected cache fill");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use maelstrom_test::{digest, paths_layer, utf8_path_buf};

    #[tokio::test]
    async fn fill_success() {
        let mut cache = LayerCache::new();
        let layer = paths_layer!(["/a"]);
        let mut r1 = assert_matches!(cache.get(&layer), CacheResult::Build(r) => r);
        let mut r2 = assert_matches!(cache.get(&layer), CacheResult::Wait(r) => r);
        let built = (digest![1], ArtifactType::Manifest);

        let built_clone = built.clone();
        let r1_task =
            tokio::task::spawn(async move { assert_eq!(r1.recv().await.unwrap(), built_clone) });

        let built_clone = built.clone();
        let r2_task =
            tokio::task::spawn(async move { assert_eq!(r2.recv().await.unwrap(), built_clone) });

        cache.fill(&layer, Ok(built.clone()));
        assert_matches!(cache.get(&layer), CacheResult::Success(ref b) if b == &built);

        r1_task.await.unwrap();
        r2_task.await.unwrap();
    }

    #[tokio::test]
    async fn fill_success_multi_key() {
        let mut cache = LayerCache::new();
        let layer1 = paths_layer!(["/a"]);
        let layer2 = paths_layer!(["/b"]);

        assert_matches!(cache.get(&layer1), CacheResult::Build(_));
        assert_matches!(cache.get(&layer2), CacheResult::Build(_));

        let mut r1 = assert_matches!(cache.get(&layer1), CacheResult::Wait(r) => r);
        let mut r2 = assert_matches!(cache.get(&layer2), CacheResult::Wait(r) => r);
        let built1 = (digest![1], ArtifactType::Manifest);
        let built2 = (digest![1], ArtifactType::Manifest);

        let built1_clone = built1.clone();
        let r1_task =
            tokio::task::spawn(async move { assert_eq!(r1.recv().await.unwrap(), built1_clone) });

        let built2_clone = built2.clone();
        let r2_task =
            tokio::task::spawn(async move { assert_eq!(r2.recv().await.unwrap(), built2_clone) });

        cache.fill(&layer1, Ok(built1.clone()));
        assert_matches!(cache.get(&layer1), CacheResult::Success(ref b) if b == &built1);
        r1_task.await.unwrap();

        cache.fill(&layer2, Ok(built2.clone()));
        assert_matches!(cache.get(&layer2), CacheResult::Success(ref b) if b == &built2);
        r2_task.await.unwrap();
    }

    #[tokio::test]
    async fn fill_failure() {
        let mut cache = LayerCache::new();
        let layer = paths_layer!(["/a"]);
        let mut r1 = assert_matches!(cache.get(&layer), CacheResult::Build(r) => r);
        let mut r2 = assert_matches!(cache.get(&layer), CacheResult::Wait(r) => r);

        let r1_task = tokio::task::spawn(async move {
            assert_eq!(r1.recv().await.unwrap_err().to_string(), "test error")
        });
        let r2_task = tokio::task::spawn(async move {
            assert_eq!(r2.recv().await.unwrap_err().to_string(), "test error")
        });

        cache.fill(&layer, Err(anyhow!("test error")));
        assert_matches!(cache.get(&layer), CacheResult::Build(_));

        r1_task.await.unwrap();
        r2_task.await.unwrap();
    }

    #[tokio::test]
    async fn fill_failure_multi_key() {
        let mut cache = LayerCache::new();
        let layer1 = paths_layer!(["/a"]);
        let layer2 = paths_layer!(["/b"]);

        assert_matches!(cache.get(&layer1), CacheResult::Build(_));
        assert_matches!(cache.get(&layer2), CacheResult::Build(_));

        let mut r1 = assert_matches!(cache.get(&layer1), CacheResult::Wait(r) => r);
        let mut r2 = assert_matches!(cache.get(&layer2), CacheResult::Wait(r) => r);

        let r1_task = tokio::task::spawn(async move {
            assert_eq!(r1.recv().await.unwrap_err().to_string(), "test error 1")
        });
        let r2_task = tokio::task::spawn(async move {
            assert_eq!(r2.recv().await.unwrap_err().to_string(), "test error 2")
        });

        cache.fill(&layer1, Err(anyhow!("test error 1")));
        assert_matches!(cache.get(&layer1), CacheResult::Build(_));

        cache.fill(&layer2, Err(anyhow!("test error 2")));
        assert_matches!(cache.get(&layer2), CacheResult::Build(_));

        r1_task.await.unwrap();
        r2_task.await.unwrap();
    }

    #[tokio::test]
    async fn cache_destroyed() {
        let mut cache = LayerCache::new();
        let layer = paths_layer!(["/a"]);
        assert_matches!(cache.get(&layer), CacheResult::Build(_));
        let mut r1 = assert_matches!(cache.get(&layer), CacheResult::Wait(r) => r);
        let mut r2 = assert_matches!(cache.get(&layer), CacheResult::Wait(r) => r);

        let r1_task = tokio::task::spawn(async move {
            assert_eq!(
                r1.recv().await.unwrap_err().to_string(),
                "building layer canceled"
            )
        });
        let r2_task = tokio::task::spawn(async move {
            assert_eq!(
                r2.recv().await.unwrap_err().to_string(),
                "building layer canceled"
            )
        });

        drop(cache);

        r1_task.await.unwrap();
        r2_task.await.unwrap();
    }
}
