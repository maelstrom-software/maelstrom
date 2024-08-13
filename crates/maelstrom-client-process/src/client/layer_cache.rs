use anyhow::{anyhow, Result};
use maelstrom_base::{ArtifactType, Sha256Digest};
use maelstrom_client_base::spec::Layer;
use std::collections::HashMap;
use tokio::sync::watch;

type BuiltLayer = (Sha256Digest, ArtifactType);

struct LayerSender(watch::Sender<Option<BuiltLayer>>);

impl LayerSender {
    fn new() -> Self {
        Self(watch::Sender::new(None))
    }

    fn send(&self, res: BuiltLayer) {
        let _ = self.0.send(Some(res));
    }

    fn subscribe(&self) -> LayerReceiver {
        LayerReceiver(self.0.subscribe())
    }
}

#[derive(Clone, Debug)]
pub struct LayerReceiver(watch::Receiver<Option<BuiltLayer>>);

impl LayerReceiver {
    pub async fn recv(&mut self) -> Result<BuiltLayer> {
        self.0
            .wait_for(|v| v.is_some())
            .await
            .map_err(|_| anyhow!("failed to build layer"))?;
        Ok(self.0.borrow().as_ref().unwrap().clone())
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

    pub fn fill_success(&mut self, layer: &Layer, cached: BuiltLayer) {
        if let Some(CacheEntry::Pending(r)) = self.cache.remove(layer) {
            r.send(cached.clone());
            self.cache.insert(layer.clone(), CacheEntry::Cached(cached));
        } else {
            panic!("unexpected cache fill");
        }
    }

    pub fn fill_failure(&mut self, layer: &Layer) {
        self.cache.remove(layer);
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
        assert_matches!(cache.get(&layer), CacheResult::Build(_));
        let mut r1 = assert_matches!(cache.get(&layer), CacheResult::Wait(r) => r);
        let mut r2 = assert_matches!(cache.get(&layer), CacheResult::Wait(r) => r);
        let built = (digest![1], ArtifactType::Manifest);

        let built_clone = built.clone();
        let r1_task =
            tokio::task::spawn(async move { assert_eq!(r1.recv().await.unwrap(), built_clone) });

        let built_clone = built.clone();
        let r2_task =
            tokio::task::spawn(async move { assert_eq!(r2.recv().await.unwrap(), built_clone) });

        cache.fill_success(&layer, built.clone());
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

        cache.fill_success(&layer1, built1.clone());
        assert_matches!(cache.get(&layer1), CacheResult::Success(ref b) if b == &built1);
        r1_task.await.unwrap();

        cache.fill_success(&layer2, built2.clone());
        assert_matches!(cache.get(&layer2), CacheResult::Success(ref b) if b == &built2);
        r2_task.await.unwrap();
    }

    #[tokio::test]
    async fn fill_failure() {
        let mut cache = LayerCache::new();
        let layer = paths_layer!(["/a"]);
        assert_matches!(cache.get(&layer), CacheResult::Build(_));
        let mut r1 = assert_matches!(cache.get(&layer), CacheResult::Wait(r) => r);
        let mut r2 = assert_matches!(cache.get(&layer), CacheResult::Wait(r) => r);

        let r1_task = tokio::task::spawn(async move { r1.recv().await.unwrap_err() });
        let r2_task = tokio::task::spawn(async move { r2.recv().await.unwrap_err() });

        cache.fill_failure(&layer);
        assert_matches!(cache.get(&layer), CacheResult::Build(_));

        r1_task.await.unwrap();
        r2_task.await.unwrap();
    }
}
