use crate::cache::{EntryKind, Key};
use maelstrom_base::{ArtifactType, NonEmpty, Sha256Digest};
use maelstrom_util::ext::OptionExt as _;
use sha2::{Digest as _, Sha256};
use std::{
    collections::{HashMap, HashSet},
    matches, mem,
    path::{Path, PathBuf},
};

#[derive(Debug, PartialEq, Eq)]
enum PendingManifestDigests {
    WaitingForDigests,
    WaitingForArtifacts { num_remaining: u64 },
}

#[derive(Debug, PartialEq, Eq)]
enum PendingBottomLayer {
    WaitingForArtifact {
        type_: ArtifactType,
    },
    WaitingForManifestDigests {
        path: PathBuf,
        pending_digests: PendingManifestDigests,
    },
    WaitingForFsLayer,
    Ready {
        fs_layer_path: PathBuf,
    },
}

impl PendingBottomLayer {
    fn assert_ready_and_get_path(&self) -> &PathBuf {
        let Self::Ready { fs_layer_path } = self else {
            panic!("bottom layer unexpectedly not ready {self:?}");
        };
        fs_layer_path
    }
}

#[derive(Debug, PartialEq, Eq)]
enum PendingTopLayer {
    NoStackedUpperLayers,
    StackedUpperLayers {
        index: usize,
        top_layer_path: PathBuf,
        top_layer_digest: Sha256Digest,
    },
}

impl PendingTopLayer {
    fn add_layer(&mut self, digest: Sha256Digest, path: PathBuf) {
        *self = match self {
            Self::NoStackedUpperLayers => Self::StackedUpperLayers {
                index: 2,
                top_layer_path: path,
                top_layer_digest: digest,
            },
            Self::StackedUpperLayers { index, .. } => Self::StackedUpperLayers {
                index: *index + 1,
                top_layer_path: path,
                top_layer_digest: digest,
            },
        }
    }

    fn assert_stacked_and_get_path(&self) -> &PathBuf {
        let Self::StackedUpperLayers { top_layer_path, .. } = self else {
            panic!("top layer unexpectedly not stacked");
        };
        top_layer_path
    }
}

pub fn upper_layer_digest(upper_layer: &Sha256Digest, lower_layer: &Sha256Digest) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(lower_layer.as_bytes());
    hasher.update(upper_layer.as_bytes());
    Sha256Digest::new(hasher.finalize().into())
}

/// Track which layers have been gotten from the cache.
#[derive(Debug)]
pub struct LayerTracker {
    layers: NonEmpty<Sha256Digest>,
    bottom_layers: HashMap<Sha256Digest, PendingBottomLayer>,
    top_fs_layer: PendingTopLayer,
    cache_keys: HashSet<Key>,
    pending_manifest_dependencies: HashMap<Sha256Digest, Vec<Sha256Digest>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FetcherResult {
    Got(PathBuf),
    Pending,
}

pub trait Fetcher {
    fn fetch_artifact(&mut self, digest: &Sha256Digest) -> FetcherResult;
    fn fetch_bottom_fs_layer(
        &mut self,
        digest: &Sha256Digest,
        artifact_type: ArtifactType,
        artifact_path: &Path,
    ) -> FetcherResult;
    fn fetch_upper_fs_layer(
        &mut self,
        digest: &Sha256Digest,
        lower_layer_path: &Path,
        upper_layer_path: &Path,
    ) -> FetcherResult;
    fn fetch_manifest_digests(&mut self, digest: &Sha256Digest, path: &Path);
}

impl LayerTracker {
    pub fn new(
        layers: &NonEmpty<(Sha256Digest, ArtifactType)>,
        fetcher: &mut impl Fetcher,
    ) -> Self {
        let mut tracker = Self {
            layers: layers.clone().map(|(d, _)| d),
            bottom_layers: HashMap::new(),
            top_fs_layer: PendingTopLayer::NoStackedUpperLayers,
            cache_keys: HashSet::new(),
            pending_manifest_dependencies: HashMap::new(),
        };
        let mut seen = HashMap::<Sha256Digest, ArtifactType>::new();
        for (digest, type_) in layers {
            if let Some(previous_type) = seen.get(digest) {
                // this needs to be avoided perhaps in the broker
                assert_eq!(
                    previous_type, type_,
                    "same digest both manifest and tar {layers:?}"
                );
            } else {
                tracker
                    .bottom_layers
                    .insert(
                        digest.clone(),
                        PendingBottomLayer::WaitingForArtifact { type_: *type_ },
                    )
                    .assert_is_none();
                seen.insert(digest.clone(), *type_).assert_is_none();
            }
        }
        for (digest, _) in seen {
            if let FetcherResult::Got(path) = fetcher.fetch_artifact(&digest) {
                tracker.got_artifact(&digest, path, fetcher);
            }
        }

        tracker
    }

    fn bottom_layers_all_ready(&self) -> bool {
        self.bottom_layers
            .values()
            .all(|e| matches!(e, PendingBottomLayer::Ready { .. }))
    }

    fn fetch_upper_layers(&mut self, fetcher: &mut impl Fetcher) {
        if self.layers.len() < 2 {
            return;
        }
        loop {
            let (upper_index, lower_digest, lower_path) = match &self.top_fs_layer {
                PendingTopLayer::NoStackedUpperLayers => {
                    let digest = &self.layers[0];
                    let path = self
                        .bottom_layers
                        .get(digest)
                        .unwrap()
                        .assert_ready_and_get_path();
                    (1, digest, path)
                }
                PendingTopLayer::StackedUpperLayers {
                    index,
                    top_layer_digest,
                    top_layer_path,
                } => (*index, top_layer_digest, top_layer_path),
            };

            if upper_index >= self.layers.len() {
                break;
            }

            let upper_digest = &self.layers[upper_index];
            let upper_path = self
                .bottom_layers
                .get(upper_digest)
                .unwrap()
                .assert_ready_and_get_path();

            let digest = upper_layer_digest(upper_digest, lower_digest);
            match fetcher.fetch_upper_fs_layer(&digest, lower_path, upper_path) {
                FetcherResult::Got(path) => {
                    self.cache_keys
                        .insert(Key::new(EntryKind::UpperFsLayer, digest.clone()));
                    self.top_fs_layer.add_layer(digest.clone(), path)
                }
                FetcherResult::Pending => break,
            }
        }
    }

    fn fetch_bottom_fs_layer(
        &mut self,
        digest: &Sha256Digest,
        type_: ArtifactType,
        path: PathBuf,
        fetcher: &mut impl Fetcher,
    ) {
        let result = fetcher.fetch_bottom_fs_layer(digest, type_, &path);
        *self.bottom_layers.get_mut(digest).unwrap() = PendingBottomLayer::WaitingForFsLayer;
        if let FetcherResult::Got(path) = result {
            self.got_bottom_fs_layer(digest, path, fetcher)
        }
    }

    pub fn got_artifact(
        &mut self,
        digest: &Sha256Digest,
        path: PathBuf,
        fetcher: &mut impl Fetcher,
    ) {
        self.cache_keys
            .insert(Key::new(EntryKind::Blob, digest.clone()));

        if self.pending_manifest_dependencies.contains_key(digest) {
            self.got_manifest_artifact(digest, fetcher);
            if !self.bottom_layers.contains_key(digest) {
                return;
            }
        }

        let PendingBottomLayer::WaitingForArtifact { type_ } =
            self.bottom_layers.get(digest).unwrap()
        else {
            panic!("unexpected got_artifact")
        };

        if *type_ == ArtifactType::Manifest {
            fetcher.fetch_manifest_digests(digest, &path);
            *self.bottom_layers.get_mut(digest).unwrap() =
                PendingBottomLayer::WaitingForManifestDigests {
                    path,
                    pending_digests: PendingManifestDigests::WaitingForDigests,
                };
            return;
        }

        self.fetch_bottom_fs_layer(digest, *type_, path, fetcher);
    }

    fn got_manifest_artifact(&mut self, digest: &Sha256Digest, fetcher: &mut impl Fetcher) {
        let layers = self.pending_manifest_dependencies.remove(digest).unwrap();
        for manifest_digest in layers {
            let PendingBottomLayer::WaitingForManifestDigests {
                path,
                pending_digests: PendingManifestDigests::WaitingForArtifacts { num_remaining },
            } = self.bottom_layers.get_mut(&manifest_digest).unwrap()
            else {
                panic!("got manifest digest for layer which wasn't expecting it");
            };
            *num_remaining -= 1;
            if *num_remaining == 0 {
                let path = path.clone();
                self.fetch_bottom_fs_layer(&manifest_digest, ArtifactType::Manifest, path, fetcher);
            }
        }
    }

    pub fn got_manifest_digests(
        &mut self,
        manifest_digest: &Sha256Digest,
        digests: HashSet<Sha256Digest>,
        fetcher: &mut impl Fetcher,
    ) {
        let bottom_layer_keys: HashSet<_> = self.bottom_layers.keys().cloned().collect();
        let PendingBottomLayer::WaitingForManifestDigests {
            path,
            pending_digests,
        } = self.bottom_layers.get_mut(manifest_digest).unwrap()
        else {
            panic!("unexpected got_manifest_digests");
        };
        let mut num_remaining = 0;
        for digest in digests {
            let cache_key = Key::new(EntryKind::Blob, digest.clone());
            if self.cache_keys.contains(&cache_key) {
                continue;
            }

            if let Some(pending_entry) = self.pending_manifest_dependencies.get_mut(&digest) {
                pending_entry.push(manifest_digest.clone());
                num_remaining += 1;
            } else if bottom_layer_keys.contains(&digest)
                || fetcher.fetch_artifact(&digest) == FetcherResult::Pending
            {
                self.pending_manifest_dependencies
                    .insert(digest, vec![manifest_digest.clone()]);
                num_remaining += 1;
            } else {
                self.cache_keys.insert(cache_key);
            }
        }
        if num_remaining > 0 {
            *pending_digests = PendingManifestDigests::WaitingForArtifacts { num_remaining };
        } else {
            let path = path.clone();
            self.fetch_bottom_fs_layer(manifest_digest, ArtifactType::Manifest, path, fetcher);
        }
    }

    pub fn got_bottom_fs_layer(
        &mut self,
        digest: &Sha256Digest,
        path: PathBuf,
        fetcher: &mut impl Fetcher,
    ) {
        let existing = mem::replace(
            self.bottom_layers.get_mut(digest).unwrap(),
            PendingBottomLayer::Ready {
                fs_layer_path: path,
            },
        );
        assert_eq!(existing, PendingBottomLayer::WaitingForFsLayer);
        self.cache_keys
            .insert(Key::new(EntryKind::BottomFsLayer, digest.clone()));

        if self.bottom_layers_all_ready() {
            self.fetch_upper_layers(fetcher);
        }
    }

    pub fn got_upper_fs_layer(
        &mut self,
        digest: &Sha256Digest,
        path: PathBuf,
        fetcher: &mut impl Fetcher,
    ) {
        self.cache_keys
            .insert(Key::new(EntryKind::UpperFsLayer, digest.clone()));
        self.top_fs_layer.add_layer(digest.clone(), path);
        self.fetch_upper_layers(fetcher);
    }

    pub fn is_complete(&self) -> bool {
        matches!(
            self.top_fs_layer,
            PendingTopLayer::StackedUpperLayers { index, .. } if index >= self.layers.len()
        ) || (self.bottom_layers_all_ready() && self.layers.len() < 2)
    }

    pub fn into_cache_keys(self) -> HashSet<Key> {
        self.cache_keys
    }

    pub fn into_path_and_cache_keys(self) -> (PathBuf, HashSet<Key>) {
        assert!(self.is_complete());
        if self.layers.len() < 2 {
            (
                self.bottom_layers
                    .get(self.layers.first())
                    .unwrap()
                    .assert_ready_and_get_path()
                    .clone(),
                self.cache_keys,
            )
        } else {
            (
                self.top_fs_layer.assert_stacked_and_get_path().clone(),
                self.cache_keys,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maelstrom_base::nonempty;
    use maelstrom_test::{digest, path_buf};
    use maelstrom_util::ext::BoolExt as _;
    use maplit::hashset;

    struct TestFetcher {
        artifacts: HashMap<Sha256Digest, FetcherResult>,
        bottom_fs_layers: HashMap<Sha256Digest, FetcherResult>,
        upper_fs_layers: HashMap<Sha256Digest, FetcherResult>,
        manifest_digests: HashSet<(Sha256Digest, PathBuf)>,
    }

    impl TestFetcher {
        fn new(
            artifacts: impl IntoIterator<Item = (Sha256Digest, FetcherResult)>,
            bottom_fs_layers: impl IntoIterator<Item = (Sha256Digest, FetcherResult)>,
            upper_fs_layers: impl IntoIterator<Item = (Sha256Digest, FetcherResult)>,
            manifest_digests: impl IntoIterator<Item = (Sha256Digest, PathBuf)>,
        ) -> Self {
            Self {
                artifacts: artifacts.into_iter().collect(),
                bottom_fs_layers: bottom_fs_layers.into_iter().collect(),
                upper_fs_layers: upper_fs_layers.into_iter().collect(),
                manifest_digests: manifest_digests.into_iter().collect(),
            }
        }
    }

    impl Fetcher for TestFetcher {
        fn fetch_artifact(&mut self, digest: &Sha256Digest) -> FetcherResult {
            self.artifacts.remove(digest).unwrap()
        }

        fn fetch_bottom_fs_layer(
            &mut self,
            digest: &Sha256Digest,
            _: ArtifactType,
            _: &Path,
        ) -> FetcherResult {
            self.bottom_fs_layers.remove(digest).unwrap()
        }

        fn fetch_upper_fs_layer(
            &mut self,
            digest: &Sha256Digest,
            _: &Path,
            _: &Path,
        ) -> FetcherResult {
            self.upper_fs_layers.remove(digest).unwrap()
        }

        fn fetch_manifest_digests(&mut self, manifest_digest: &Sha256Digest, path: &Path) {
            self.manifest_digests
                .remove(&(manifest_digest.clone(), path.to_path_buf()))
                .assert_is_true();
        }
    }

    impl Drop for TestFetcher {
        fn drop(&mut self) {
            assert_eq!(self.artifacts, Default::default());
            assert_eq!(self.bottom_fs_layers, Default::default());
            assert_eq!(self.upper_fs_layers, Default::default());
            assert_eq!(self.manifest_digests, Default::default());
        }
    }

    macro_rules! upper_digest {
        ($n1:expr, $($n:expr),*) => {
            upper_layer_digest(&digest!($n1), &upper_digest!($($n),*))
        };
        ($n1:expr) => {
            digest!($n1)
        }
    }

    #[test]
    fn one_layer_everything_in_cache_into_path_and_cache_keys() {
        let layers = nonempty![(digest!(1), ArtifactType::Tar)];
        let mut fetcher = TestFetcher::new(
            [(digest!(1), FetcherResult::Got(path_buf!("/blob/1")))],
            [(digest!(1), FetcherResult::Got(path_buf!("/fs_b/1")))],
            [],
            [],
        );
        let tracker = LayerTracker::new(&layers, &mut fetcher);

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_b/1"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                }
            ),
        );
    }

    #[test]
    fn one_layer_manifest_everything_in_cache_into_path_and_cache_keys() {
        let layers = nonempty![(digest!(1), ArtifactType::Manifest)];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
            ],
            [(digest!(1), FetcherResult::Got(path_buf!("/fs_b/1")))],
            [],
            [(digest!(1), path_buf!("/blob/1"))],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);
        tracker.got_manifest_digests(&digest!(1), hashset! { digest!(2) }, &mut fetcher);

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_b/1"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::Blob, digest!(2)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                }
            ),
        );
    }

    #[test]
    fn one_layer_pending_then_got_into_path_and_cache_keys() {
        let layers = nonempty![(digest!(1), ArtifactType::Tar)];
        let mut fetcher = TestFetcher::new(
            [(digest!(1), FetcherResult::Pending)],
            [(digest!(1), FetcherResult::Pending)],
            [],
            [],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);
        tracker.got_artifact(&digest!(1), path_buf!("/blob/1"), &mut fetcher);
        tracker.got_bottom_fs_layer(&digest!(1), path_buf!("/fs_b/1"), &mut fetcher);

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_b/1"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                }
            ),
        );
    }

    #[test]
    fn one_layer_manifest_pending_then_got_into_path_and_cache_keys() {
        let layers = nonempty![(digest!(1), ArtifactType::Manifest)];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Pending),
                (digest!(2), FetcherResult::Pending),
            ],
            [(digest!(1), FetcherResult::Pending)],
            [],
            [(digest!(1), path_buf!("/blob/1"))],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);

        tracker.got_artifact(&digest!(1), path_buf!("/blob/1"), &mut fetcher);

        tracker.got_manifest_digests(&digest!(1), hashset! { digest!(2) }, &mut fetcher);
        tracker.got_artifact(&digest!(2), path_buf!("/blob/2"), &mut fetcher);

        tracker.got_bottom_fs_layer(&digest!(1), path_buf!("/fs_b/1"), &mut fetcher);

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_b/1"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::Blob, digest!(2)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                }
            ),
        );
    }

    #[test]
    fn two_layers_everything_in_cache_into_path_and_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
            ],
            [
                (digest!(1), FetcherResult::Got(path_buf!("/fs_b/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
            ],
            [(
                upper_digest!(1, 2),
                FetcherResult::Got(path_buf!("/fs_u/2")),
            )],
            [],
        );
        let tracker = LayerTracker::new(&layers, &mut fetcher);

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_u/2"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::Blob, digest!(2)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                    Key::new(EntryKind::BottomFsLayer, digest!(2)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(1, 2)),
                }
            ),
        );
    }

    #[test]
    fn two_layers_one_manifest_everything_in_cache_into_path_and_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Manifest)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
                (digest!(3), FetcherResult::Got(path_buf!("/blob/3"))),
                (digest!(4), FetcherResult::Got(path_buf!("/blob/4"))),
            ],
            [
                (digest!(1), FetcherResult::Got(path_buf!("/fs_b/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
            ],
            [(
                upper_digest!(1, 2),
                FetcherResult::Got(path_buf!("/fs_u/2")),
            )],
            [(digest!(1), path_buf!("/blob/1"))],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);

        tracker.got_manifest_digests(
            &digest!(1),
            hashset! { digest!(3), digest!(4) },
            &mut fetcher,
        );

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_u/2"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::Blob, digest!(2)),
                    Key::new(EntryKind::Blob, digest!(3)),
                    Key::new(EntryKind::Blob, digest!(4)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                    Key::new(EntryKind::BottomFsLayer, digest!(2)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(1, 2)),
                }
            ),
        );
    }

    #[test]
    fn two_layers_one_artifact_gotten_one_pending_into_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Pending),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
            ],
            [(digest!(2), FetcherResult::Got(path_buf!("/fs_b/2")))],
            [],
            [],
        );
        let tracker = LayerTracker::new(&layers, &mut fetcher);

        assert!(!tracker.is_complete());
        assert_eq!(
            tracker.into_cache_keys(),
            hashset! {
                Key::new(EntryKind::Blob, digest!(2)),
                Key::new(EntryKind::BottomFsLayer, digest!(2)),
            }
        );
    }

    #[test]
    fn two_layers_one_artifact_gotten_with_pending_manifest_one_pending_into_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Manifest),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Pending),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
                (digest!(3), FetcherResult::Got(path_buf!("/blob/3"))),
                (digest!(4), FetcherResult::Pending),
            ],
            [],
            [],
            [(digest!(2), path_buf!("/blob/2"))],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);

        tracker.got_manifest_digests(
            &digest!(2),
            hashset! { digest!(3), digest!(4) },
            &mut fetcher,
        );

        assert!(!tracker.is_complete());
        assert_eq!(
            tracker.into_cache_keys(),
            hashset! {
                Key::new(EntryKind::Blob, digest!(2)),
                Key::new(EntryKind::Blob, digest!(3)),
            }
        );
    }

    #[test]
    fn two_layers_one_artifact_gotten_one_pending_then_got_into_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Pending),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
            ],
            [
                (digest!(1), FetcherResult::Got(path_buf!("/fs_b/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
            ],
            [(upper_digest!(1, 2), FetcherResult::Pending)],
            [],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);
        tracker.got_artifact(&digest!(1), path_buf!("/blob/1"), &mut fetcher);

        assert!(!tracker.is_complete());
        assert_eq!(
            tracker.into_cache_keys(),
            hashset! {
                Key::new(EntryKind::Blob, digest!(1)),
                Key::new(EntryKind::Blob, digest!(2)),
                Key::new(EntryKind::BottomFsLayer, digest!(1)),
                Key::new(EntryKind::BottomFsLayer, digest!(2)),
            }
        );
    }

    #[test]
    fn two_layers_one_artifact_gotten_one_manifest_pending_then_got_into_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Manifest),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Pending),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
                (digest!(3), FetcherResult::Got(path_buf!("/blob/3"))),
                (digest!(4), FetcherResult::Pending),
            ],
            [
                (digest!(1), FetcherResult::Got(path_buf!("/fs_b/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
            ],
            [(upper_digest!(1, 2), FetcherResult::Pending)],
            [(digest!(2), path_buf!("/blob/2"))],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);

        tracker.got_manifest_digests(
            &digest!(2),
            hashset! { digest!(3), digest!(4) },
            &mut fetcher,
        );
        tracker.got_artifact(&digest!(1), path_buf!("/blob/1"), &mut fetcher);
        tracker.got_artifact(&digest!(4), path_buf!("/blob/4"), &mut fetcher);

        assert!(!tracker.is_complete());
        assert_eq!(
            tracker.into_cache_keys(),
            hashset! {
                Key::new(EntryKind::Blob, digest!(1)),
                Key::new(EntryKind::Blob, digest!(2)),
                Key::new(EntryKind::Blob, digest!(3)),
                Key::new(EntryKind::Blob, digest!(4)),
                Key::new(EntryKind::BottomFsLayer, digest!(1)),
                Key::new(EntryKind::BottomFsLayer, digest!(2)),
            }
        );
    }

    #[test]
    fn two_layers_one_bottom_layer_gotten_one_pending_into_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
            ],
            [
                (digest!(1), FetcherResult::Pending),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
            ],
            [],
            [],
        );
        let tracker = LayerTracker::new(&layers, &mut fetcher);

        assert!(!tracker.is_complete());
        assert_eq!(
            tracker.into_cache_keys(),
            hashset! {
                Key::new(EntryKind::Blob, digest!(1)),
                Key::new(EntryKind::Blob, digest!(2)),
                Key::new(EntryKind::BottomFsLayer, digest!(2)),
            }
        );
    }

    #[test]
    fn two_layers_one_bottom_layer_gotten_one_pending_then_gotten_into_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
            ],
            [
                (digest!(1), FetcherResult::Pending),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
            ],
            [(upper_digest!(1, 2), FetcherResult::Pending)],
            [],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);
        tracker.got_bottom_fs_layer(&digest!(1), path_buf!("/fs_b/1"), &mut fetcher);

        assert!(!tracker.is_complete());
        assert_eq!(
            tracker.into_cache_keys(),
            hashset! {
                Key::new(EntryKind::Blob, digest!(1)),
                Key::new(EntryKind::Blob, digest!(2)),
                Key::new(EntryKind::BottomFsLayer, digest!(1)),
                Key::new(EntryKind::BottomFsLayer, digest!(2)),
            }
        );
    }

    #[test]
    fn two_layers_bottom_layers_in_cache_upper_layer_pending_into_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
            ],
            [
                (digest!(1), FetcherResult::Got(path_buf!("/fs_b/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
            ],
            [(upper_digest!(1, 2), FetcherResult::Pending)],
            [],
        );
        let tracker = LayerTracker::new(&layers, &mut fetcher);

        assert!(!tracker.is_complete());
        assert_eq!(
            tracker.into_cache_keys(),
            hashset! {
                Key::new(EntryKind::Blob, digest!(1)),
                Key::new(EntryKind::Blob, digest!(2)),
                Key::new(EntryKind::BottomFsLayer, digest!(1)),
                Key::new(EntryKind::BottomFsLayer, digest!(2)),
            }
        );
    }

    #[test]
    fn two_layers_bottom_layers_in_cache_upper_layer_pending_then_gotten_into_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
            ],
            [
                (digest!(1), FetcherResult::Got(path_buf!("/fs_b/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
            ],
            [(upper_digest!(1, 2), FetcherResult::Pending)],
            [],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);
        tracker.got_upper_fs_layer(&upper_digest!(1, 2), path_buf!("/fs_u/2"), &mut fetcher);

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_cache_keys(),
            hashset! {
                Key::new(EntryKind::Blob, digest!(1)),
                Key::new(EntryKind::Blob, digest!(2)),
                Key::new(EntryKind::BottomFsLayer, digest!(1)),
                Key::new(EntryKind::BottomFsLayer, digest!(2)),
                Key::new(EntryKind::UpperFsLayer, upper_digest!(1, 2)),
            }
        );
    }

    #[test]
    fn two_layers_everything_pending_then_gotten_into_path_and_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Pending),
                (digest!(2), FetcherResult::Pending),
            ],
            [
                (digest!(1), FetcherResult::Pending),
                (digest!(2), FetcherResult::Pending),
            ],
            [(upper_digest!(1, 2), FetcherResult::Pending)],
            [],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);
        tracker.got_artifact(&digest!(2), path_buf!("/blob/2"), &mut fetcher);
        tracker.got_bottom_fs_layer(&digest!(2), path_buf!("/fs_b/2"), &mut fetcher);

        tracker.got_artifact(&digest!(1), path_buf!("/blob/1"), &mut fetcher);
        tracker.got_bottom_fs_layer(&digest!(1), path_buf!("/fs_b/1"), &mut fetcher);

        tracker.got_upper_fs_layer(&upper_digest!(1, 2), path_buf!("/fs_u/2"), &mut fetcher);

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_u/2"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::Blob, digest!(2)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                    Key::new(EntryKind::BottomFsLayer, digest!(2)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(1, 2)),
                }
            ),
        );
    }

    #[test]
    fn three_layers_everything_in_cache_into_path_and_cache_keys() {
        let layers = nonempty![
            (digest!(3), ArtifactType::Tar),
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
                (digest!(3), FetcherResult::Got(path_buf!("/blob/3"))),
            ],
            [
                (digest!(1), FetcherResult::Got(path_buf!("/fs_b/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
                (digest!(3), FetcherResult::Got(path_buf!("/fs_b/3"))),
            ],
            [
                (
                    upper_digest!(2, 3),
                    FetcherResult::Got(path_buf!("/fs_u/2")),
                ),
                (
                    upper_digest!(1, 2, 3),
                    FetcherResult::Got(path_buf!("/fs_u/3")),
                ),
            ],
            [],
        );
        let tracker = LayerTracker::new(&layers, &mut fetcher);

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_u/3"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::Blob, digest!(2)),
                    Key::new(EntryKind::Blob, digest!(3)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                    Key::new(EntryKind::BottomFsLayer, digest!(2)),
                    Key::new(EntryKind::BottomFsLayer, digest!(3)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(2, 3)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(1, 2, 3)),
                }
            ),
        );
    }

    #[test]
    fn three_manifest_layers_with_duplicates() {
        let layers = nonempty![
            (digest!(3), ArtifactType::Manifest),
            (digest!(2), ArtifactType::Manifest),
            (digest!(1), ArtifactType::Manifest)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
                (digest!(3), FetcherResult::Got(path_buf!("/blob/3"))),
                (digest!(4), FetcherResult::Got(path_buf!("/blob/4"))),
                (digest!(5), FetcherResult::Got(path_buf!("/blob/5"))),
            ],
            [
                (digest!(1), FetcherResult::Got(path_buf!("/fs_b/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
                (digest!(3), FetcherResult::Got(path_buf!("/fs_b/3"))),
            ],
            [
                (
                    upper_digest!(2, 3),
                    FetcherResult::Got(path_buf!("/fs_u/2")),
                ),
                (
                    upper_digest!(1, 2, 3),
                    FetcherResult::Got(path_buf!("/fs_u/3")),
                ),
            ],
            [
                (digest!(1), path_buf!("/blob/1")),
                (digest!(2), path_buf!("/blob/2")),
                (digest!(3), path_buf!("/blob/3")),
            ],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);

        tracker.got_manifest_digests(
            &digest!(1),
            hashset! { digest!(4), digest!(5) },
            &mut fetcher,
        );
        tracker.got_manifest_digests(
            &digest!(2),
            hashset! { digest!(4), digest!(5) },
            &mut fetcher,
        );
        tracker.got_manifest_digests(
            &digest!(3),
            hashset! { digest!(4), digest!(5) },
            &mut fetcher,
        );

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_u/3"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::Blob, digest!(2)),
                    Key::new(EntryKind::Blob, digest!(3)),
                    Key::new(EntryKind::Blob, digest!(4)),
                    Key::new(EntryKind::Blob, digest!(5)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                    Key::new(EntryKind::BottomFsLayer, digest!(2)),
                    Key::new(EntryKind::BottomFsLayer, digest!(3)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(2, 3)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(1, 2, 3)),
                }
            ),
        );
    }

    #[test]
    fn six_layers_with_duplicates_three_pending_then_got_into_path_and_cache_keys() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar),
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar),
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Pending),
            ],
            [
                (digest!(1), FetcherResult::Got(path_buf!("/fs_b/1"))),
                (digest!(2), FetcherResult::Pending),
            ],
            [
                (
                    upper_digest!(1, 2),
                    FetcherResult::Got(path_buf!("/fs_u/2")),
                ),
                (
                    upper_digest!(2, 1, 2),
                    FetcherResult::Got(path_buf!("/fs_u/3")),
                ),
                (
                    upper_digest!(1, 2, 1, 2),
                    FetcherResult::Got(path_buf!("/fs_u/4")),
                ),
                (
                    upper_digest!(2, 1, 2, 1, 2),
                    FetcherResult::Got(path_buf!("/fs_u/5")),
                ),
                (
                    upper_digest!(1, 2, 1, 2, 1, 2),
                    FetcherResult::Got(path_buf!("/fs_u/6")),
                ),
            ],
            [],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);
        tracker.got_artifact(&digest!(2), path_buf!("/blob/2"), &mut fetcher);
        tracker.got_bottom_fs_layer(&digest!(2), path_buf!("/fs_b/2"), &mut fetcher);

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_u/6"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::Blob, digest!(2)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                    Key::new(EntryKind::BottomFsLayer, digest!(2)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(1, 2, 1, 2, 1, 2)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(2, 1, 2, 1, 2)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(1, 2, 1, 2)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(2, 1, 2)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(1, 2)),
                }
            ),
        );
    }

    #[test]
    fn manifest_depends_on_existing_layer_fetch_manifest_dependency_after_layer() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Manifest)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/blob/2"))),
            ],
            [
                (digest!(1), FetcherResult::Got(path_buf!("/fs_b/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
            ],
            [(
                upper_digest!(1, 2),
                FetcherResult::Got(path_buf!("/fs_u/3")),
            )],
            [(digest!(1), path_buf!("/blob/1"))],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);

        tracker.got_manifest_digests(&digest!(1), hashset! { digest!(2) }, &mut fetcher);

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_u/3"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::Blob, digest!(2)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                    Key::new(EntryKind::BottomFsLayer, digest!(2)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(1, 2)),
                }
            ),
        );
    }

    #[test]
    fn manifest_depends_on_existing_layer_fetch_manifest_dependency_before_layer() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Manifest)
        ];
        let mut fetcher = TestFetcher::new(
            [
                (digest!(1), FetcherResult::Got(path_buf!("/blob/1"))),
                (digest!(2), FetcherResult::Pending),
            ],
            [
                (digest!(1), FetcherResult::Got(path_buf!("/fs_b/1"))),
                (digest!(2), FetcherResult::Got(path_buf!("/fs_b/2"))),
            ],
            [(
                upper_digest!(1, 2),
                FetcherResult::Got(path_buf!("/fs_u/3")),
            )],
            [(digest!(1), path_buf!("/blob/1"))],
        );
        let mut tracker = LayerTracker::new(&layers, &mut fetcher);

        tracker.got_manifest_digests(&digest!(1), hashset! { digest!(2) }, &mut fetcher);
        tracker.got_artifact(&digest!(2), path_buf!("/blob/2"), &mut fetcher);

        assert!(tracker.is_complete(), "{tracker:#?}");
        assert_eq!(
            tracker.into_path_and_cache_keys(),
            (
                path_buf!("/fs_u/3"),
                hashset! {
                    Key::new(EntryKind::Blob, digest!(1)),
                    Key::new(EntryKind::Blob, digest!(2)),
                    Key::new(EntryKind::BottomFsLayer, digest!(1)),
                    Key::new(EntryKind::BottomFsLayer, digest!(2)),
                    Key::new(EntryKind::UpperFsLayer, upper_digest!(1, 2)),
                }
            ),
        );
    }
}
