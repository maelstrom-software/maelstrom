use maelstrom_base::{ArtifactType, NonEmpty, Sha256Digest};
use maelstrom_util::ext::{BoolExt as _, OptionExt as _};
use std::{
    collections::{HashMap, HashSet},
    iter,
    path::PathBuf,
};

struct DuplicateEntry {
    from: usize,
    to: usize,
}

/// Track which layers have been gotten from the cache.
pub struct LayerTracker {
    pending: HashMap<Sha256Digest, usize>,
    paths: NonEmpty<PathBuf>,
    duplicates: Vec<DuplicateEntry>,
    gotten: HashSet<Sha256Digest>,
}

pub enum FetcherResult {
    Got(PathBuf),
    Pending,
}

impl LayerTracker {
    pub fn new(
        layers: &NonEmpty<(Sha256Digest, ArtifactType)>,
        mut fetcher: impl FnMut(&Sha256Digest, ArtifactType) -> FetcherResult,
    ) -> Self {
        let mut tracker = LayerTracker {
            pending: HashMap::new(),
            paths: NonEmpty::collect(iter::repeat(PathBuf::new()).take(layers.len())).unwrap(),
            duplicates: vec![],
            gotten: HashSet::new(),
        };
        let mut fetched = HashMap::<Sha256Digest, usize>::new();
        for (idx, (digest, type_)) in layers.iter().enumerate() {
            match fetched.get(digest) {
                Some(from_idx) => {
                    tracker.duplicates.push(DuplicateEntry {
                        from: *from_idx,
                        to: idx,
                    });
                }
                None => {
                    match fetcher(digest, *type_) {
                        FetcherResult::Got(path) => {
                            tracker.paths[idx] = path;
                            tracker.gotten.insert(digest.clone()).assert_is_true();
                        }
                        FetcherResult::Pending => {
                            tracker.pending.insert(digest.clone(), idx);
                        }
                    }
                    fetched.insert(digest.clone(), idx).assert_is_none();
                }
            }
        }
        tracker
    }

    pub fn got(&mut self, digest: &Sha256Digest, path: PathBuf) {
        self.gotten.insert(digest.clone()).assert_is_true();
        self.paths[self.pending.remove(digest).unwrap()] = path;
    }

    pub fn is_complete(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn into_digests(self) -> HashSet<Sha256Digest> {
        self.gotten
    }

    pub fn into_paths_and_digests(mut self) -> (NonEmpty<PathBuf>, HashSet<Sha256Digest>) {
        assert!(self.is_complete());
        for DuplicateEntry { from, to } in self.duplicates {
            self.paths[to] = self.paths[from].clone();
        }
        (self.paths, self.gotten)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maelstrom_base::nonempty;
    use maelstrom_test::{digest, digest_hash_set, path_buf, path_buf_nonempty};

    struct Fetcher {
        map: HashMap<Sha256Digest, FetcherResult>,
    }

    impl Fetcher {
        fn new(iter: impl IntoIterator<Item = (Sha256Digest, FetcherResult)>) -> Self {
            Fetcher {
                map: HashMap::from_iter(iter),
            }
        }

        fn fetch(&mut self, digest: &Sha256Digest, _: ArtifactType) -> FetcherResult {
            self.map.remove(digest).unwrap()
        }
    }

    #[test]
    fn two_elements_both_gotten_into_paths_and_digests() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = Fetcher::new([
            (digest!(1), FetcherResult::Got(path_buf!("/1"))),
            (digest!(2), FetcherResult::Got(path_buf!("/2"))),
        ]);
        let tracker = LayerTracker::new(&layers, |digest, type_| fetcher.fetch(digest, type_));

        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_paths_and_digests(),
            (path_buf_nonempty!["/2", "/1"], digest_hash_set! {1, 2}),
        );
    }

    #[test]
    fn two_elements_one_gotten_one_pending_into_digests() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = Fetcher::new([
            (digest!(1), FetcherResult::Got(path_buf!("/1"))),
            (digest!(2), FetcherResult::Pending),
        ]);
        let tracker = LayerTracker::new(&layers, |digest, type_| fetcher.fetch(digest, type_));

        assert!(!tracker.is_complete());
        assert_eq!(tracker.into_digests(), digest_hash_set! {1});
    }

    #[test]
    fn two_elements_one_gotten_one_pending_then_got_into_paths_and_digests() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = Fetcher::new([
            (digest!(1), FetcherResult::Got(path_buf!("/1"))),
            (digest!(2), FetcherResult::Pending),
        ]);
        let mut tracker = LayerTracker::new(&layers, |digest, type_| fetcher.fetch(digest, type_));

        assert!(!tracker.is_complete());

        tracker.got(&digest!(2), path_buf!("/2"));
        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_paths_and_digests(),
            (path_buf_nonempty!["/2", "/1"], digest_hash_set! {1, 2}),
        );
    }

    #[test]
    fn two_elements_both_pending_into_digests() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = Fetcher::new([
            (digest!(1), FetcherResult::Pending),
            (digest!(2), FetcherResult::Pending),
        ]);
        let tracker = LayerTracker::new(&layers, |digest, type_| fetcher.fetch(digest, type_));

        assert!(!tracker.is_complete());
        assert_eq!(tracker.into_digests(), digest_hash_set! {});
    }

    #[test]
    fn two_elements_both_pending_then_got_then_got_into_paths_and_digests() {
        let layers = nonempty![
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar)
        ];
        let mut fetcher = Fetcher::new([
            (digest!(1), FetcherResult::Pending),
            (digest!(2), FetcherResult::Pending),
        ]);
        let mut tracker = LayerTracker::new(&layers, |digest, type_| fetcher.fetch(digest, type_));

        assert!(!tracker.is_complete());
        tracker.got(&digest!(2), path_buf!("/2"));
        assert!(!tracker.is_complete());
        tracker.got(&digest!(1), path_buf!("/1"));
        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_paths_and_digests(),
            (path_buf_nonempty!["/2", "/1"], digest_hash_set! {1, 2}),
        );
    }

    #[test]
    fn six_elements_three_gotten_three_pending_then_got_into_paths_and_digests() {
        let layers = nonempty![
            (digest!(1), ArtifactType::Tar),
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar),
            (digest!(2), ArtifactType::Tar),
            (digest!(1), ArtifactType::Tar),
            (digest!(2), ArtifactType::Tar)
        ];
        let mut fetcher = Fetcher::new([
            (digest!(1), FetcherResult::Got(path_buf!("/1"))),
            (digest!(2), FetcherResult::Pending),
        ]);
        let mut tracker = LayerTracker::new(&layers, |digest, type_| fetcher.fetch(digest, type_));

        assert!(!tracker.is_complete());
        tracker.got(&digest!(2), path_buf!("/2"));
        assert!(tracker.is_complete());
        assert_eq!(
            tracker.into_paths_and_digests(),
            (
                path_buf_nonempty!["/1", "/2", "/1", "/2", "/1", "/2"],
                digest_hash_set! {1, 2}
            ),
        );
    }
}
