//! Manage downloading, extracting, and storing of image files specified by jobs.

use meticulous_base::{JobId, Sha256Digest};
use meticulous_util::heap::{Heap, HeapDeps, HeapIndex};
use std::{
    collections::{hash_map, HashMap},
    num::NonZeroU32,
    path::{Path, PathBuf},
};

/*              _     _ _
 *  _ __  _   _| |__ | (_) ___
 * | '_ \| | | | '_ \| | |/ __|
 * | |_) | |_| | |_) | | | (__
 * | .__/ \__,_|_.__/|_|_|\___|
 * |_|
 *  FIGLET: public
 */

/// Dependencies that [Cache] has on the file system.
pub trait CacheFs {
    /// Return a random u64. This is used for creating unique path names in the directory removal
    /// code path.
    fn rand_u64(&mut self) -> u64;

    /// Return true if a file (or directory, or symlink, etc.) exists with the given path, and
    /// false otherwise. Panic on file system error.
    fn file_exists(&mut self, path: &Path) -> bool;

    /// Rename `source` to `destination`. Panic on file system error. Assume that all intermediate
    /// directories exist for `destination`, and that `source` and `destination` are on the same
    /// file system.
    fn rename(&mut self, source: &Path, destination: &Path);

    /// Remove `path`, and if `path` is a directory, all descendants of `path`. Do this on a
    /// separate thread. Panic on file system error.
    fn remove_recursively_on_thread(&mut self, path: PathBuf);

    /// Ensure `path` exists and is a directory. If it doesn't exist, recusively ensure its parent exists,
    /// then create it. Panic on file system error or if `path` or any of its ancestors aren't
    /// directories.
    fn mkdir_recursively(&mut self, path: &Path);

    /// Return and iterator that will yield all of the children of a directory. Panic on file
    /// system error or if `path` doesn't exist or isn't a directory.
    fn read_dir(&mut self, path: &Path) -> Box<dyn Iterator<Item = PathBuf>>;
}

/// The standard implementation of CacheFs that uses [std] and [rand].
pub struct StdCacheFs;

impl CacheFs for StdCacheFs {
    fn rand_u64(&mut self) -> u64 {
        rand::random()
    }

    fn file_exists(&mut self, path: &Path) -> bool {
        path.try_exists().unwrap()
    }

    fn rename(&mut self, source: &Path, destination: &Path) {
        std::fs::rename(source, destination).unwrap()
    }

    fn remove_recursively_on_thread(&mut self, path: PathBuf) {
        std::thread::spawn(move || std::fs::remove_dir_all(path).unwrap());
    }

    fn mkdir_recursively(&mut self, path: &Path) {
        std::fs::create_dir_all(path).unwrap();
    }

    fn read_dir(&mut self, path: &Path) -> Box<dyn Iterator<Item = PathBuf>> {
        Box::new(
            std::fs::read_dir(path)
                .unwrap()
                .map(|de| de.unwrap().path()),
        )
    }
}

/// Manage a directory of downloaded, extracted images. Coordinate fetching of these images, and
/// removing them when they are no longer in use and the amount of space used by the directory has
/// grown too large.
pub struct Cache<FsT> {
    fs: FsT,
    root: PathBuf,
    entries: CacheMap,
    heap: Heap<CacheMap>,
    next_priority: u64,
    bytes_used: u64,
    bytes_used_goal: u64,
}

impl<FsT: CacheFs> Cache<FsT> {
    /// Create a new [Cache] rooted at `root`. The directory `root` and all necessary ancestors
    /// will be created, along with `{root}/removing` and `{root}/sha256`. Any pre-existing entries
    /// in `{root}/removing` and `{root}/sha256` will be removed. That implies that the [Cache]
    /// doesn't currently keep data stored across invocations.
    ///
    /// `bytes_used_goal` is the goal on-disk size for the cache. The cache will periodically grow
    /// larger than this size, but then shrink back down to this size. Ideally, the cache would use
    /// this as a hard upper bound, but that's not how it currently works.
    pub fn new(root: &Path, mut fs: FsT, bytes_used_goal: u64) -> Self {
        let mut path = root.to_owned();

        path.push("removing");
        fs.mkdir_recursively(&path);
        for child in fs.read_dir(&path) {
            fs.remove_recursively_on_thread(child);
        }
        path.pop();

        path.push("sha256");
        if fs.file_exists(&path) {
            Self::remove_in_background(&mut fs, root, &path);
        }
        fs.mkdir_recursively(&path);
        path.pop();

        Cache {
            fs,
            root: root.to_owned(),
            entries: CacheMap(HashMap::default()),
            heap: Heap::default(),
            next_priority: 0,
            bytes_used: 0,
            bytes_used_goal,
        }
    }
}

/*             _            _
 *  _ __  _ __(_)_   ____ _| |_ ___
 * | '_ \| '__| \ \ / / _` | __/ _ \
 * | |_) | |  | |\ V / (_| | ||  __/
 * | .__/|_|  |_| \_/ \__,_|\__\___|
 * |_|
 *  FIGLET: private
 */

/// An entry for a specific [Sha256Digest] in the [Cache]'s hash table. There is one of these for
/// every subdirectory in the `sha256` subdirectory of the [Cache]'s root directory.
enum CacheEntry {
    /// The artifact is being downloaded, extracted, and having its checksum validated. There is
    /// probably a subdirectory for this [Sha256Digest], but there might not yet be one, depending
    /// on where the extraction process is.
    DownloadingAndExtracting(Vec<JobId>),

    /// The artifact has been successfully downloaded and extracted, and the subdirectory is
    /// currently being used by at least one job. We refcount this state since there may be
    /// multiple jobs that use the same artifact.
    InUse {
        bytes_used: u64,
        refcount: NonZeroU32,
    },

    /// The artifact has been successfully downloaded and extracted, but no jobs are
    /// currently using it. The `priority` is provided by [Cache] and is used by the [Heap] to
    /// determine which entry should be removed first when freeing up space.
    InHeap {
        bytes_used: u64,
        priority: u64,
        heap_index: HeapIndex,
    },
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum GetArtifact {
    Success(PathBuf),
    Wait,
    Get(PathBuf),
}

impl<FsT: CacheFs> Cache<FsT> {
    fn remove_in_background(fs: &mut impl CacheFs, root: &Path, source: &Path) {
        let mut target = root.to_owned();
        target.push("removing");
        loop {
            let key = fs.rand_u64();
            target.push(format!("{key:016x}"));
            if !fs.file_exists(&target) {
                break;
            } else {
                target.pop();
            }
        }
        fs.rename(source, &target);
        fs.remove_recursively_on_thread(target);
    }

    fn cache_path(root: &Path, digest: &Sha256Digest) -> PathBuf {
        let mut path = root.to_owned();
        path.push("sha256");
        path.push(digest.to_string());
        path
    }

    pub fn get_artifact(&mut self, request_id: JobId, artifact: Sha256Digest) -> GetArtifact {
        let cache_path = Self::cache_path(&self.root, &artifact);
        match self.entries.0.entry(artifact) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(CacheEntry::DownloadingAndExtracting(vec![request_id]));
                GetArtifact::Get(cache_path)
            }
            hash_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    CacheEntry::DownloadingAndExtracting(requests) => {
                        requests.push(request_id);
                        GetArtifact::Wait
                    }
                    CacheEntry::InUse { refcount, .. } => {
                        *refcount = refcount.checked_add(1).unwrap();
                        GetArtifact::Success(cache_path)
                    }
                    CacheEntry::InHeap {
                        bytes_used,
                        heap_index,
                        ..
                    } => {
                        let heap_index = *heap_index;
                        *entry = CacheEntry::InUse {
                            refcount: NonZeroU32::new(1).unwrap(),
                            bytes_used: *bytes_used,
                        };
                        self.heap.remove(&mut self.entries, heap_index);
                        GetArtifact::Success(cache_path)
                    }
                }
            }
        }
    }

    pub fn got_artifact_failure(&mut self, digest: Sha256Digest) -> Vec<JobId> {
        let Some(CacheEntry::DownloadingAndExtracting(requests)) = self.entries.0.remove(&digest)
        else {
            panic!("Got got_artifact in unexpected state");
        };
        let cache_path = Self::cache_path(&self.root, &digest);
        if self.fs.file_exists(&cache_path) {
            Self::remove_in_background(&mut self.fs, &self.root, &cache_path);
        }
        requests
    }

    fn possibly_remove_some(&mut self) {
        while self.bytes_used > self.bytes_used_goal {
            let Some(digest) = self.heap.pop(&mut self.entries) else {
                break;
            };
            let Some(CacheEntry::InHeap { bytes_used, .. }) = self.entries.0.remove(&digest) else {
                panic!("Entry popped off of heap was in unexpected state");
            };
            Self::remove_in_background(
                &mut self.fs,
                &self.root,
                &Self::cache_path(&self.root, &digest),
            );
            self.bytes_used = self.bytes_used.checked_sub(bytes_used).unwrap();
        }
    }

    pub fn got_artifact_success(
        &mut self,
        digest: Sha256Digest,
        bytes_used: u64,
    ) -> Vec<(JobId, PathBuf)> {
        let entry = self
            .entries
            .0
            .get_mut(&digest)
            .expect("Got DownloadingAndExtracting in unexpected state");
        let CacheEntry::DownloadingAndExtracting(requests) = entry else {
            panic!("Got DownloadingAndExtracting in unexpected state");
        };
        let refcount = requests.len().try_into().unwrap();
        let result: Vec<_> = requests
            .drain(..)
            .map(|jid| (jid, Self::cache_path(&self.root, &digest)))
            .collect();
        // Refcount must be > 0 since we don't allow cancellation of gets.
        *entry = CacheEntry::InUse {
            bytes_used,
            refcount: NonZeroU32::new(refcount).unwrap(),
        };
        self.bytes_used = self.bytes_used.checked_add(bytes_used).unwrap();
        self.possibly_remove_some();
        result
    }

    pub fn decrement_refcount(&mut self, digest: Sha256Digest) {
        let entry = self
            .entries
            .0
            .get_mut(&digest)
            .expect("Got DecrementRefcount in unexpected state");
        let CacheEntry::InUse {
            bytes_used,
            refcount,
        } = entry
        else {
            panic!("Got DecrementRefcount with existing zero refcount");
        };
        match NonZeroU32::new(refcount.get() - 1) {
            Some(new_refcount) => *refcount = new_refcount,
            None => {
                *entry = CacheEntry::InHeap {
                    bytes_used: *bytes_used,
                    priority: self.next_priority,
                    heap_index: HeapIndex::default(),
                };
                self.heap.push(&mut self.entries, digest);
                self.next_priority = self.next_priority.checked_add(1).unwrap();
                self.possibly_remove_some();
            }
        }
    }
}

struct CacheMap(HashMap<Sha256Digest, CacheEntry>);

impl HeapDeps for CacheMap {
    type Element = Sha256Digest;

    fn is_element_less_than(&self, lhs: &Self::Element, rhs: &Self::Element) -> bool {
        let lhs_priority = match self.0.get(lhs) {
            Some(CacheEntry::InHeap { priority, .. }) => *priority,
            _ => panic!("Element should be in heap"),
        };
        let rhs_priority = match self.0.get(rhs) {
            Some(CacheEntry::InHeap { priority, .. }) => *priority,
            _ => panic!("Element should be in heap"),
        };
        lhs_priority.cmp(&rhs_priority) == std::cmp::Ordering::Less
    }

    fn update_index(&mut self, elem: &Self::Element, idx: HeapIndex) {
        match self.0.get_mut(elem) {
            Some(CacheEntry::InHeap { heap_index, .. }) => *heap_index = idx,
            _ => panic!("Element should be in heap"),
        };
    }
}

/*  _            _
 * | |_ ___  ___| |_ ___
 * | __/ _ \/ __| __/ __|
 * | ||  __/\__ \ |_\__ \
 *  \__\___||___/\__|___/
 *  FIGLET: tests
 */

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use meticulous_test::*;
    use std::{cell::RefCell, collections::HashSet, rc::Rc};
    use TestMessage::*;

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        FileExists(PathBuf),
        Rename(PathBuf, PathBuf),
        RemoveRecursively(PathBuf),
        MkdirRecursively(PathBuf),
        ReadDir(PathBuf),
    }

    #[derive(Default)]
    struct TestCacheFs {
        messages: Rc<RefCell<Vec<TestMessage>>>,
        existing_files: HashSet<PathBuf>,
        directories: HashMap<PathBuf, Vec<PathBuf>>,
        last_random_number: u64,
    }

    impl CacheFs for TestCacheFs {
        fn rand_u64(&mut self) -> u64 {
            self.last_random_number += 1;
            self.last_random_number
        }

        fn file_exists(&mut self, path: &Path) -> bool {
            self.messages.borrow_mut().push(FileExists(path.to_owned()));
            self.existing_files.contains(path)
        }

        fn rename(&mut self, source: &Path, destination: &Path) {
            self.messages
                .borrow_mut()
                .push(Rename(source.to_owned(), destination.to_owned()));
        }

        fn remove_recursively_on_thread(&mut self, path: PathBuf) {
            self.messages
                .borrow_mut()
                .push(RemoveRecursively(path.to_owned()));
        }

        fn mkdir_recursively(&mut self, path: &Path) {
            self.messages
                .borrow_mut()
                .push(MkdirRecursively(path.to_owned()));
        }

        fn read_dir(&mut self, path: &Path) -> Box<dyn Iterator<Item = PathBuf>> {
            self.messages.borrow_mut().push(ReadDir(path.to_owned()));
            Box::new(
                self.directories
                    .get(path)
                    .unwrap_or(&vec![])
                    .clone()
                    .into_iter(),
            )
        }
    }

    struct Fixture {
        messages: Rc<RefCell<Vec<TestMessage>>>,
        cache: Cache<TestCacheFs>,
    }

    impl Fixture {
        fn new_with_fs_and_clear_messages(
            test_cache_fs: TestCacheFs,
            bytes_used_goal: u64,
        ) -> Self {
            let mut fixture = Fixture::new(test_cache_fs, bytes_used_goal);
            fixture.clear_messages();
            fixture
        }

        fn new_and_clear_messages(bytes_used_goal: u64) -> Self {
            Self::new_with_fs_and_clear_messages(TestCacheFs::default(), bytes_used_goal)
        }

        fn new(test_cache_fs: TestCacheFs, bytes_used_goal: u64) -> Self {
            let messages = test_cache_fs.messages.clone();
            let cache = Cache::new(Path::new("/cache/root"), test_cache_fs, bytes_used_goal);
            Fixture { messages, cache }
        }

        fn expect_messages_in_any_order(&mut self, expected: Vec<TestMessage>) {
            let mut messages = self.messages.borrow_mut();
            for perm in expected.clone().into_iter().permutations(expected.len()) {
                if perm == *messages {
                    messages.clear();
                    return;
                }
            }
            panic!(
                "Expected messages didn't match actual messages in any order.\n\
                 Expected: {expected:#?}\nActual: {messages:#?}"
            );
        }

        fn expect_messages_in_specific_order(&mut self, expected: Vec<TestMessage>) {
            assert!(
                *self.messages.borrow() == expected,
                "Expected messages didn't match actual messages in specific order.\n\
                 Expected: {:#?}\nActual: {:#?}",
                expected,
                self.messages.borrow()
            );
            self.clear_messages();
        }

        fn clear_messages(&mut self) {
            self.messages.borrow_mut().clear();
        }

        fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest, expected: GetArtifact) {
            let result = self.cache.get_artifact(jid, digest);
            assert_eq!(result, expected);
            self.expect_messages_in_any_order(vec![]);
        }

        fn get_artifact_ign(&mut self, jid: JobId, digest: Sha256Digest) {
            self.cache.get_artifact(jid, digest);
            self.expect_messages_in_any_order(vec![]);
        }

        fn got_artifact_success(
            &mut self,
            digest: Sha256Digest,
            bytes_used: u64,
            expected: Vec<(JobId, PathBuf)>,
            expected_fs_operations: Vec<TestMessage>,
        ) {
            let result = self.cache.got_artifact_success(digest, bytes_used);
            assert_eq!(result, expected);
            self.expect_messages_in_any_order(expected_fs_operations);
        }

        fn got_artifact_failure(
            &mut self,
            digest: Sha256Digest,
            expected: Vec<JobId>,
            expected_fs_operations: Vec<TestMessage>,
        ) {
            let result = self.cache.got_artifact_failure(digest);
            assert_eq!(result, expected);
            self.expect_messages_in_any_order(expected_fs_operations);
        }

        fn got_artifact_success_ign(&mut self, digest: Sha256Digest, bytes_used: u64) {
            self.cache.got_artifact_success(digest, bytes_used);
            self.clear_messages();
        }

        fn decrement_refcount(&mut self, digest: Sha256Digest, expected: Vec<TestMessage>) {
            self.cache.decrement_refcount(digest);
            self.expect_messages_in_any_order(expected);
        }

        fn decrement_refcount_ign(&mut self, digest: Sha256Digest) {
            self.cache.decrement_refcount(digest);
            self.clear_messages();
        }
    }

    #[test]
    fn get_request_for_empty() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact(
            jid!(1),
            digest!(42),
            GetArtifact::Get(long_path!("/cache/root/sha256", 42)),
        );
        fixture.got_artifact_success(
            digest!(42),
            100,
            vec![(jid!(1), long_path!("/cache/root/sha256", 42))],
            vec![],
        );
    }

    #[test]
    fn get_request_for_empty_larger_than_goal_ok_then_removes_on_decrement_refcount() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact_ign(jid!(1), digest!(42));
        fixture.got_artifact_success(
            digest!(42),
            10000,
            vec![(jid!(1), long_path!("/cache/root/sha256", 42))],
            vec![],
        );

        fixture.decrement_refcount(
            digest!(42),
            vec![
                FileExists(short_path!("/cache/root/removing", 1)),
                Rename(
                    long_path!("/cache/root/sha256", 42),
                    short_path!("/cache/root/removing", 1),
                ),
                RemoveRecursively(short_path!("/cache/root/removing", 1)),
            ],
        );
    }

    #[test]
    fn cache_entries_are_removed_in_lru_order() {
        let mut fixture = Fixture::new_and_clear_messages(10);

        fixture.get_artifact_ign(jid!(1), digest!(1));
        fixture.got_artifact_success_ign(digest!(1), 4);
        fixture.decrement_refcount(digest!(1), vec![]);

        fixture.get_artifact_ign(jid!(2), digest!(2));
        fixture.got_artifact_success_ign(digest!(2), 4);
        fixture.decrement_refcount(digest!(2), vec![]);

        fixture.get_artifact_ign(jid!(3), digest!(3));
        fixture.got_artifact_success(
            digest!(3),
            4,
            vec![(jid!(3), long_path!("/cache/root/sha256", 3))],
            vec![
                FileExists(short_path!("/cache/root/removing", 1)),
                Rename(
                    long_path!("/cache/root/sha256", 1),
                    short_path!("/cache/root/removing", 1),
                ),
                RemoveRecursively(short_path!("/cache/root/removing", 1)),
            ],
        );
        fixture.decrement_refcount(digest!(3), vec![]);

        fixture.get_artifact_ign(jid!(4), digest!(4));
        fixture.got_artifact_success(
            digest!(4),
            4,
            vec![(jid!(4), long_path!("/cache/root/sha256", 4))],
            vec![
                FileExists(short_path!("/cache/root/removing", 2)),
                Rename(
                    long_path!("/cache/root/sha256", 2),
                    short_path!("/cache/root/removing", 2),
                ),
                RemoveRecursively(short_path!("/cache/root/removing", 2)),
            ],
        );
        fixture.decrement_refcount(digest!(4), vec![]);
    }

    #[test]
    fn lru_order_augmented_by_last_use() {
        let mut fixture = Fixture::new_and_clear_messages(10);

        fixture.get_artifact_ign(jid!(1), digest!(1));
        fixture.got_artifact_success_ign(digest!(1), 3);

        fixture.get_artifact_ign(jid!(2), digest!(2));
        fixture.got_artifact_success_ign(digest!(2), 3);

        fixture.get_artifact_ign(jid!(3), digest!(3));
        fixture.got_artifact_success_ign(digest!(3), 3);

        fixture.decrement_refcount(digest!(3), vec![]);
        fixture.decrement_refcount(digest!(2), vec![]);
        fixture.decrement_refcount(digest!(1), vec![]);

        fixture.get_artifact_ign(jid!(4), digest!(4));
        fixture.got_artifact_success(
            digest!(4),
            3,
            vec![(jid!(4), long_path!("/cache/root/sha256", 4))],
            vec![
                FileExists(short_path!("/cache/root/removing", 1)),
                Rename(
                    long_path!("/cache/root/sha256", 3),
                    short_path!("/cache/root/removing", 1),
                ),
                RemoveRecursively(short_path!("/cache/root/removing", 1)),
            ],
        );
    }

    #[test]
    fn multiple_get_requests_for_empty() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact_ign(jid!(1), digest!(42));
        fixture.get_artifact(jid!(2), digest!(42), GetArtifact::Wait);
        fixture.get_artifact(jid!(3), digest!(42), GetArtifact::Wait);

        fixture.got_artifact_success(
            digest!(42),
            100,
            vec![
                (jid!(1), long_path!("/cache/root/sha256", 42)),
                (jid!(2), long_path!("/cache/root/sha256", 42)),
                (jid!(3), long_path!("/cache/root/sha256", 42)),
            ],
            vec![],
        );
    }

    #[test]
    fn multiple_get_requests_for_empty_larger_than_goal_remove_on_last_decrement() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact_ign(jid!(1), digest!(42));
        fixture.get_artifact(jid!(2), digest!(42), GetArtifact::Wait);
        fixture.get_artifact(jid!(3), digest!(42), GetArtifact::Wait);

        fixture.got_artifact_success(
            digest!(42),
            10000,
            vec![
                (jid!(1), long_path!("/cache/root/sha256", 42)),
                (jid!(2), long_path!("/cache/root/sha256", 42)),
                (jid!(3), long_path!("/cache/root/sha256", 42)),
            ],
            vec![],
        );

        fixture.decrement_refcount(digest!(42), vec![]);
        fixture.decrement_refcount(digest!(42), vec![]);
        fixture.decrement_refcount(
            digest!(42),
            vec![
                FileExists(short_path!("/cache/root/removing", 1)),
                Rename(
                    long_path!("/cache/root/sha256", 42),
                    short_path!("/cache/root/removing", 1),
                ),
                RemoveRecursively(short_path!("/cache/root/removing", 1)),
            ],
        );
    }

    #[test]
    fn get_request_for_currently_used() {
        let mut fixture = Fixture::new_and_clear_messages(10);

        fixture.get_artifact_ign(jid!(1), digest!(42));
        fixture.got_artifact_success_ign(digest!(42), 100);

        fixture.get_artifact(
            jid!(1),
            digest!(42),
            GetArtifact::Success(long_path!("/cache/root/sha256", 42)),
        );

        fixture.decrement_refcount(digest!(42), vec![]);
        fixture.decrement_refcount(
            digest!(42),
            vec![
                FileExists(short_path!("/cache/root/removing", 1)),
                Rename(
                    long_path!("/cache/root/sha256", 42),
                    short_path!("/cache/root/removing", 1),
                ),
                RemoveRecursively(short_path!("/cache/root/removing", 1)),
            ],
        );
    }

    #[test]
    fn get_request_for_cached_followed_by_big_get_does_not_evict_until_decrement_refcount() {
        let mut fixture = Fixture::new_and_clear_messages(100);

        fixture.get_artifact_ign(jid!(1), digest!(42));
        fixture.got_artifact_success_ign(digest!(42), 10);
        fixture.decrement_refcount_ign(digest!(42));

        fixture.get_artifact(
            jid!(2),
            digest!(42),
            GetArtifact::Success(long_path!("/cache/root/sha256", 42)),
        );
        fixture.get_artifact(
            jid!(3),
            digest!(43),
            GetArtifact::Get(long_path!("/cache/root/sha256", 43)),
        );
        fixture.got_artifact_success(
            digest!(43),
            100,
            vec![(jid!(3), long_path!("/cache/root/sha256", 43))],
            vec![],
        );

        fixture.decrement_refcount(
            digest!(42),
            vec![
                FileExists(short_path!("/cache/root/removing", 1)),
                Rename(
                    long_path!("/cache/root/sha256", 42),
                    short_path!("/cache/root/removing", 1),
                ),
                RemoveRecursively(short_path!("/cache/root/removing", 1)),
            ],
        );
    }

    #[test]
    fn get_request_for_empty_with_download_and_extract_failure_and_no_files_created() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact_ign(jid!(1), digest!(42));
        fixture.got_artifact_failure(
            digest!(42),
            vec![jid!(1)],
            vec![FileExists(long_path!("/cache/root/sha256", 42))],
        );
    }

    #[test]
    fn preexisting_directories_do_not_affect_get_request() {
        let mut test_cache_fs = TestCacheFs::default();
        test_cache_fs
            .existing_files
            .insert(long_path!("/cache/root/sha256", 42));
        let mut fixture = Fixture::new_with_fs_and_clear_messages(test_cache_fs, 1000);

        fixture.get_artifact(
            jid!(1),
            digest!(42),
            GetArtifact::Get(long_path!("/cache/root/sha256", 42)),
        );
    }

    #[test]
    fn get_request_for_empty_with_download_and_extract_failure_and_files_created() {
        let mut test_cache_fs = TestCacheFs::default();
        test_cache_fs
            .existing_files
            .insert(long_path!("/cache/root/sha256", 42));
        let mut fixture = Fixture::new_with_fs_and_clear_messages(test_cache_fs, 1000);

        fixture.get_artifact_ign(jid!(1), digest!(42));

        fixture.got_artifact_failure(
            digest!(42),
            vec![jid!(1)],
            vec![
                FileExists(long_path!("/cache/root/sha256", 42)),
                FileExists(short_path!("/cache/root/removing", 1)),
                Rename(
                    long_path!("/cache/root/sha256", 42),
                    short_path!("/cache/root/removing", 1),
                ),
                RemoveRecursively(short_path!("/cache/root/removing", 1)),
            ],
        );
    }

    #[test]
    fn multiple_get_requests_for_empty_with_download_and_extract_failure() {
        let mut test_cache_fs = TestCacheFs::default();
        test_cache_fs
            .existing_files
            .insert(long_path!("/cache/root/sha256", 42));
        let mut fixture = Fixture::new_with_fs_and_clear_messages(test_cache_fs, 1000);

        fixture.get_artifact_ign(jid!(1), digest!(42));
        fixture.get_artifact_ign(jid!(2), digest!(42));
        fixture.get_artifact_ign(jid!(3), digest!(42));

        fixture.got_artifact_failure(
            digest!(42),
            vec![jid!(1), jid!(2), jid!(3)],
            vec![
                FileExists(long_path!("/cache/root/sha256", 42)),
                FileExists(short_path!("/cache/root/removing", 1)),
                Rename(
                    long_path!("/cache/root/sha256", 42),
                    short_path!("/cache/root/removing", 1),
                ),
                RemoveRecursively(short_path!("/cache/root/removing", 1)),
            ],
        );
    }

    #[test]
    fn get_after_error_retries() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact_ign(jid!(1), digest!(42));

        fixture.got_artifact_failure(
            digest!(42),
            vec![jid!(1)],
            vec![FileExists(long_path!("/cache/root/sha256", 42))],
        );

        fixture.get_artifact(
            jid!(2),
            digest!(42),
            GetArtifact::Get(long_path!("/cache/root/sha256", 42)),
        );
    }

    #[test]
    fn rename_retries_until_unique_path_name() {
        let mut test_cache_fs = TestCacheFs::default();
        test_cache_fs
            .existing_files
            .insert(long_path!("/cache/root/sha256", 42));
        test_cache_fs
            .existing_files
            .insert(short_path!("/cache/root/removing", 1));
        test_cache_fs
            .existing_files
            .insert(short_path!("/cache/root/removing", 2));
        test_cache_fs
            .existing_files
            .insert(short_path!("/cache/root/removing", 3));
        let mut fixture = Fixture::new_with_fs_and_clear_messages(test_cache_fs, 1000);

        fixture.get_artifact_ign(jid!(1), digest!(42));

        fixture.got_artifact_failure(
            digest!(42),
            vec![jid!(1)],
            vec![
                FileExists(long_path!("/cache/root/sha256", 42)),
                FileExists(short_path!("/cache/root/removing", 1)),
                FileExists(short_path!("/cache/root/removing", 2)),
                FileExists(short_path!("/cache/root/removing", 3)),
                FileExists(short_path!("/cache/root/removing", 4)),
                Rename(
                    long_path!("/cache/root/sha256", 42),
                    short_path!("/cache/root/removing", 4),
                ),
                RemoveRecursively(short_path!("/cache/root/removing", 4)),
            ],
        );
    }

    #[test]
    fn new_ensures_directories_exist() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 1000);
        fixture.expect_messages_in_specific_order(vec![
            MkdirRecursively(path_buf!("/cache/root/removing")),
            ReadDir(path_buf!("/cache/root/removing")),
            FileExists(path_buf!("/cache/root/sha256")),
            MkdirRecursively(path_buf!("/cache/root/sha256")),
        ]);
    }

    #[test]
    fn new_restarts_old_removes() {
        let mut test_cache_fs = TestCacheFs::default();
        test_cache_fs.directories.insert(
            path_buf!("/cache/root/removing"),
            vec![
                short_path!("/cache/root/removing", 10),
                short_path!("/cache/root/removing", 20),
            ],
        );
        let mut fixture = Fixture::new(test_cache_fs, 1000);
        fixture.expect_messages_in_specific_order(vec![
            MkdirRecursively(path_buf!("/cache/root/removing")),
            ReadDir(path_buf!("/cache/root/removing")),
            RemoveRecursively(short_path!("/cache/root/removing", 10)),
            RemoveRecursively(short_path!("/cache/root/removing", 20)),
            FileExists(path_buf!("/cache/root/sha256")),
            MkdirRecursively(path_buf!("/cache/root/sha256")),
        ]);
    }

    #[test]
    fn new_removes_old_sha256_if_it_exists() {
        let mut test_cache_fs = TestCacheFs::default();
        test_cache_fs
            .existing_files
            .insert(path_buf!("/cache/root/sha256"));
        let mut fixture = Fixture::new(test_cache_fs, 1000);
        fixture.expect_messages_in_specific_order(vec![
            MkdirRecursively(path_buf!("/cache/root/removing")),
            ReadDir(path_buf!("/cache/root/removing")),
            FileExists(path_buf!("/cache/root/sha256")),
            FileExists(short_path!("/cache/root/removing", 1)),
            Rename(
                path_buf!("/cache/root/sha256"),
                short_path!("/cache/root/removing", 1),
            ),
            RemoveRecursively(short_path!("/cache/root/removing", 1)),
            MkdirRecursively(path_buf!("/cache/root/sha256")),
        ]);
    }
}
