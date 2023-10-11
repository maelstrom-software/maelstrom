use super::GetArtifact;
use crate::config;
use meticulous_base::{ClientId, JobId, Sha256Digest};
use meticulous_util::heap::{Heap, HeapDeps, HeapIndex};
use std::{
    collections::{hash_map, HashMap, HashSet},
    num::NonZeroU32,
    path::{Path, PathBuf},
};

pub trait CacheFs {
    /// Rename `source` to `destination`. Panic on file system error. Assume that all intermediate
    /// directories exist for `destination`, and that `source` and `destination` are on the same
    /// file system.
    fn rename(&mut self, source: &Path, destination: &Path);

    fn remove(&mut self, path: &Path);

    /// Ensure `path` exists and is a directory. If it doesn't exist, recusively ensure its parent exists,
    /// then create it. Panic on file system error or if `path` or any of its ancestors aren't
    /// directories.
    fn mkdir_recursively(&mut self, path: &Path);

    /// Return and iterator that will yield all of the children of a directory. Panic on file
    /// system error or if `path` doesn't exist or isn't a directory.
    fn read_dir(&mut self, path: &Path) -> Box<dyn Iterator<Item = PathBuf>>;

    fn file_size(&mut self, path: &Path) -> u64;
}

pub struct StdCacheFs;

impl CacheFs for StdCacheFs {
    fn rename(&mut self, source: &Path, destination: &Path) {
        std::fs::rename(source, destination).unwrap()
    }

    fn remove(&mut self, path: &Path) {
        std::fs::remove_file(path).unwrap()
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

    fn file_size(&mut self, path: &Path) -> u64 {
        let metadata = std::fs::metadata(path).unwrap();
        metadata.len()
    }
}

/// An entry for a specific [Sha256Digest] in the [Cache]'s hash table. There is one of these for
/// every subdirectory in the `sha256` subdirectory of the [Cache]'s root directory.
enum CacheEntry {
    /// The artifact is being downloaded, extracted, and having its checksum validated. There is
    /// probably a subdirectory for this [Sha256Digest], but there might not yet be one, depending
    /// on where the extraction process is.
    Waiting(Vec<JobId>, HashSet<ClientId>),

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
    pub fn new(
        mut fs: FsT,
        root: config::CacheRoot,
        bytes_used_goal: config::CacheBytesUsedTarget,
    ) -> Self {
        let root = root.into_inner();
        let mut path = root.clone();

        path.push("tmp");
        fs.mkdir_recursively(&path);
        for child in fs.read_dir(&path) {
            fs.remove(&child);
        }
        path.pop();

        let mut result = Cache {
            fs,
            root,
            entries: CacheMap(HashMap::default()),
            heap: Heap::default(),
            next_priority: 0,
            bytes_used: 0,
            bytes_used_goal: bytes_used_goal.into_inner(),
        };

        path.push("sha256");
        result.fs.mkdir_recursively(&path);
        for child in result.fs.read_dir(&path) {
            if let Some((left, right)) =
                child.file_name().unwrap().to_string_lossy().split_once('.')
            {
                if right == "tar" {
                    if let Ok(digest) = left.parse::<Sha256Digest>() {
                        let bytes_used = result.fs.file_size(&child);
                        result.entries.0.insert(
                            digest.clone(),
                            CacheEntry::InHeap {
                                bytes_used,
                                priority: result.next_priority,
                                heap_index: HeapIndex::default(),
                            },
                        );
                        result.heap.push(&mut result.entries, digest);
                        result.next_priority = result.next_priority.checked_add(1).unwrap();
                        result.bytes_used = result.bytes_used.checked_add(bytes_used).unwrap();
                        continue;
                    }
                }
            }
            result.fs.remove(&child);
        }
        result.possibly_remove_some();

        result
    }

    pub fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact {
        let entry = self
            .entries
            .0
            .entry(digest)
            .or_insert(CacheEntry::Waiting(Vec::default(), HashSet::default()));
        match entry {
            CacheEntry::Waiting(requests, clients) => {
                requests.push(jid);
                if clients.insert(jid.0) {
                    GetArtifact::Get
                } else {
                    GetArtifact::Wait
                }
            }
            CacheEntry::InUse { refcount, .. } => {
                *refcount = refcount.checked_add(1).unwrap();
                GetArtifact::Success
            }
            CacheEntry::InHeap {
                bytes_used,
                heap_index,
                ..
            } => {
                let heap_index = *heap_index;
                *entry = CacheEntry::InUse {
                    bytes_used: *bytes_used,
                    refcount: NonZeroU32::new(1).unwrap(),
                };
                self.heap.remove(&mut self.entries, heap_index);
                GetArtifact::Success
            }
        }
    }

    pub fn got_artifact(
        &mut self,
        digest: Sha256Digest,
        path: &Path,
        bytes_used: u64,
    ) -> Vec<JobId> {
        let mut result = vec![];
        let new_path = self.cache_path(&digest);
        match self.entries.0.entry(digest.clone()) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(CacheEntry::InHeap {
                    bytes_used,
                    priority: self.next_priority,
                    heap_index: HeapIndex::default(),
                });
                self.heap.push(&mut self.entries, digest);
                self.next_priority = self.next_priority.checked_add(1).unwrap();
            }
            hash_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    CacheEntry::Waiting(jids, _) => {
                        let refcount = NonZeroU32::new(u32::try_from(jids.len()).unwrap()).unwrap();
                        std::mem::swap(jids, &mut result);
                        *entry = CacheEntry::InUse {
                            bytes_used,
                            refcount,
                        };
                    }
                    CacheEntry::InUse { .. } | CacheEntry::InHeap { .. } => {
                        self.fs.remove(path);
                        return vec![];
                    }
                }
            }
        }
        self.fs.rename(path, &new_path);
        self.bytes_used = self.bytes_used.checked_add(bytes_used).unwrap();
        self.possibly_remove_some();
        result
    }

    pub fn decrement_refcount(&mut self, digest: Sha256Digest) {
        let entry = self.entries.0.get_mut(&digest).unwrap();
        let CacheEntry::InUse {
            bytes_used,
            refcount,
        } = entry
        else {
            panic!()
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

    pub fn client_disconnected(&mut self, cid: ClientId) {
        self.entries.0.retain(|_, e| {
            let CacheEntry::Waiting(jids, clients) = e else {
                return true;
            };
            jids.retain(|jid| jid.0 != cid);
            clients.retain(|c| *c != cid);
            !jids.is_empty()
        })
    }

    pub fn get_artifact_for_worker(&mut self, digest: &Sha256Digest) -> Option<(PathBuf, u64)> {
        let Some(CacheEntry::InUse {
            refcount,
            bytes_used,
        }) = self.entries.0.get_mut(digest)
        else {
            return None;
        };
        *refcount = refcount.checked_add(1).unwrap();
        let bytes_used = *bytes_used;
        Some((self.cache_path(digest), bytes_used))
    }

    fn cache_path(&self, digest: &Sha256Digest) -> PathBuf {
        let mut path = self.root.to_owned();
        path.push("sha256");
        path.push(format!("{digest}.tar"));
        path
    }

    fn possibly_remove_some(&mut self) {
        while self.bytes_used > self.bytes_used_goal {
            let Some(digest) = self.heap.pop(&mut self.entries) else {
                break;
            };
            let Some(CacheEntry::InHeap { bytes_used, .. }) = self.entries.0.remove(&digest) else {
                panic!("Entry popped off of heap was in unexpected state");
            };
            self.fs.remove(&self.cache_path(&digest));
            self.bytes_used = self.bytes_used.checked_sub(bytes_used).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meticulous_test::*;
    use std::{cell::RefCell, rc::Rc};
    use TestMessage::*;

    #[derive(Debug, PartialEq)]
    enum TestMessage {
        Rename(PathBuf, PathBuf),
        Remove(PathBuf),
        MkdirRecursively(PathBuf),
        ReadDir(PathBuf),
        FileSize(PathBuf),
    }

    #[derive(Default)]
    struct TestCacheFs {
        messages: Vec<TestMessage>,
        files: HashMap<PathBuf, u64>,
        directories: HashMap<PathBuf, Vec<PathBuf>>,
    }

    impl CacheFs for Rc<RefCell<TestCacheFs>> {
        fn rename(&mut self, source: &Path, destination: &Path) {
            self.borrow_mut()
                .messages
                .push(Rename(source.to_owned(), destination.to_owned()));
        }

        fn remove(&mut self, target: &Path) {
            self.borrow_mut().messages.push(Remove(target.to_owned()));
        }

        fn mkdir_recursively(&mut self, path: &Path) {
            self.borrow_mut()
                .messages
                .push(MkdirRecursively(path.to_owned()));
        }

        fn read_dir(&mut self, path: &Path) -> Box<dyn Iterator<Item = PathBuf>> {
            self.borrow_mut().messages.push(ReadDir(path.to_owned()));
            Box::new(
                self.borrow()
                    .directories
                    .get(path)
                    .unwrap_or(&vec![])
                    .clone()
                    .into_iter(),
            )
        }

        fn file_size(&mut self, path: &Path) -> u64 {
            self.borrow_mut().messages.push(FileSize(path.to_owned()));
            *self.borrow().files.get(path).unwrap()
        }
    }

    struct Fixture {
        fs: Rc<RefCell<TestCacheFs>>,
        cache: Cache<Rc<RefCell<TestCacheFs>>>,
    }

    impl Fixture {
        fn new(fs: TestCacheFs, bytes_used_target: u64) -> Self {
            let fs = Rc::new(RefCell::new(fs));
            let cache = Cache::new(
                fs.clone(),
                Path::new("/z").to_owned().into(),
                bytes_used_target.into(),
            );
            Fixture { fs, cache }
        }

        fn new_and_clear_fs_operations(fs: TestCacheFs, bytes_used_goal: u64) -> Self {
            let mut result = Self::new(fs, bytes_used_goal);
            result.clear_fs_operations();
            result
        }

        fn clear_fs_operations(&mut self) {
            self.fs.borrow_mut().messages.clear();
        }

        fn expect_fs_operations(&mut self, expected: Vec<TestMessage>) {
            assert!(
                *self.fs.borrow().messages == expected,
                "Expected messages didn't match actual messages.\n\
                 Expected: {:#?}\nActual: {:#?}",
                expected,
                self.fs.borrow().messages
            );
            self.fs.borrow_mut().messages.clear();
        }

        fn get_artifact(
            &mut self,
            jid: JobId,
            digest: Sha256Digest,
            expected: GetArtifact,
            expected_fs_operations: Vec<TestMessage>,
        ) {
            let result = self.cache.get_artifact(jid, digest);
            assert_eq!(result, expected);
            self.expect_fs_operations(expected_fs_operations);
        }

        fn get_artifact_ign(&mut self, jid: JobId, digest: Sha256Digest) {
            _ = self.cache.get_artifact(jid, digest);
            self.clear_fs_operations();
        }

        fn got_artifact(
            &mut self,
            digest: Sha256Digest,
            path: PathBuf,
            bytes_used: u64,
            expected: Vec<JobId>,
            expected_fs_operations: Vec<TestMessage>,
        ) {
            let result = self.cache.got_artifact(digest, &path, bytes_used);
            assert_eq!(result, expected);
            self.expect_fs_operations(expected_fs_operations);
        }

        fn got_artifact_ign(&mut self, digest: Sha256Digest, path: PathBuf, bytes_used: u64) {
            _ = self.cache.got_artifact(digest, &path, bytes_used);
            self.clear_fs_operations();
        }

        fn decrement_refcount(
            &mut self,
            digest: Sha256Digest,
            expected_fs_operations: Vec<TestMessage>,
        ) {
            self.cache.decrement_refcount(digest);
            self.expect_fs_operations(expected_fs_operations);
        }

        fn decrement_refcount_ign(&mut self, digest: Sha256Digest) {
            self.cache.decrement_refcount(digest);
            self.clear_fs_operations();
        }

        fn get_artifact_for_worker(
            &mut self,
            digest: Sha256Digest,
            expected: Option<(PathBuf, u64)>,
        ) {
            assert_eq!(self.cache.get_artifact_for_worker(&digest), expected);
        }
    }

    #[test]
    fn test_new_with_empty_fs() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.expect_fs_operations(vec![
            MkdirRecursively(path_buf!("/z/tmp")),
            ReadDir(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
        ]);
    }

    #[test]
    fn test_new_with_garbage_in_tmp() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/tmp"),
                vec![path_buf!("/z/tmp/one"), path_buf!("/z/tmp/two")],
            )]),
            ..Default::default()
        };
        let mut fixture = Fixture::new(fs, 0);
        fixture.expect_fs_operations(vec![
            MkdirRecursively(path_buf!("/z/tmp")),
            ReadDir(path_buf!("/z/tmp")),
            Remove(path_buf!("/z/tmp/one")),
            Remove(path_buf!("/z/tmp/two")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
        ]);
    }

    #[test]
    fn test_new_with_garbage_and_treasure_in_sha256() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![
                    path_buf!("/z/sha256/one"),
                    long_path!("/z/sha256", 1, "tar"),
                    path_buf!("/z/sha256/two.tar"),
                    long_path!("/z/sha256", 2, "tar"),
                    long_path!("/z/sha256", 3, "tar.gz"),
                ],
            )]),
            files: HashMap::from([
                (long_path!("/z/sha256", 1, "tar"), 1000),
                (long_path!("/z/sha256", 2, "tar"), 100),
            ]),
            ..Default::default()
        };
        let mut fixture = Fixture::new(fs, 2000);
        fixture.expect_fs_operations(vec![
            MkdirRecursively(path_buf!("/z/tmp")),
            ReadDir(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
            Remove(path_buf!("/z/sha256/one")),
            FileSize(long_path!("/z/sha256", 1, "tar")),
            Remove(path_buf!("/z/sha256/two.tar")),
            FileSize(long_path!("/z/sha256", 2, "tar")),
            Remove(long_path!("/z/sha256", 3, "tar.gz")),
        ]);
        assert_eq!(fixture.cache.bytes_used, 1100);
    }

    #[test]
    fn test_new_with_too_much_in_sha256() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![
                    long_path!("/z/sha256", 1, "tar"),
                    long_path!("/z/sha256", 2, "tar"),
                    long_path!("/z/sha256", 3, "tar"),
                ],
            )]),
            files: HashMap::from([
                (long_path!("/z/sha256", 1, "tar"), 1001),
                (long_path!("/z/sha256", 2, "tar"), 1002),
                (long_path!("/z/sha256", 3, "tar"), 1003),
            ]),
            ..Default::default()
        };
        let mut fixture = Fixture::new(fs, 1003);
        fixture.expect_fs_operations(vec![
            MkdirRecursively(path_buf!("/z/tmp")),
            ReadDir(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
            FileSize(long_path!("/z/sha256", 1, "tar")),
            FileSize(long_path!("/z/sha256", 2, "tar")),
            FileSize(long_path!("/z/sha256", 3, "tar")),
            Remove(long_path!("/z/sha256", 1, "tar")),
            Remove(long_path!("/z/sha256", 2, "tar")),
        ]);
        assert_eq!(fixture.cache.bytes_used, 1003);
    }

    #[test]
    fn test_get_artifact_once() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 1000);
        fixture.get_artifact(jid!(1, 1001), digest!(1), GetArtifact::Get, vec![]);
    }

    #[test]
    fn test_get_artifact_again_from_same_client() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 1000);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact(jid!(1, 1002), digest!(1), GetArtifact::Wait, vec![]);
    }

    #[test]
    fn test_get_artifact_again_from_different_client() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 1000);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact(jid!(2, 1001), digest!(1), GetArtifact::Get, vec![]);
    }

    #[test]
    fn test_get_artifact_in_use() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.got_artifact_ign(digest!(1), short_path!("/z/tmp", 1, "tar"), 1);
        fixture.get_artifact(jid!(2, 1001), digest!(1), GetArtifact::Success, vec![]);

        // Refcount should be 2.
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "tar"))]);
    }

    #[test]
    fn test_get_artifact_in_heap_removes_from_heap() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![long_path!("/z/sha256", 112358, "tar")],
            )]),
            files: HashMap::from([(long_path!("/z/sha256", 112358, "tar"), 1)]),
            ..Default::default()
        };
        let mut fixture = Fixture::new_and_clear_fs_operations(fs, 11);
        fixture.get_artifact(
            jid!(1, 112358),
            digest!(112358),
            GetArtifact::Success,
            vec![],
        );

        // Since the artifact we just was pulled out of the heap, we should be able to churn the
        // heap a bunch and never remove our artifact.
        for i in 0..10 {
            fixture.got_artifact(
                digest!(i),
                short_path!("/z/tmp", i, "tar"),
                1,
                vec![],
                vec![Rename(
                    short_path!("/z/tmp", i, "tar"),
                    long_path!("/z/sha256", i, "tar"),
                )],
            );
        }
        for i in 10..100 {
            fixture.got_artifact(
                digest!(i),
                short_path!("/z/tmp", i, "tar"),
                1,
                vec![],
                vec![
                    Rename(
                        short_path!("/z/tmp", i, "tar"),
                        long_path!("/z/sha256", i, "tar"),
                    ),
                    Remove(long_path!("/z/sha256", i - 10, "tar")),
                ],
            );
        }
    }

    #[test]
    fn test_get_artifact_in_heap_sets_refcount() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![long_path!("/z/sha256", 1, "tar")],
            )]),
            files: HashMap::from([(long_path!("/z/sha256", 1, "tar"), 1)]),
            ..Default::default()
        };
        let mut fixture = Fixture::new_and_clear_fs_operations(fs, 1);
        fixture.get_artifact(jid!(1, 1001), digest!(1), GetArtifact::Success, vec![]);

        fixture.get_artifact_ign(jid!(1, 1002), digest!(2));
        fixture.got_artifact_ign(digest!(2), short_path!("/z/tmp", 2, "tar"), 1);

        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "tar"))]);
    }

    #[test]
    fn test_got_artifact_no_entry() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 1000);
        fixture.got_artifact(
            digest!(1),
            short_path!("/z/tmp", 1, "tar"),
            10,
            vec![],
            vec![Rename(
                short_path!("/z/tmp", 1, "tar"),
                long_path!("/z/sha256", 1, "tar"),
            )],
        );
        assert_eq!(fixture.cache.bytes_used, 10);
    }

    #[test]
    fn test_got_artifact_no_entry_pushes_out_old() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![long_path!("/z/sha256", 1, "tar")],
            )]),
            files: HashMap::from([(long_path!("/z/sha256", 1, "tar"), 1)]),
            ..Default::default()
        };
        let mut fixture = Fixture::new_and_clear_fs_operations(fs, 1);
        fixture.got_artifact(
            digest!(2),
            short_path!("/z/tmp", 1, "tar"),
            1,
            vec![],
            vec![
                Rename(
                    short_path!("/z/tmp", 1, "tar"),
                    long_path!("/z/sha256", 2, "tar"),
                ),
                Remove(long_path!("/z/sha256", 1, "tar")),
            ],
        );
        assert_eq!(fixture.cache.bytes_used, 1);
    }

    #[test]
    fn test_got_artifact_no_entry_goes_into_heap() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 10);
        for i in 1..=10 {
            fixture.got_artifact(
                digest!(i),
                short_path!("/z/tmp", i, "tar"),
                1,
                vec![],
                vec![Rename(
                    short_path!("/z/tmp", i, "tar"),
                    long_path!("/z/sha256", i, "tar"),
                )],
            );
        }
        for i in 11..=20 {
            fixture.got_artifact(
                digest!(i),
                short_path!("/z/tmp", i, "tar"),
                1,
                vec![],
                vec![
                    Rename(
                        short_path!("/z/tmp", i, "tar"),
                        long_path!("/z/sha256", i, "tar"),
                    ),
                    Remove(long_path!("/z/sha256", i - 10, "tar")),
                ],
            );
        }
    }

    #[test]
    fn test_got_artifact_with_waiters() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact_ign(jid!(2, 1001), digest!(1));
        fixture.get_artifact_ign(jid!(1, 1002), digest!(1));

        fixture.got_artifact(
            digest!(1),
            short_path!("/z/tmp", 1, "tar"),
            10,
            vec![jid!(1, 1001), jid!(2, 1001), jid!(1, 1002)],
            vec![Rename(
                short_path!("/z/tmp", 1, "tar"),
                long_path!("/z/sha256", 1, "tar"),
            )],
        );
        assert_eq!(fixture.cache.bytes_used, 10);

        // Refcount should be 3.
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "tar"))]);
    }

    #[test]
    fn test_got_artifact_with_waiter_pushes_out_old() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![long_path!("/z/sha256", 1, "tar")],
            )]),
            files: HashMap::from([(long_path!("/z/sha256", 1, "tar"), 1)]),
            ..Default::default()
        };
        let mut fixture = Fixture::new_and_clear_fs_operations(fs, 1);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(2));
        fixture.got_artifact(
            digest!(2),
            short_path!("/z/tmp", 1, "tar"),
            1,
            vec![jid!(1, 1001)],
            vec![
                Rename(
                    short_path!("/z/tmp", 1, "tar"),
                    long_path!("/z/sha256", 2, "tar"),
                ),
                Remove(long_path!("/z/sha256", 1, "tar")),
            ],
        );
        assert_eq!(fixture.cache.bytes_used, 1);
    }

    #[test]
    fn test_got_artifact_already_in_use() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.got_artifact_ign(digest!(1), short_path!("/z/tmp", 1, "tar"), 10);

        fixture.got_artifact(
            digest!(1),
            short_path!("/z/tmp", 2, "tar"),
            10,
            vec![],
            vec![Remove(short_path!("/z/tmp", 2, "tar"))],
        );

        // Refcount should be 1.
        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "tar"))]);
    }

    #[test]
    fn test_got_artifact_already_in_cache() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![long_path!("/z/sha256", 1, "tar")],
            )]),
            files: HashMap::from([(long_path!("/z/sha256", 1, "tar"), 1000)]),
            ..Default::default()
        };
        let mut fixture = Fixture::new_and_clear_fs_operations(fs, 1000);
        fixture.got_artifact(
            digest!(1),
            short_path!("/z/tmp", 1, "tar"),
            10,
            vec![],
            vec![Remove(short_path!("/z/tmp", 1, "tar"))],
        );
    }

    #[test]
    fn test_decrement_refcount_sets_priority_properly() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 10);
        for i in 0..10 {
            fixture.get_artifact_ign(jid!(1, 1000 + i), digest!(i));
            fixture.got_artifact_ign(digest!(i), short_path!("/z/tmp", i, "tar"), 1);
            fixture.decrement_refcount(digest!(i), vec![]);
        }
        for i in 10..100 {
            fixture.get_artifact_ign(jid!(1, 1000 + i), digest!(i));
            fixture.got_artifact(
                digest!(i),
                short_path!("/z/tmp", i, "tar"),
                1,
                vec![jid!(1, 1000 + i)],
                vec![
                    Rename(
                        short_path!("/z/tmp", i, "tar"),
                        long_path!("/z/sha256", i, "tar"),
                    ),
                    Remove(long_path!("/z/sha256", i - 10, "tar")),
                ],
            );
            fixture.decrement_refcount(digest!(i), vec![]);
        }
    }

    #[test]
    #[should_panic]
    fn test_decrement_refcount_not_in_cache_panics() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 10);
        fixture.decrement_refcount_ign(digest!(1));
    }

    #[test]
    #[should_panic]
    fn test_decrement_refcount_waiting_panics() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 10);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.decrement_refcount_ign(digest!(1));
    }

    #[test]
    #[should_panic]
    fn test_decrement_refcount_in_heap_panics() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 10);
        fixture.got_artifact_ign(digest!(1), short_path!("/z/tmp", 1, "tar"), 1);
        fixture.decrement_refcount_ign(digest!(1));
    }

    #[test]
    fn test_client_disconnected_one_client_one_jid() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.cache.client_disconnected(cid!(1));
        fixture.got_artifact(
            digest!(1),
            short_path!("/z/tmp", 1, "tar"),
            1,
            vec![],
            vec![
                Rename(
                    short_path!("/z/tmp", 1, "tar"),
                    long_path!("/z/sha256", 1, "tar"),
                ),
                Remove(long_path!("/z/sha256", 1, "tar")),
            ],
        );
    }

    #[test]
    fn test_client_disconnected_one_client_three_jids() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact_ign(jid!(1, 1002), digest!(1));
        fixture.get_artifact_ign(jid!(1, 1003), digest!(1));
        fixture.cache.client_disconnected(cid!(1));
        fixture.got_artifact(
            digest!(1),
            short_path!("/z/tmp", 1, "tar"),
            1,
            vec![],
            vec![
                Rename(
                    short_path!("/z/tmp", 1, "tar"),
                    long_path!("/z/sha256", 1, "tar"),
                ),
                Remove(long_path!("/z/sha256", 1, "tar")),
            ],
        );
    }

    #[test]
    fn test_client_disconnected_many_clients() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact_ign(jid!(1, 1002), digest!(1));
        fixture.get_artifact_ign(jid!(3, 1003), digest!(1));
        fixture.cache.client_disconnected(cid!(1));
        fixture.get_artifact(jid!(1, 1003), digest!(1), GetArtifact::Get, vec![]);
        fixture.get_artifact(jid!(3, 1003), digest!(1), GetArtifact::Wait, vec![]);
        fixture.got_artifact(
            digest!(1),
            short_path!("/z/tmp", 1, "tar"),
            1,
            vec![jid!(3, 1003), jid!(1, 1003), jid!(3, 1003)],
            vec![Rename(
                short_path!("/z/tmp", 1, "tar"),
                long_path!("/z/sha256", 1, "tar"),
            )],
        );
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "tar"))]);
    }

    #[test]
    fn test_get_artifact_for_worker_no_entry() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_for_worker(digest!(1), None);
    }

    #[test]
    fn test_get_artifact_for_worker_waiting() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact_for_worker(digest!(1), None);
    }

    #[test]
    fn test_get_artifact_for_worker_in_cache() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 1);
        fixture.got_artifact_ign(digest!(1), short_path!("/z/tmp", 1, "tar"), 1);
        fixture.get_artifact_for_worker(digest!(1), None);
    }

    #[test]
    fn test_get_artifact_for_worker_in_use() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.got_artifact_ign(digest!(1), short_path!("/z/tmp", 1, "tar"), 42);
        fixture.get_artifact_for_worker(digest!(1), Some((long_path!("/z/sha256", 1, "tar"), 42)));

        // Refcount should be 2.
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "tar"))]);
    }
}
