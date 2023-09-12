//! Manage downloading, extracting, and storing of image files specified by executions.

use crate::{
    heap::{Heap, HeapDeps, HeapIndex},
    Result, Sha256Digest,
};
use std::{
    collections::HashMap,
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

/// [Cache]'s external dependencies that must be fulfilled by its caller.
pub trait CacheDeps {
    /// Used to associate [Message::GetRequest] messages with [CacheDeps::get_completed] calls. The
    /// caller is responsible for generating these and ensuring that they are unique. The cache
    /// actually doesn't care if they are unique, but the caller would likely be confused if they
    /// weren't.
    type RequestId: Copy;

    /// Download `digest` from somewhere and extract it into `path`. Assume that `path` does not exist, but
    /// that its parent directory does. Validate the digest while downloading and extracting. When
    /// finished, deliver a [Message::DownloadAndExtractCompleted].
    fn download_and_extract(&mut self, digest: Sha256Digest, path: PathBuf);

    /// Receive notification that a [Message::GetRequest] has completed. If `path` is [None],
    /// then there was an error and the artifact isn't available. Otherwise, the artifact will
    /// remain available at the given `path` until a [Message::DecrementRefcount] is sent to the
    /// cache.
    fn get_completed(
        &mut self,
        request_id: Self::RequestId,
        digest: Sha256Digest,
        path: Option<PathBuf>,
    );

    type Fs: CacheFs;

    /// Get a reference to the CacheFs being used.
    fn fs(&mut self) -> &mut Self::Fs;
}

/// Messages sent to [Cache::receive_message]. This is the primary way to interact with the
/// [Cache].
pub enum Message<RequestIdT> {
    /// Request a [CacheHandle] for a given [Sha256Digest]. Eventually, the [Cache] will call
    /// [CacheDeps::get_completed] in response to this message.
    GetRequest(RequestIdT, Sha256Digest),

    /// Tell the [Cache] that a [CacheDeps::download_and_extract] has completed.
    DownloadAndExtractCompleted(Sha256Digest, Result<u64>),

    /// Tell the [Cache] to decrement the refcount on a digest and path. Once the refcount reaches
    /// zero, the cache is free to delete the underlying directory.
    DecrementRefcount(Sha256Digest),
}

/// Manage a directory of downloaded, extracted images. Coordinate fetching of these images, and
/// removing them when they are no longer in use and the amount of space used by the directory has
/// grown too large.
pub struct Cache<CacheDepsT: CacheDeps> {
    root: PathBuf,
    entries: HashMap<Sha256Digest, CacheEntry<CacheDepsT>>,
    heap: Heap<HashMap<Sha256Digest, CacheEntry<CacheDepsT>>>,
    next_priority: u64,
    bytes_used: u64,
    bytes_used_goal: u64,
}

impl<CacheDepsT: CacheDeps> Cache<CacheDepsT> {
    /// Create a new [Cache] rooted at `root`. The directory `root` and all necessary ancestors
    /// will be created, along with `{root}/removing` and `{root}/sha256`. Any pre-existing entries
    /// in `{root}/removing` and `{root}/sha256` will be removed. That implies that the [Cache]
    /// doesn't currently keep data stored across invocations.
    ///
    /// `bytes_used_goal` is the goal on-disk size for the cache. The cache will periodically grow
    /// larger than this size, but then shrink back down to this size. Ideally, the cache would use
    /// this as a hard upper bound, but that's not how it currently works.
    pub fn new(root: &Path, deps: &mut CacheDepsT, bytes_used_goal: u64) -> Self {
        let mut path = root.to_owned();

        path.push("removing");
        let fs = deps.fs();
        fs.mkdir_recursively(&path);
        for child in fs.read_dir(&path) {
            fs.remove_recursively_on_thread(child);
        }
        path.pop();

        path.push("sha256");
        if fs.file_exists(&path) {
            Self::remove_in_background(fs, root, &path);
        }
        fs.mkdir_recursively(&path);
        path.pop();

        Cache {
            root: root.to_owned(),
            entries: HashMap::default(),
            heap: Heap::default(),
            next_priority: 0,
            bytes_used: 0,
            bytes_used_goal,
        }
    }

    /// Receive a message and act on it. See [Message].
    pub fn receive_message(&mut self, deps: &mut CacheDepsT, msg: Message<CacheDepsT::RequestId>) {
        match msg {
            Message::GetRequest(request_id, digest) => {
                self.receive_get_request(deps, request_id, digest)
            }
            Message::DownloadAndExtractCompleted(digest, Err(_)) => {
                self.receive_download_and_extract_error(deps, digest)
            }
            Message::DownloadAndExtractCompleted(digest, Ok(bytes_used)) => {
                self.receive_download_and_extract_success(deps, digest, bytes_used)
            }
            Message::DecrementRefcount(digest) => self.receive_decrement_refcount(deps, digest),
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
enum CacheEntry<CacheDepsT: CacheDeps> {
    /// The artifact is being downloaded, extracted, and having its checksum validated. There is
    /// probably a subdirectory for this [Sha256Digest], but there might not yet be one, depending
    /// on where the extraction process is.
    DownloadingAndExtracting(Vec<CacheDepsT::RequestId>),

    /// The artifact has been successfully downloaded and extracted, and the subdirectory is
    /// currently being used by at least one execution. We refcount this state since there may be
    /// multiple executions that use the same artifact.
    InUse {
        bytes_used: u64,
        refcount: NonZeroU32,
    },

    /// The artifact has been successfully downloaded and extracted, but no executions are
    /// currently using it. The `priority` is provided by [Cache] and is used by the [Heap] to
    /// determine which entry should be removed first when freeing up space.
    InHeap {
        bytes_used: u64,
        priority: u64,
        heap_index: HeapIndex,
    },
}

impl<CacheDepsT: CacheDeps> Cache<CacheDepsT> {
    fn remove_in_background(fs: &mut CacheDepsT::Fs, root: &Path, source: &Path) {
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

    fn send_get_completed_successfully(
        deps: &mut CacheDepsT,
        root: &Path,
        request_id: CacheDepsT::RequestId,
        digest: Sha256Digest,
    ) {
        let path = Self::cache_path(root, &digest);
        deps.get_completed(request_id, digest, Some(path));
    }

    fn receive_get_request(
        &mut self,
        deps: &mut CacheDepsT,
        request_id: CacheDepsT::RequestId,
        digest: Sha256Digest,
    ) {
        match self.entries.get_mut(&digest) {
            None => {
                let cache_path = Self::cache_path(&self.root, &digest);
                deps.download_and_extract(digest.clone(), cache_path);
                self.entries.insert(
                    digest,
                    CacheEntry::DownloadingAndExtracting(Vec::from([request_id])),
                );
            }
            Some(CacheEntry::DownloadingAndExtracting(requests)) => {
                requests.push(request_id);
            }
            Some(CacheEntry::InUse { refcount, .. }) => {
                *refcount = refcount.checked_add(1).unwrap();
                Self::send_get_completed_successfully(deps, &self.root, request_id, digest);
            }
            Some(entry @ CacheEntry::InHeap { .. }) => {
                let CacheEntry::InHeap {
                    bytes_used,
                    heap_index,
                    ..
                } = *entry
                else {
                    unreachable!()
                };
                *entry = CacheEntry::InUse {
                    refcount: NonZeroU32::new(1).unwrap(),
                    bytes_used,
                };
                self.heap.remove(&mut self.entries, heap_index);
                Self::send_get_completed_successfully(deps, &self.root, request_id, digest);
            }
        }
    }

    fn receive_download_and_extract_error(&mut self, deps: &mut CacheDepsT, digest: Sha256Digest) {
        match self.entries.remove(&digest) {
            Some(CacheEntry::DownloadingAndExtracting(requests)) => {
                for request_id in requests.iter() {
                    deps.get_completed(*request_id, digest.clone(), None);
                }
                let cache_path = Self::cache_path(&self.root, &digest);
                if deps.fs().file_exists(&cache_path) {
                    Self::remove_in_background(deps.fs(), &self.root, &cache_path);
                }
            }
            _ => {
                panic!("Got DownloadingAndExtracting in unexpected state");
            }
        }
    }

    fn possibly_remove_some(&mut self, deps: &mut CacheDepsT) {
        while self.bytes_used > self.bytes_used_goal {
            match self.heap.pop(&mut self.entries) {
                None => {
                    break;
                }
                Some(digest) => match self.entries.remove(&digest) {
                    Some(CacheEntry::InHeap { bytes_used, .. }) => {
                        let path = Self::cache_path(&self.root, &digest);
                        Self::remove_in_background(deps.fs(), &self.root, &path);
                        self.bytes_used = self.bytes_used.checked_sub(bytes_used).unwrap();
                    }
                    _ => {
                        panic!("Entry popped off of heap was in unexpected state");
                    }
                },
            }
        }
    }

    fn receive_download_and_extract_success(
        &mut self,
        deps: &mut CacheDepsT,
        digest: Sha256Digest,
        bytes_used: u64,
    ) {
        match self.entries.get_mut(&digest) {
            Some(entry @ CacheEntry::DownloadingAndExtracting(_)) => {
                let CacheEntry::DownloadingAndExtracting(requests) = entry else {
                    unreachable!()
                };
                let mut refcount = 0;
                for request_id in requests.iter() {
                    refcount += 1;
                    Self::send_get_completed_successfully(
                        deps,
                        &self.root,
                        *request_id,
                        digest.clone(),
                    );
                }
                // Refcount must be > 0 since we don't allow cancellation of gets.
                *entry = CacheEntry::InUse {
                    bytes_used,
                    refcount: NonZeroU32::new(refcount).unwrap(),
                };
                self.bytes_used = self.bytes_used.checked_add(bytes_used).unwrap();
                self.possibly_remove_some(deps);
            }
            _ => {
                panic!("Got DownloadingAndExtracting in unexpected state");
            }
        }
    }

    fn receive_decrement_refcount(&mut self, deps: &mut CacheDepsT, digest: Sha256Digest) {
        let entry = self
            .entries
            .get_mut(&digest)
            .expect("Got DecrementRefcount in unexpected state");
        match entry {
            CacheEntry::InUse {
                bytes_used,
                refcount,
            } => match NonZeroU32::new(refcount.get() - 1) {
                Some(new_refcount) => *refcount = new_refcount,
                None => {
                    let priority = self.next_priority;
                    self.next_priority = self.next_priority.checked_add(1).unwrap();
                    *entry = CacheEntry::InHeap {
                        bytes_used: *bytes_used,
                        priority,
                        heap_index: HeapIndex::default(),
                    };
                    self.heap.push(&mut self.entries, digest);
                    self.possibly_remove_some(deps);
                }
            },
            _ => {
                panic!("Got DecrementRefcount with existing zero refcount");
            }
        }
    }
}

impl<CacheDepsT: CacheDeps> HeapDeps for HashMap<Sha256Digest, CacheEntry<CacheDepsT>> {
    type Element = Sha256Digest;

    fn is_element_less_than(&self, lhs: &Self::Element, rhs: &Self::Element) -> bool {
        let lhs_priority = match self.get(lhs) {
            Some(CacheEntry::InHeap { priority, .. }) => *priority,
            _ => panic!("Element should be in heap"),
        };
        let rhs_priority = match self.get(rhs) {
            Some(CacheEntry::InHeap { priority, .. }) => *priority,
            _ => panic!("Element should be in heap"),
        };
        lhs_priority.cmp(&rhs_priority) == std::cmp::Ordering::Less
    }

    fn update_index(&mut self, elem: &Self::Element, idx: HeapIndex) {
        match self.get_mut(elem) {
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
    use super::Message::*;
    use super::*;
    use crate::test::*;
    use anyhow::anyhow;
    use itertools::Itertools;
    use std::collections::HashSet;
    use TestMessage::*;

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        FileExists(PathBuf),
        Rename(PathBuf, PathBuf),
        RemoveRecursively(PathBuf),
        MkdirRecursively(PathBuf),
        ReadDir(PathBuf),
        DownloadAndExtract(Sha256Digest, PathBuf),
        GetRequestSucceeded(u32, Sha256Digest, PathBuf),
        GetRequestFailed(u32, Sha256Digest),
    }

    #[derive(Default)]
    struct TestCacheDeps {
        messages: Vec<TestMessage>,
        existing_files: HashSet<PathBuf>,
        directories: HashMap<PathBuf, Vec<PathBuf>>,
        last_random_number: u64,
    }

    impl CacheFs for TestCacheDeps {
        fn rand_u64(&mut self) -> u64 {
            self.last_random_number += 1;
            self.last_random_number
        }

        fn file_exists(&mut self, path: &Path) -> bool {
            self.messages.push(FileExists(path.to_owned()));
            self.existing_files.contains(path)
        }

        fn rename(&mut self, source: &Path, destination: &Path) {
            self.messages
                .push(Rename(source.to_owned(), destination.to_owned()));
        }

        fn remove_recursively_on_thread(&mut self, path: PathBuf) {
            self.messages.push(RemoveRecursively(path.to_owned()));
        }

        fn mkdir_recursively(&mut self, path: &Path) {
            self.messages.push(MkdirRecursively(path.to_owned()));
        }

        fn read_dir(&mut self, path: &Path) -> Box<dyn Iterator<Item = PathBuf>> {
            self.messages.push(ReadDir(path.to_owned()));
            Box::new(
                self.directories
                    .get(path)
                    .unwrap_or(&vec![])
                    .clone()
                    .into_iter(),
            )
        }
    }

    impl CacheDeps for TestCacheDeps {
        type RequestId = u32;

        fn download_and_extract(&mut self, digest: Sha256Digest, prefix: PathBuf) {
            self.messages.push(DownloadAndExtract(digest, prefix))
        }

        fn get_completed(
            &mut self,
            request_id: Self::RequestId,
            digest: Sha256Digest,
            path: Option<PathBuf>,
        ) {
            self.messages.push(match path {
                Some(path) => GetRequestSucceeded(request_id, digest, path),
                None => GetRequestFailed(request_id, digest),
            });
        }

        type Fs = Self;

        fn fs(&mut self) -> &mut Self::Fs {
            self
        }
    }

    struct Fixture {
        test_cache_deps: TestCacheDeps,
        cache: Cache<TestCacheDeps>,
    }

    impl Fixture {
        fn new_and_clear_messages(bytes_used_goal: u64) -> Self {
            let mut fixture = Fixture::new(TestCacheDeps::default(), bytes_used_goal);
            fixture.clear_messages();
            fixture
        }

        fn new(mut test_cache_deps: TestCacheDeps, bytes_used_goal: u64) -> Self {
            let cache = Cache::new(
                Path::new("/cache/root"),
                &mut test_cache_deps,
                bytes_used_goal,
            );
            Fixture {
                test_cache_deps,
                cache,
            }
        }

        fn expect_messages_in_any_order(&mut self, expected: Vec<TestMessage>) {
            let messages = &mut self.test_cache_deps.messages;
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
                *self.test_cache_deps.messages == expected,
                "Expected messages didn't match actual messages in specific order.\n\
                 Expected: {:#?}\nActual: {:#?}",
                expected,
                self.test_cache_deps.messages
            );
            self.test_cache_deps.messages.clear();
        }

        fn clear_messages(&mut self) {
            self.test_cache_deps.messages.clear();
        }
    }

    macro_rules! request_id {
        ($n:expr) => {
            $n as u32
        };
    }

    macro_rules! script_test {
        ($test_name:ident; $fixture:expr; $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?) => {
            #[test]
            fn $test_name() {
                let mut fixture = $fixture;
                $(
                    fixture.cache.receive_message(&mut fixture.test_cache_deps, $in_msg);
                    fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
                )+
            }
        };
    }

    script_test! {
        get_request_for_empty;
        Fixture::new_and_clear_messages(1000);

        GetRequest(request_id!(1), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DownloadAndExtractCompleted(digest!(42), Ok(100)) => {
            GetRequestSucceeded(request_id!(1), digest!(42), long_path!("/cache/root/sha256", 42)),
        };
    }

    script_test! {
        get_request_for_empty_larger_than_goal_ok_then_removes_on_decrement_refcount;
        Fixture::new_and_clear_messages(1000);

        GetRequest(request_id!(1), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DownloadAndExtractCompleted(digest!(42), Ok(10000)) => {
            GetRequestSucceeded(request_id!(1), digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DecrementRefcount(digest!(42)) => {
            FileExists(short_path!("/cache/root/removing", 1)),
            Rename(long_path!("/cache/root/sha256", 42), short_path!("/cache/root/removing", 1)),
            RemoveRecursively(short_path!("/cache/root/removing", 1)),
        };
    }

    script_test! {
        cache_entries_are_removed_in_lru_order;
        Fixture::new_and_clear_messages(10);

        GetRequest(request_id!(1), digest!(1)) => {
            DownloadAndExtract(digest!(1), long_path!("/cache/root/sha256", 1)),
        };
        DownloadAndExtractCompleted(digest!(1), Ok(4)) => {
            GetRequestSucceeded(request_id!(1), digest!(1), long_path!("/cache/root/sha256", 1)),
        };
        DecrementRefcount(digest!(1)) => {};

        GetRequest(request_id!(2), digest!(2)) => {
            DownloadAndExtract(digest!(2), long_path!("/cache/root/sha256", 2)),
        };
        DownloadAndExtractCompleted(digest!(2), Ok(4)) => {
            GetRequestSucceeded(request_id!(2), digest!(2), long_path!("/cache/root/sha256", 2)),
        };
        DecrementRefcount(digest!(2)) => {};

        GetRequest(request_id!(3), digest!(3)) => {
            DownloadAndExtract(digest!(3), long_path!("/cache/root/sha256", 3)),
        };
        DownloadAndExtractCompleted(digest!(3), Ok(4)) => {
            GetRequestSucceeded(request_id!(3), digest!(3), long_path!("/cache/root/sha256", 3)),
            FileExists(short_path!("/cache/root/removing", 1)),
            Rename(long_path!("/cache/root/sha256", 1), short_path!("/cache/root/removing", 1)),
            RemoveRecursively(short_path!("/cache/root/removing", 1)),
        };
        DecrementRefcount(digest!(3)) => {};

        GetRequest(request_id!(4), digest!(4)) => {
            DownloadAndExtract(digest!(4), long_path!("/cache/root/sha256", 4)),
        };
        DownloadAndExtractCompleted(digest!(4), Ok(4)) => {
            GetRequestSucceeded(request_id!(4), digest!(4), long_path!("/cache/root/sha256", 4)),
            FileExists(short_path!("/cache/root/removing", 2)),
            Rename(long_path!("/cache/root/sha256", 2), short_path!("/cache/root/removing", 2)),
            RemoveRecursively(short_path!("/cache/root/removing", 2)),
        };
        DecrementRefcount(digest!(4)) => {};
    }

    script_test! {
        lru_order_augmented_by_last_use;
        Fixture::new_and_clear_messages(10);

        GetRequest(request_id!(1), digest!(1)) => {
            DownloadAndExtract(digest!(1), long_path!("/cache/root/sha256", 1)),
        };
        DownloadAndExtractCompleted(digest!(1), Ok(3)) => {
            GetRequestSucceeded(request_id!(1), digest!(1), long_path!("/cache/root/sha256", 1)),
        };

        GetRequest(request_id!(2), digest!(2)) => {
            DownloadAndExtract(digest!(2), long_path!("/cache/root/sha256", 2)),
        };
        DownloadAndExtractCompleted(digest!(2), Ok(3)) => {
            GetRequestSucceeded(request_id!(2), digest!(2), long_path!("/cache/root/sha256", 2)),
        };

        GetRequest(request_id!(3), digest!(3)) => {
            DownloadAndExtract(digest!(3), long_path!("/cache/root/sha256", 3)),
        };
        DownloadAndExtractCompleted(digest!(3), Ok(3)) => {
            GetRequestSucceeded(request_id!(3), digest!(3), long_path!("/cache/root/sha256", 3)),
        };

        DecrementRefcount(digest!(3)) => {};
        DecrementRefcount(digest!(2)) => {};
        DecrementRefcount(digest!(1)) => {};

        GetRequest(request_id!(4), digest!(4)) => {
            DownloadAndExtract(digest!(4), long_path!("/cache/root/sha256", 4)),
        };
        DownloadAndExtractCompleted(digest!(4), Ok(3)) => {
            GetRequestSucceeded(request_id!(4), digest!(4), long_path!("/cache/root/sha256", 4)),
            FileExists(short_path!("/cache/root/removing", 1)),
            Rename(long_path!("/cache/root/sha256", 3), short_path!("/cache/root/removing", 1)),
            RemoveRecursively(short_path!("/cache/root/removing", 1)),
        };
    }

    script_test! {
        multiple_get_requests_for_empty;
        Fixture::new_and_clear_messages(1000);

        GetRequest(request_id!(1), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42))
        };

        GetRequest(request_id!(2), digest!(42)) => {};
        GetRequest(request_id!(3), digest!(42)) => {};

        DownloadAndExtractCompleted(digest!(42), Ok(100)) => {
            GetRequestSucceeded(request_id!(1), digest!(42), long_path!("/cache/root/sha256", 42)),
            GetRequestSucceeded(request_id!(2), digest!(42), long_path!("/cache/root/sha256", 42)),
            GetRequestSucceeded(request_id!(3), digest!(42), long_path!("/cache/root/sha256", 42)),
        };
    }

    script_test! {
        multiple_get_requests_for_empty_larger_than_goal_remove_on_last_decrement;
        Fixture::new_and_clear_messages(1000);

        GetRequest(request_id!(1), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42))
        };

        GetRequest(request_id!(2), digest!(42)) => {};
        GetRequest(request_id!(3), digest!(42)) => {};

        DownloadAndExtractCompleted(digest!(42), Ok(10000)) => {
            GetRequestSucceeded(request_id!(1), digest!(42), long_path!("/cache/root/sha256", 42)),
            GetRequestSucceeded(request_id!(2), digest!(42), long_path!("/cache/root/sha256", 42)),
            GetRequestSucceeded(request_id!(3), digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DecrementRefcount(digest!(42)) => {};
        DecrementRefcount(digest!(42)) => {};
        DecrementRefcount(digest!(42)) => {
            FileExists(short_path!("/cache/root/removing", 1)),
            Rename(long_path!("/cache/root/sha256", 42), short_path!("/cache/root/removing", 1)),
            RemoveRecursively(short_path!("/cache/root/removing", 1)),
        };
    }

    script_test! {
        get_request_for_currently_used;
        Fixture::new_and_clear_messages(10);

        GetRequest(request_id!(1), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DownloadAndExtractCompleted(digest!(42), Ok(100)) => {
            GetRequestSucceeded(request_id!(1), digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        GetRequest(request_id!(2), digest!(42)) => {
            GetRequestSucceeded(request_id!(2), digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DecrementRefcount(digest!(42)) => {};
        DecrementRefcount(digest!(42)) => {
            FileExists(short_path!("/cache/root/removing", 1)),
            Rename(long_path!("/cache/root/sha256", 42), short_path!("/cache/root/removing", 1)),
            RemoveRecursively(short_path!("/cache/root/removing", 1)),
        };
    }

    script_test! {
        get_request_for_cached_followed_by_big_get_does_not_evict_until_decrement_refcount;
        Fixture::new_and_clear_messages(100);

        GetRequest(request_id!(1), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DownloadAndExtractCompleted(digest!(42), Ok(10)) => {
            GetRequestSucceeded(request_id!(1), digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DecrementRefcount(digest!(42)) => {};

        GetRequest(request_id!(2), digest!(42)) => {
            GetRequestSucceeded(request_id!(2), digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        GetRequest(request_id!(3), digest!(43)) => {
            DownloadAndExtract(digest!(43), long_path!("/cache/root/sha256", 43)),
        };

        DownloadAndExtractCompleted(digest!(43), Ok(100)) => {
            GetRequestSucceeded(request_id!(3), digest!(43), long_path!("/cache/root/sha256", 43)),
        };

        DecrementRefcount(digest!(42)) => {
            FileExists(short_path!("/cache/root/removing", 1)),
            Rename(long_path!("/cache/root/sha256", 42), short_path!("/cache/root/removing", 1)),
            RemoveRecursively(short_path!("/cache/root/removing", 1)),
        };
    }

    script_test! {
        get_request_for_empty_with_download_and_extract_failure_and_no_files_created;
        Fixture::new_and_clear_messages(1000);

        GetRequest(request_id!(1), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DownloadAndExtractCompleted(digest!(42), Err(anyhow!("foo"))) => {
            FileExists(long_path!("/cache/root/sha256", 42)),
            GetRequestFailed(request_id!(1), digest!(42)),
        };
    }

    script_test! {
        get_request_for_empty_with_download_and_extract_failure_and_files_created;
        {
            let mut fixture = Fixture::new_and_clear_messages(1000);
            fixture.test_cache_deps.existing_files.insert(long_path!("/cache/root/sha256", 42));
            fixture
        };

        GetRequest(request_id!(1), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DownloadAndExtractCompleted(digest!(42), Err(anyhow!("foo"))) => {
            FileExists(long_path!("/cache/root/sha256", 42)),
            FileExists(short_path!("/cache/root/removing", 1)),
            Rename(long_path!("/cache/root/sha256", 42), short_path!("/cache/root/removing", 1)),
            RemoveRecursively(short_path!("/cache/root/removing", 1)),
            GetRequestFailed(request_id!(1), digest!(42)),
        };
    }

    script_test! {
        multiple_get_requests_for_empty_with_download_and_extract_failure;
        {
            let mut fixture = Fixture::new_and_clear_messages(1000);
            fixture.test_cache_deps.existing_files.insert(long_path!("/cache/root/sha256", 42));
            fixture
        };

        GetRequest(request_id!(1), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        GetRequest(request_id!(2), digest!(42)) => {};
        GetRequest(request_id!(3), digest!(42)) => {};

        DownloadAndExtractCompleted(digest!(42), Err(anyhow!("foo"))) => {
            FileExists(long_path!("/cache/root/sha256", 42)),
            FileExists(short_path!("/cache/root/removing", 1)),
            Rename(long_path!("/cache/root/sha256", 42), short_path!("/cache/root/removing", 1)),
            RemoveRecursively(short_path!("/cache/root/removing", 1)),
            GetRequestFailed(request_id!(1), digest!(42)),
            GetRequestFailed(request_id!(2), digest!(42)),
            GetRequestFailed(request_id!(3), digest!(42)),
        };
    }

    script_test! {
        get_after_error_retries;
        Fixture::new_and_clear_messages(1000);

        GetRequest(request_id!(1), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DownloadAndExtractCompleted(digest!(42), Err(anyhow!("foo"))) => {
            FileExists(long_path!("/cache/root/sha256", 42)),
            GetRequestFailed(request_id!(1), digest!(42)),
        };

        GetRequest(request_id!(2), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42)),
        };
    }

    script_test! {
        rename_retries_until_unique_path_name;
        {
            let mut fixture = Fixture::new_and_clear_messages(1000);
            fixture.test_cache_deps.existing_files.insert(long_path!("/cache/root/sha256", 42));
            fixture.test_cache_deps.existing_files.insert(short_path!("/cache/root/removing", 1));
            fixture.test_cache_deps.existing_files.insert(short_path!("/cache/root/removing", 2));
            fixture.test_cache_deps.existing_files.insert(short_path!("/cache/root/removing", 3));
            fixture
        };

        GetRequest(request_id!(1), digest!(42)) => {
            DownloadAndExtract(digest!(42), long_path!("/cache/root/sha256", 42)),
        };

        DownloadAndExtractCompleted(digest!(42), Err(anyhow!("foo"))) => {
            FileExists(long_path!("/cache/root/sha256", 42)),
            FileExists(short_path!("/cache/root/removing", 1)),
            FileExists(short_path!("/cache/root/removing", 2)),
            FileExists(short_path!("/cache/root/removing", 3)),
            FileExists(short_path!("/cache/root/removing", 4)),
            Rename(long_path!("/cache/root/sha256", 42), short_path!("/cache/root/removing", 4)),
            RemoveRecursively(short_path!("/cache/root/removing", 4)),
            GetRequestFailed(request_id!(1), digest!(42)),
        };
    }

    #[test]
    fn new_ensures_directories_exist() {
        let mut fixture = Fixture::new(TestCacheDeps::default(), 1000);
        fixture.expect_messages_in_specific_order(vec![
            MkdirRecursively(path_buf!("/cache/root/removing")),
            ReadDir(path_buf!("/cache/root/removing")),
            FileExists(path_buf!("/cache/root/sha256")),
            MkdirRecursively(path_buf!("/cache/root/sha256")),
        ]);
    }

    #[test]
    fn new_restarts_old_removes() {
        let mut test_cache_deps = TestCacheDeps::default();
        test_cache_deps.directories.insert(
            path_buf!("/cache/root/removing"),
            vec![
                short_path!("/cache/root/removing", 10),
                short_path!("/cache/root/removing", 20),
            ],
        );
        let mut fixture = Fixture::new(test_cache_deps, 1000);
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
        let mut test_cache_deps = TestCacheDeps::default();
        test_cache_deps
            .existing_files
            .insert(path_buf!("/cache/root/sha256"));
        let mut fixture = Fixture::new(test_cache_deps, 1000);
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
