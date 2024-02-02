//! Manage the contents of the broker's artifact cache.
//!
//! All artifacts for a job need to be in cache before the job can be scheduled on the worker. The
//! worker may query the broker to fill in holes in its own cache. The broker's cache is filled, on
//! request, by the client.

use anyhow::{anyhow, bail, Result};
use bytesize::ByteSize;
use maelstrom_base::{manifest::ManifestReader, ClientId, JobId, Sha256Digest};
use maelstrom_util::{
    config::{CacheBytesUsedTarget, CacheRoot},
    heap::{Heap, HeapDeps, HeapIndex},
};
use slog::debug;
use std::{
    collections::{hash_map, HashMap, HashSet},
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    io,
    num::NonZeroU32,
    path::{Path, PathBuf},
};

/// [`Cache`]'s dependencies on the host's file system.
pub trait CacheFs {
    /// Rename `source` to `destination`. Panic on file system error. Assume that all intermediate
    /// directories exist for `destination`, and that `source` and `destination` are on the same
    /// file system.
    fn rename(&mut self, source: &Path, destination: &Path);

    /// Remove file at `path`. Panic on file system error, including if `path` points to a
    /// directory, `path` doesn't exist, or the user doesn't have permissions to remove the file.
    fn remove_file(&mut self, path: &Path);

    /// Ensure `path` exists and is a directory. If it doesn't exist, recusively ensure its parent exists,
    /// then create it. Panic on file system error or if `path` or any of its ancestors aren't
    /// directories.
    fn mkdir_recursively(&mut self, path: &Path);

    /// Return and iterator that will yield all of the children of a directory. Panic on file
    /// system error or if `path` doesn't exist or isn't a directory.
    fn read_dir(&mut self, path: &Path) -> Box<dyn Iterator<Item = PathBuf>>;

    /// Return the size, in bytes, of the file (or directory) at `path`. Panic on file system error
    /// or if `path` doesn't exist.
    fn file_size(&mut self, path: &Path) -> u64;

    type File: io::Read + io::Seek + 'static;

    /// Return an object that can be used to read a file. Panic on file system error or if `path`
    /// doesn't exist
    fn open_file(&mut self, path: &Path) -> Self::File;
}

/// Implement [`CacheFs`] using `std::fs`.
pub struct StdCacheFs(maelstrom_util::fs::Fs);

impl StdCacheFs {
    pub fn new() -> Self {
        Self(maelstrom_util::fs::Fs::new())
    }
}

impl CacheFs for StdCacheFs {
    fn rename(&mut self, source: &Path, destination: &Path) {
        self.0.rename(source, destination).unwrap()
    }

    fn remove_file(&mut self, path: &Path) {
        self.0.remove_file(path).unwrap()
    }

    fn mkdir_recursively(&mut self, path: &Path) {
        self.0.create_dir_all(path).unwrap();
    }

    fn read_dir(&mut self, path: &Path) -> Box<dyn Iterator<Item = PathBuf>> {
        Box::new(self.0.read_dir(path).unwrap().map(|de| de.unwrap().path()))
    }

    fn file_size(&mut self, path: &Path) -> u64 {
        self.0.metadata(path).unwrap().len()
    }

    type File = std::fs::File;

    fn open_file(&mut self, path: &Path) -> Self::File {
        self.0.open_file(path).unwrap().into_inner()
    }
}

/// Return value for [`Cache::get_artifact`].
#[derive(Debug, PartialEq)]
pub enum GetArtifact {
    /// The artifact was in the cache, and was successfully gotten. The caller now has a reference
    /// on the artifact that will eventually need to be dropped with [`Cache::decrement_refcount`].
    /// The caller is free to use the artifact until they call [`Cache::decrement_refcount`].
    Success,

    /// The artifact was not in the cache, but the requesting client has already been asked to push
    /// the artifact. The caller will know the artifact has been gotten when
    /// [`Cache::got_artifact`] returns the `JobId`. The caller has no reference count at this
    /// time.
    Wait,

    /// The artifact was not in the cache, and the caller must initiate a transfer (push) from the
    /// client. The caller will know the artifact has been gotten when [`Cache::got_artifact`]
    /// returns the `JobId`. The caller has no reference count at this
    /// time.
    Get,
}

/// An error indicating that [`Cache::get_artifact_for_worker`] was called illegally.
///
/// That method should only be called when the artifact in question is in the cache and has a
/// non-zero reference count. This is because the broker gets a reference count and holds it for
/// all artifacts that are currently in the queue or being processed by workers. So, the worker
/// should only request an artifact when it's been assigned a job, which means the broker should
/// have a reference on the artifact.
///
/// However, this can go wrong for a few reasons. Most obviously, the worker could be malicious. We
/// don't want to provide a way for malicious workers to crash the broker. Less obvious are various
/// race conditions that can occur in error conditions. The job could be canceled, for example. So,
/// we return this error instead of panicking.
#[derive(Clone, Debug, PartialEq)]
pub struct GetArtifactForWorkerError;

impl Display for GetArtifactForWorkerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "artifact not in cache")
    }
}

impl Error for GetArtifactForWorkerError {}

/// An entry for a specific [`Sha256Digest`] in the [`Cache`]'s hash table. There is one of these for
/// every subdirectory in the `sha256` subdirectory of the [`Cache`]'s root directory.
enum CacheEntry {
    /// The artifact is being downloaded, extracted, and having its checksum validated. There is
    /// possibly a subdirectory for this [`Sha256Digest`], depending on where the extraction
    /// process is.
    Waiting(Vec<JobId>, HashSet<ClientId>),

    /// The artifact has been successfully downloaded and extracted, and the subdirectory is
    /// currently being used by at least one job. We refcount this state since there may be
    /// multiple jobs that use the same artifact.
    InUse {
        bytes_used: u64,
        refcount: NonZeroU32,
    },

    /// The artifact has been successfully downloaded and extracted, but no jobs are currently
    /// using it. [`Cache`] hands out `priority` values in a monotonically increasing way. This are
    /// used by the [`Heap`] to implement an LRU cache.
    InHeap {
        bytes_used: u64,
        priority: u64,
        heap_index: HeapIndex,
    },
}

/// An implementation of the "newtype" pattern used to implement [`HeapDeps`] on
/// [`HashMap<Sha256Digest, CacheEntry>`].
///
/// This implementation compares two [`Sha256Digest`]s based on their priority, which is a measure
/// of how recently an entry has been used.
#[derive(Default)]
struct CacheMap(HashMap<Sha256Digest, CacheEntry>);

impl std::ops::Deref for CacheMap {
    type Target = HashMap<Sha256Digest, CacheEntry>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for CacheMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

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

fn try_read_cache_file(fs: &mut impl CacheFs, path: &Path) -> Result<(Sha256Digest, u64)> {
    let path_str = path.file_name().unwrap().to_string_lossy();
    let (left, right) = path_str.split_once('.').ok_or(anyhow!("bad filename"))?;
    if right != "bin" {
        bail!("bad extension")
    }
    let digest = left.parse::<Sha256Digest>()?;
    let size = fs.file_size(path);
    Ok((digest, size))
}

/// The actual cache.
///
/// Two caches shouldn't use the same working directory simultaneously. However, one cache can use
/// a previous cache's directory. In other words, the directory persists across cache lifetimes.
pub struct Cache<FsT> {
    fs: FsT,
    root: PathBuf,
    entries: CacheMap,
    heap: Heap<CacheMap>,
    next_priority: u64,
    bytes_used: u64,
    bytes_used_target: u64,
    log: slog::Logger,
}

impl<FsT: CacheFs> Cache<FsT> {
    /// Make a new cache rooted at `root`, with a target size of `bytes_used_target`.
    ///
    /// This function will attempt to create all of the directories it needs, panicking if it
    /// cannot. If there are existing entries in the cache, this function will scan them and
    /// incorporate them into the new cache. If there are garbage files in the directories, likely
    /// from incomplete downloads in the previous instance, this function will remove them.
    pub fn new(
        mut fs: FsT,
        root: CacheRoot,
        bytes_used_target: CacheBytesUsedTarget,
        log: slog::Logger,
    ) -> Self {
        let root = root.into_inner();
        let mut path = root.clone();

        path.push("tmp");
        fs.mkdir_recursively(&path);
        for child in fs.read_dir(&path) {
            fs.remove_file(&child);
        }
        path.pop();

        let mut result = Cache {
            fs,
            root,
            entries: CacheMap::default(),
            heap: Heap::default(),
            next_priority: 0,
            bytes_used: 0,
            bytes_used_target: bytes_used_target.into_inner(),
            log,
        };

        path.push("sha256");
        result.fs.mkdir_recursively(&path);
        for child in result.fs.read_dir(&path) {
            if let Ok((digest, size)) = try_read_cache_file(&mut result.fs, &child) {
                result.entries.insert(
                    digest.clone(),
                    CacheEntry::InHeap {
                        bytes_used: size,
                        priority: result.next_priority,
                        heap_index: HeapIndex::default(),
                    },
                );
                result.heap.push(&mut result.entries, digest);
                result.next_priority = result.next_priority.checked_add(1).unwrap();
                result.bytes_used = result.bytes_used.checked_add(size).unwrap();
                continue;
            }
            result.fs.remove_file(&child);
        }
        result.possibly_remove_some();

        debug!(result.log, "cache starting";
            "entries" => %result.entries.len(),
            "bytes_used" => %ByteSize::b(result.bytes_used),
            "byte_used_target" => %ByteSize::b(result.bytes_used_target));

        result
    }

    /// Attempt to get an artifact from the cache.
    ///
    /// See [`GetArtifact`] for details on what the return values mean.
    pub fn get_artifact(&mut self, jid: JobId, digest: Sha256Digest) -> GetArtifact {
        let entry = self
            .entries
            .0
            .entry(digest.clone())
            .or_insert(CacheEntry::Waiting(Vec::default(), HashSet::default()));
        match entry {
            CacheEntry::Waiting(requests, clients) => {
                requests.push(jid);
                if clients.insert(jid.cid) {
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

    /// Tell the cache that a artifact has been successfully retrieved and should be incorporated
    /// into the cache.
    ///
    /// It is assumed that the caller has validated the checksum of the file.
    ///
    /// `path` must be a file in the temporary directory provided by [`Self::tmp_path`]. This
    /// function will move the file from the temporary directory into its correct place in the
    /// cache. For this reason, the file must be on the same file system as the cache directory.
    /// [`Self::tmp_path`] ensures this.
    ///
    /// The return value is a vector of `JobId` that are now no longer blocked by this artifact.
    /// These were `JobId`s provided by previous calls to [`Self::get_artifact`]. Each entry in
    /// this vec has its own refcount, and thus, [`Self::decrement_refcount`] must be called
    /// appropriately.
    pub fn got_artifact(&mut self, digest: Sha256Digest, size: u64, path: &Path) -> Vec<JobId> {
        let mut result = vec![];
        let new_path = self.cache_path(&digest);
        match self.entries.entry(digest.clone()) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(CacheEntry::InHeap {
                    bytes_used: size,
                    priority: self.next_priority,
                    heap_index: HeapIndex::default(),
                });
                self.heap.push(&mut self.entries, digest.clone());
                self.next_priority = self.next_priority.checked_add(1).unwrap();
            }
            hash_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    CacheEntry::Waiting(jids, _) => {
                        let refcount = NonZeroU32::new(u32::try_from(jids.len()).unwrap()).unwrap();
                        std::mem::swap(jids, &mut result);
                        *entry = CacheEntry::InUse {
                            bytes_used: size,
                            refcount,
                        };
                    }
                    CacheEntry::InUse { .. } | CacheEntry::InHeap { .. } => {
                        self.fs.remove_file(path);
                        return vec![];
                    }
                }
            }
        }
        self.fs.rename(path, &new_path);
        self.bytes_used = self.bytes_used.checked_add(size).unwrap();
        debug!(self.log, "cache added artifact";
            "digest" => %digest,
            "artifact_bytes_used" => %ByteSize::b(size),
            "entries" => %self.entries.len(),
            "bytes_used" => %ByteSize::b(self.bytes_used),
            "byte_used_target" => %ByteSize::b(self.bytes_used_target)
        );
        self.possibly_remove_some();
        result
    }

    pub fn read_manifest(
        &mut self,
        digest: Sha256Digest,
    ) -> Result<ManifestReader<impl io::Read + io::Seek + 'static>> {
        let path = self.cache_path(&digest);
        Ok(ManifestReader::new(self.fs.open_file(&path))?)
    }

    /// Decrement the refcount for a digest. Once the refcount for an artifact reaches zero, the
    /// cache is free to remove that artifact as it attempts to keep the cache below the target
    /// size. On the other hand, as long as the refcount is non-zero, the holder of a refcount can
    /// be assured that the artifact won't go away, not matter how large the cache is, and how much
    /// larger it is than the target size.
    pub fn decrement_refcount(&mut self, digest: Sha256Digest) {
        let entry = self.entries.get_mut(&digest).unwrap();
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

    /// Notify the cache that a client disconnected.
    ///
    /// This will clear out any `JobId`s from `cid` for any waiting cache entries, guaranteeing
    /// that `got_artifact` will never return a `JobId` with `cid` in it.
    ///
    /// This fuction does nothing about refcounts held on behalf of the given client. The Cache
    /// doesn't track which client holds which refcounts. When a client disconnects, its the
    /// caller's responsibility to also call `decrement_refcount` appropriately.
    pub fn client_disconnected(&mut self, cid: ClientId) {
        self.entries.retain(|_, e| {
            let CacheEntry::Waiting(jids, clients) = e else {
                return true;
            };
            jids.retain(|jid| jid.cid != cid);
            clients.retain(|c| *c != cid);
            !jids.is_empty()
        })
    }

    /// Get an artifact for a worker.
    ///
    /// On success, this will give the caller a refcount as well as the path to the artifact and
    /// the size of the file in bytes.
    ///
    /// See the comment for [`GetArtifactForWorkerError`].
    pub fn get_artifact_for_worker(
        &mut self,
        digest: &Sha256Digest,
    ) -> Result<(PathBuf, u64), GetArtifactForWorkerError> {
        let Some(CacheEntry::InUse {
            bytes_used,
            refcount,
        }) = self.entries.get_mut(digest)
        else {
            return Err(GetArtifactForWorkerError);
        };
        *refcount = refcount.checked_add(1).unwrap();
        let bytes_used = *bytes_used;
        Ok((self.cache_path(digest), bytes_used))
    }

    /// Return a [`PathBuf`] that contains the temporary directory for the cache. This is where
    /// inbound artifacts should go before [`Self::got_artifact`] is called.
    pub fn tmp_path(&self) -> PathBuf {
        let mut path = self.root.clone();
        path.push("tmp");
        path
    }

    /// Return the path of a cached artifact.
    fn cache_path(&self, digest: &Sha256Digest) -> PathBuf {
        let mut path = self.root.clone();
        path.push("sha256");
        path.push(format!("{digest}.bin"));
        path
    }

    /// Try to ensure that the size of the cache is smaller than or equal to `byte_used_target`.
    fn possibly_remove_some(&mut self) {
        while self.bytes_used > self.bytes_used_target {
            let Some(digest) = self.heap.pop(&mut self.entries) else {
                break;
            };
            let Some(CacheEntry::InHeap { bytes_used, .. }) = self.entries.remove(&digest) else {
                panic!("Entry popped off of heap was in unexpected state");
            };
            self.fs.remove_file(&self.cache_path(&digest));
            self.bytes_used = self.bytes_used.checked_sub(bytes_used).unwrap();
            debug!(self.log, "cache removed artifact";
                "digest" => %digest,
                "artifact_bytes_used" => %ByteSize::b(bytes_used),
                "entries" => %self.entries.len(),
                "bytes_used" => %ByteSize::b(self.bytes_used),
                "byte_used_target" => %ByteSize::b(self.bytes_used_target)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maelstrom_base::manifest::{
        ManifestEntry, ManifestEntryData, ManifestEntryMetadata, ManifestWriter, Mode,
        UnixTimestamp,
    };
    use maelstrom_test::*;
    use std::{cell::RefCell, rc::Rc};
    use TestMessage::*;

    #[derive(Debug, PartialEq)]
    enum TestMessage {
        Rename(PathBuf, PathBuf),
        Remove(PathBuf),
        MkdirRecursively(PathBuf),
        ReadDir(PathBuf),
        FileSize(PathBuf),
        OpenFile(PathBuf),
    }

    #[derive(Default)]
    struct TestCacheFs {
        messages: Vec<TestMessage>,
        files: HashMap<PathBuf, Vec<u8>>,
        directories: HashMap<PathBuf, Vec<PathBuf>>,
    }

    impl CacheFs for Rc<RefCell<TestCacheFs>> {
        fn rename(&mut self, source: &Path, destination: &Path) {
            self.borrow_mut()
                .messages
                .push(Rename(source.to_owned(), destination.to_owned()));
        }

        fn remove_file(&mut self, target: &Path) {
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
            self.borrow().files.get(path).unwrap().len() as u64
        }

        type File = std::io::Cursor<Vec<u8>>;

        fn open_file(&mut self, path: &Path) -> Self::File {
            self.borrow_mut().messages.push(OpenFile(path.to_owned()));
            std::io::Cursor::new(self.borrow().files.get(path).unwrap().clone())
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
                slog::Logger::root(slog::Discard, slog::o!()),
            );
            Fixture { fs, cache }
        }

        fn new_and_clear_fs_operations(fs: TestCacheFs, bytes_used_target: u64) -> Self {
            let mut result = Self::new(fs, bytes_used_target);
            result.clear_fs_operations();
            result
        }

        fn clear_fs_operations(&mut self) {
            self.fs.borrow_mut().messages.clear();
        }

        fn expect_fs_operations(&mut self, expected: Vec<TestMessage>) {
            assert!(
                *self.fs.borrow().messages == expected,
                "Expected messages didn't match actual messages.\n{}",
                colored_diff::PrettyDifference {
                    expected: &format!("{:#?}", expected),
                    actual: &format!("{:#?}", self.fs.borrow().messages)
                }
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
            size: u64,
            path: PathBuf,
            expected: Vec<JobId>,
            expected_fs_operations: Vec<TestMessage>,
        ) {
            let result = self.cache.got_artifact(digest, size, &path);
            assert_eq!(result, expected);
            self.expect_fs_operations(expected_fs_operations);
        }

        fn got_artifact_ign(&mut self, digest: Sha256Digest, size: u64, path: PathBuf) {
            _ = self.cache.got_artifact(digest, size, &path);
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
            expected: Result<(PathBuf, u64), GetArtifactForWorkerError>,
        ) {
            assert_eq!(self.cache.get_artifact_for_worker(&digest), expected);
        }

        fn read_manifest(
            &mut self,
            digest: Sha256Digest,
            expected_fs_operations: Vec<TestMessage>,
        ) -> Result<ManifestReader<impl io::Read + io::Seek + 'static>> {
            let reader = self.cache.read_manifest(digest);
            self.expect_fs_operations(expected_fs_operations);
            reader
        }
    }

    #[test]
    fn new_with_empty_fs() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.expect_fs_operations(vec![
            MkdirRecursively(path_buf!("/z/tmp")),
            ReadDir(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
        ]);
    }

    #[test]
    fn new_with_garbage_in_tmp() {
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
    fn new_with_garbage_and_treasure_in_sha256() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![
                    path_buf!("/z/sha256/one"),
                    long_path!("/z/sha256", 1, "bin"),
                    path_buf!("/z/sha256/two.tar"),
                    long_path!("/z/sha256", 2, "bin"),
                    long_path!("/z/sha256", 3, "tar.gz"),
                ],
            )]),
            files: HashMap::from([
                (long_path!("/z/sha256", 1, "bin"), vec![0; 1000]),
                (long_path!("/z/sha256", 2, "bin"), vec![0; 100]),
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
            FileSize(long_path!("/z/sha256", 1, "bin")),
            Remove(path_buf!("/z/sha256/two.tar")),
            FileSize(long_path!("/z/sha256", 2, "bin")),
            Remove(long_path!("/z/sha256", 3, "tar.gz")),
        ]);
        assert_eq!(fixture.cache.bytes_used, 1100);
    }

    #[test]
    fn new_with_too_much_in_sha256() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![
                    long_path!("/z/sha256", 1, "bin"),
                    long_path!("/z/sha256", 2, "bin"),
                    long_path!("/z/sha256", 3, "bin"),
                ],
            )]),
            files: HashMap::from([
                (long_path!("/z/sha256", 1, "bin"), vec![0; 1001]),
                (long_path!("/z/sha256", 2, "bin"), vec![0; 1002]),
                (long_path!("/z/sha256", 3, "bin"), vec![0; 1003]),
            ]),
            ..Default::default()
        };
        let mut fixture = Fixture::new(fs, 1003);
        fixture.expect_fs_operations(vec![
            MkdirRecursively(path_buf!("/z/tmp")),
            ReadDir(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
            FileSize(long_path!("/z/sha256", 1, "bin")),
            FileSize(long_path!("/z/sha256", 2, "bin")),
            FileSize(long_path!("/z/sha256", 3, "bin")),
            Remove(long_path!("/z/sha256", 1, "bin")),
            Remove(long_path!("/z/sha256", 2, "bin")),
        ]);
        assert_eq!(fixture.cache.bytes_used, 1003);
    }

    #[test]
    fn get_artifact_once() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 1000);
        fixture.get_artifact(jid!(1, 1001), digest!(1), GetArtifact::Get, vec![]);
    }

    #[test]
    fn get_artifact_again_from_same_client() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 1000);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact(jid!(1, 1002), digest!(1), GetArtifact::Wait, vec![]);
    }

    #[test]
    fn get_artifact_again_from_different_client() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 1000);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact(jid!(2, 1001), digest!(1), GetArtifact::Get, vec![]);
    }

    #[test]
    fn get_artifact_in_use() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.got_artifact_ign(digest!(1), 1, short_path!("/z/tmp", 1, "bin"));
        fixture.get_artifact(jid!(2, 1001), digest!(1), GetArtifact::Success, vec![]);

        // Refcount should be 2.
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "bin"))]);
    }

    #[test]
    fn get_artifact_in_heap_removes_from_heap() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![long_path!("/z/sha256", 112358, "bin")],
            )]),
            files: HashMap::from([(long_path!("/z/sha256", 112358, "bin"), vec![0; 1])]),
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
                1,
                short_path!("/z/tmp", i, "bin"),
                vec![],
                vec![Rename(
                    short_path!("/z/tmp", i, "bin"),
                    long_path!("/z/sha256", i, "bin"),
                )],
            );
        }
        for i in 10..100 {
            fixture.got_artifact(
                digest!(i),
                1,
                short_path!("/z/tmp", i, "bin"),
                vec![],
                vec![
                    Rename(
                        short_path!("/z/tmp", i, "bin"),
                        long_path!("/z/sha256", i, "bin"),
                    ),
                    Remove(long_path!("/z/sha256", i - 10, "bin")),
                ],
            );
        }
    }

    #[test]
    fn get_artifact_in_heap_sets_refcount() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![long_path!("/z/sha256", 1, "bin")],
            )]),
            files: HashMap::from([(long_path!("/z/sha256", 1, "bin"), vec![0; 1])]),
            ..Default::default()
        };
        let mut fixture = Fixture::new_and_clear_fs_operations(fs, 1);
        fixture.get_artifact(jid!(1, 1001), digest!(1), GetArtifact::Success, vec![]);

        fixture.get_artifact_ign(jid!(1, 1002), digest!(2));
        fixture.got_artifact_ign(digest!(2), 1, short_path!("/z/tmp", 2, "bin"));

        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "bin"))]);
    }

    #[test]
    fn got_artifact_no_entry() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 1000);
        fixture.got_artifact(
            digest!(1),
            10,
            short_path!("/z/tmp", 1, "bin"),
            vec![],
            vec![Rename(
                short_path!("/z/tmp", 1, "bin"),
                long_path!("/z/sha256", 1, "bin"),
            )],
        );
        assert_eq!(fixture.cache.bytes_used, 10);
    }

    #[test]
    fn got_artifact_no_entry_pushes_out_old() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![long_path!("/z/sha256", 1, "bin")],
            )]),
            files: HashMap::from([(long_path!("/z/sha256", 1, "bin"), vec![0; 1])]),
            ..Default::default()
        };
        let mut fixture = Fixture::new_and_clear_fs_operations(fs, 1);
        fixture.got_artifact(
            digest!(2),
            1,
            short_path!("/z/tmp", 1, "bin"),
            vec![],
            vec![
                Rename(
                    short_path!("/z/tmp", 1, "bin"),
                    long_path!("/z/sha256", 2, "bin"),
                ),
                Remove(long_path!("/z/sha256", 1, "bin")),
            ],
        );
        assert_eq!(fixture.cache.bytes_used, 1);
    }

    #[test]
    fn got_artifact_no_entry_goes_into_heap() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 10);
        for i in 1..=10 {
            fixture.got_artifact(
                digest!(i),
                1,
                short_path!("/z/tmp", i, "bin"),
                vec![],
                vec![Rename(
                    short_path!("/z/tmp", i, "bin"),
                    long_path!("/z/sha256", i, "bin"),
                )],
            );
        }
        for i in 11..=20 {
            fixture.got_artifact(
                digest!(i),
                1,
                short_path!("/z/tmp", i, "bin"),
                vec![],
                vec![
                    Rename(
                        short_path!("/z/tmp", i, "bin"),
                        long_path!("/z/sha256", i, "bin"),
                    ),
                    Remove(long_path!("/z/sha256", i - 10, "bin")),
                ],
            );
        }
    }

    #[test]
    fn got_artifact_with_waiters() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact_ign(jid!(2, 1001), digest!(1));
        fixture.get_artifact_ign(jid!(1, 1002), digest!(1));

        fixture.got_artifact(
            digest!(1),
            10,
            short_path!("/z/tmp", 1, "bin"),
            vec![jid!(1, 1001), jid!(2, 1001), jid!(1, 1002)],
            vec![Rename(
                short_path!("/z/tmp", 1, "bin"),
                long_path!("/z/sha256", 1, "bin"),
            )],
        );
        assert_eq!(fixture.cache.bytes_used, 10);

        // Refcount should be 3.
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "bin"))]);
    }

    #[test]
    fn got_artifact_with_waiter_pushes_out_old() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![long_path!("/z/sha256", 1, "bin")],
            )]),
            files: HashMap::from([(long_path!("/z/sha256", 1, "bin"), vec![0; 1])]),
            ..Default::default()
        };
        let mut fixture = Fixture::new_and_clear_fs_operations(fs, 1);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(2));
        fixture.got_artifact(
            digest!(2),
            1,
            short_path!("/z/tmp", 1, "bin"),
            vec![jid!(1, 1001)],
            vec![
                Rename(
                    short_path!("/z/tmp", 1, "bin"),
                    long_path!("/z/sha256", 2, "bin"),
                ),
                Remove(long_path!("/z/sha256", 1, "bin")),
            ],
        );
        assert_eq!(fixture.cache.bytes_used, 1);
    }

    #[test]
    fn got_artifact_already_in_use() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.got_artifact_ign(digest!(1), 10, short_path!("/z/tmp", 1, "bin"));

        fixture.got_artifact(
            digest!(1),
            10,
            short_path!("/z/tmp", 2, "bin"),
            vec![],
            vec![Remove(short_path!("/z/tmp", 2, "bin"))],
        );

        // Refcount should be 1.
        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "bin"))]);
    }

    #[test]
    fn got_artifact_already_in_cache() {
        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![long_path!("/z/sha256", 1, "bin")],
            )]),
            files: HashMap::from([(long_path!("/z/sha256", 1, "bin"), vec![0; 1000])]),
            ..Default::default()
        };
        let mut fixture = Fixture::new_and_clear_fs_operations(fs, 1000);
        fixture.got_artifact(
            digest!(1),
            10,
            short_path!("/z/tmp", 1, "bin"),
            vec![],
            vec![Remove(short_path!("/z/tmp", 1, "bin"))],
        );
    }

    #[test]
    fn decrement_refcount_sets_priority_properly() {
        let mut fixture = Fixture::new_and_clear_fs_operations(TestCacheFs::default(), 10);
        for i in 0..10 {
            fixture.get_artifact_ign(jid!(1, 1000 + i), digest!(i));
            fixture.got_artifact_ign(digest!(i), 1, short_path!("/z/tmp", i, "bin"));
            fixture.decrement_refcount(digest!(i), vec![]);
        }
        for i in 10..100 {
            fixture.get_artifact_ign(jid!(1, 1000 + i), digest!(i));
            fixture.got_artifact(
                digest!(i),
                1,
                short_path!("/z/tmp", i, "bin"),
                vec![jid!(1, 1000 + i)],
                vec![
                    Rename(
                        short_path!("/z/tmp", i, "bin"),
                        long_path!("/z/sha256", i, "bin"),
                    ),
                    Remove(long_path!("/z/sha256", i - 10, "bin")),
                ],
            );
            fixture.decrement_refcount(digest!(i), vec![]);
        }
    }

    #[test]
    #[should_panic]
    fn decrement_refcount_not_in_cache_panics() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 10);
        fixture.decrement_refcount_ign(digest!(1));
    }

    #[test]
    #[should_panic]
    fn decrement_refcount_waiting_panics() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 10);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.decrement_refcount_ign(digest!(1));
    }

    #[test]
    #[should_panic]
    fn decrement_refcount_in_heap_panics() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 10);
        fixture.got_artifact_ign(digest!(1), 1, short_path!("/z/tmp", 1, "bin"));
        fixture.decrement_refcount_ign(digest!(1));
    }

    #[test]
    fn client_disconnected_one_client_one_jid() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.cache.client_disconnected(cid!(1));
        fixture.got_artifact(
            digest!(1),
            1,
            short_path!("/z/tmp", 1, "bin"),
            vec![],
            vec![
                Rename(
                    short_path!("/z/tmp", 1, "bin"),
                    long_path!("/z/sha256", 1, "bin"),
                ),
                Remove(long_path!("/z/sha256", 1, "bin")),
            ],
        );
    }

    #[test]
    fn client_disconnected_one_client_three_jids() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact_ign(jid!(1, 1002), digest!(1));
        fixture.get_artifact_ign(jid!(1, 1003), digest!(1));
        fixture.cache.client_disconnected(cid!(1));
        fixture.got_artifact(
            digest!(1),
            1,
            short_path!("/z/tmp", 1, "bin"),
            vec![],
            vec![
                Rename(
                    short_path!("/z/tmp", 1, "bin"),
                    long_path!("/z/sha256", 1, "bin"),
                ),
                Remove(long_path!("/z/sha256", 1, "bin")),
            ],
        );
    }

    #[test]
    fn client_disconnected_many_clients() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact_ign(jid!(1, 1002), digest!(1));
        fixture.get_artifact_ign(jid!(3, 1003), digest!(1));
        fixture.cache.client_disconnected(cid!(1));
        fixture.get_artifact(jid!(1, 1003), digest!(1), GetArtifact::Get, vec![]);
        fixture.get_artifact(jid!(3, 1003), digest!(1), GetArtifact::Wait, vec![]);
        fixture.got_artifact(
            digest!(1),
            1,
            short_path!("/z/tmp", 1, "bin"),
            vec![jid!(3, 1003), jid!(1, 1003), jid!(3, 1003)],
            vec![Rename(
                short_path!("/z/tmp", 1, "bin"),
                long_path!("/z/sha256", 1, "bin"),
            )],
        );
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "bin"))]);
    }

    #[test]
    fn get_artifact_for_worker_no_entry() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_for_worker(digest!(1), Err(GetArtifactForWorkerError));
    }

    #[test]
    fn get_artifact_for_worker_waiting() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.get_artifact_for_worker(digest!(1), Err(GetArtifactForWorkerError));
    }

    #[test]
    fn get_artifact_for_worker_in_cache() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 1);
        fixture.got_artifact_ign(digest!(1), 1, short_path!("/z/tmp", 1, "bin"));
        fixture.get_artifact_for_worker(digest!(1), Err(GetArtifactForWorkerError));
    }

    #[test]
    fn get_artifact_for_worker_in_use() {
        let mut fixture = Fixture::new(TestCacheFs::default(), 0);
        fixture.get_artifact_ign(jid!(1, 1001), digest!(1));
        fixture.got_artifact_ign(digest!(1), 42, short_path!("/z/tmp", 1, "bin"));
        fixture.get_artifact_for_worker(digest!(1), Ok((long_path!("/z/sha256", 1, "bin"), 42)));

        // Refcount should be 2.
        fixture.decrement_refcount(digest!(1), vec![]);
        fixture.decrement_refcount(digest!(1), vec![Remove(long_path!("/z/sha256", 1, "bin"))]);
    }

    #[test]
    fn tmp_path() {
        let fixture = Fixture::new(TestCacheFs::default(), 0);
        assert_eq!(fixture.cache.tmp_path(), PathBuf::from("/z/tmp"));
    }

    #[test]
    fn read_manifest() {
        let mut manifest_data = vec![];
        let mut writer = ManifestWriter::new(&mut manifest_data).unwrap();
        let entries = vec![ManifestEntry {
            path: "foobar.txt".into(),
            metadata: ManifestEntryMetadata {
                size: 11,
                mode: Mode(0o0555),
                mtime: UnixTimestamp(1705538554),
            },
            data: ManifestEntryData::File(Some(digest![43])),
        }];
        writer.write_entries(&entries).unwrap();

        let fs = TestCacheFs {
            directories: HashMap::from([(
                path_buf!("/z/sha256"),
                vec![long_path!("/z/sha256", 1, "bin")],
            )]),
            files: HashMap::from([(long_path!("/z/sha256", 1, "bin"), manifest_data)]),
            ..Default::default()
        };
        let mut fixture = Fixture::new_and_clear_fs_operations(fs, 1000);
        let reader = fixture
            .read_manifest(
                digest![1],
                vec![OpenFile(long_path!("/z/sha256", 1, "bin"))],
            )
            .unwrap();
        assert_eq!(reader.map(|e| e.unwrap()).collect::<Vec<_>>(), entries);
    }
}
