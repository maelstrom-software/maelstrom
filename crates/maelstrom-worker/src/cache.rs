//! Manage downloading, extracting, and storing of artifacts specified by jobs.

pub mod fs;

use bytesize::ByteSize;
use fs::{FileType, Fs};
use maelstrom_base::{JobId, Sha256Digest};
use maelstrom_util::{
    config::common::CacheSize,
    heap::{Heap, HeapDeps, HeapIndex},
    root::RootBuf,
};
use slog::{debug, Logger};
use std::{
    cmp::Ordering,
    collections::{hash_map::Entry as HashEntry, HashMap, HashSet},
    ffi::OsString,
    fmt::{self, Debug, Display, Formatter},
    hash::Hash,
    iter::IntoIterator,
    mem,
    num::NonZeroU32,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    string::ToString,
};

const CACHEDIR_TAG_CONTENTS: [u8; 43] = *b"Signature: 8a477f597d28d172789f06886806bc55";

/// Type returned from [`Cache::get_artifact`].
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum GetArtifact {
    /// The artifact is in the cache. The caller has been given a reference that must later be
    /// released by calling [`Cache::decrement_ref_count`]. The artifact can be found at the path
    /// returned by [`Cache::cache_path`] as long as the caller has an outstanding ref count.
    Success,

    /// The artifact is not in the cache and is currently being retrieved. There is nothing for the
    /// caller to do other than wait. The caller's [`JobId`] will be returned at some point from a
    /// call to [`Cache::got_artifact_success`] or [`Cache::got_artifact_failure`].
    Wait,

    /// The artifact is not in the cache but is not currently being retrieved. It's caller's
    /// responsibility to start the retrieval process. The artifact should be put in the path
    /// provided by [`Cache::cache_path`], and then either [`Cache::got_artifact_success`] or
    /// [`Cache::got_artifact_failure`] should be called.
    Get,
}

/// Type passed to [`Cache::got_artifact_success`].
#[derive(Clone)]
pub enum GotArtifact<FsT: Fs> {
    Symlink { target: PathBuf },
    File { source: FsT::TempFile },
    Directory { source: FsT::TempDir, size: u64 },
}

impl<FsT: Fs> Debug for GotArtifact<FsT>
where
    FsT::TempFile: Debug,
    FsT::TempDir: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Symlink { target } => f
                .debug_struct("GotArtifact::Symlink")
                .field("target", target)
                .finish(),
            Self::File { source } => f
                .debug_struct("GotArtifact::File")
                .field("source", source)
                .finish(),
            Self::Directory { source, size } => f
                .debug_struct("GotArtifact::File")
                .field("source", source)
                .field("size", size)
                .finish(),
        }
    }
}

impl<FsT: Fs> Eq for GotArtifact<FsT>
where
    FsT::TempFile: Eq,
    FsT::TempDir: Eq,
{
}

impl<FsT: Fs> Ord for GotArtifact<FsT>
where
    FsT::TempFile: Ord,
    FsT::TempDir: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (
                Self::Symlink {
                    target: self_target,
                },
                Self::Symlink {
                    target: other_target,
                },
            ) => self_target.cmp(other_target),
            (
                Self::File {
                    source: self_source,
                },
                Self::File {
                    source: other_source,
                },
            ) => self_source.cmp(other_source),
            (
                Self::Directory {
                    source: self_source,
                    size: self_size,
                },
                Self::Directory {
                    source: other_source,
                    size: other_size,
                },
            ) => self_source
                .cmp(other_source)
                .then(self_size.cmp(other_size)),
            (Self::Symlink { .. }, _) => Ordering::Less,
            (Self::File { .. }, Self::Directory { .. }) => Ordering::Less,
            (Self::File { .. }, Self::Symlink { .. }) => Ordering::Greater,
            (Self::Directory { .. }, _) => Ordering::Greater,
        }
    }
}

impl<FsT: Fs> PartialEq for GotArtifact<FsT>
where
    FsT::TempFile: PartialEq,
    FsT::TempDir: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Symlink {
                    target: self_target,
                },
                Self::Symlink {
                    target: other_target,
                },
            ) => self_target.eq(other_target),
            (
                Self::File {
                    source: self_source,
                },
                Self::File {
                    source: other_source,
                },
            ) => self_source.eq(other_source),
            (
                Self::Directory {
                    source: self_source,
                    size: self_size,
                },
                Self::Directory {
                    source: other_source,
                    size: other_size,
                },
            ) => self_source.eq(other_source) && self_size.eq(other_size),
            _ => false,
        }
    }
}

impl<FsT: Fs> PartialOrd for GotArtifact<FsT>
where
    GotArtifact<FsT>: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub trait KeyKind: Clone + Copy + Debug + Display + Eq + Hash + PartialEq {
    type Iterator: Iterator<Item = Self>;
    fn iter() -> Self::Iterator;
}

#[derive(
    Clone, Copy, Debug, strum::Display, PartialEq, Eq, PartialOrd, Ord, Hash, strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum EntryKind {
    Blob,
    BottomFsLayer,
    UpperFsLayer,
}

impl KeyKind for EntryKind {
    type Iterator = <Self as strum::IntoEnumIterator>::Iterator;

    fn iter() -> Self::Iterator {
        <Self as strum::IntoEnumIterator>::iter()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Key<KeyKindT> {
    pub kind: KeyKindT,
    pub digest: Sha256Digest,
}

impl<KeyKindT> Key<KeyKindT> {
    pub fn new(kind: KeyKindT, digest: Sha256Digest) -> Self {
        Self { kind, digest }
    }
}

/// An entry for a specific [`Key`] in the [`Cache`]'s hash table. There is one of these for every
/// entry in the per-kind subdirectories of the `sha256` subdirectory of the [`Cache`]'s root
/// directory.
enum Entry {
    /// The artifact is being gotten by one of the clients.
    Getting(Vec<JobId>),

    /// The artifact has been successfully gotten, and is currently being used by at least one job.
    /// We reference count this state since there may be multiple jobs using the same artifact.
    InUse {
        file_type: FileType,
        bytes_used: u64,
        ref_count: NonZeroU32,
    },

    /// The artifact has been successfully gotten, but no jobs are currently using it. The
    /// `priority` is provided by [`Cache`] and is used by the [`Heap`] to determine which entry
    /// should be removed first when freeing up space.
    InHeap {
        file_type: FileType,
        bytes_used: u64,
        priority: u64,
        heap_index: HeapIndex,
    },
}

/// An implementation of the "newtype" pattern so that we can implement [`HeapDeps`] on a
/// [`HashMap`].
struct Map<KeyKindT: KeyKind>(HashMap<Key<KeyKindT>, Entry>);

impl<KeyKindT: KeyKind> Default for Map<KeyKindT> {
    fn default() -> Self {
        Self(HashMap::default())
    }
}

impl<KeyKindT: KeyKind> Deref for Map<KeyKindT> {
    type Target = HashMap<Key<KeyKindT>, Entry>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<KeyKindT: KeyKind> DerefMut for Map<KeyKindT> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<KeyKindT: KeyKind> HeapDeps for Map<KeyKindT> {
    type Element = Key<KeyKindT>;

    fn is_element_less_than(&self, lhs: &Self::Element, rhs: &Self::Element) -> bool {
        let lhs_priority = match self.get(lhs) {
            Some(Entry::InHeap { priority, .. }) => *priority,
            _ => panic!("Element should be in heap"),
        };
        let rhs_priority = match self.get(rhs) {
            Some(Entry::InHeap { priority, .. }) => *priority,
            _ => panic!("Element should be in heap"),
        };
        lhs_priority.cmp(&rhs_priority) == Ordering::Less
    }

    fn update_index(&mut self, elem: &Self::Element, idx: HeapIndex) {
        match self.get_mut(elem) {
            Some(Entry::InHeap { heap_index, .. }) => *heap_index = idx,
            _ => panic!("Element should be in heap"),
        };
    }
}

pub struct CacheDir;

/// Manage a directory of downloaded, extracted artifacts. Coordinate fetching of these artifacts,
/// and removing them when they are no longer in use and the amount of space used by the directory
/// has grown too large.
pub struct Cache<FsT, KeyKindT: KeyKind> {
    fs: FsT,
    root: PathBuf,
    entries: Map<KeyKindT>,
    heap: Heap<Map<KeyKindT>>,
    next_priority: u64,
    bytes_used: u64,
    bytes_used_target: u64,
    log: Logger,
}

impl<FsT: Fs, KeyKindT: KeyKind> Cache<FsT, KeyKindT> {
    /// Create a new [Cache] rooted at `root`. The directory `root` and all necessary ancestors
    /// will be created, along with `{root}/removing` and `{root}/{kind}/sha256`. Any pre-existing
    /// entries in `{root}/removing` and `{root}/{kind}/sha256` will be removed. That implies that
    /// the [Cache] doesn't currently keep data stored across invocations.
    ///
    /// `bytes_used_target` is the goal on-disk size for the cache. The cache will periodically grow
    /// larger than this size, but then shrink back down to this size. Ideally, the cache would use
    /// this as a hard upper bound, but that's not how it currently works.
    pub fn new(fs: FsT, root: RootBuf<CacheDir>, size: CacheSize, log: Logger) -> Self {
        let root = root.into_path_buf();

        // Everything in the `removing` subdirectory needs to be removed in the background. Start
        // those threads up.
        let removing = root.join("removing");
        fs.mkdir_recursively(&removing).unwrap();
        for dirent in fs.read_dir(&removing).unwrap() {
            let (name, metadata) = dirent.unwrap();
            let path = removing.join(name);
            if metadata.type_ == FileType::Directory {
                fs.rmdir_recursively_on_thread(path).unwrap();
            } else {
                fs.remove(&path).unwrap();
            }
        }

        // If there are any files or directories in the top-level directory that shouldn't be
        // there, move them to `removing` and then remove them in the background. Note that we
        // don't retain "tmp". We want to empty it out when we start up and create a new one for
        // this instance.
        Self::remove_all_from_directory_except(
            &fs,
            &root,
            &root,
            ["CACHEDIR.TAG", "removing", "sha256"],
        );

        let cachedir_tag = root.join("CACHEDIR.TAG");
        if fs.metadata(&cachedir_tag).unwrap().is_none() {
            fs.create_file(&cachedir_tag, &CACHEDIR_TAG_CONTENTS)
                .unwrap();
        }

        fs.mkdir_recursively(&root.join("tmp")).unwrap();

        let sha256 = root.join("sha256");
        fs.mkdir_recursively(&sha256).unwrap();
        Self::remove_all_from_directory_except(&fs, &root, &sha256, KeyKindT::iter());

        for kind in KeyKindT::iter() {
            let kind_dir = sha256.join(kind.to_string());
            if let Some(metadata) = fs.metadata(&kind_dir).unwrap() {
                Self::remove_in_background(&fs, &root, &kind_dir, metadata.type_);
            }
            fs.mkdir_recursively(&kind_dir).unwrap();
        }

        Cache {
            fs,
            root,
            entries: Map::default(),
            heap: Heap::default(),
            next_priority: 0,
            bytes_used: 0,
            bytes_used_target: size.into(),
            log,
        }
    }

    /// Attempt to fetch `artifact` from the cache. See [`GetArtifact`] for the meaning of the
    /// return values.
    pub fn get_artifact(
        &mut self,
        kind: KeyKindT,
        digest: Sha256Digest,
        jid: JobId,
    ) -> GetArtifact {
        match self.entries.entry(Key::new(kind, digest)) {
            HashEntry::Vacant(entry) => {
                entry.insert(Entry::Getting(vec![jid]));
                GetArtifact::Get
            }
            HashEntry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Entry::Getting(jobs) => {
                        jobs.push(jid);
                        GetArtifact::Wait
                    }
                    Entry::InUse { ref_count, .. } => {
                        *ref_count = ref_count.checked_add(1).unwrap();
                        GetArtifact::Success
                    }
                    Entry::InHeap {
                        file_type,
                        bytes_used,
                        heap_index,
                        ..
                    } => {
                        let heap_index = *heap_index;
                        *entry = Entry::InUse {
                            file_type: *file_type,
                            ref_count: NonZeroU32::new(1).unwrap(),
                            bytes_used: *bytes_used,
                        };
                        self.heap.remove(&mut self.entries, heap_index);
                        GetArtifact::Success
                    }
                }
            }
        }
    }

    /// Notify the cache that an artifact fetch has failed. The returned vector lists the jobs that
    /// are affected and that need to be canceled.
    pub fn got_artifact_failure(&mut self, kind: KeyKindT, digest: &Sha256Digest) -> Vec<JobId> {
        let Some(Entry::Getting(jobs)) = self.entries.remove(&Key::new(kind, digest.clone()))
        else {
            panic!("Got got_artifact in unexpected state");
        };
        jobs
    }

    /// Notify the cache that an artifact fetch has successfully completed. The returned vector
    /// lists the jobs that are affected, and the path they can use to access the artifact.
    pub fn got_artifact_success(
        &mut self,
        kind: KeyKindT,
        digest: &Sha256Digest,
        artifact: GotArtifact<FsT>,
    ) -> Vec<JobId> {
        let path = self.cache_path(kind, digest);
        let (file_type, bytes_used) = match artifact {
            GotArtifact::Directory { source, size } => {
                self.fs.persist_temp_dir(source, &path).unwrap();
                (FileType::Directory, size)
            }
            GotArtifact::File { source } => {
                self.fs.persist_temp_file(source, &path).unwrap();
                let metadata = self.fs.metadata(&path).unwrap().unwrap();
                assert!(matches!(metadata.type_, FileType::File));
                (FileType::File, metadata.size)
            }
            GotArtifact::Symlink { target } => {
                self.fs.symlink(&target, &path).unwrap();
                let metadata = self.fs.metadata(&path).unwrap().unwrap();
                assert!(matches!(metadata.type_, FileType::Symlink));
                (FileType::Symlink, metadata.size)
            }
        };
        let key = Key::new(kind, digest.clone());
        let entry = self
            .entries
            .get_mut(&key)
            .expect("Got DownloadingAndExtracting in unexpected state");
        let Entry::Getting(jobs) = entry else {
            panic!("Got DownloadingAndExtracting in unexpected state");
        };
        let ref_count = jobs.len().try_into().unwrap();
        let jobs = mem::take(jobs);
        // Reference count must be > 0 since we don't allow cancellation of gets.
        *entry = Entry::InUse {
            file_type,
            bytes_used,
            ref_count: NonZeroU32::new(ref_count).unwrap(),
        };
        self.bytes_used = self.bytes_used.checked_add(bytes_used).unwrap();
        debug!(self.log, "cache added artifact";
            "kind" => ?kind,
            "digest" => %digest,
            "artifact_bytes_used" => %ByteSize::b(bytes_used),
            "entries" => %self.entries.len(),
            "file_type" => %file_type,
            "bytes_used" => %ByteSize::b(self.bytes_used),
            "byte_used_target" => %ByteSize::b(self.bytes_used_target)
        );
        self.possibly_remove_some();
        jobs
    }

    /// Notify the cache that a reference to an artifact is no longer needed.
    pub fn decrement_ref_count(&mut self, kind: KeyKindT, digest: &Sha256Digest) {
        let key = Key::new(kind, digest.clone());
        let entry = self
            .entries
            .get_mut(&key)
            .expect("Got decrement_ref_count in unexpected state");
        let Entry::InUse {
            file_type,
            bytes_used,
            ref_count,
        } = entry
        else {
            panic!("Got decrement_ref_count with existing zero reference count");
        };
        match NonZeroU32::new(ref_count.get() - 1) {
            Some(new_ref_count) => *ref_count = new_ref_count,
            None => {
                *entry = Entry::InHeap {
                    file_type: *file_type,
                    bytes_used: *bytes_used,
                    priority: self.next_priority,
                    heap_index: HeapIndex::default(),
                };
                self.heap.push(&mut self.entries, key.clone());
                self.next_priority = self.next_priority.checked_add(1).unwrap();
                self.possibly_remove_some();
            }
        }
    }

    /// Return the directory path for the artifact referenced by `digest`.
    pub fn cache_path(&self, kind: KeyKindT, digest: &Sha256Digest) -> PathBuf {
        let mut path = self.root.join("sha256");
        path.push(kind.to_string());
        path.push(digest.to_string());
        path
    }

    pub fn temp_dir(&self) -> FsT::TempDir {
        self.fs.temp_dir(&self.root.join("tmp")).unwrap()
    }

    pub fn temp_file(&self) -> FsT::TempFile {
        self.fs.temp_file(&self.root.join("tmp")).unwrap()
    }

    fn remove_all_from_directory_except<S>(
        fs: &impl Fs,
        root: &Path,
        dir: &Path,
        except: impl IntoIterator<Item = S>,
    ) where
        S: ToString,
    {
        let except = except
            .into_iter()
            .map(|e| OsString::from(e.to_string()))
            .collect::<HashSet<_>>();
        for (entry_name, metadata) in fs.read_dir(dir).unwrap().map(|de| de.unwrap()) {
            if !except.contains(&entry_name) {
                Self::remove_in_background(fs, root, &dir.join(entry_name), metadata.type_);
            }
        }
    }

    /// Remove all files and directories rooted in `source` in a separate thread.
    fn remove_in_background(fs: &impl Fs, root: &Path, source: &Path, type_: FileType) {
        if type_ == FileType::Directory {
            let removing = root.join("removing");
            let target = loop {
                let target = removing.join(format!("{:016x}", fs.rand_u64()));
                if fs.metadata(&target).unwrap().is_none() {
                    break target;
                }
            };
            fs.rename(source, &target).unwrap();
            fs.rmdir_recursively_on_thread(target).unwrap();
        } else {
            fs.remove(source).unwrap();
        }
    }

    /// Check to see if the cache is over its goal size, and if so, try to remove the least
    /// recently used artifacts.
    fn possibly_remove_some(&mut self) {
        while self.bytes_used > self.bytes_used_target {
            let Some(key) = self.heap.pop(&mut self.entries) else {
                break;
            };
            let Some(Entry::InHeap {
                file_type,
                bytes_used,
                ..
            }) = self.entries.remove(&key)
            else {
                panic!("Entry popped off of heap was in unexpected state");
            };
            let cache_path = self.cache_path(key.kind, &key.digest);
            Self::remove_in_background(&self.fs, &self.root, &cache_path, file_type);
            self.bytes_used = self.bytes_used.checked_sub(bytes_used).unwrap();
            debug!(self.log, "cache removed artifact";
                "key" => ?key,
                "artifact_bytes_used" => %ByteSize::b(bytes_used),
                "entries" => %self.entries.len(),
                "bytes_used" => %ByteSize::b(self.bytes_used),
                "byte_used_target" => %ByteSize::b(self.bytes_used_target)
            );
        }
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
    use crate::cache::fs::Metadata;
    use fs::{
        test::{self, fs},
        TempDir as _, TempFile as _,
    };
    use maelstrom_test::*;
    use slog::{o, Discard};
    use std::{iter, rc::Rc};
    use TestKeyKind::*;

    #[derive(Clone, Copy, Debug, strum::Display, strum::EnumIter, Eq, Hash, PartialEq)]
    #[strum(serialize_all = "snake_case")]
    pub enum TestKeyKind {
        Apple,
        Orange,
    }

    impl KeyKind for TestKeyKind {
        type Iterator = <Self as strum::IntoEnumIterator>::Iterator;

        fn iter() -> Self::Iterator {
            <Self as strum::IntoEnumIterator>::iter()
        }
    }

    struct Fixture {
        cache: Cache<Rc<test::Fs>, TestKeyKind>,
        fs: Rc<test::Fs>,
    }

    impl Fixture {
        fn new(bytes_used_target: u64, root: test::Entry) -> Self {
            let fs = Rc::new(test::Fs::new(root));
            let cache = Cache::new(
                fs.clone(),
                "/z".parse().unwrap(),
                ByteSize::b(bytes_used_target).into(),
                Logger::root(Discard, o!()),
            );
            Fixture { fs, cache }
        }

        #[track_caller]
        fn assert_fs(&self, expected_fs: test::Entry) {
            self.fs.assert_tree(expected_fs);
        }

        #[track_caller]
        fn assert_pending_recursive_rmdirs<const N: usize>(&self, paths: [&str; N]) {
            self.fs.assert_recursive_rmdirs(HashSet::from_iter(
                paths.into_iter().map(|s| s.to_string()),
            ))
        }

        #[track_caller]
        fn assert_file_exists(
            &self,
            kind: TestKeyKind,
            digest: Sha256Digest,
            expected_metadata: Metadata,
        ) {
            let cache_path = self.cache.cache_path(kind, &digest);
            assert_eq!(
                self.fs.metadata(&cache_path).unwrap().unwrap(),
                expected_metadata
            );
        }

        #[track_caller]
        fn assert_file_does_not_exist(&self, kind: TestKeyKind, digest: Sha256Digest) {
            let cache_path = self.cache.cache_path(kind, &digest);
            assert!(self.fs.metadata(&cache_path).unwrap().is_none());
        }

        #[track_caller]
        fn assert_bytes_used(&self, expected_bytes_used: u64) {
            assert_eq!(self.cache.bytes_used, expected_bytes_used);
        }

        #[track_caller]
        fn get_artifact(
            &mut self,
            kind: TestKeyKind,
            digest: Sha256Digest,
            jid: JobId,
            expected: GetArtifact,
        ) {
            let result = self.cache.get_artifact(kind, digest, jid);
            assert_eq!(result, expected);
        }

        #[track_caller]
        fn got_artifact_success_directory(
            &mut self,
            kind: TestKeyKind,
            digest: Sha256Digest,
            size: u64,
            expected: Vec<JobId>,
        ) {
            let source = self.cache.temp_dir();
            self.got_artifact_success(
                kind,
                digest,
                GotArtifact::Directory { source, size },
                expected,
            )
        }

        #[track_caller]
        fn got_artifact_success_file(
            &mut self,
            kind: TestKeyKind,
            digest: Sha256Digest,
            size: u64,
            expected: Vec<JobId>,
        ) {
            let source = self.cache.temp_file();
            let source_path = source.path();
            self.fs.remove(source_path).unwrap();
            self.fs
                .create_file(
                    source_path,
                    &iter::repeat(0u8)
                        .take(size.try_into().unwrap())
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            self.got_artifact_success(kind, digest, GotArtifact::File { source }, expected)
        }

        #[track_caller]
        fn got_artifact_success_symlink(
            &mut self,
            kind: TestKeyKind,
            digest: Sha256Digest,
            target: &str,
            expected: Vec<JobId>,
        ) {
            let target = target.into();
            self.got_artifact_success(kind, digest, GotArtifact::Symlink { target }, expected)
        }

        #[track_caller]
        fn got_artifact_success(
            &mut self,
            kind: TestKeyKind,
            digest: Sha256Digest,
            artifact: GotArtifact<Rc<test::Fs>>,
            expected: Vec<JobId>,
        ) {
            let result = self.cache.got_artifact_success(kind, &digest, artifact);
            assert_eq!(result, expected);
        }

        #[track_caller]
        fn got_artifact_failure(
            &mut self,
            kind: TestKeyKind,
            digest: Sha256Digest,
            expected: Vec<JobId>,
        ) {
            let result = self.cache.got_artifact_failure(kind, &digest);
            assert_eq!(result, expected);
        }

        fn decrement_ref_count(&mut self, kind: TestKeyKind, digest: Sha256Digest) {
            self.cache.decrement_ref_count(kind, &digest);
        }
    }

    #[test]
    fn new_does_proper_cleanup() {
        let fixture = Fixture::new(
            1000,
            fs! {
                z {
                    removing {
                        "0000000000000001" { a(1) },
                        "0000000000000002"(2),
                        "0000000000000003" -> "foo",
                    },
                    sha256 {
                        apple {
                            bad_apple {
                                foo(20),
                                symlink -> "bad_apple",
                                more_bad_apple {
                                    bar(20),
                                },
                            },
                        },
                        banana {
                            bread(20),
                        },
                    },
                    garbage {
                        foo(10),
                        symlink -> "garbage",
                        more_garbage {
                            bar(10),
                        },
                    },
                },
            },
        );
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(43),
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
                removing {
                    "0000000000000001" { a(1) },
                    "0000000000000002" {
                        foo(10),
                        symlink -> "garbage",
                        more_garbage {
                            bar(10),
                        },
                    },
                    "0000000000000003" {
                        bread(20),
                    },
                    "0000000000000004" {
                        bad_apple {
                            foo(20),
                            symlink -> "bad_apple",
                            more_bad_apple {
                                bar(20),
                            },
                        },
                    },
                },
            },
        });
        fixture.assert_pending_recursive_rmdirs([
            "/z/removing/0000000000000001",
            "/z/removing/0000000000000002",
            "/z/removing/0000000000000003",
            "/z/removing/0000000000000004",
        ]);
    }

    #[test]
    fn success_flow() {
        let mut fixture = Fixture::new(10, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(42), jid!(2), GetArtifact::Wait);
        fixture.get_artifact(Apple, digest!(42), jid!(3), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_bytes_used(0);

        fixture.got_artifact_success_file(Apple, digest!(42), 1, vec![jid!(1), jid!(2), jid!(3)]);
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);

        fixture.get_artifact(Apple, digest!(42), jid!(4), GetArtifact::Success);
        fixture.get_artifact(Apple, digest!(42), jid!(5), GetArtifact::Success);
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);
    }

    #[test]
    fn success_flow_with_files() {
        let mut fixture = Fixture::new(1, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(Apple, digest!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(1), jid!(2), GetArtifact::Wait);
        fixture.assert_bytes_used(0);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(43),
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
                removing {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.got_artifact_success_file(Apple, digest!(1), 6, vec![jid!(1), jid!(2)]);
        fixture.assert_bytes_used(6);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(43),
                sha256 {
                    apple {
                        "0000000000000000000000000000000000000000000000000000000000000001"(6),
                    },
                    orange {},
                },
                tmp {},
                removing {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.assert_bytes_used(0);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(43),
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
                removing {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn success_flow_with_symlinks() {
        let mut fixture = Fixture::new(1, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(Apple, digest!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(1), jid!(2), GetArtifact::Wait);
        fixture.assert_bytes_used(0);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(43),
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
                removing {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.got_artifact_success_symlink(Apple, digest!(1), "target", vec![jid!(1), jid!(2)]);
        fixture.assert_bytes_used(6);
        fixture.assert_fs(fs!{
            z {
                "CACHEDIR.TAG"(43),
                sha256 {
                    apple {
                        "0000000000000000000000000000000000000000000000000000000000000001" -> "target",
                    },
                    orange {},
                },
                tmp {},
                removing {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.assert_bytes_used(0);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(43),
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
                removing {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn success_flow_with_directories() {
        let mut fixture = Fixture::new(1, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(Apple, digest!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(1), jid!(2), GetArtifact::Wait);
        fixture.assert_bytes_used(0);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(43),
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
                removing {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.got_artifact_success_directory(Apple, digest!(1), 6, vec![jid!(1), jid!(2)]);
        fixture.assert_bytes_used(6);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(43),
                sha256 {
                    apple {
                        "0000000000000000000000000000000000000000000000000000000000000001" {},
                    },
                    orange {},
                },
                tmp {},
                removing {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.assert_bytes_used(0);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(43),
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
                removing {
                    "0000000000000001" {},
                },
            },
        });
        fixture.assert_pending_recursive_rmdirs(["/z/removing/0000000000000001"]);
    }

    #[test]
    fn artifact_stays_in_cache() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(Apple, digest!(42), 1, vec![jid!(1)]);
        fixture.decrement_ref_count(Apple, digest!(42));

        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Success);
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);
    }

    #[test]
    fn large_artifact_does_not_stay_in_cache() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(Apple, digest!(42), 100, vec![jid!(1)]);
        fixture.assert_bytes_used(100);
        fixture.decrement_ref_count(Apple, digest!(42));

        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_bytes_used(0);
        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
    }

    #[test]
    fn large_artifact_stays_in_cache_while_referenced() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(Apple, digest!(42), 100, vec![jid!(1)]);

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Success);

        fixture.decrement_ref_count(Apple, digest!(42));
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(100));
        fixture.assert_bytes_used(100);

        fixture.decrement_ref_count(Apple, digest!(42));
        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_bytes_used(0);
        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
    }

    #[test]
    fn different_key_kinds_are_independent() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(42), jid!(2), GetArtifact::Wait);
        fixture.get_artifact(Apple, digest!(42), jid!(3), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_bytes_used(0);

        fixture.get_artifact(Orange, digest!(42), jid!(4), GetArtifact::Get);
        fixture.get_artifact(Orange, digest!(42), jid!(5), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(Orange, digest!(42));
        fixture.assert_bytes_used(0);

        fixture.got_artifact_success_file(Apple, digest!(42), 100, vec![jid!(1), jid!(2), jid!(3)]);
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(100));
        fixture.assert_file_does_not_exist(Orange, digest!(42));
        fixture.assert_bytes_used(100);

        fixture.get_artifact(Orange, digest!(42), jid!(6), GetArtifact::Wait);

        fixture.got_artifact_success_file(Orange, digest!(42), 99, vec![jid!(4), jid!(5), jid!(6)]);
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(100));
        fixture.assert_file_exists(Orange, digest!(42), Metadata::file(99));
        fixture.assert_bytes_used(199);

        fixture.decrement_ref_count(Apple, digest!(42));
        fixture.decrement_ref_count(Apple, digest!(42));
        fixture.decrement_ref_count(Apple, digest!(42));
        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_file_exists(Orange, digest!(42), Metadata::file(99));
        fixture.assert_bytes_used(99);

        fixture.decrement_ref_count(Orange, digest!(42));
        fixture.decrement_ref_count(Orange, digest!(42));
        fixture.decrement_ref_count(Orange, digest!(42));
        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_file_does_not_exist(Orange, digest!(42));
        fixture.assert_bytes_used(0);
    }

    #[test]
    fn entries_are_removed_in_lru_order() {
        let mut fixture = Fixture::new(3, fs! {});

        fixture.get_artifact(Apple, digest!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(Orange, digest!(2), jid!(2), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(3), jid!(3), GetArtifact::Get);
        fixture.get_artifact(Orange, digest!(4), jid!(4), GetArtifact::Get);
        fixture.assert_bytes_used(0);

        fixture.got_artifact_success_file(Apple, digest!(1), 1, vec![jid!(1)]);
        fixture.got_artifact_success_file(Orange, digest!(2), 1, vec![jid!(2)]);
        fixture.got_artifact_success_file(Apple, digest!(3), 1, vec![jid!(3)]);
        fixture.got_artifact_success_file(Orange, digest!(4), 1, vec![jid!(4)]);

        fixture.assert_file_exists(Apple, digest!(1), Metadata::file(1));
        fixture.assert_file_exists(Orange, digest!(2), Metadata::file(1));
        fixture.assert_file_exists(Apple, digest!(3), Metadata::file(1));
        fixture.assert_file_exists(Orange, digest!(4), Metadata::file(1));
        fixture.assert_bytes_used(4);

        fixture.decrement_ref_count(Orange, digest!(4));

        fixture.assert_file_exists(Apple, digest!(1), Metadata::file(1));
        fixture.assert_file_exists(Orange, digest!(2), Metadata::file(1));
        fixture.assert_file_exists(Apple, digest!(3), Metadata::file(1));
        fixture.assert_file_does_not_exist(Orange, digest!(4));
        fixture.assert_bytes_used(3);
        fixture.get_artifact(Orange, digest!(4), jid!(14), GetArtifact::Get);

        fixture.decrement_ref_count(Apple, digest!(3));
        fixture.decrement_ref_count(Orange, digest!(2));
        fixture.decrement_ref_count(Apple, digest!(1));

        fixture.assert_file_exists(Apple, digest!(1), Metadata::file(1));
        fixture.assert_file_exists(Orange, digest!(2), Metadata::file(1));
        fixture.assert_file_exists(Apple, digest!(3), Metadata::file(1));
        fixture.assert_file_does_not_exist(Orange, digest!(4));
        fixture.assert_bytes_used(3);

        fixture.get_artifact(Apple, digest!(5), jid!(5), GetArtifact::Get);
        fixture.got_artifact_success_file(Apple, digest!(5), 1, vec![jid!(5)]);

        fixture.assert_file_exists(Apple, digest!(1), Metadata::file(1));
        fixture.assert_file_exists(Orange, digest!(2), Metadata::file(1));
        fixture.assert_file_does_not_exist(Apple, digest!(3));
        fixture.assert_file_does_not_exist(Orange, digest!(4));
        fixture.assert_file_exists(Apple, digest!(5), Metadata::file(1));
        fixture.assert_bytes_used(3);
        fixture.get_artifact(Apple, digest!(3), jid!(13), GetArtifact::Get);

        fixture.get_artifact(Orange, digest!(2), jid!(12), GetArtifact::Success);
        fixture.decrement_ref_count(Orange, digest!(2));

        fixture.get_artifact(Orange, digest!(6), jid!(6), GetArtifact::Get);
        fixture.got_artifact_success_file(Orange, digest!(6), 1, vec![jid!(6)]);

        fixture.assert_file_does_not_exist(Apple, digest!(1));
        fixture.assert_file_exists(Orange, digest!(2), Metadata::file(1));
        fixture.assert_file_does_not_exist(Apple, digest!(3));
        fixture.assert_file_does_not_exist(Orange, digest!(4));
        fixture.assert_file_exists(Apple, digest!(5), Metadata::file(1));
        fixture.assert_file_exists(Orange, digest!(6), Metadata::file(1));
        fixture.assert_bytes_used(3);
        fixture.get_artifact(Apple, digest!(1), jid!(11), GetArtifact::Get);
    }

    #[test]
    fn failure_flow() {
        let mut fixture = Fixture::new(10, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(42), jid!(2), GetArtifact::Wait);
        fixture.get_artifact(Apple, digest!(42), jid!(3), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_bytes_used(0);

        fixture.got_artifact_failure(Apple, digest!(42), vec![jid!(1), jid!(2), jid!(3)]);
        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_bytes_used(0);

        fixture.get_artifact(Apple, digest!(42), jid!(4), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(42), jid!(5), GetArtifact::Wait);
        fixture.get_artifact(Apple, digest!(42), jid!(6), GetArtifact::Wait);

        fixture.got_artifact_success_file(Apple, digest!(42), 1, vec![jid!(4), jid!(5), jid!(6)]);
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);
    }

    #[test]
    fn cache_path() {
        let fixture = Fixture::new(10, fs! {});
        let cache_path: PathBuf = long_path!("/z/sha256/apple", 42);
        assert_eq!(fixture.cache.cache_path(Apple, &digest!(42)), cache_path);
    }

    #[test]
    fn temp_file() {
        let fixture = Fixture::new(10, fs! {});
        let temp_file = fixture.cache.temp_file();
        assert_eq!(temp_file.path(), Path::new("/z/tmp/000"));
        assert_eq!(
            fixture.fs.metadata(temp_file.path()).unwrap().unwrap(),
            Metadata::file(0)
        );
    }

    #[test]
    fn temp_dir() {
        let fixture = Fixture::new(10, fs! {});
        let temp_dir = fixture.cache.temp_dir();
        assert_eq!(temp_dir.path(), Path::new("/z/tmp/000"));
        assert_eq!(
            fixture.fs.metadata(temp_dir.path()).unwrap().unwrap(),
            Metadata::directory(0)
        );
    }
}
