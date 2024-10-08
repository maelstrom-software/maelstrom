//! Manage downloading, extracting, and storing of artifacts specified by jobs.

pub mod fs;

use bytesize::ByteSize;
use fs::{FileType, Fs, Metadata};
use maelstrom_base::{JobId, Sha256Digest};
use maelstrom_util::{
    config::common::CacheSize,
    heap::{Heap, HeapDeps, HeapIndex},
    root::{Root, RootBuf},
};
use slog::{debug, Logger};
use std::{
    cmp::Ordering,
    collections::{hash_map::Entry as HashEntry, HashMap, HashSet},
    ffi::{OsStr, OsString},
    fmt::{self, Debug, Display, Formatter},
    hash::Hash,
    iter::IntoIterator,
    mem,
    num::NonZeroU32,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    str::FromStr as _,
    string::ToString,
};

const CACHEDIR_TAG: &str = "CACHEDIR.TAG";
const CACHEDIR_TAG_CONTENTS: [u8; 43] = *b"Signature: 8a477f597d28d172789f06886806bc55";
const CACHEDIR_TAG_CONTENTS_LEN: usize = CACHEDIR_TAG_CONTENTS.len();
const CACHEDIR_TAG_CONTENTS_LEN_U64: u64 = CACHEDIR_TAG_CONTENTS_LEN as u64;

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

pub struct EntryPath;

struct CachedirTagPath;
struct RemovingDir;
struct TmpDir;
struct Sha256Dir;
struct KindDir;
struct SizeFile;

/// Manage a directory of downloaded, extracted artifacts. Coordinate fetching of these artifacts,
/// and removing them when they are no longer in use and the amount of space used by the directory
/// has grown too large.
pub struct Cache<FsT, KeyKindT: KeyKind> {
    fs: FsT,
    removing: RootBuf<RemovingDir>,
    tmp: RootBuf<TmpDir>,
    sha256: RootBuf<Sha256Dir>,
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
        let cachedir_tag = root.join::<CachedirTagPath>(CACHEDIR_TAG);
        let removing = root.join::<RemovingDir>("removing");
        let sha256 = root.join::<Sha256Dir>("sha256");
        let tmp = root.join::<TmpDir>("tmp");

        // First, make sure the root directory exists.
        fs.mkdir_recursively(&root).unwrap();

        // Next, see if the `CACHEDIR.TAG` file exists and is correctly formed. We use this to
        // decide if this is a "new style" cache directory that can be re-used across invocations.
        // In the future, if we change the layout of the cache directory, we may need a proper
        // version file, but for now, we can just use `CACHEDIR.TAG`.
        let preserve_directory_contents = match fs.metadata(&cachedir_tag).unwrap() {
            Some(Metadata {
                type_: FileType::File,
                size: CACHEDIR_TAG_CONTENTS_LEN_U64,
            }) => {
                let mut contents = [0u8; CACHEDIR_TAG_CONTENTS_LEN];
                fs.read_file(&cachedir_tag, &mut contents[..]).unwrap();
                contents == CACHEDIR_TAG_CONTENTS
            }
            _ => false,
        };

        if preserve_directory_contents {
            Self::new_preserve_directory_contents(fs, root, removing, tmp, sha256, size, log)
        } else {
            Self::new_clear_directory_contents(
                fs,
                root,
                cachedir_tag,
                removing,
                tmp,
                sha256,
                size,
                log,
            )
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn new_clear_directory_contents(
        fs: FsT,
        root: RootBuf<CacheDir>,
        cachedir_tag: RootBuf<CachedirTagPath>,
        removing: RootBuf<RemovingDir>,
        tmp: RootBuf<TmpDir>,
        sha256: RootBuf<Sha256Dir>,
        size: CacheSize,
        log: Logger,
    ) -> Self {
        // First, make sure the `removing` directory exists and is empty.
        ensure_removing_directory(&fs, &root, &removing);

        // Next, remove everything else in the cache directory.
        remove_all_from_directory_except(&fs, &removing, &root, ["removing"]);

        // Finally, create all of the files all directories that should be there.
        fs.create_file(&cachedir_tag, &CACHEDIR_TAG_CONTENTS)
            .unwrap();
        fs.mkdir(&tmp).unwrap();
        fs.mkdir(&sha256).unwrap();
        for kind in KeyKindT::iter() {
            fs.mkdir(&kind_dir(&sha256, kind)).unwrap();
        }

        Cache {
            fs,
            removing,
            tmp,
            sha256,
            entries: Map::default(),
            heap: Heap::default(),
            next_priority: 0,
            bytes_used: 0,
            bytes_used_target: size.into(),
            log,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn new_preserve_directory_contents(
        fs: FsT,
        root: RootBuf<CacheDir>,
        removing: RootBuf<RemovingDir>,
        tmp: RootBuf<TmpDir>,
        sha256: RootBuf<Sha256Dir>,
        size: CacheSize,
        log: Logger,
    ) -> Self {
        // First, make sure the `removing` directory exists and is empty.
        ensure_removing_directory(&fs, &root, &removing);

        // Next, remove any garbage in the cache directory.
        remove_all_from_directory_except(
            &fs,
            &removing,
            &root,
            ["removing", "sha256", CACHEDIR_TAG],
        );

        // Next, create the sha256 and tmp directories.
        fs.mkdir(&tmp).unwrap();
        fs.mkdir_recursively(&sha256).unwrap();

        // Next, remove any old or garbage sha256 subdirectories and create any missing ones.
        remove_all_from_directory_except(&fs, &removing, &sha256, KeyKindT::iter());
        for kind in KeyKindT::iter() {
            fs.mkdir_recursively(&kind_dir(&sha256, kind)).unwrap();
        }

        let mut entries = Map::<KeyKindT>::default();
        let mut heap = Heap::<Map<KeyKindT>>::default();
        let mut next_priority = 0u64;
        let mut bytes_used = 0u64;

        // Finally, Go through the sha256 subdirs and decide what to do with the contents.
        for kind in KeyKindT::iter() {
            let kind_dir: RootBuf<KindDir> = sha256.join(kind.to_string());
            let mut directory_sizes = HashMap::<Sha256Digest, (bool, Option<u64>)>::new();
            for (name, metadata) in fs.read_dir(&kind_dir).unwrap().map(Result::unwrap) {
                match cache_file_name_type(&fs, &kind_dir, &name, metadata) {
                    CacheFileNameType::File(digest, size) => {
                        let key = Key::new(kind, digest);
                        entries.insert(
                            key.clone(),
                            Entry::InHeap {
                                file_type: metadata.type_,
                                bytes_used: metadata.size,
                                priority: next_priority,
                                heap_index: Default::default(),
                            },
                        );
                        heap.push(&mut entries, key);
                        next_priority = next_priority.checked_add(1).unwrap();
                        bytes_used = bytes_used.checked_add(size).unwrap();
                    }
                    CacheFileNameType::Directory(digest) => {
                        directory_sizes.entry(digest).or_default().0 = true;
                    }
                    CacheFileNameType::DirectorySize(digest, size) => {
                        directory_sizes.entry(digest).or_default().1 = Some(size);
                    }
                    CacheFileNameType::Other => {
                        let path: RootBuf<EntryPath> = kind_dir.join(name);
                        if metadata.is_directory() {
                            rmdir_in_background(&fs, &removing, &path);
                        } else {
                            fs.remove(&path).unwrap();
                        }
                    }
                }
            }

            for (digest, entry) in directory_sizes {
                match entry {
                    (true, Some(size)) => {
                        let key = Key::new(kind, digest);
                        entries.insert(
                            key.clone(),
                            Entry::InHeap {
                                file_type: FileType::Directory,
                                bytes_used: size,
                                priority: next_priority,
                                heap_index: Default::default(),
                            },
                        );
                        heap.push(&mut entries, key);
                        next_priority = next_priority.checked_add(1).unwrap();
                        bytes_used = bytes_used.checked_add(size).unwrap();
                    }
                    (false, Some(_)) => {
                        fs.remove(&size_file_name(&cache_file_name(&kind_dir, &digest)))
                            .unwrap();
                    }
                    (true, None) => {
                        rmdir_in_background(&fs, &removing, &cache_file_name(&kind_dir, &digest));
                    }
                    _ => {
                        unreachable!();
                    }
                }
            }
        }

        let mut cache = Cache {
            fs,
            removing,
            tmp,
            sha256,
            entries,
            heap,
            next_priority,
            bytes_used,
            bytes_used_target: size.into(),
            log,
        };
        cache.possibly_remove_some();
        cache
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
                write_size_file(&self.fs, &path, size);
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
    pub fn cache_path(&self, kind: KeyKindT, digest: &Sha256Digest) -> RootBuf<EntryPath> {
        let kind_dir: RootBuf<KindDir> = self.sha256.join(kind.to_string());
        cache_file_name(&kind_dir, digest)
    }

    pub fn temp_dir(&self) -> FsT::TempDir {
        self.fs.temp_dir(&self.tmp).unwrap()
    }

    pub fn temp_file(&self) -> FsT::TempFile {
        self.fs.temp_file(&self.tmp).unwrap()
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
            if file_type == FileType::Directory {
                rmdir_in_background(&self.fs, &self.removing, &cache_path);
                self.fs.remove(&size_file_name(&cache_path)).unwrap();
            } else {
                self.fs.remove(&cache_path).unwrap();
            }
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

enum CacheFileNameType {
    Other,
    File(Sha256Digest, u64),
    Directory(Sha256Digest),
    DirectorySize(Sha256Digest, u64),
}

fn remove_all_from_directory_except<S>(
    fs: &impl Fs,
    removing: &Root<RemovingDir>,
    dir: &Path,
    except: impl IntoIterator<Item = S>,
) where
    S: ToString,
{
    let except = except
        .into_iter()
        .map(|e| OsString::from(e.to_string()))
        .collect::<HashSet<_>>();
    for (entry_name, metadata) in fs.read_dir(dir).unwrap().map(Result::unwrap) {
        if !except.contains(&entry_name) {
            let path = dir.join(entry_name);
            if metadata.is_directory() {
                rmdir_in_background(fs, removing, &path);
            } else {
                fs.remove(&path).unwrap();
            }
        }
    }
}

/// Remove all files and directories rooted in `source` in a separate thread.
fn rmdir_in_background(fs: &impl Fs, removing: &Root<RemovingDir>, source: &Path) {
    let target = loop {
        let target: RootBuf<()> = removing.join(format!("{:016x}", fs.rand_u64()));
        if fs.metadata(&target).unwrap().is_none() {
            break target;
        }
    };
    fs.rename(source, &target).unwrap();
    fs.rmdir_recursively_on_thread(target.into_path_buf())
        .unwrap();
}

fn cache_file_name(dir: &Root<KindDir>, digest: &Sha256Digest) -> RootBuf<EntryPath> {
    dir.join(digest.to_string())
}

fn size_file_name(base: &Root<EntryPath>) -> RootBuf<SizeFile> {
    let mut result = base.to_owned().into_path_buf();
    result.set_extension("size");
    RootBuf::new(result)
}

fn read_size_file(
    fs: &impl Fs,
    kind_dir: &Root<KindDir>,
    digest: &Sha256Digest,
    metadata: Metadata,
) -> Option<u64> {
    if metadata.size != 8 {
        return None;
    }
    let mut contents = [0u8; 8];
    let bytes_read = fs
        .read_file(
            &size_file_name(&cache_file_name(kind_dir, digest)),
            &mut contents,
        )
        .ok()?;
    (bytes_read == 8).then_some(u64::from_be_bytes(contents))
}

fn cache_file_name_type(
    fs: &impl Fs,
    dir: &Root<KindDir>,
    name: &OsStr,
    metadata: Metadata,
) -> CacheFileNameType {
    if let Some(name) = name.to_str() {
        if let Ok(digest) = Sha256Digest::from_str(name) {
            return match metadata.type_ {
                FileType::File => CacheFileNameType::File(digest, metadata.size),
                FileType::Directory => CacheFileNameType::Directory(digest),
                _ => CacheFileNameType::Other,
            };
        }
        if metadata.type_ == FileType::File {
            if let Some(name) = name.strip_suffix(".size") {
                if let Ok(digest) = Sha256Digest::from_str(name) {
                    if let Some(size) = read_size_file(fs, dir, &digest, metadata) {
                        return CacheFileNameType::DirectorySize(digest, size);
                    }
                }
            }
        }
    }

    CacheFileNameType::Other
}

fn write_size_file(fs: &impl Fs, base: &Root<EntryPath>, size: u64) {
    fs.create_file(&size_file_name(base), &size.to_be_bytes())
        .unwrap();
}

fn ensure_removing_directory(fs: &impl Fs, root: &Root<CacheDir>, removing: &Root<RemovingDir>) {
    match fs.metadata(removing).unwrap() {
        Some(metadata) if metadata.is_directory() => {
            let removing_tmp = loop {
                let target: RootBuf<()> = root.join(format!("removing.{:016x}", fs.rand_u64()));
                if fs.metadata(&target).unwrap().is_none() {
                    break target;
                }
            };
            fs.rename(removing, &removing_tmp).unwrap();
        }
        Some(_) => {
            fs.remove(removing).unwrap();
        }
        None => {}
    }
    fs.mkdir(removing).unwrap();
}

fn kind_dir(sha256: &Root<Sha256Dir>, kind: impl KeyKind) -> RootBuf<KindDir> {
    sha256.join(kind.to_string())
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
    use fs::{
        test::{self, fs, Entry},
        Metadata, TempDir as _, TempFile as _,
    };
    use maelstrom_test::*;
    use slog::{o, Discard};
    use std::rc::Rc;
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
        fn new(bytes_used_target: u64, root: Entry) -> Self {
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
        fn assert_fs(&self, expected_fs: Entry) {
            self.fs.assert_tree(expected_fs);
        }

        #[track_caller]
        fn assert_fs_entry(&self, path: &str, expected_entry: Entry) {
            self.fs.assert_entry(Path::new(path), expected_entry);
        }

        #[track_caller]
        fn assert_pending_recursive_rmdirs<const N: usize>(&self, paths: [&str; N]) {
            self.fs.assert_recursive_rmdirs(HashSet::from_iter(
                paths.into_iter().map(ToString::to_string),
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
            contents: Entry,
            size: u64,
            expected: Vec<JobId>,
        ) {
            let source = self.cache.temp_dir();
            self.fs.graft(source.path(), contents);
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
            contents: &[u8],
            expected: Vec<JobId>,
        ) {
            let source = self.cache.temp_file();
            self.fs.graft(source.path(), Entry::file(contents));
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
    fn success_flow() {
        let mut fixture = Fixture::new(10, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(42), jid!(2), GetArtifact::Wait);
        fixture.get_artifact(Apple, digest!(42), jid!(3), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_bytes_used(0);

        fixture.got_artifact_success_file(
            Apple,
            digest!(42),
            b"a",
            vec![jid!(1), jid!(2), jid!(3)],
        );
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
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.got_artifact_success_file(Apple, digest!(1), b"123456", vec![jid!(1), jid!(2)]);
        fixture.assert_bytes_used(6);
        fixture.assert_fs_entry(
            "/z/sha256",
            fs! {
                apple {
                    "0000000000000000000000000000000000000000000000000000000000000001"(b"123456"),
                },
                orange {},
            },
        );
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn success_flow_with_symlinks() {
        let mut fixture = Fixture::new(1, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(Apple, digest!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(1), jid!(2), GetArtifact::Wait);
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.got_artifact_success_symlink(Apple, digest!(1), "target", vec![jid!(1), jid!(2)]);
        fixture.assert_bytes_used(6);
        fixture.assert_fs_entry(
            "/z/sha256",
            fs! {
                apple {
                    "0000000000000000000000000000000000000000000000000000000000000001" -> "target",
                },
                orange {},
            },
        );
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn success_flow_with_directories() {
        let mut fixture = Fixture::new(1, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(Apple, digest!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(1), jid!(2), GetArtifact::Wait);
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.got_artifact_success_directory(
            Apple,
            digest!(1),
            fs! {
                foo(b"bar"),
                symlink -> "foo",
            },
            6,
            vec![jid!(1), jid!(2)],
        );
        fixture.assert_bytes_used(6);
        fixture.assert_fs_entry(
            "/z/sha256",
            fs! {
                apple {
                    "0000000000000000000000000000000000000000000000000000000000000001" {
                        foo(b"bar"),
                        symlink -> "foo",
                    },
                    "0000000000000000000000000000000000000000000000000000000000000001.size"(&6u64.to_be_bytes()),
                },
                orange {},
            },
        );
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.decrement_ref_count(Apple, digest!(1));
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs(["/z/removing/0000000000000001"]);
    }

    #[test]
    fn artifact_stays_in_cache() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(Apple, digest!(42), b"a", vec![jid!(1)]);
        fixture.decrement_ref_count(Apple, digest!(42));

        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Success);
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);
    }

    #[test]
    fn large_artifact_does_not_stay_in_cache() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(Apple, digest!(42), b"123456789", vec![jid!(1)]);
        fixture.assert_bytes_used(9);
        fixture.decrement_ref_count(Apple, digest!(42));

        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_bytes_used(0);
        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
    }

    #[test]
    fn large_artifact_stays_in_cache_while_referenced() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(Apple, digest!(42), b"123456789", vec![jid!(1)]);

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Success);

        fixture.decrement_ref_count(Apple, digest!(42));
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(9));
        fixture.assert_bytes_used(9);

        fixture.decrement_ref_count(Apple, digest!(42));
        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_bytes_used(0);
        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
    }

    #[test]
    fn different_key_kinds_are_independent() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.get_artifact(Apple, digest!(42), jid!(1), GetArtifact::Get);
        fixture.get_artifact(Apple, digest!(42), jid!(2), GetArtifact::Wait);
        fixture.get_artifact(Apple, digest!(42), jid!(3), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_bytes_used(0);

        fixture.get_artifact(Orange, digest!(42), jid!(4), GetArtifact::Get);
        fixture.get_artifact(Orange, digest!(42), jid!(5), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(Orange, digest!(42));
        fixture.assert_bytes_used(0);

        fixture.got_artifact_success_file(
            Apple,
            digest!(42),
            b"abc",
            vec![jid!(1), jid!(2), jid!(3)],
        );
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(3));
        fixture.assert_file_does_not_exist(Orange, digest!(42));
        fixture.assert_bytes_used(3);

        fixture.get_artifact(Orange, digest!(42), jid!(6), GetArtifact::Wait);

        fixture.got_artifact_success_file(
            Orange,
            digest!(42),
            b"1234",
            vec![jid!(4), jid!(5), jid!(6)],
        );
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(3));
        fixture.assert_file_exists(Orange, digest!(42), Metadata::file(4));
        fixture.assert_bytes_used(7);

        fixture.decrement_ref_count(Apple, digest!(42));
        fixture.decrement_ref_count(Apple, digest!(42));
        fixture.decrement_ref_count(Apple, digest!(42));
        fixture.assert_file_does_not_exist(Apple, digest!(42));
        fixture.assert_file_exists(Orange, digest!(42), Metadata::file(4));
        fixture.assert_bytes_used(4);

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

        fixture.got_artifact_success_file(Apple, digest!(1), b"a", vec![jid!(1)]);
        fixture.got_artifact_success_file(Orange, digest!(2), b"b", vec![jid!(2)]);
        fixture.got_artifact_success_file(Apple, digest!(3), b"c", vec![jid!(3)]);
        fixture.got_artifact_success_file(Orange, digest!(4), b"d", vec![jid!(4)]);

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
        fixture.got_artifact_success_file(Apple, digest!(5), b"e", vec![jid!(5)]);

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
        fixture.got_artifact_success_file(Orange, digest!(6), b"f", vec![jid!(6)]);

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

        fixture.got_artifact_success_file(
            Apple,
            digest!(42),
            b"a",
            vec![jid!(4), jid!(5), jid!(6)],
        );
        fixture.assert_file_exists(Apple, digest!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);
    }

    #[test]
    fn cache_path() {
        let fixture = Fixture::new(10, fs! {});
        let cache_path: PathBuf = long_path!("/z/sha256/apple", 42);
        assert_eq!(
            fixture
                .cache
                .cache_path(Apple, &digest!(42))
                .into_path_buf(),
            cache_path
        );
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

    #[test]
    fn new_with_no_cache_directory() {
        let fixture = Fixture::new(1000, fs! {});
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                removing {
                },
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn new_without_cachedir_tag_does_proper_cleanup_with_removing_dir() {
        let fixture = Fixture::new(
            1000,
            fs! {
                z {
                    removing {
                        "0000000000000001" { a(b"a contents") },
                        "0000000000000002"(b"2 contents"),
                        "0000000000000003" -> "foo",
                    },
                    sha256 {
                        apple {
                            bad_apple {
                                foo(b"foo contents"),
                                symlink -> "bad_apple",
                                more_bad_apple {
                                    bar(b"bar contents"),
                                },
                            },
                        },
                        banana {
                            bread(b"bread contents"),
                        },
                    },
                    garbage {
                        foo(b"more foo contents"),
                        symlink -> "garbage",
                        more_garbage {
                            bar(b"more bar contents"),
                        },
                    },
            foo(b"foo contents"),
            symlink -> "garbage",
                },
            },
        );
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                removing {
                    "0000000000000002" {
                        foo(b"more foo contents"),
                        symlink -> "garbage",
                        more_garbage {
                            bar(b"more bar contents"),
                        },
                    },
                    "0000000000000003" {
                        "0000000000000001" { a(b"a contents") },
                        "0000000000000002"(b"2 contents"),
                        "0000000000000003" -> "foo",
                    },
                    "0000000000000004" {
                        apple {
                            bad_apple {
                                foo(b"foo contents"),
                                symlink -> "bad_apple",
                                more_bad_apple {
                                    bar(b"bar contents"),
                                },
                            },
                        },
                        banana {
                            bread(b"bread contents"),
                        },
                    },
                },
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([
            "/z/removing/0000000000000002",
            "/z/removing/0000000000000003",
            "/z/removing/0000000000000004",
        ]);
    }

    #[test]
    fn new_without_cachedir_tag_does_proper_cleanup_with_removing_file() {
        let fixture = Fixture::new(
            1000,
            fs! {
                z {
                    removing (b"removing"),
                    sha256 {
                        apple {
                            bad_apple {
                                foo(b"foo contents"),
                                symlink -> "bad_apple",
                                more_bad_apple {
                                    bar(b"bar contents"),
                                },
                            },
                        },
                        banana {
                            bread(b"bread contents"),
                        },
                    },
                    garbage {
                        foo(b"more foo contents"),
                        symlink -> "garbage",
                        more_garbage {
                            bar(b"more bar contents"),
                        },
                    },
            foo(b"foo contents"),
            symlink -> "garbage",
                },
            },
        );
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                removing {
                    "0000000000000001" {
                        foo(b"more foo contents"),
                        symlink -> "garbage",
                        more_garbage {
                            bar(b"more bar contents"),
                        },
                    },
                    "0000000000000002" {
                        apple {
                            bad_apple {
                                foo(b"foo contents"),
                                symlink -> "bad_apple",
                                more_bad_apple {
                                    bar(b"bar contents"),
                                },
                            },
                        },
                        banana {
                            bread(b"bread contents"),
                        },
                    },
                },
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([
            "/z/removing/0000000000000001",
            "/z/removing/0000000000000002",
        ]);
    }

    #[test]
    fn new_without_cachedir_tag_does_proper_cleanup_with_no_removing_file_or_dir() {
        let fixture = Fixture::new(
            1000,
            fs! {
                z {
                    sha256 {
                        apple {
                            bad_apple {
                                foo(b"foo contents"),
                                symlink -> "bad_apple",
                                more_bad_apple {
                                    bar(b"bar contents"),
                                },
                            },
                        },
                        banana {
                            bread(b"bread contents"),
                        },
                    },
                    garbage {
                        foo(b"more foo contents"),
                        symlink -> "garbage",
                        more_garbage {
                            bar(b"more bar contents"),
                        },
                    },
            foo(b"foo contents"),
            symlink -> "garbage",
                },
            },
        );
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                removing {
                    "0000000000000001" {
                        foo(b"more foo contents"),
                        symlink -> "garbage",
                        more_garbage {
                            bar(b"more bar contents"),
                        },
                    },
                    "0000000000000002" {
                        apple {
                            bad_apple {
                                foo(b"foo contents"),
                                symlink -> "bad_apple",
                                more_bad_apple {
                                    bar(b"bar contents"),
                                },
                            },
                        },
                        banana {
                            bread(b"bread contents"),
                        },
                    },
                },
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([
            "/z/removing/0000000000000001",
            "/z/removing/0000000000000002",
        ]);
    }

    #[test]
    fn new_with_cachedir() {
        let mut fixture = Fixture::new(
            1000,
            fs! {
                z {
                    "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                    removing {
                        "0000000000000001" { a(b"a contents") },
                        "0000000000000002"(b"2 contents"),
                        "0000000000000003" -> "foo",
                    },
                    sha256 {
                        apple {
                            "0000000000000000000000000000000000000000000000000000000000000001" -> "target",
                            "0000000000000000000000000000000000000000000000000000000000000002"(b"02"),
                            "0000000000000000000000000000000000000000000000000000000000000003" {
                                foo(b"foo contents"),
                                symlink -> "bad_apple",
                                more_bad_apple {
                                    bar(b"bar contents"),
                                },
                            },
                            "0000000000000000000000000000000000000000000000000000000000000004.size"(b"ab"),
                            garbage_file(b"ab"),
                            garbage_directory {
                                blah(b"blah contents"),
                            },
                            "0000000000000000000000000000000000000000000000000000000000000005" {
                                keep(b"keep"),
                            },
                            "0000000000000000000000000000000000000000000000000000000000000005.size"(b"\0\0\0\0\0\0\0\xff"),
                        },
                        banana {
                            bread(b"bread contents"),
                        },
                    },
                    garbage {
                        foo(b"more foo contents"),
                        symlink -> "garbage",
                        more_garbage {
                            bar(b"more bar contents"),
                        },
                    },
                    foo(b"foo contents"),
                    symlink -> "garbage",
                    tmp {
                        "tmp.1"(b"tmp.1"),
                    },
                },
            },
        );
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                removing {
                    "0000000000000002" {
                        foo(b"more foo contents"),
                        symlink -> "garbage",
                        more_garbage {
                            bar(b"more bar contents"),
                        },
                    },
                    "0000000000000003" {
                        "0000000000000001" { a(b"a contents") },
                        "0000000000000002"(b"2 contents"),
                        "0000000000000003" -> "foo",
                    },
                    "0000000000000004" {
                        "tmp.1"(b"tmp.1"),
                    },
                    "0000000000000005" {
                        bread(b"bread contents"),
                    },
                    "0000000000000006" {
                        blah(b"blah contents"),
                    },
                    "0000000000000007" {
                        foo(b"foo contents"),
                        symlink -> "bad_apple",
                        more_bad_apple {
                            bar(b"bar contents"),
                        },
                    },
                },
                sha256 {
                    apple {
                        "0000000000000000000000000000000000000000000000000000000000000002"(b"02"),
                        "0000000000000000000000000000000000000000000000000000000000000005" {
                            keep(b"keep"),
                        },
                        "0000000000000000000000000000000000000000000000000000000000000005.size"(b"\0\0\0\0\0\0\0\xff"),
                    },
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([
            "/z/removing/0000000000000002",
            "/z/removing/0000000000000003",
            "/z/removing/0000000000000004",
            "/z/removing/0000000000000005",
            "/z/removing/0000000000000006",
            "/z/removing/0000000000000007",
        ]);

        fixture.assert_bytes_used(257);
        fixture.get_artifact(Apple, digest!(2), jid!(1), GetArtifact::Success);
        fixture.get_artifact(Apple, digest!(5), jid!(1), GetArtifact::Success);
    }
}
