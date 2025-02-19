//! Manage downloading, extracting, and storing of artifacts specified by jobs.

pub mod fs;

use crate::{
    config::common::CacheSize,
    heap::{Heap, HeapDeps, HeapIndex},
    root::{Root, RootBuf},
};
use anyhow::{bail, Context as _, Error, Result};
use bytesize::ByteSize;
use derive_more::{Debug, Deref, DerefMut};
use fs::{FileType, Fs, Metadata};
use maelstrom_base::{JobId, Sha256Digest};
use slog::{debug, info, warn, Logger};
use std::{
    cmp::Ordering,
    collections::{hash_map::Entry as HashEntry, HashMap, HashSet},
    ffi::{OsStr, OsString},
    hash::Hash,
    iter::IntoIterator,
    mem,
    num::NonZeroU32,
    path::{Path, PathBuf},
    result,
    str::FromStr as _,
    string::ToString,
};

const CACHEDIR_TAG: &str = "CACHEDIR.TAG";
const CACHEDIR_TAG_CONTENTS: [u8; 43] = *b"Signature: 8a477f597d28d172789f06886806bc55";
const CACHEDIR_TAG_CONTENTS_LEN: usize = CACHEDIR_TAG_CONTENTS.len();
const CACHEDIR_TAG_CONTENTS_LEN_U64: u64 = CACHEDIR_TAG_CONTENTS_LEN as u64;
const LOCK_FILE: &str = "lock";
const REMOVING: &str = "removing";
const SHA256: &str = "sha256";

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
#[derive(Clone, Debug)]
#[debug(bound(FsT::TempFile: Debug, FsT::TempDir: Debug))]
pub enum GotArtifact<FsT: Fs> {
    Symlink { target: PathBuf },
    File { source: FsT::TempFile },
    Directory { source: FsT::TempDir, size: u64 },
}

impl<FsT: Fs> GotArtifact<FsT> {
    pub fn symlink(target: PathBuf) -> Self {
        Self::Symlink { target }
    }

    pub fn file(source: FsT::TempFile) -> Self {
        Self::File { source }
    }

    pub fn directory(source: FsT::TempDir, size: u64) -> Self {
        Self::Directory { source, size }
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

pub trait Key: Clone + Debug + Eq + Hash {
    fn kinds() -> impl Iterator<Item = &'static str>;
    fn from_kind_and_digest(kind: &'static str, digest: Sha256Digest) -> Self;
    fn kind(&self) -> &'static str;
    fn digest(&self) -> &Sha256Digest;
}

pub trait GetStrategy {
    type Getter: Eq + Hash;
    fn getter_from_job_id(jid: JobId) -> Self::Getter;
}

/// An entry for a specific [`Key`] in the [`Cache`]'s hash table. There is one of these for every
/// entry in the per-kind subdirectories of the `sha256` subdirectory of the [`Cache`]'s root
/// directory.
enum Entry<GetStrategyT: GetStrategy> {
    /// The artifact is being gotten by one of the clients.
    Getting {
        jobs: Vec<JobId>,
        getters: HashSet<GetStrategyT::Getter>,
    },

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
#[derive(Deref, DerefMut)]
struct Map<KeyT: Key, GetStrategyT: GetStrategy>(HashMap<KeyT, Entry<GetStrategyT>>);

impl<KeyT: Key, GetStrategyT: GetStrategy> Default for Map<KeyT, GetStrategyT> {
    fn default() -> Self {
        Self(HashMap::default())
    }
}

impl<KeyT: Key, GetStrategyT: GetStrategy> HeapDeps for Map<KeyT, GetStrategyT> {
    type Element = KeyT;

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

#[derive(Clone)]
pub struct TempFileFactory<FsT> {
    fs: FsT,
    tmp: RootBuf<TmpDir>,
}

impl<FsT: Fs> TempFileFactory<FsT> {
    pub fn temp_dir(&self) -> Result<FsT::TempDir> {
        Ok(self.fs.temp_dir(&self.tmp)?)
    }

    pub fn temp_file(&self) -> Result<FsT::TempFile> {
        Ok(self.fs.temp_file(&self.tmp)?)
    }
}

pub struct CacheDir;

pub struct EntryPath;

struct CachedirTagPath;
struct LockFilePath;
struct RemovingRoot;
struct RemovingPath;
struct TmpDir;
struct Sha256Dir;
struct KindDir;
struct SizeFile;

/// Manage a directory of downloaded, extracted artifacts. Coordinate fetching of these artifacts,
/// and removing them when they are no longer in use and the amount of space used by the directory
/// has grown too large.
pub struct Cache<FsT: Fs, KeyT: Key, GetStrategyT: GetStrategy> {
    fs: FsT,
    root: RootBuf<CacheDir>,
    removing: RootBuf<RemovingRoot>,
    sha256: RootBuf<Sha256Dir>,
    entries: Map<KeyT, GetStrategyT>,
    heap: Heap<Map<KeyT, GetStrategyT>>,
    next_priority: u64,
    bytes_used: u64,
    bytes_used_target: u64,
    getting: usize,
    log: Logger,
    _lock_file: FsT::FileLock,
}

impl<FsT: Fs, KeyT: Key, GetStrategyT: GetStrategy> Cache<FsT, KeyT, GetStrategyT> {
    /// Create a new [Cache] rooted at `root`. The directory `root` and all necessary ancestors
    /// will be created, along with `{root}/removing` and `{root}/{kind}/sha256`. Any pre-existing
    /// entries in `{root}/removing` and `{root}/{kind}/sha256` will be removed. That implies that
    /// the [Cache] doesn't currently keep data stored across invocations.
    ///
    /// `bytes_used_target` is the goal on-disk size for the cache. The cache will periodically grow
    /// larger than this size, but then shrink back down to this size. Ideally, the cache would use
    /// this as a hard upper bound, but that's not how it currently works.
    pub fn new(
        fs: FsT,
        root: RootBuf<CacheDir>,
        size: CacheSize,
        log: Logger,
        log_initial_message_at_info: bool,
    ) -> Result<(Self, TempFileFactory<FsT>)> {
        let cachedir_tag = root.join::<CachedirTagPath>(CACHEDIR_TAG);
        let lock_file_path = root.join::<LockFilePath>(LOCK_FILE);
        let removing = root.join::<RemovingRoot>(REMOVING);
        let sha256 = root.join::<Sha256Dir>(SHA256);
        let tmp = root.join::<TmpDir>("tmp");

        // First, make sure the root directory exists.
        fs.mkdir_recursively(&root)
            .with_context(|| format!("creating directory {root:?}"))?;

        // Next, get an exclusive lock on the lock file.
        let Some(lock_file) = fs
            .possibly_create_file_and_try_exclusive_lock(&lock_file_path)
            .with_context(|| format!("creating and locking lock file {lock_file_path:?}"))?
        else {
            bail!("lock file {lock_file_path:?} is held by a different process");
        };

        // Next, see if the `CACHEDIR.TAG` file exists and is correctly formed. We use this to
        // decide if this is a "new style" cache directory that can be re-used across invocations.
        // In the future, if we change the layout of the cache directory, we may need a proper
        // version file, but for now, we can just use `CACHEDIR.TAG`.
        let preserve_directory_contents = match fs
            .metadata(&cachedir_tag)
            .with_context(|| format!("getting metedata for {cachedir_tag:?}"))?
        {
            Some(Metadata {
                type_: FileType::File,
                size: CACHEDIR_TAG_CONTENTS_LEN_U64,
            }) => {
                let mut contents = [0u8; CACHEDIR_TAG_CONTENTS_LEN];
                fs.read_file(&cachedir_tag, &mut contents[..])
                    .with_context(|| format!("reading {cachedir_tag:?}"))?;
                contents == CACHEDIR_TAG_CONTENTS
            }
            _ => false,
        };

        let (cache, temp_file_factory) = if preserve_directory_contents {
            Self::new_preserve_directory_contents(
                fs, lock_file, root, removing, tmp, sha256, size, log,
            )
        } else {
            Self::new_clear_directory_contents(
                fs,
                lock_file,
                root,
                cachedir_tag,
                removing,
                tmp,
                sha256,
                size,
                log,
            )
        }?;

        if log_initial_message_at_info {
            info!(cache.log, "cache initialized";
                "entries" => %cache.entries.len(),
                "bytes_used" => %ByteSize::b(cache.bytes_used),
                "byte_used_target" => %ByteSize::b(cache.bytes_used_target)
            );
        } else {
            debug!(cache.log, "cache initialized";
                "entries" => %cache.entries.len(),
                "bytes_used" => %ByteSize::b(cache.bytes_used),
                "byte_used_target" => %ByteSize::b(cache.bytes_used_target)
            );
        }

        Ok((cache, temp_file_factory))
    }

    #[allow(clippy::too_many_arguments)]
    fn new_clear_directory_contents(
        fs: FsT,
        lock_file: FsT::FileLock,
        root: RootBuf<CacheDir>,
        cachedir_tag: RootBuf<CachedirTagPath>,
        removing: RootBuf<RemovingRoot>,
        tmp: RootBuf<TmpDir>,
        sha256: RootBuf<Sha256Dir>,
        size: CacheSize,
        log: Logger,
    ) -> Result<(Self, TempFileFactory<FsT>)> {
        // First, make sure the `removing` directory exists and is empty.
        ensure_removing_directory(&fs, &root, &removing)
            .context("creating `removing` directory")?;

        // Next, remove everything else in the cache directory.
        remove_all_from_directory_except(&fs, &removing, &root, [LOCK_FILE, REMOVING])
            .context("clearing previous contents of cache directory")?;

        // Finally, create all of the files all directories that should be there.
        fs.create_file(&cachedir_tag, &CACHEDIR_TAG_CONTENTS)
            .with_context(|| format!("creating {cachedir_tag:?}"))?;
        fs.mkdir(&tmp).with_context(|| format!("mkdir {tmp:?}"))?;
        fs.mkdir(&sha256)
            .with_context(|| format!("mkdir {sha256:?}"))?;
        for kind in KeyT::kinds() {
            fs.mkdir(&kind_dir(&sha256, kind))
                .with_context(|| format!("mkdir {:?}", kind_dir(&sha256, kind)))?;
        }

        let cache = Cache {
            fs: fs.clone(),
            root,
            removing,
            sha256,
            entries: Map::default(),
            heap: Heap::default(),
            getting: 0,
            next_priority: 0,
            bytes_used: 0,
            bytes_used_target: size.into(),
            log,
            _lock_file: lock_file,
        };
        let temp_file_factory = TempFileFactory { fs, tmp };
        Ok((cache, temp_file_factory))
    }

    #[allow(clippy::too_many_arguments)]
    fn new_preserve_directory_contents(
        fs: FsT,
        lock_file: FsT::FileLock,
        root: RootBuf<CacheDir>,
        removing: RootBuf<RemovingRoot>,
        tmp: RootBuf<TmpDir>,
        sha256: RootBuf<Sha256Dir>,
        size: CacheSize,
        log: Logger,
    ) -> Result<(Self, TempFileFactory<FsT>)> {
        // First, make sure the `removing` directory exists and is empty.
        ensure_removing_directory(&fs, &root, &removing)?;

        // Next, remove any garbage in the cache directory.
        remove_all_from_directory_except(
            &fs,
            &removing,
            &root,
            [LOCK_FILE, REMOVING, SHA256, CACHEDIR_TAG],
        )?;

        // Next, create the sha256 and tmp directories.
        fs.mkdir(&tmp)?;
        fs.mkdir_recursively(&sha256)?;

        // Next, remove any old or garbage sha256 subdirectories and create any missing ones.
        remove_all_from_directory_except(&fs, &removing, &sha256, KeyT::kinds())?;
        for kind in KeyT::kinds() {
            fs.mkdir_recursively(&kind_dir(&sha256, kind))?;
        }

        let mut entries = Map::<KeyT, GetStrategyT>::default();
        let mut heap = Heap::<Map<KeyT, GetStrategyT>>::default();
        let mut next_priority = 0u64;
        let mut bytes_used = 0u64;

        // Finally, Go through the sha256 subdirs and decide what to do with the contents.
        for kind in KeyT::kinds() {
            let kind_dir: RootBuf<KindDir> = sha256.join(kind);
            let mut directory_sizes = HashMap::<Sha256Digest, (bool, Option<u64>)>::new();
            for entry in fs.read_dir(&kind_dir)? {
                let (name, metadata) = entry?;
                match cache_file_name_type(&fs, &kind_dir, &name, metadata) {
                    CacheFileNameType::File(digest, size) => {
                        let key = KeyT::from_kind_and_digest(kind, digest);
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
                            rmdir_in_background(&fs, &removing, &path)?;
                        } else {
                            fs.remove(&path)?;
                        }
                    }
                }
            }

            for (digest, entry) in directory_sizes {
                match entry {
                    (true, Some(size)) => {
                        let key = KeyT::from_kind_and_digest(kind, digest);
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
                        fs.remove(&size_file_name(&cache_file_name(&kind_dir, &digest)))?;
                    }
                    (true, None) => {
                        rmdir_in_background(&fs, &removing, &cache_file_name(&kind_dir, &digest))?;
                    }
                    _ => {
                        unreachable!();
                    }
                }
            }
        }

        let mut cache = Cache {
            fs: fs.clone(),
            root,
            removing,
            sha256,
            entries,
            heap,
            getting: 0,
            next_priority,
            bytes_used,
            bytes_used_target: size.into(),
            log,
            _lock_file: lock_file,
        };

        cache.possibly_remove_some_can_error()?;
        let temp_file_factory = TempFileFactory { fs, tmp };
        Ok((cache, temp_file_factory))
    }

    /// Attempt to fetch `artifact` from the cache. See [`GetArtifact`] for the meaning of the
    /// return values.
    pub fn get_artifact(&mut self, key: KeyT, jid: JobId) -> GetArtifact {
        match self.entries.entry(key) {
            HashEntry::Vacant(entry) => {
                entry.insert(Entry::Getting {
                    jobs: vec![jid],
                    getters: HashSet::from([GetStrategyT::getter_from_job_id(jid)]),
                });
                self.getting = self.getting.checked_add(1).unwrap();
                GetArtifact::Get
            }
            HashEntry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Entry::Getting { jobs, getters } => {
                        jobs.push(jid);
                        let getter = GetStrategyT::getter_from_job_id(jid);
                        if getters.insert(getter) {
                            GetArtifact::Get
                        } else {
                            GetArtifact::Wait
                        }
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
    pub fn got_artifact_failure(&mut self, key: &KeyT) -> Vec<JobId> {
        if let Some(Entry::Getting { jobs, .. }) = self.entries.remove(key) {
            self.getting = self.getting.checked_sub(1).unwrap();
            jobs
        } else {
            vec![]
        }
    }

    /// Notify the cache that an artifact fetch has successfully completed. The returned vector
    /// lists the jobs that are affected, and the path they can use to access the artifact.
    pub fn got_artifact_success(
        &mut self,
        key: &KeyT,
        artifact: GotArtifact<FsT>,
    ) -> result::Result<Vec<JobId>, (Error, Vec<JobId>)> {
        fn update_cache_directory<FsT: Fs>(
            fs: &FsT,
            path: &Root<EntryPath>,
            artifact: GotArtifact<FsT>,
        ) -> Result<(FileType, u64)> {
            match artifact {
                GotArtifact::Directory { source, size } => {
                    write_size_file(fs, path, size)?;
                    fs.persist_temp_dir(source, path)?;
                    Ok((FileType::Directory, size))
                }
                GotArtifact::File { source } => {
                    fs.persist_temp_file(source, path)?;
                    let metadata = fs.metadata(path)?.unwrap();
                    assert!(matches!(metadata.type_, FileType::File));
                    Ok((FileType::File, metadata.size))
                }
                GotArtifact::Symlink { target } => {
                    fs.symlink(&target, path)?;
                    let metadata = fs.metadata(path)?.unwrap();
                    assert!(matches!(metadata.type_, FileType::Symlink));
                    Ok((FileType::Symlink, metadata.size))
                }
            }
        }

        fn dispose_of_artifact<FsT: Fs>(
            fs: &FsT,
            removing: &Root<RemovingRoot>,
            artifact: GotArtifact<FsT>,
        ) -> result::Result<(), FsT::Error> {
            match artifact {
                GotArtifact::Directory { source, .. } => {
                    let target = get_unused_removing_path(fs, removing)?;
                    fs.persist_temp_dir(source, &target)?;
                    fs.rmdir_recursively_on_thread(target.into_path_buf())?;
                }
                GotArtifact::File { source } => {
                    let target = get_unused_removing_path(fs, removing)?;
                    fs.persist_temp_file(source, &target)?;
                    fs.remove(&target)?;
                }
                GotArtifact::Symlink { .. } => {}
            }
            Ok(())
        }

        fn dispose_of_artifact_or_warn<FsT: Fs>(
            fs: &FsT,
            log: &Logger,
            removing: &Root<RemovingRoot>,
            artifact: GotArtifact<FsT>,
        ) {
            if let Err(err) = dispose_of_artifact(fs, removing, artifact) {
                warn!(log, "error encountered disposing of unnecessary artifact"; "error" => %err);
            }
        }

        let cache_path = self.cache_path(key);

        let Some(entry) = self.entries.get_mut(key) else {
            debug!(self.log, "cache disposing of added artifact because it is no longer required";
                "key" => ?key,
            );
            dispose_of_artifact_or_warn(&self.fs, &self.log, &self.removing, artifact);
            return Ok(vec![]);
        };

        let Entry::Getting { jobs, .. } = entry else {
            debug!(self.log, "cache disposing of added artifact because it is no longer required";
                "key" => ?key,
            );
            dispose_of_artifact_or_warn(&self.fs, &self.log, &self.removing, artifact);
            return Ok(vec![]);
        };

        let (file_type, bytes_used) = match update_cache_directory(&self.fs, &cache_path, artifact)
        {
            Err(err) => {
                return Err((err, self.got_artifact_failure(key)));
            }
            Ok((file_type, bytes_used)) => (file_type, bytes_used),
        };

        // Reference count must be > 0 since we don't allow cancellation of gets.
        let ref_count = NonZeroU32::new(jobs.len().try_into().unwrap()).unwrap();
        let jobs = mem::take(jobs);
        *entry = Entry::InUse {
            file_type,
            bytes_used,
            ref_count,
        };
        self.getting = self.getting.checked_sub(1).unwrap();
        self.bytes_used = self.bytes_used.checked_add(bytes_used).unwrap();
        debug!(self.log, "cache added artifact";
            "key" => ?key,
            "artifact_bytes_used" => %ByteSize::b(bytes_used),
            "entries" => %(self.entries.len() - self.getting),
            "file_type" => %file_type,
            "bytes_used" => %ByteSize::b(self.bytes_used),
            "byte_used_target" => %ByteSize::b(self.bytes_used_target)
        );
        self.possibly_remove_some_ignore_error();
        Ok(jobs)
    }

    /// Assuming that a reference count is already is already held for the artifact, increment it
    /// by one and return the size of the artifact. If no reference count is already held, return
    /// None.
    #[must_use]
    pub fn try_increment_ref_count(&mut self, key: &KeyT) -> Option<u64> {
        if let Some(Entry::InUse {
            ref_count,
            bytes_used,
            ..
        }) = self.entries.get_mut(key)
        {
            *ref_count = ref_count.checked_add(1).unwrap();
            Some(*bytes_used)
        } else {
            None
        }
    }

    /// Assuming that a reference count is already is already held for the artifact, return the
    /// size of the artifact, otherwise return None.
    #[must_use]
    pub fn try_get_size(&mut self, key: &KeyT) -> Option<u64> {
        if let Some(Entry::InUse { bytes_used, .. }) = self.entries.get_mut(key) {
            Some(*bytes_used)
        } else {
            None
        }
    }

    /// Notify the cache that a reference to an artifact is no longer needed.
    pub fn decrement_ref_count(&mut self, key: &KeyT) {
        let entry = self
            .entries
            .get_mut(key)
            .expect("got decrement_ref_count for key with no entry");
        let Entry::InUse {
            file_type,
            bytes_used,
            ref_count,
        } = entry
        else {
            panic!("got decrement_ref_count with for key with zero reference count");
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
                self.possibly_remove_some_ignore_error();
            }
        }
    }

    pub fn getter_disconnected(&mut self, getter: GetStrategyT::Getter) {
        self.entries.retain(|_, e| {
            let Entry::Getting { jobs, getters } = e else {
                return true;
            };
            jobs.retain(|jid| GetStrategyT::getter_from_job_id(*jid) != getter);
            getters.remove(&getter);
            let keep = !jobs.is_empty();
            if !keep {
                self.getting = self.getting.checked_sub(1).unwrap();
            }
            keep
        });
    }

    pub fn root(&self) -> &Root<CacheDir> {
        &self.root
    }

    /// Return the directory path for the artifact referenced by `digest`.
    pub fn cache_path(&self, key: &KeyT) -> RootBuf<EntryPath> {
        cache_file_name(self.sha256.join(key.kind()).as_root(), key.digest())
    }

    /// Check to see if the cache is over its goal size, and if so, try to remove the least
    /// recently used artifacts.
    fn possibly_remove_some_ignore_error(&mut self) {
        if let Err(err) = self.possibly_remove_some_can_error() {
            warn!(self.log, "error removing unused entries from cache to reclaim space"; "error" => %err);
        }
    }

    fn possibly_remove_some_can_error(&mut self) -> Result<()> {
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
            let cache_path = self.cache_path(&key);
            if file_type == FileType::Directory {
                rmdir_in_background(&self.fs, &self.removing, &cache_path)?;
                self.fs.remove(&size_file_name(&cache_path))?;
            } else {
                self.fs.remove(&cache_path)?;
            }
            self.bytes_used = self.bytes_used.checked_sub(bytes_used).unwrap();
            debug!(self.log, "cache removed unused artifact";
                "key" => ?key,
                "artifact_bytes_used" => %ByteSize::b(bytes_used),
                "entries" => %(self.entries.len() - self.getting),
                "bytes_used" => %ByteSize::b(self.bytes_used),
                "byte_used_target" => %ByteSize::b(self.bytes_used_target)
            );
        }
        Ok(())
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
    removing: &Root<RemovingRoot>,
    dir: &Path,
    except: impl IntoIterator<Item = S>,
) -> Result<()>
where
    S: ToString,
{
    let except = except
        .into_iter()
        .map(|e| OsString::from(e.to_string()))
        .collect::<HashSet<_>>();
    for entry in fs.read_dir(dir)? {
        let (entry_name, metadata) = entry?;
        if !except.contains(&entry_name) {
            let path = dir.join(entry_name);
            if metadata.is_directory() {
                rmdir_in_background(fs, removing, &path)?;
            } else {
                fs.remove(&path)?;
            }
        }
    }
    Ok(())
}

/// Return an unused path in the provided directory.
fn get_unused_removing_path<FsT: Fs>(
    fs: &FsT,
    removing: &Root<RemovingRoot>,
) -> result::Result<RootBuf<RemovingPath>, FsT::Error> {
    loop {
        let target = removing.join(format!("{:016x}", fs.rand_u64()));
        if fs.metadata(&target)?.is_none() {
            break Ok(target);
        }
    }
}

/// Remove all files and directories rooted in `source` in a separate thread.
fn rmdir_in_background(fs: &impl Fs, removing: &Root<RemovingRoot>, source: &Path) -> Result<()> {
    let target = get_unused_removing_path(fs, removing)?;
    fs.rename(source, &target)?;
    fs.rmdir_recursively_on_thread(target.into_path_buf())?;
    Ok(())
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

fn write_size_file(fs: &impl Fs, base: &Root<EntryPath>, size: u64) -> Result<()> {
    Ok(fs.create_file(&size_file_name(base), &size.to_be_bytes())?)
}

fn ensure_removing_directory(
    fs: &impl Fs,
    root: &Root<CacheDir>,
    removing: &Root<RemovingRoot>,
) -> Result<()> {
    match fs.metadata(removing)? {
        Some(metadata) if metadata.is_directory() => {
            let removing_tmp = loop {
                let target: RootBuf<()> = root.join(format!("removing.{:016x}", fs.rand_u64()));
                if fs.metadata(&target)?.is_none() {
                    break target;
                }
            };
            fs.rename(removing, &removing_tmp)?;
        }
        Some(_) => {
            fs.remove(removing)?;
        }
        None => {}
    }
    fs.mkdir(removing)?;
    Ok(())
}

fn kind_dir(sha256: &Root<Sha256Dir>, kind: &'static str) -> RootBuf<KindDir> {
    sha256.join(kind)
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
    use super::{
        fs::{
            test::{self, Entry},
            Metadata, TempDir as _, TempFile as _,
        },
        Debug, *,
    };
    use crate::fs;
    use maelstrom_base::digest;
    use maelstrom_test::*;
    use slog::{o, Discard};
    use strum::{EnumString, IntoStaticStr, VariantNames};

    #[derive(Clone, Copy, Debug, EnumString, Eq, Hash, IntoStaticStr, PartialEq, VariantNames)]
    #[strum(serialize_all = "snake_case")]
    enum TestKeyKind {
        Apple,
        Orange,
    }

    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct TestKey {
        kind: TestKeyKind,
        digest: Sha256Digest,
    }

    impl Key for TestKey {
        fn kinds() -> impl Iterator<Item = &'static str> {
            TestKeyKind::VARIANTS.iter().copied()
        }

        fn from_kind_and_digest(kind: &'static str, digest: Sha256Digest) -> Self {
            Self {
                kind: kind.try_into().unwrap(),
                digest,
            }
        }

        fn kind(&self) -> &'static str {
            self.kind.into()
        }

        fn digest(&self) -> &Sha256Digest {
            &self.digest
        }
    }

    macro_rules! apple {
        ($digest:expr) => {
            TestKey {
                kind: TestKeyKind::Apple,
                digest: digest!($digest),
            }
        };
    }

    macro_rules! orange {
        ($digest:expr) => {
            TestKey {
                kind: TestKeyKind::Orange,
                digest: digest!($digest),
            }
        };
    }

    #[derive(Eq, Hash, PartialEq)]
    enum TestGetter {
        Small,
        Large,
    }

    struct TestGetStrategy;

    impl GetStrategy for TestGetStrategy {
        type Getter = TestGetter;

        fn getter_from_job_id(job_id: JobId) -> Self::Getter {
            if job_id.cid.as_u32() < 100 {
                TestGetter::Small
            } else {
                TestGetter::Large
            }
        }
    }

    struct Fixture {
        fs: test::Fs,
        cache: Cache<test::Fs, TestKey, TestGetStrategy>,
        temp_file_factory: TempFileFactory<test::Fs>,
    }

    impl Fixture {
        fn new(bytes_used_target: u64, root: Entry) -> Self {
            let fs = test::Fs::new(root);
            let (cache, temp_file_factory) = Cache::new(
                fs.clone(),
                "/z".parse().unwrap(),
                ByteSize::b(bytes_used_target).into(),
                Logger::root(Discard, o!()),
                false,
            )
            .unwrap();
            Fixture {
                fs,
                cache,
                temp_file_factory,
            }
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
        fn assert_file_exists(&self, key: TestKey, expected_metadata: Metadata) {
            let cache_path = self.cache.cache_path(&key);
            assert_eq!(
                self.fs.metadata(&cache_path).unwrap().unwrap(),
                expected_metadata
            );
        }

        #[track_caller]
        fn assert_file_does_not_exist(&self, key: TestKey) {
            let cache_path = self.cache.cache_path(&key);
            assert!(self.fs.metadata(&cache_path).unwrap().is_none());
        }

        #[track_caller]
        fn assert_bytes_used(&self, expected_bytes_used: u64) {
            assert_eq!(self.cache.bytes_used, expected_bytes_used);
        }

        #[track_caller]
        fn get_artifact(&mut self, key: TestKey, jid: JobId, expected: GetArtifact) {
            let result = self.cache.get_artifact(key, jid);
            assert_eq!(result, expected);
        }

        #[track_caller]
        fn got_artifact_success_directory(
            &mut self,
            key: TestKey,
            contents: Entry,
            size: u64,
            expected: Vec<JobId>,
        ) {
            let source = self.temp_file_factory.temp_dir().unwrap();
            self.fs.graft(source.path(), contents);
            self.got_artifact_success(key, GotArtifact::directory(source, size), expected)
        }

        #[track_caller]
        fn got_artifact_success_file(
            &mut self,
            key: TestKey,
            contents: &[u8],
            expected: Vec<JobId>,
        ) {
            let source = self.temp_file_factory.temp_file().unwrap();
            self.fs.graft(source.path(), Entry::file(contents));
            self.got_artifact_success(key, GotArtifact::file(source), expected)
        }

        #[track_caller]
        fn got_artifact_success_symlink(
            &mut self,
            key: TestKey,
            target: &str,
            expected: Vec<JobId>,
        ) {
            let target = target.into();
            self.got_artifact_success(key, GotArtifact::symlink(target), expected)
        }

        #[track_caller]
        fn got_artifact_success(
            &mut self,
            key: TestKey,
            artifact: GotArtifact<test::Fs>,
            expected: Vec<JobId>,
        ) {
            let result = self.cache.got_artifact_success(&key, artifact).unwrap();
            assert_eq!(result, expected);
        }

        #[track_caller]
        fn got_artifact_failure(&mut self, key: TestKey, expected: Vec<JobId>) {
            let result = self.cache.got_artifact_failure(&key);
            assert_eq!(result, expected);
        }

        fn try_increment_ref_count(&mut self, key: TestKey, expected: Option<u64>) {
            assert_eq!(self.cache.try_increment_ref_count(&key), expected);
        }

        fn decrement_ref_count(&mut self, key: TestKey) {
            self.cache.decrement_ref_count(&key);
        }
    }

    #[test]
    fn success_flow_with_single_getter() {
        let mut fixture = Fixture::new(10, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(apple!(42), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(42), jid!(2), GetArtifact::Wait);
        fixture.get_artifact(apple!(42), jid!(3), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(apple!(42));
        fixture.assert_bytes_used(0);

        fixture.got_artifact_success_file(apple!(42), b"a", vec![jid!(1), jid!(2), jid!(3)]);
        fixture.assert_file_exists(apple!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);

        fixture.get_artifact(apple!(42), jid!(4), GetArtifact::Success);
        fixture.get_artifact(apple!(42), jid!(5), GetArtifact::Success);
        fixture.assert_file_exists(apple!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);
    }

    #[test]
    fn success_flow_with_multiple_getters() {
        let mut fixture = Fixture::new(10, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(apple!(42), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(42), jid!(2), GetArtifact::Wait);
        fixture.get_artifact(apple!(42), jid!(100), GetArtifact::Get);
        fixture.get_artifact(apple!(42), jid!(101), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(apple!(42));
        fixture.assert_bytes_used(0);

        fixture.got_artifact_success_file(
            apple!(42),
            b"a",
            vec![jid!(1), jid!(2), jid!(100), jid!(101)],
        );
        fixture.assert_file_exists(apple!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);

        fixture.get_artifact(apple!(42), jid!(4), GetArtifact::Success);
        fixture.get_artifact(apple!(42), jid!(5), GetArtifact::Success);
        fixture.assert_file_exists(apple!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);
    }

    #[test]
    fn success_flow_with_files() {
        let mut fixture = Fixture::new(1, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(2), GetArtifact::Wait);
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.got_artifact_success_file(apple!(1), b"123456", vec![jid!(1), jid!(2)]);
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

        fixture.decrement_ref_count(apple!(1));
        fixture.decrement_ref_count(apple!(1));
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn success_flow_with_symlinks() {
        let mut fixture = Fixture::new(1, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(2), GetArtifact::Wait);
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.got_artifact_success_symlink(apple!(1), "target", vec![jid!(1), jid!(2)]);
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

        fixture.decrement_ref_count(apple!(1));
        fixture.decrement_ref_count(apple!(1));
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn success_flow_with_directories() {
        let mut fixture = Fixture::new(1, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(2), GetArtifact::Wait);
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs([]);

        fixture.got_artifact_success_directory(
            apple!(1),
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

        fixture.decrement_ref_count(apple!(1));
        fixture.decrement_ref_count(apple!(1));
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs(["/z/removing/0000000000000001"]);
    }

    #[test]
    fn success_flow_with_failure_persisting() {
        let mut fixture = Fixture::new(1, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(2), GetArtifact::Wait);
        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry("/z/sha256", fs! { apple {}, orange {} });
        fixture.assert_pending_recursive_rmdirs([]);

        let source = fixture.temp_file_factory.temp_dir().unwrap();
        fixture.fs.graft(source.path(), fs! { foo(b"bar") });
        fixture.fs.set_failure(true);
        let (err, jids) = fixture
            .cache
            .got_artifact_success(&apple!(1), GotArtifact::directory(source, 6))
            .unwrap_err();
        assert_eq!(jids, vec![jid!(1), jid!(2)]);
        assert_eq!(err.to_string(), "Test");

        fixture.assert_bytes_used(0);
        fixture.assert_fs_entry(
            "/z/sha256",
            fs! {
                apple {},
                orange {},
            },
        );
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn artifact_stays_in_cache() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.get_artifact(apple!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(apple!(42), b"a", vec![jid!(1)]);
        fixture.decrement_ref_count(apple!(42));

        fixture.assert_file_exists(apple!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);

        fixture.get_artifact(apple!(42), jid!(1), GetArtifact::Success);
        fixture.assert_file_exists(apple!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);
    }

    #[test]
    fn large_artifact_does_not_stay_in_cache() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.get_artifact(apple!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(apple!(42), b"123456789", vec![jid!(1)]);
        fixture.assert_bytes_used(9);
        fixture.decrement_ref_count(apple!(42));

        fixture.assert_file_does_not_exist(apple!(42));
        fixture.assert_bytes_used(0);
        fixture.get_artifact(apple!(42), jid!(1), GetArtifact::Get);
    }

    #[test]
    fn large_artifact_stays_in_cache_while_referenced() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.get_artifact(apple!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(apple!(42), b"123456789", vec![jid!(1)]);

        fixture.get_artifact(apple!(42), jid!(1), GetArtifact::Success);

        fixture.decrement_ref_count(apple!(42));
        fixture.assert_file_exists(apple!(42), Metadata::file(9));
        fixture.assert_bytes_used(9);

        fixture.decrement_ref_count(apple!(42));
        fixture.assert_file_does_not_exist(apple!(42));
        fixture.assert_bytes_used(0);
        fixture.get_artifact(apple!(42), jid!(1), GetArtifact::Get);
    }

    #[test]
    fn different_key_kinds_are_independent() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.get_artifact(apple!(42), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(42), jid!(2), GetArtifact::Wait);
        fixture.get_artifact(apple!(42), jid!(3), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(apple!(42));
        fixture.assert_bytes_used(0);

        fixture.get_artifact(orange!(42), jid!(4), GetArtifact::Get);
        fixture.get_artifact(orange!(42), jid!(5), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(orange!(42));
        fixture.assert_bytes_used(0);

        fixture.got_artifact_success_file(apple!(42), b"abc", vec![jid!(1), jid!(2), jid!(3)]);
        fixture.assert_file_exists(apple!(42), Metadata::file(3));
        fixture.assert_file_does_not_exist(orange!(42));
        fixture.assert_bytes_used(3);

        fixture.get_artifact(orange!(42), jid!(6), GetArtifact::Wait);

        fixture.got_artifact_success_file(orange!(42), b"1234", vec![jid!(4), jid!(5), jid!(6)]);
        fixture.assert_file_exists(apple!(42), Metadata::file(3));
        fixture.assert_file_exists(orange!(42), Metadata::file(4));
        fixture.assert_bytes_used(7);

        fixture.decrement_ref_count(apple!(42));
        fixture.decrement_ref_count(apple!(42));
        fixture.decrement_ref_count(apple!(42));
        fixture.assert_file_does_not_exist(apple!(42));
        fixture.assert_file_exists(orange!(42), Metadata::file(4));
        fixture.assert_bytes_used(4);

        fixture.decrement_ref_count(orange!(42));
        fixture.decrement_ref_count(orange!(42));
        fixture.decrement_ref_count(orange!(42));
        fixture.assert_file_does_not_exist(apple!(42));
        fixture.assert_file_does_not_exist(orange!(42));
        fixture.assert_bytes_used(0);
    }

    #[test]
    fn entries_are_removed_in_lru_order() {
        let mut fixture = Fixture::new(3, fs! {});

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(orange!(2), jid!(2), GetArtifact::Get);
        fixture.get_artifact(apple!(3), jid!(3), GetArtifact::Get);
        fixture.get_artifact(orange!(4), jid!(4), GetArtifact::Get);
        fixture.assert_bytes_used(0);

        fixture.got_artifact_success_file(apple!(1), b"a", vec![jid!(1)]);
        fixture.got_artifact_success_file(orange!(2), b"b", vec![jid!(2)]);
        fixture.got_artifact_success_file(apple!(3), b"c", vec![jid!(3)]);
        fixture.got_artifact_success_file(orange!(4), b"d", vec![jid!(4)]);

        fixture.assert_file_exists(apple!(1), Metadata::file(1));
        fixture.assert_file_exists(orange!(2), Metadata::file(1));
        fixture.assert_file_exists(apple!(3), Metadata::file(1));
        fixture.assert_file_exists(orange!(4), Metadata::file(1));
        fixture.assert_bytes_used(4);

        fixture.decrement_ref_count(orange!(4));

        fixture.assert_file_exists(apple!(1), Metadata::file(1));
        fixture.assert_file_exists(orange!(2), Metadata::file(1));
        fixture.assert_file_exists(apple!(3), Metadata::file(1));
        fixture.assert_file_does_not_exist(orange!(4));
        fixture.assert_bytes_used(3);
        fixture.get_artifact(orange!(4), jid!(14), GetArtifact::Get);

        fixture.decrement_ref_count(apple!(3));
        fixture.decrement_ref_count(orange!(2));
        fixture.decrement_ref_count(apple!(1));

        fixture.assert_file_exists(apple!(1), Metadata::file(1));
        fixture.assert_file_exists(orange!(2), Metadata::file(1));
        fixture.assert_file_exists(apple!(3), Metadata::file(1));
        fixture.assert_file_does_not_exist(orange!(4));
        fixture.assert_bytes_used(3);

        fixture.get_artifact(apple!(5), jid!(5), GetArtifact::Get);
        fixture.got_artifact_success_file(apple!(5), b"e", vec![jid!(5)]);

        fixture.assert_file_exists(apple!(1), Metadata::file(1));
        fixture.assert_file_exists(orange!(2), Metadata::file(1));
        fixture.assert_file_does_not_exist(apple!(3));
        fixture.assert_file_does_not_exist(orange!(4));
        fixture.assert_file_exists(apple!(5), Metadata::file(1));
        fixture.assert_bytes_used(3);
        fixture.get_artifact(apple!(3), jid!(13), GetArtifact::Get);

        fixture.get_artifact(orange!(2), jid!(12), GetArtifact::Success);
        fixture.decrement_ref_count(orange!(2));

        fixture.get_artifact(orange!(6), jid!(6), GetArtifact::Get);
        fixture.got_artifact_success_file(orange!(6), b"f", vec![jid!(6)]);

        fixture.assert_file_does_not_exist(apple!(1));
        fixture.assert_file_exists(orange!(2), Metadata::file(1));
        fixture.assert_file_does_not_exist(apple!(3));
        fixture.assert_file_does_not_exist(orange!(4));
        fixture.assert_file_exists(apple!(5), Metadata::file(1));
        fixture.assert_file_exists(orange!(6), Metadata::file(1));
        fixture.assert_bytes_used(3);
        fixture.get_artifact(apple!(1), jid!(11), GetArtifact::Get);
    }

    #[test]
    fn failure_flow() {
        let mut fixture = Fixture::new(10, fs! {});
        fixture.assert_bytes_used(0);

        fixture.get_artifact(apple!(42), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(42), jid!(2), GetArtifact::Wait);
        fixture.get_artifact(apple!(42), jid!(3), GetArtifact::Wait);
        fixture.assert_file_does_not_exist(apple!(42));
        fixture.assert_bytes_used(0);

        fixture.got_artifact_failure(apple!(42), vec![jid!(1), jid!(2), jid!(3)]);
        fixture.assert_file_does_not_exist(apple!(42));
        fixture.assert_bytes_used(0);

        fixture.get_artifact(apple!(42), jid!(4), GetArtifact::Get);
        fixture.get_artifact(apple!(42), jid!(5), GetArtifact::Wait);
        fixture.get_artifact(apple!(42), jid!(6), GetArtifact::Wait);

        fixture.got_artifact_success_file(apple!(42), b"a", vec![jid!(4), jid!(5), jid!(6)]);
        fixture.assert_file_exists(apple!(42), Metadata::file(1));
        fixture.assert_bytes_used(1);
    }

    #[test]
    fn try_increment_ref_count() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.get_artifact(apple!(2), jid!(1), GetArtifact::Get);

        fixture.get_artifact(apple!(3), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(apple!(3), b"abc", vec![jid!(1)]);
        fixture.decrement_ref_count(apple!(3));

        fixture.get_artifact(apple!(4), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(apple!(4), b"def", vec![jid!(1)]);

        fixture.try_increment_ref_count(apple!(1), None);
        fixture.try_increment_ref_count(apple!(2), None);
        fixture.try_increment_ref_count(apple!(3), None);
        fixture.try_increment_ref_count(apple!(4), Some(3));

        fixture.get_artifact(apple!(5), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(apple!(5), b"0123456789", vec![jid!(1)]);
        fixture.assert_bytes_used(13);

        fixture.decrement_ref_count(apple!(4));
        fixture.assert_bytes_used(13);

        fixture.decrement_ref_count(apple!(4));
        fixture.assert_bytes_used(10);
    }

    #[test]
    fn got_artifact_failure_no_entry() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.got_artifact_failure(apple!(1), vec![]);
    }

    #[test]
    fn got_artifact_failure_in_use() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(apple!(1), b"abc", vec![jid!(1)]);

        fixture.got_artifact_failure(apple!(1), vec![]);
    }

    #[test]
    fn got_artifact_failure_in_cache() {
        let mut fixture = Fixture::new(100, fs! {});

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(apple!(1), b"abc", vec![jid!(1)]);
        fixture.decrement_ref_count(apple!(1));

        fixture.got_artifact_failure(apple!(1), vec![]);
    }

    #[test]
    fn got_artifact_success_directory_no_entry() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.got_artifact_success_directory(apple!(1), fs! { foo(b"foo") }, 1, vec![]);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                "lock"(b""),
                removing {
                    "0000000000000001" {
                        foo(b"foo"),
                    },
                },
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs(["/z/removing/0000000000000001"]);
    }

    #[test]
    fn got_artifact_success_file_no_entry() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.got_artifact_success_file(apple!(1), b"abc", vec![]);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                "lock"(b""),
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
    fn got_artifact_success_symlink_no_entry() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.got_artifact_success_symlink(apple!(1), "/target", vec![]);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                "lock"(b""),
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
    fn got_artifact_success_directory_in_use() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(100), GetArtifact::Get);
        fixture.got_artifact_success_directory(
            apple!(1),
            fs! { foo(b"foo") },
            1,
            vec![jid!(1), jid!(100)],
        );
        fixture.got_artifact_success_directory(apple!(1), fs! { foo(b"bar") }, 1, vec![]);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                "lock"(b""),
                removing {
                    "0000000000000001" {
                        foo(b"bar"),
                    },
                },
                sha256 {
                    apple {
                        "0000000000000000000000000000000000000000000000000000000000000001" {
                            foo(b"foo"),
                        },
                        "0000000000000000000000000000000000000000000000000000000000000001.size"(b"\0\0\0\0\0\0\0\x01"),
                    },
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs(["/z/removing/0000000000000001"]);
    }

    #[test]
    fn got_artifact_success_file_in_use() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(100), GetArtifact::Get);
        fixture.got_artifact_success_file(apple!(1), b"foo", vec![jid!(1), jid!(100)]);
        fixture.got_artifact_success_file(apple!(1), b"bar", vec![]);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                "lock"(b""),
                removing {
                },
                sha256 {
                    apple {
                        "0000000000000000000000000000000000000000000000000000000000000001"(b"foo"),
                    },
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn got_artifact_success_symlink_in_use() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(100), GetArtifact::Get);
        fixture.got_artifact_success_symlink(apple!(1), "/foo", vec![jid!(1), jid!(100)]);
        fixture.got_artifact_success_symlink(apple!(1), "/bar", vec![]);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                "lock"(b""),
                removing {
                },
                sha256 {
                    apple {
                        "0000000000000000000000000000000000000000000000000000000000000001" -> "/foo",
                    },
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn got_artifact_success_directory_in_cache() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(100), GetArtifact::Get);
        fixture.got_artifact_success_directory(
            apple!(1),
            fs! { foo(b"foo") },
            1,
            vec![jid!(1), jid!(100)],
        );
        fixture.decrement_ref_count(apple!(1));
        fixture.decrement_ref_count(apple!(1));
        fixture.got_artifact_success_directory(apple!(1), fs! { foo(b"bar") }, 1, vec![]);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                "lock"(b""),
                removing {
                    "0000000000000001" {
                        foo(b"bar"),
                    },
                },
                sha256 {
                    apple {
                        "0000000000000000000000000000000000000000000000000000000000000001" {
                            foo(b"foo"),
                        },
                        "0000000000000000000000000000000000000000000000000000000000000001.size"(b"\0\0\0\0\0\0\0\x01"),
                    },
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs(["/z/removing/0000000000000001"]);
    }

    #[test]
    fn got_artifact_success_file_in_cache() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(100), GetArtifact::Get);
        fixture.got_artifact_success_file(apple!(1), b"foo", vec![jid!(1), jid!(100)]);
        fixture.decrement_ref_count(apple!(1));
        fixture.decrement_ref_count(apple!(1));
        fixture.got_artifact_success_file(apple!(1), b"bar", vec![]);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                "lock"(b""),
                removing {
                },
                sha256 {
                    apple {
                        "0000000000000000000000000000000000000000000000000000000000000001"(b"foo"),
                    },
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn got_artifact_success_symlink_in_cache() {
        let mut fixture = Fixture::new(10, fs! {});

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(100), GetArtifact::Get);
        fixture.got_artifact_success_symlink(apple!(1), "/foo", vec![jid!(1), jid!(100)]);
        fixture.decrement_ref_count(apple!(1));
        fixture.decrement_ref_count(apple!(1));
        fixture.got_artifact_success_symlink(apple!(1), "/bar", vec![]);
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                "lock"(b""),
                removing {
                },
                sha256 {
                    apple {
                        "0000000000000000000000000000000000000000000000000000000000000001" -> "/foo",
                    },
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);
    }

    #[test]
    fn getter_disconnected_getting() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(100), GetArtifact::Get);

        fixture.cache.getter_disconnected(TestGetter::Large);

        // Only jid!(1) is returned.
        fixture.got_artifact_success_file(apple!(1), b"foo", vec![jid!(1)]);

        fixture.assert_bytes_used(3);
        fixture.decrement_ref_count(apple!(1));
        fixture.assert_bytes_used(0);
    }

    #[test]
    fn getter_disconnected_in_use() {
        let mut fixture = Fixture::new(1, fs! {});

        fixture.get_artifact(apple!(1), jid!(1), GetArtifact::Get);
        fixture.get_artifact(apple!(1), jid!(100), GetArtifact::Get);

        fixture.got_artifact_success_file(apple!(1), b"foo", vec![jid!(1), jid!(100)]);

        // No effect since the cache has already turned the jid into a refcount.
        fixture.cache.getter_disconnected(TestGetter::Large);

        fixture.assert_bytes_used(3);
        fixture.decrement_ref_count(apple!(1));
        fixture.assert_bytes_used(3);
        fixture.decrement_ref_count(apple!(1));
        fixture.assert_bytes_used(0);
    }

    #[test]
    fn cache_path() {
        let fixture = Fixture::new(10, fs! {});
        assert_eq!(
            fixture.cache.cache_path(&apple!(42)).into_path_buf(),
            PathBuf::from(format!("/z/sha256/apple/{:0>64x}", 42)),
        );
    }

    #[test]
    fn temp_file() {
        let fixture = Fixture::new(10, fs! {});
        let temp_file = fixture.temp_file_factory.temp_file().unwrap();
        assert_eq!(temp_file.path(), Path::new("/z/tmp/000"));
        assert_eq!(
            fixture.fs.metadata(temp_file.path()).unwrap().unwrap(),
            Metadata::file(0)
        );
    }

    #[test]
    fn temp_dir() {
        let fixture = Fixture::new(10, fs! {});
        let temp_dir = fixture.temp_file_factory.temp_dir().unwrap();
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
                "lock"(b""),
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
                "lock"(b""),
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
                "lock"(b""),
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
                "lock"(b""),
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
                "lock"(b""),
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
        fixture.get_artifact(apple!(2), jid!(1), GetArtifact::Success);
        fixture.get_artifact(apple!(5), jid!(1), GetArtifact::Success);
    }

    #[test]
    fn new_without_cachedir_tag_with_lock_file_contends() {
        let fixture = Fixture::new(
            1000,
            fs! {
                z {
                    "lock"(b""),
                },
            },
        );
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                "lock"(b""),
                removing {},
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);

        let Err(err) = Cache::<test::Fs, TestKey, TestGetStrategy>::new(
            fixture.fs.clone(),
            "/z".parse().unwrap(),
            ByteSize::b(1).into(),
            Logger::root(Discard, o!()),
            false,
        ) else {
            panic!("expected error");
        };

        assert_eq!(
            err.to_string(),
            "lock file \"/z/lock\" is held by a different process"
        );
    }

    #[test]
    fn new_without_cachedir_tag_without_lock_file_contends() {
        let fixture = Fixture::new(1000, fs! {});
        fixture.assert_fs(fs! {
            z {
                "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                "lock"(b""),
                removing {},
                sha256 {
                    apple {},
                    orange {},
                },
                tmp {},
            },
        });
        fixture.assert_pending_recursive_rmdirs([]);

        let Err(err) = Cache::<test::Fs, TestKey, TestGetStrategy>::new(
            fixture.fs.clone(),
            "/z".parse().unwrap(),
            ByteSize::b(1).into(),
            Logger::root(Discard, o!()),
            false,
        ) else {
            panic!("expected error");
        };

        assert_eq!(
            err.to_string(),
            "lock file \"/z/lock\" is held by a different process"
        );
    }

    #[test]
    fn new_with_cachedir_tag_with_lock_file_contends() {
        let fixture = Fixture::new(
            1000,
            fs! {
                z {
                    "CACHEDIR.TAG"(&CACHEDIR_TAG_CONTENTS),
                    "lock"(b""),
                    removing {},
                    sha256 {
                        apple {},
                        orange {},
                    },
                    tmp {},
                },
            },
        );
        fixture.assert_pending_recursive_rmdirs([
            "/z/removing/0000000000000002",
            "/z/removing/0000000000000003",
        ]);

        let Err(err) = Cache::<test::Fs, TestKey, TestGetStrategy>::new(
            fixture.fs.clone(),
            "/z".parse().unwrap(),
            ByteSize::b(1).into(),
            Logger::root(Discard, o!()),
            false,
        ) else {
            panic!("expected error");
        };

        assert_eq!(
            err.to_string(),
            "lock file \"/z/lock\" is held by a different process"
        );
    }
}
