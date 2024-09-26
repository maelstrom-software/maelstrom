//! Manage downloading, extracting, and storing of artifacts specified by jobs.

use bytesize::ByteSize;
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
    error,
    ffi::OsString,
    fmt::{self, Debug, Formatter},
    fs::{self, File},
    io::{self, ErrorKind, Write as _},
    iter::IntoIterator,
    mem,
    num::NonZeroU32,
    ops::{Deref, DerefMut},
    os::unix::fs as unix_fs,
    path::{Path, PathBuf},
    string::ToString,
    thread,
};
use strum::Display;
use tempfile::{NamedTempFile, TempDir, TempPath};

const CACHEDIR_TAG_CONTENTS: [u8; 43] = *b"Signature: 8a477f597d28d172789f06886806bc55";

/// A type used to represent a temporary file. The assumption is that the implementer may want to
/// make the type [`Drop`] so that the temporary file is cleaned up if it isn't consumed.
pub trait FsTempFile: Debug {
    /// Return the path to the temporary file. Can be used to open the file to write into it.
    fn path(&self) -> &Path;

    /// Make the file temporary no more, by moving it to `target`. Will panic on file system error
    /// or if the parent directory of `target` doesn't exist, or if `target` already exists.
    fn persist(self, target: &Path);
}

/// A type used to represent a temporary directory. The assumption is that the implementer may want
/// to make the type [`Drop`] so that the temporary directory is cleaned up if it isn't consumed.
pub trait FsTempDir: Debug {
    /// Return the path to the temporary directory. Can be used to create files in the directory
    /// before it is made persistent.
    fn path(&self) -> &Path;

    /// Make the directory temporary no more, by moving it to `target`. Will panic on file system
    /// error or if the parent directory of `target` doesn't exist, or if `target` already exists.
    fn persist(self, target: &Path);
}

/// The type of a file, as far as the cache cares.
#[derive(Clone, Copy, Debug, Display, PartialEq)]
pub enum FileType {
    Directory,
    File,
    Symlink,
    Other,
}

impl From<fs::FileType> for FileType {
    fn from(file_type: fs::FileType) -> Self {
        if file_type.is_dir() {
            FileType::Directory
        } else if file_type.is_file() {
            FileType::File
        } else if file_type.is_symlink() {
            FileType::Symlink
        } else {
            FileType::Other
        }
    }
}

/// The file metadata the cache cares about.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FileMetadata {
    type_: FileType,
    size: u64,
}

impl FileMetadata {
    pub fn directory(size: u64) -> Self {
        Self {
            type_: FileType::Directory,
            size,
        }
    }

    pub fn file(size: u64) -> Self {
        Self {
            type_: FileType::File,
            size,
        }
    }

    pub fn symlink(size: u64) -> Self {
        Self {
            type_: FileType::Symlink,
            size,
        }
    }
}

impl From<fs::Metadata> for FileMetadata {
    fn from(metadata: fs::Metadata) -> Self {
        Self {
            type_: metadata.file_type().into(),
            size: metadata.len(),
        }
    }
}

/// Dependencies that [`Cache`] has on the file system.
pub trait Fs {
    /// Error type for methods.
    type Error: error::Error;

    /// Return a random u64. This is used for creating unique path names in the directory removal
    /// code path.
    fn rand_u64(&self) -> u64;

    /// Rename `source` to `destination`. Panic on file system error. Assume that all intermediate
    /// directories exist for `destination`, and that `source` and `destination` are on the same
    /// file system.
    fn rename(&self, source: &Path, destination: &Path) -> Result<(), Self::Error>;

    /// Remove `path`. Panic on file system error. Assume that `path` actually exists and is not a
    /// directory.
    fn remove(&self, path: &Path) -> Result<(), Self::Error>;

    /// Remove `path` and all its descendants. `path` must exist, and it must be a directory. The
    /// function will return an error immediately if `path` doesn't exist, can't be resolved, or
    /// doesn't point to a directory. Otherwise, the removal will happen in the background on
    /// another thread. If an error occurs there, the calling function won't be notified.
    fn rmdir_recursively_on_thread(&self, path: PathBuf) -> Result<(), Self::Error>;

    /// Ensure `path` exists and is a directory. If it doesn't exist, recusively ensure its parent exists,
    /// then create it. Panic on file system error or if `path` or any of its ancestors aren't
    /// directories.
    fn mkdir_recursively(&self, path: &Path) -> Result<(), Self::Error>;

    /// Return and iterator that will yield all of the children of a directory. Panic on file
    /// system error or if `path` doesn't exist or isn't a directory.
    fn read_dir(
        &self,
        path: &Path,
    ) -> Result<impl Iterator<Item = Result<(OsString, FileMetadata), Self::Error>>, Self::Error>;

    /// Create a file with given `path` and `contents`. Panic on file system error, including if
    /// the file already exists.
    fn create_file(&self, path: &Path, contents: &[u8]) -> Result<(), Self::Error>;

    /// Create a symlink at `link` that points to `target`. Panic on file system error.
    fn symlink(&self, target: &Path, link: &Path) -> Result<(), Self::Error>;

    /// Get the metadata of the file at `path`. Panic on file system error. If `path` doesn't
    /// exist, return None. If the last component is a symlink, return the metadata about the
    /// symlink instead of trying to resolve it.
    fn metadata(&self, path: &Path) -> Result<Option<FileMetadata>, Self::Error>;

    /// The type returned by the [`Self::temp_file`] method. Some implementations may make this
    /// type [`Drop`] so that the temporary file can be cleaned up when it is closed.
    type TempFile: FsTempFile;

    /// Create a new temporary file in the directory `parent`. Panic on file system error or if
    /// `parent` isn't a directory.
    fn temp_file(&self, parent: &Path) -> Self::TempFile;

    /// The type returned by the [`Self::temp_dir`] method. Some implementations may make this
    /// type [`Drop`] so that the temporary directory can be cleaned up when it is closed.
    type TempDir: FsTempDir;

    /// Create a new temporary directory in the directory `parent`. Panic on file system error or
    /// if `parent` isn't a directory.
    fn temp_dir(&self, parent: &Path) -> Self::TempDir;
}

#[derive(Debug)]
pub struct StdTempFile(TempPath);

impl FsTempFile for StdTempFile {
    fn path(&self) -> &Path {
        &self.0
    }

    fn persist(self, target: &Path) {
        self.0.persist(target).unwrap();
    }
}

#[derive(Debug)]
pub struct StdTempDir(TempDir);

impl FsTempDir for StdTempDir {
    fn path(&self) -> &Path {
        self.0.path()
    }

    fn persist(self, target: &Path) {
        fs::rename(self.0.into_path(), target).unwrap();
    }
}

/// The standard implementation of CacheFs that uses [std] and [rand].
pub struct StdFs;

impl Fs for StdFs {
    type Error = io::Error;
    type TempFile = StdTempFile;
    type TempDir = StdTempDir;

    fn rand_u64(&self) -> u64 {
        rand::random()
    }

    fn rename(&self, source: &Path, destination: &Path) -> io::Result<()> {
        fs::rename(source, destination)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn rmdir_recursively_on_thread(&self, path: PathBuf) -> io::Result<()> {
        if !fs::metadata(&path)?.is_dir() {
            // We want ErrorKind::NotADirectory, but it's currently unstable.
            Err(io::Error::from(ErrorKind::InvalidInput))
        } else {
            thread::spawn(move || {
                let _ = fs::remove_dir_all(path);
            });
            Ok(())
        }
    }

    fn mkdir_recursively(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn read_dir(
        &self,
        path: &Path,
    ) -> io::Result<impl Iterator<Item = io::Result<(OsString, FileMetadata)>>> {
        fs::read_dir(path).map(|dirents| {
            dirents.map(|dirent| -> io::Result<_> {
                let dirent = dirent?;
                Ok((dirent.file_name(), dirent.metadata()?.into()))
            })
        })
    }

    fn create_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        File::create_new(path)?.write_all(contents)
    }

    fn symlink(&self, target: &Path, link: &Path) -> io::Result<()> {
        unix_fs::symlink(target, link)
    }

    fn metadata(&self, path: &Path) -> io::Result<Option<FileMetadata>> {
        match fs::symlink_metadata(path) {
            Ok(metadata) => Ok(Some(metadata.into())),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn temp_file(&self, parent: &Path) -> Self::TempFile {
        StdTempFile(NamedTempFile::new_in(parent).unwrap().into_temp_path())
    }

    fn temp_dir(&self, parent: &Path) -> Self::TempDir {
        StdTempDir(TempDir::new_in(parent).unwrap())
    }
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, strum::EnumIter)]
pub enum EntryKind {
    Blob,
    BottomFsLayer,
    UpperFsLayer,
}

impl EntryKind {
    fn iter() -> impl DoubleEndedIterator<Item = Self> {
        <Self as strum::IntoEnumIterator>::iter()
    }
}

impl fmt::Display for EntryKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Blob => write!(f, "blob"),
            Self::BottomFsLayer => write!(f, "bottom_fs_layer"),
            Self::UpperFsLayer => write!(f, "upper_fs_layer"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Key {
    pub kind: EntryKind,
    pub digest: Sha256Digest,
}

impl Key {
    pub fn new(kind: EntryKind, digest: Sha256Digest) -> Self {
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
#[derive(Default)]
struct Map(HashMap<Key, Entry>);

impl Deref for Map {
    type Target = HashMap<Key, Entry>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Map {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl HeapDeps for Map {
    type Element = Key;

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
pub struct Cache<FsT> {
    fs: FsT,
    root: PathBuf,
    entries: Map,
    heap: Heap<Map>,
    next_priority: u64,
    bytes_used: u64,
    bytes_used_target: u64,
    log: Logger,
}

impl<FsT: Fs> Cache<FsT> {
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
        Self::remove_all_from_directory_except(&fs, &root, &sha256, EntryKind::iter());

        for kind in EntryKind::iter() {
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
        kind: EntryKind,
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
    pub fn got_artifact_failure(&mut self, kind: EntryKind, digest: &Sha256Digest) -> Vec<JobId> {
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
        kind: EntryKind,
        digest: &Sha256Digest,
        artifact: GotArtifact<FsT>,
    ) -> Vec<JobId> {
        let path = self.cache_path(kind, digest);
        let (file_type, bytes_used) = match artifact {
            GotArtifact::Directory { source, size } => {
                source.persist(&path);
                (FileType::Directory, size)
            }
            GotArtifact::File { source } => {
                source.persist(&path);
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
    pub fn decrement_ref_count(&mut self, kind: EntryKind, digest: &Sha256Digest) {
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
    pub fn cache_path(&self, kind: EntryKind, digest: &Sha256Digest) -> PathBuf {
        let mut path = self.root.join("sha256");
        path.push(kind.to_string());
        path.push(digest.to_string());
        path
    }

    pub fn temp_dir(&self) -> FsT::TempDir {
        self.fs.temp_dir(&self.root.join("tmp"))
    }

    pub fn temp_file(&self) -> FsT::TempFile {
        self.fs.temp_file(&self.root.join("tmp"))
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
    use itertools::Itertools;
    use maelstrom_test::*;
    use slog::{o, Discard};
    use std::{
        cell::{Cell, RefCell},
        cmp::Ordering,
        ffi::OsStr,
        path::Component,
        rc::Rc,
    };
    use TestMessage::*;

    #[derive(Clone, Debug, PartialEq)]
    enum TestMessage {
        Rename(PathBuf, PathBuf),
        Remove(PathBuf),
        RemoveRecursively(PathBuf),
        MkdirRecursively(PathBuf),
        ReadDir(PathBuf),
        CreateFile(PathBuf, Box<[u8]>),
        Symlink(PathBuf, PathBuf),
        Metadata(PathBuf),
        TempFile(PathBuf),
        TempDir(PathBuf),
        PersistTempFile(PathBuf, PathBuf),
        PersistTempDir(PathBuf, PathBuf),
    }

    #[derive(Debug)]
    struct TestTempFile {
        path: PathBuf,
        messages: Rc<RefCell<Vec<TestMessage>>>,
    }

    impl PartialOrd for TestTempFile {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for TestTempFile {
        fn cmp(&self, other: &Self) -> Ordering {
            self.path.cmp(&other.path)
        }
    }

    impl PartialEq for TestTempFile {
        fn eq(&self, other: &Self) -> bool {
            self.path.eq(&other.path)
        }
    }

    impl Eq for TestTempFile {}

    impl FsTempFile for TestTempFile {
        fn path(&self) -> &Path {
            &self.path
        }

        fn persist(self, target: &Path) {
            self.messages
                .borrow_mut()
                .push(PersistTempFile(self.path, target.to_owned()));
        }
    }

    #[derive(Debug)]
    struct TestTempDir {
        path: PathBuf,
        messages: Rc<RefCell<Vec<TestMessage>>>,
    }

    impl PartialOrd for TestTempDir {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for TestTempDir {
        fn cmp(&self, other: &Self) -> Ordering {
            self.path.cmp(&other.path)
        }
    }

    impl PartialEq for TestTempDir {
        fn eq(&self, other: &Self) -> bool {
            self.path.eq(&other.path)
        }
    }

    impl Eq for TestTempDir {}

    impl FsTempDir for TestTempDir {
        fn path(&self) -> &Path {
            &self.path
        }

        fn persist(self, target: &Path) {
            self.messages
                .borrow_mut()
                .push(PersistTempDir(self.path, target.to_owned()));
        }
    }

    #[derive(Default)]
    struct TestFs {
        messages: Rc<RefCell<Vec<TestMessage>>>,
        directories: HashMap<PathBuf, Vec<(PathBuf, FileMetadata)>>,
        metadata: HashMap<PathBuf, FileMetadata>,
        last_random_number: Cell<u64>,
    }

    #[derive(Debug, Display, PartialEq)]
    enum TestFsError {
        Exists,
        IsDir,
        Inval,
        NoEnt,
        NotDir,
        NotEmpty,
    }

    impl error::Error for TestFsError {}

    impl Fs for TestFs {
        type Error = TestFsError;

        type TempFile = TestTempFile;

        type TempDir = TestTempDir;

        fn rand_u64(&self) -> u64 {
            let result = self.last_random_number.get() + 1;
            self.last_random_number.set(result);
            result
        }

        fn rename(&self, source: &Path, destination: &Path) -> Result<(), TestFsError> {
            self.messages
                .borrow_mut()
                .push(Rename(source.to_owned(), destination.to_owned()));
            Ok(())
        }

        fn remove(&self, path: &Path) -> Result<(), TestFsError> {
            self.messages.borrow_mut().push(Remove(path.to_owned()));
            Ok(())
        }

        fn rmdir_recursively_on_thread(&self, path: PathBuf) -> Result<(), TestFsError> {
            self.messages
                .borrow_mut()
                .push(RemoveRecursively(path.to_owned()));
            Ok(())
        }

        fn mkdir_recursively(&self, path: &Path) -> Result<(), TestFsError> {
            self.messages
                .borrow_mut()
                .push(MkdirRecursively(path.to_owned()));
            Ok(())
        }

        fn read_dir(
            &self,
            path: &Path,
        ) -> Result<impl Iterator<Item = Result<(OsString, FileMetadata), TestFsError>>, TestFsError>
        {
            self.messages.borrow_mut().push(ReadDir(path.to_owned()));
            Ok(self
                .directories
                .get(path)
                .unwrap_or(&vec![])
                .clone()
                .into_iter()
                .map(|(path, metadata)| Ok((path.file_name().unwrap().to_owned(), metadata))))
        }

        fn create_file(&self, path: &Path, contents: &[u8]) -> Result<(), TestFsError> {
            self.messages.borrow_mut().push(CreateFile(
                path.to_owned(),
                contents.to_vec().into_boxed_slice(),
            ));
            Ok(())
        }

        fn symlink(&self, target: &Path, link: &Path) -> Result<(), TestFsError> {
            self.messages
                .borrow_mut()
                .push(Symlink(target.to_owned(), link.to_owned()));
            Ok(())
        }

        fn metadata(&self, path: &Path) -> Result<Option<FileMetadata>, TestFsError> {
            self.messages.borrow_mut().push(Metadata(path.to_owned()));
            Ok(self.metadata.get(path).copied())
        }

        fn temp_file(&self, parent: &Path) -> Self::TempFile {
            let path = parent.join(format!("{:0>16x}", self.rand_u64()));
            self.messages.borrow_mut().push(TempFile(path.clone()));
            TestTempFile {
                path,
                messages: self.messages.clone(),
            }
        }

        fn temp_dir(&self, parent: &Path) -> Self::TempDir {
            let path = parent.join(format!("{:0>16x}", self.rand_u64()));
            self.messages.borrow_mut().push(TempDir(path.clone()));
            TestTempDir {
                path,
                messages: self.messages.clone(),
            }
        }
    }

    struct Fixture {
        messages: Rc<RefCell<Vec<TestMessage>>>,
        cache: Cache<TestFs>,
    }

    impl Fixture {
        fn new_with_fs_and_clear_messages(test_cache_fs: TestFs, bytes_used_target: u64) -> Self {
            let mut fixture = Fixture::new(test_cache_fs, bytes_used_target);
            fixture.clear_messages();
            fixture
        }

        fn new_and_clear_messages(bytes_used_target: u64) -> Self {
            Self::new_with_fs_and_clear_messages(TestFs::default(), bytes_used_target)
        }

        fn new(test_cache_fs: TestFs, bytes_used_target: u64) -> Self {
            let messages = test_cache_fs.messages.clone();
            let cache = Cache::new(
                test_cache_fs,
                "/z".parse().unwrap(),
                ByteSize::b(bytes_used_target).into(),
                Logger::root(Discard, o!()),
            );
            Fixture { messages, cache }
        }

        #[track_caller]
        fn expect_messages_in_any_order(&mut self, expected: Vec<TestMessage>) {
            let mut messages = self.messages.borrow_mut();
            for perm in expected.clone().into_iter().permutations(expected.len()) {
                if perm == *messages {
                    messages.clear();
                    return;
                }
            }
            panic!(
                "Expected messages didn't match actual messages in any order.\n{}",
                colored_diff::PrettyDifference {
                    expected: &format!("{:#?}", expected),
                    actual: &format!("{:#?}", messages)
                }
            );
        }

        #[track_caller]
        fn expect_messages_in_specific_order(&mut self, expected: Vec<TestMessage>) {
            assert!(
                *self.messages.borrow() == expected,
                "Expected messages didn't match actual messages in specific order.\n{}",
                colored_diff::PrettyDifference {
                    expected: &format!("{:#?}", expected),
                    actual: &format!("{:#?}", self.messages.borrow())
                }
            );
            self.clear_messages();
        }

        fn clear_messages(&mut self) {
            self.messages.borrow_mut().clear();
        }

        #[track_caller]
        fn get_artifact(&mut self, digest: Sha256Digest, jid: JobId, expected: GetArtifact) {
            let result = self.cache.get_artifact(EntryKind::Blob, digest, jid);
            assert_eq!(result, expected);
            self.expect_messages_in_any_order(vec![]);
        }

        #[track_caller]
        fn get_artifact_ign(&mut self, digest: Sha256Digest, jid: JobId) {
            self.cache.get_artifact(EntryKind::Blob, digest, jid);
            self.expect_messages_in_any_order(vec![]);
        }

        #[track_caller]
        fn got_artifact_success_directory(
            &mut self,
            digest: Sha256Digest,
            source: PathBuf,
            size: u64,
            expected: Vec<JobId>,
            expected_fs_operations: Vec<TestMessage>,
        ) {
            let source = TestTempDir {
                path: source,
                messages: self.messages.clone(),
            };
            self.got_artifact_success(
                digest,
                GotArtifact::Directory { source, size },
                expected,
                expected_fs_operations,
            )
        }

        #[track_caller]
        fn got_artifact_success_file(
            &mut self,
            digest: Sha256Digest,
            source: PathBuf,
            expected: Vec<JobId>,
            expected_fs_operations: Vec<TestMessage>,
        ) {
            let source = TestTempFile {
                path: source,
                messages: self.messages.clone(),
            };
            self.got_artifact_success(
                digest,
                GotArtifact::File { source },
                expected,
                expected_fs_operations,
            )
        }

        #[track_caller]
        fn got_artifact_success_symlink(
            &mut self,
            digest: Sha256Digest,
            target: PathBuf,
            expected: Vec<JobId>,
            expected_fs_operations: Vec<TestMessage>,
        ) {
            self.got_artifact_success(
                digest,
                GotArtifact::Symlink { target },
                expected,
                expected_fs_operations,
            )
        }

        #[track_caller]
        fn got_artifact_success(
            &mut self,
            digest: Sha256Digest,
            artifact: GotArtifact<TestFs>,
            expected: Vec<JobId>,
            expected_fs_operations: Vec<TestMessage>,
        ) {
            let result = self
                .cache
                .got_artifact_success(EntryKind::Blob, &digest, artifact);
            assert_eq!(result, expected);
            self.expect_messages_in_any_order(expected_fs_operations);
        }

        #[track_caller]
        fn got_artifact_failure(&mut self, digest: Sha256Digest, expected: Vec<JobId>) {
            let result = self.cache.got_artifact_failure(EntryKind::Blob, &digest);
            assert_eq!(result, expected);
            self.expect_messages_in_any_order(vec![]);
        }

        #[track_caller]
        fn got_artifact_success_directory_ign(&mut self, digest: Sha256Digest, size: u64) {
            let source = TestTempDir {
                path: "/foo".into(),
                messages: self.messages.clone(),
            };
            self.got_artifact_success_ign(digest, GotArtifact::Directory { source, size })
        }

        #[track_caller]
        fn got_artifact_success_ign(
            &mut self,
            digest: Sha256Digest,
            artifact: GotArtifact<TestFs>,
        ) {
            self.cache
                .got_artifact_success(EntryKind::Blob, &digest, artifact);
            self.clear_messages();
        }

        #[track_caller]
        fn decrement_ref_count(&mut self, digest: Sha256Digest, expected: Vec<TestMessage>) {
            self.cache.decrement_ref_count(EntryKind::Blob, &digest);
            self.expect_messages_in_any_order(expected);
        }

        #[track_caller]
        fn decrement_ref_count_ign(&mut self, digest: Sha256Digest) {
            self.cache.decrement_ref_count(EntryKind::Blob, &digest);
            self.clear_messages();
        }
    }

    #[test]
    fn get_miss_filled_with_directory() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact(digest!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_directory(
            digest!(42),
            short_path!("/z/tmp", 1),
            100,
            vec![jid!(1)],
            vec![PersistTempDir(
                short_path!("/z/tmp", 1),
                long_path!("/z/sha256/blob", 42),
            )],
        );
    }

    #[test]
    fn get_miss_filled_with_file() {
        let mut test_cache_fs = TestFs::default();
        test_cache_fs
            .metadata
            .insert(long_path!("/z/sha256/blob", 42), FileMetadata::file(100));
        let mut fixture = Fixture::new_with_fs_and_clear_messages(test_cache_fs, 1000);

        fixture.get_artifact(digest!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_file(
            digest!(42),
            short_path!("/z/tmp", 1),
            vec![jid!(1)],
            vec![
                PersistTempFile(short_path!("/z/tmp", 1), long_path!("/z/sha256/blob", 42)),
                Metadata(long_path!("/z/sha256/blob", 42)),
            ],
        );
    }

    #[test]
    fn get_miss_filled_with_symlink() {
        let mut test_cache_fs = TestFs::default();
        test_cache_fs
            .metadata
            .insert(long_path!("/z/sha256/blob", 42), FileMetadata::symlink(10));
        let mut fixture = Fixture::new_with_fs_and_clear_messages(test_cache_fs, 1000);

        fixture.get_artifact(digest!(42), jid!(1), GetArtifact::Get);
        fixture.got_artifact_success_symlink(
            digest!(42),
            path_buf!("/somewhere"),
            vec![jid!(1)],
            vec![
                Symlink(path_buf!("/somewhere"), long_path!("/z/sha256/blob", 42)),
                Metadata(long_path!("/z/sha256/blob", 42)),
            ],
        );
    }

    #[test]
    fn get_miss_filled_with_directory_larger_than_goal_ok_then_removes_on_decrement_ref_count() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact_ign(digest!(42), jid!(1));
        fixture.got_artifact_success_directory(
            digest!(42),
            short_path!("/z/tmp", 1),
            10000,
            vec![jid!(1)],
            vec![PersistTempDir(
                short_path!("/z/tmp", 1),
                long_path!("/z/sha256/blob", 42),
            )],
        );

        fixture.decrement_ref_count(
            digest!(42),
            vec![
                Metadata(short_path!("/z/removing", 1)),
                Rename(
                    long_path!("/z/sha256/blob", 42),
                    short_path!("/z/removing", 1),
                ),
                RemoveRecursively(short_path!("/z/removing", 1)),
            ],
        );
    }

    #[test]
    fn get_miss_filled_with_file_larger_than_goal_ok_then_removes_on_decrement_ref_count() {
        let mut test_cache_fs = TestFs::default();
        test_cache_fs
            .metadata
            .insert(long_path!("/z/sha256/blob", 42), FileMetadata::file(10000));
        let mut fixture = Fixture::new_with_fs_and_clear_messages(test_cache_fs, 1000);

        fixture.get_artifact_ign(digest!(42), jid!(1));
        fixture.got_artifact_success_file(
            digest!(42),
            short_path!("/z/tmp", 1),
            vec![jid!(1)],
            vec![
                PersistTempFile(short_path!("/z/tmp", 1), long_path!("/z/sha256/blob", 42)),
                Metadata(long_path!("/z/sha256/blob", 42)),
            ],
        );

        fixture.decrement_ref_count(digest!(42), vec![Remove(long_path!("/z/sha256/blob", 42))]);
    }

    #[test]
    fn get_miss_filled_with_symlink_larger_than_goal_ok_then_removes_on_decrement_ref_count() {
        let mut test_cache_fs = TestFs::default();
        test_cache_fs
            .metadata
            .insert(long_path!("/z/sha256/blob", 42), FileMetadata::symlink(10));
        let mut fixture = Fixture::new_with_fs_and_clear_messages(test_cache_fs, 1);

        fixture.get_artifact_ign(digest!(42), jid!(1));
        fixture.got_artifact_success_symlink(
            digest!(42),
            path_buf!("/somewhere"),
            vec![jid!(1)],
            vec![
                Symlink(path_buf!("/somewhere"), long_path!("/z/sha256/blob", 42)),
                Metadata(long_path!("/z/sha256/blob", 42)),
            ],
        );

        fixture.decrement_ref_count(digest!(42), vec![Remove(long_path!("/z/sha256/blob", 42))]);
    }

    #[test]
    fn cache_entries_are_removed_in_lru_order() {
        let mut fixture = Fixture::new_and_clear_messages(10);

        fixture.get_artifact_ign(digest!(1), jid!(1));
        fixture.got_artifact_success_directory_ign(digest!(1), 4);
        fixture.decrement_ref_count(digest!(1), vec![]);

        fixture.get_artifact_ign(digest!(2), jid!(2));
        fixture.got_artifact_success_directory_ign(digest!(2), 4);
        fixture.decrement_ref_count(digest!(2), vec![]);

        fixture.get_artifact_ign(digest!(3), jid!(3));
        fixture.got_artifact_success_directory(
            digest!(3),
            short_path!("/z/tmp", 1),
            4,
            vec![jid!(3)],
            vec![
                PersistTempDir(short_path!("/z/tmp", 1), long_path!("/z/sha256/blob", 3)),
                Metadata(short_path!("/z/removing", 1)),
                Rename(
                    long_path!("/z/sha256/blob", 1),
                    short_path!("/z/removing", 1),
                ),
                RemoveRecursively(short_path!("/z/removing", 1)),
            ],
        );
        fixture.decrement_ref_count(digest!(3), vec![]);

        fixture.get_artifact_ign(digest!(4), jid!(4));
        fixture.got_artifact_success_directory(
            digest!(4),
            short_path!("/z/tmp", 2),
            4,
            vec![jid!(4)],
            vec![
                PersistTempDir(short_path!("/z/tmp", 2), long_path!("/z/sha256/blob", 4)),
                Metadata(short_path!("/z/removing", 2)),
                Rename(
                    long_path!("/z/sha256/blob", 2),
                    short_path!("/z/removing", 2),
                ),
                RemoveRecursively(short_path!("/z/removing", 2)),
            ],
        );
        fixture.decrement_ref_count(digest!(4), vec![]);
    }

    #[test]
    fn lru_order_augmented_by_last_use() {
        let mut fixture = Fixture::new_and_clear_messages(10);

        fixture.get_artifact_ign(digest!(1), jid!(1));
        fixture.got_artifact_success_directory_ign(digest!(1), 3);

        fixture.get_artifact_ign(digest!(2), jid!(2));
        fixture.got_artifact_success_directory_ign(digest!(2), 3);

        fixture.get_artifact_ign(digest!(3), jid!(3));
        fixture.got_artifact_success_directory_ign(digest!(3), 3);

        fixture.decrement_ref_count(digest!(3), vec![]);
        fixture.decrement_ref_count(digest!(2), vec![]);
        fixture.decrement_ref_count(digest!(1), vec![]);

        fixture.get_artifact_ign(digest!(4), jid!(4));
        fixture.got_artifact_success_directory(
            digest!(4),
            short_path!("/z/tmp", 1),
            3,
            vec![jid!(4)],
            vec![
                PersistTempDir(short_path!("/z/tmp", 1), long_path!("/z/sha256/blob", 4)),
                Metadata(short_path!("/z/removing", 1)),
                Rename(
                    long_path!("/z/sha256/blob", 3),
                    short_path!("/z/removing", 1),
                ),
                RemoveRecursively(short_path!("/z/removing", 1)),
            ],
        );
    }

    #[test]
    fn multiple_get_requests_for_empty() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact_ign(digest!(42), jid!(1));
        fixture.get_artifact(digest!(42), jid!(2), GetArtifact::Wait);
        fixture.get_artifact(digest!(42), jid!(3), GetArtifact::Wait);

        fixture.got_artifact_success_directory(
            digest!(42),
            short_path!("/z/tmp", 1),
            100,
            vec![jid!(1), jid!(2), jid!(3)],
            vec![PersistTempDir(
                short_path!("/z/tmp", 1),
                long_path!("/z/sha256/blob", 42),
            )],
        );
    }

    #[test]
    fn multiple_get_requests_for_empty_larger_than_goal_remove_on_last_decrement() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact_ign(digest!(42), jid!(1));
        fixture.get_artifact(digest!(42), jid!(2), GetArtifact::Wait);
        fixture.get_artifact(digest!(42), jid!(3), GetArtifact::Wait);

        fixture.got_artifact_success_directory(
            digest!(42),
            short_path!("/z/tmp", 1),
            10000,
            vec![jid!(1), jid!(2), jid!(3)],
            vec![PersistTempDir(
                short_path!("/z/tmp", 1),
                long_path!("/z/sha256/blob", 42),
            )],
        );

        fixture.decrement_ref_count(digest!(42), vec![]);
        fixture.decrement_ref_count(digest!(42), vec![]);
        fixture.decrement_ref_count(
            digest!(42),
            vec![
                Metadata(short_path!("/z/removing", 1)),
                Rename(
                    long_path!("/z/sha256/blob", 42),
                    short_path!("/z/removing", 1),
                ),
                RemoveRecursively(short_path!("/z/removing", 1)),
            ],
        );
    }

    #[test]
    fn get_request_for_currently_used() {
        let mut fixture = Fixture::new_and_clear_messages(10);

        fixture.get_artifact_ign(digest!(42), jid!(1));
        fixture.got_artifact_success_directory_ign(digest!(42), 100);

        fixture.get_artifact(digest!(42), jid!(1), GetArtifact::Success);

        fixture.decrement_ref_count(digest!(42), vec![]);
        fixture.decrement_ref_count(
            digest!(42),
            vec![
                Metadata(short_path!("/z/removing", 1)),
                Rename(
                    long_path!("/z/sha256/blob", 42),
                    short_path!("/z/removing", 1),
                ),
                RemoveRecursively(short_path!("/z/removing", 1)),
            ],
        );
    }

    #[test]
    fn get_request_for_cached_followed_by_big_get_does_not_evict_until_decrement_ref_count() {
        let mut fixture = Fixture::new_and_clear_messages(100);

        fixture.get_artifact_ign(digest!(42), jid!(1));
        fixture.got_artifact_success_directory_ign(digest!(42), 10);
        fixture.decrement_ref_count_ign(digest!(42));

        fixture.get_artifact(digest!(42), jid!(2), GetArtifact::Success);
        fixture.get_artifact(digest!(43), jid!(3), GetArtifact::Get);
        fixture.got_artifact_success_directory(
            digest!(43),
            short_path!("/z/tmp", 1),
            100,
            vec![jid!(3)],
            vec![PersistTempDir(
                short_path!("/z/tmp", 1),
                long_path!("/z/sha256/blob", 43),
            )],
        );

        fixture.decrement_ref_count(
            digest!(42),
            vec![
                Metadata(short_path!("/z/removing", 1)),
                Rename(
                    long_path!("/z/sha256/blob", 42),
                    short_path!("/z/removing", 1),
                ),
                RemoveRecursively(short_path!("/z/removing", 1)),
            ],
        );
    }

    #[test]
    fn get_request_for_empty_with_get_failure() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact_ign(digest!(42), jid!(1));
        fixture.got_artifact_failure(digest!(42), vec![jid!(1)]);
    }

    #[test]
    fn preexisting_directories_do_not_affect_get_request() {
        let mut test_cache_fs = TestFs::default();
        test_cache_fs
            .metadata
            .insert(long_path!("/z/sha256/blob", 42), FileMetadata::directory(1));
        let mut fixture = Fixture::new_with_fs_and_clear_messages(test_cache_fs, 1000);

        fixture.get_artifact(digest!(42), jid!(1), GetArtifact::Get);
    }

    #[test]
    fn multiple_get_requests_for_empty_with_download_and_extract_failure() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact_ign(digest!(42), jid!(1));
        fixture.get_artifact_ign(digest!(42), jid!(2));
        fixture.get_artifact_ign(digest!(42), jid!(3));

        fixture.got_artifact_failure(digest!(42), vec![jid!(1), jid!(2), jid!(3)]);
    }

    #[test]
    fn get_after_error_retries() {
        let mut fixture = Fixture::new_and_clear_messages(1000);

        fixture.get_artifact_ign(digest!(42), jid!(1));
        fixture.got_artifact_failure(digest!(42), vec![jid!(1)]);
        fixture.get_artifact(digest!(42), jid!(2), GetArtifact::Get);
    }

    #[test]
    fn rename_retries_until_unique_path_name() {
        let mut test_cache_fs = TestFs::default();
        test_cache_fs.directories.insert(
            path_buf!("/z"),
            vec![(path_buf!("/z/tmp"), FileMetadata::directory(10))],
        );
        test_cache_fs
            .metadata
            .insert(path_buf!("/z/tmp"), FileMetadata::directory(1));
        test_cache_fs
            .metadata
            .insert(short_path!("/z/removing", 1), FileMetadata::file(42));
        test_cache_fs
            .metadata
            .insert(short_path!("/z/removing", 2), FileMetadata::file(42));
        test_cache_fs
            .metadata
            .insert(short_path!("/z/removing", 3), FileMetadata::file(42));
        let mut fixture = Fixture::new(test_cache_fs, 1000);
        fixture.expect_messages_in_specific_order(vec![
            MkdirRecursively(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z")),
            Metadata(short_path!("/z/removing", 1)),
            Metadata(short_path!("/z/removing", 2)),
            Metadata(short_path!("/z/removing", 3)),
            Metadata(short_path!("/z/removing", 4)),
            Rename(path_buf!("/z/tmp"), short_path!("/z/removing", 4)),
            RemoveRecursively(short_path!("/z/removing", 4)),
            Metadata(path_buf!("/z/CACHEDIR.TAG")),
            CreateFile(
                path_buf!("/z/CACHEDIR.TAG"),
                boxed_u8!(&CACHEDIR_TAG_CONTENTS),
            ),
            MkdirRecursively(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
            Metadata(path_buf!("/z/sha256/blob")),
            MkdirRecursively(path_buf!("/z/sha256/blob")),
            Metadata(path_buf!("/z/sha256/bottom_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/bottom_fs_layer")),
            Metadata(path_buf!("/z/sha256/upper_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/upper_fs_layer")),
        ]);
    }

    #[test]
    fn new_ensures_directories_exist() {
        let mut fixture = Fixture::new(TestFs::default(), 1000);
        fixture.expect_messages_in_specific_order(vec![
            MkdirRecursively(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z")),
            Metadata(path_buf!("/z/CACHEDIR.TAG")),
            CreateFile(
                path_buf!("/z/CACHEDIR.TAG"),
                boxed_u8!(&CACHEDIR_TAG_CONTENTS),
            ),
            MkdirRecursively(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
            Metadata(path_buf!("/z/sha256/blob")),
            MkdirRecursively(path_buf!("/z/sha256/blob")),
            Metadata(path_buf!("/z/sha256/bottom_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/bottom_fs_layer")),
            Metadata(path_buf!("/z/sha256/upper_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/upper_fs_layer")),
        ]);
    }

    #[test]
    fn new_does_not_write_cachedir_tag_if_it_exists() {
        let mut test_cache_fs = TestFs::default();
        test_cache_fs
            .metadata
            .insert(path_buf!("/z/CACHEDIR.TAG"), FileMetadata::file(42));
        let mut fixture = Fixture::new(test_cache_fs, 1000);
        fixture.expect_messages_in_specific_order(vec![
            MkdirRecursively(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z")),
            Metadata(path_buf!("/z/CACHEDIR.TAG")),
            MkdirRecursively(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
            Metadata(path_buf!("/z/sha256/blob")),
            MkdirRecursively(path_buf!("/z/sha256/blob")),
            Metadata(path_buf!("/z/sha256/bottom_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/bottom_fs_layer")),
            Metadata(path_buf!("/z/sha256/upper_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/upper_fs_layer")),
        ]);
    }

    #[test]
    fn new_restarts_old_removes() {
        let mut test_cache_fs = TestFs::default();
        test_cache_fs.directories.insert(
            path_buf!("/z"),
            vec![(path_buf!("/z/removing"), FileMetadata::directory(10))],
        );
        test_cache_fs.directories.insert(
            path_buf!("/z/removing"),
            vec![
                (short_path!("/z/removing", 10), FileMetadata::directory(10)),
                (short_path!("/z/removing", 20), FileMetadata::file(10)),
                (short_path!("/z/removing", 30), FileMetadata::symlink(10)),
            ],
        );
        let mut fixture = Fixture::new(test_cache_fs, 1000);
        fixture.expect_messages_in_specific_order(vec![
            MkdirRecursively(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z/removing")),
            RemoveRecursively(short_path!("/z/removing", 10)),
            Remove(short_path!("/z/removing", 20)),
            Remove(short_path!("/z/removing", 30)),
            ReadDir(path_buf!("/z")),
            Metadata(path_buf!("/z/CACHEDIR.TAG")),
            CreateFile(
                path_buf!("/z/CACHEDIR.TAG"),
                boxed_u8!(&CACHEDIR_TAG_CONTENTS),
            ),
            MkdirRecursively(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
            Metadata(path_buf!("/z/sha256/blob")),
            MkdirRecursively(path_buf!("/z/sha256/blob")),
            Metadata(path_buf!("/z/sha256/bottom_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/bottom_fs_layer")),
            Metadata(path_buf!("/z/sha256/upper_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/upper_fs_layer")),
        ]);
    }

    #[test]
    fn new_removes_top_level_garbage() {
        let mut test_cache_fs = TestFs::default();
        test_cache_fs.directories.insert(
            path_buf!("/z"),
            vec![
                (path_buf!("/z/blah"), FileMetadata::directory(10)),
                (path_buf!("/z/CACHEDIR.TAG"), FileMetadata::file(43)),
                (path_buf!("/z/sha256"), FileMetadata::directory(10)),
                (path_buf!("/z/baz"), FileMetadata::directory(10)),
                (path_buf!("/z/removing"), FileMetadata::directory(10)),
            ],
        );
        let mut fixture = Fixture::new(test_cache_fs, 1000);
        fixture.expect_messages_in_specific_order(vec![
            MkdirRecursively(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z")),
            Metadata(short_path!("/z/removing", 1)),
            Rename(path_buf!("/z/blah"), short_path!("/z/removing", 1)),
            RemoveRecursively(short_path!("/z/removing", 1)),
            Metadata(short_path!("/z/removing", 2)),
            Rename(path_buf!("/z/baz"), short_path!("/z/removing", 2)),
            RemoveRecursively(short_path!("/z/removing", 2)),
            Metadata(path_buf!("/z/CACHEDIR.TAG")),
            CreateFile(
                path_buf!("/z/CACHEDIR.TAG"),
                boxed_u8!(&CACHEDIR_TAG_CONTENTS),
            ),
            MkdirRecursively(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
            Metadata(path_buf!("/z/sha256/blob")),
            MkdirRecursively(path_buf!("/z/sha256/blob")),
            Metadata(path_buf!("/z/sha256/bottom_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/bottom_fs_layer")),
            Metadata(path_buf!("/z/sha256/upper_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/upper_fs_layer")),
        ]);
    }

    #[test]
    fn new_removes_garbage_in_sha256() {
        let mut test_cache_fs = TestFs::default();
        test_cache_fs.directories.insert(
            path_buf!("/z/sha256"),
            vec![
                (path_buf!("/z/sha256/blah"), FileMetadata::directory(10)),
                (path_buf!("/z/sha256/blob"), FileMetadata::directory(10)),
                (path_buf!("/z/sha256/baz"), FileMetadata::directory(10)),
                (
                    path_buf!("/z/sha256/bottom_fs_layer"),
                    FileMetadata::directory(10),
                ),
            ],
        );
        let mut fixture = Fixture::new(test_cache_fs, 1000);
        fixture.expect_messages_in_specific_order(vec![
            MkdirRecursively(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z")),
            Metadata(path_buf!("/z/CACHEDIR.TAG")),
            CreateFile(
                path_buf!("/z/CACHEDIR.TAG"),
                boxed_u8!(&CACHEDIR_TAG_CONTENTS),
            ),
            MkdirRecursively(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
            Metadata(short_path!("/z/removing", 1)),
            Rename(path_buf!("/z/sha256/blah"), short_path!("/z/removing", 1)),
            RemoveRecursively(short_path!("/z/removing", 1)),
            Metadata(short_path!("/z/removing", 2)),
            Rename(path_buf!("/z/sha256/baz"), short_path!("/z/removing", 2)),
            RemoveRecursively(short_path!("/z/removing", 2)),
            Metadata(path_buf!("/z/sha256/blob")),
            MkdirRecursively(path_buf!("/z/sha256/blob")),
            Metadata(path_buf!("/z/sha256/bottom_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/bottom_fs_layer")),
            Metadata(path_buf!("/z/sha256/upper_fs_layer")),
            MkdirRecursively(path_buf!("/z/sha256/upper_fs_layer")),
        ]);
    }

    #[test]
    fn new_removes_old_sha256_if_it_exists() {
        let mut test_cache_fs = TestFs::default();
        test_cache_fs
            .metadata
            .insert(path_buf!("/z/sha256/blob"), FileMetadata::directory(1));
        test_cache_fs.metadata.insert(
            path_buf!("/z/sha256/bottom_fs_layer"),
            FileMetadata::directory(1),
        );
        test_cache_fs.metadata.insert(
            path_buf!("/z/sha256/upper_fs_layer"),
            FileMetadata::directory(1),
        );
        test_cache_fs
            .metadata
            .insert(path_buf!("/z/sha256/blob"), FileMetadata::directory(42));
        test_cache_fs.metadata.insert(
            path_buf!("/z/sha256/bottom_fs_layer"),
            FileMetadata::directory(42),
        );
        test_cache_fs.metadata.insert(
            path_buf!("/z/sha256/upper_fs_layer"),
            FileMetadata::directory(42),
        );
        let mut fixture = Fixture::new(test_cache_fs, 1000);
        fixture.expect_messages_in_specific_order(vec![
            MkdirRecursively(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z/removing")),
            ReadDir(path_buf!("/z")),
            Metadata(path_buf!("/z/CACHEDIR.TAG")),
            CreateFile(
                path_buf!("/z/CACHEDIR.TAG"),
                boxed_u8!(&CACHEDIR_TAG_CONTENTS),
            ),
            MkdirRecursively(path_buf!("/z/tmp")),
            MkdirRecursively(path_buf!("/z/sha256")),
            ReadDir(path_buf!("/z/sha256")),
            Metadata(path_buf!("/z/sha256/blob")),
            Metadata(short_path!("/z/removing", 1)),
            Rename(path_buf!("/z/sha256/blob"), short_path!("/z/removing", 1)),
            RemoveRecursively(short_path!("/z/removing", 1)),
            MkdirRecursively(path_buf!("/z/sha256/blob")),
            Metadata(path_buf!("/z/sha256/bottom_fs_layer")),
            Metadata(short_path!("/z/removing", 2)),
            Rename(
                path_buf!("/z/sha256/bottom_fs_layer"),
                short_path!("/z/removing", 2),
            ),
            RemoveRecursively(short_path!("/z/removing", 2)),
            MkdirRecursively(path_buf!("/z/sha256/bottom_fs_layer")),
            Metadata(path_buf!("/z/sha256/upper_fs_layer")),
            Metadata(short_path!("/z/removing", 3)),
            Rename(
                path_buf!("/z/sha256/upper_fs_layer"),
                short_path!("/z/removing", 3),
            ),
            RemoveRecursively(short_path!("/z/removing", 3)),
            MkdirRecursively(path_buf!("/z/sha256/upper_fs_layer")),
        ]);
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum FsEntry {
        File { size: u64 },
        Directory { entries: Vec<(String, FsEntry)> },
        Symlink { target: String },
    }

    impl FsEntry {
        fn file(size: u64) -> Self {
            Self::File { size }
        }

        fn directory<const N: usize>(entries: [(&str, FsEntry); N]) -> Self {
            Self::Directory {
                entries: entries
                    .into_iter()
                    .map(|(name, entry)| (name.to_owned(), entry))
                    .collect(),
            }
        }

        fn symlink(target: impl ToString) -> Self {
            Self::Symlink {
                target: target.to_string(),
            }
        }

        fn metadata(&self) -> FileMetadata {
            match self {
                FsEntry::File { size } => FileMetadata::file(*size),
                FsEntry::Symlink { target } => {
                    FileMetadata::symlink(target.len().try_into().unwrap())
                }
                FsEntry::Directory { entries } => {
                    FileMetadata::directory(entries.len().try_into().unwrap())
                }
            }
        }

        fn is_directory(&self) -> bool {
            matches!(self, Self::Directory { .. })
        }
    }

    macro_rules! fs {
        (@expand [] -> [$($expanded:tt)*]) => {
            [$($expanded)*]
        };
        (@expand [$name:ident($size:literal) $(,$($tail:tt)*)?] -> [$($($expanded:tt)+)?]) => {
            fs!(
                @expand
                [$($($tail)*)?] -> [
                    $($($expanded)+,)?
                    (stringify!($name), FsEntry::file($size))
                ]
            )
        };
        (@expand [$name:ident -> $target:literal $(,$($tail:tt)*)?] -> [$($($expanded:tt)+)?]) => {
            fs!(
                @expand
                [$($($tail)*)?] -> [
                    $($($expanded)+,)?
                    (stringify!($name), FsEntry::symlink($target))
                ]
            )
        };
        (@expand [$name:ident { $($dirents:tt)* } $(,$($tail:tt)*)?] -> [$($($expanded:tt)+)?]) => {
            fs!(
                @expand
                [$($($tail)*)?] -> [
                    $($($expanded)+,)?
                    (stringify!($name), fs!($($dirents)*))
                ]
            )
        };
        ($($body:tt)*) => (FsEntry::directory(fs!(@expand [$($body)*] -> [])));
    }

    #[test]
    fn fs_empty() {
        assert_eq!(fs! {}, FsEntry::directory([]));
    }

    #[test]
    fn fs_one_file_no_comma() {
        assert_eq!(
            fs! {
                foo(42)
            },
            FsEntry::directory([("foo", FsEntry::file(42))])
        );
    }

    #[test]
    fn fs_one_file_comma() {
        assert_eq!(
            fs! {
                foo(42),
            },
            FsEntry::directory([("foo", FsEntry::file(42))])
        );
    }

    #[test]
    fn fs_one_symlink_no_comma() {
        assert_eq!(
            fs! {
                foo -> "/target"
            },
            FsEntry::directory([("foo", FsEntry::symlink("/target"))])
        );
    }

    #[test]
    fn fs_one_symlink_comma() {
        assert_eq!(
            fs! {
                foo -> "/target",
            },
            FsEntry::directory([("foo", FsEntry::symlink("/target"))])
        );
    }

    #[test]
    fn fs_one_directory_no_comma() {
        assert_eq!(
            fs! {
                foo {}
            },
            FsEntry::directory([("foo", FsEntry::directory([]))])
        );
    }

    #[test]
    fn fs_one_directory_comma() {
        assert_eq!(
            fs! {
                foo {},
            },
            FsEntry::directory([("foo", FsEntry::directory([]))])
        );
    }

    #[test]
    fn fs_one_directory_with_no_files() {
        assert_eq!(
            fs! {
                foo {},
            },
            FsEntry::directory([("foo", FsEntry::directory([]))])
        );
    }

    #[test]
    fn fs_kitchen_sink() {
        assert_eq!(
            fs! {
                foo(42),
                bar -> "/target/1",
                baz {
                    zero {},
                    one {
                        foo(43)
                    },
                    two {
                        foo(44),
                        bar -> "/target/2"
                    },
                }
            },
            FsEntry::directory([
                ("foo", FsEntry::file(42)),
                ("bar", FsEntry::symlink("/target/1")),
                (
                    "baz",
                    FsEntry::directory([
                        ("zero", FsEntry::directory([])),
                        ("one", FsEntry::directory([("foo", FsEntry::file(43))])),
                        (
                            "two",
                            FsEntry::directory([
                                ("foo", FsEntry::file(44)),
                                ("bar", FsEntry::symlink("/target/2")),
                            ])
                        ),
                    ])
                )
            ])
        );
    }

    #[derive(Debug)]
    struct TestFsState {
        root: FsEntry,
        last_random_number: u64,
    }

    #[derive(Debug)]
    struct TestFs2 {
        state: Rc<RefCell<TestFsState>>,
    }

    enum LookupIndexPath<'state> {
        /// The path resolved to an actual entry. This contains the entry and the path to it.
        Found(&'state FsEntry, Vec<usize>),

        /// There was entry at the given path, but the parent directory exists. This contains the
        /// parent entry (which is guaranteed to be a directory) and the path to it.
        #[expect(dead_code)]
        FoundParent(&'state FsEntry, Vec<usize>),

        /// There was no entry at the given path, nor does its parent directory exist. However, all
        /// ancestors that do exist are directories.
        NotFound,

        /// A symlink in the path didn't resolve.
        DanglingSymlink,

        /// An ancestor in the path was a file.
        FileAncestor,
    }

    impl TestFs2 {
        fn new(root: FsEntry) -> Self {
            assert!(root.is_directory());
            Self {
                state: Rc::new(RefCell::new(TestFsState {
                    root,
                    last_random_number: 0,
                })),
            }
        }

        #[track_caller]
        fn assert_tree(&self, expected: FsEntry) {
            assert_eq!(self.state.borrow().root, expected);
        }

        fn resolve_index_path(
            root: &FsEntry,
            index_path: impl IntoIterator<Item = usize>,
        ) -> &FsEntry {
            let mut cur = root;
            for index in index_path {
                let FsEntry::Directory { entries } = cur else {
                    panic!("intermediate path entry not a directory");
                };
                cur = &entries[index].1;
            }
            cur
        }

        fn resolve_index_path_mut_as_directory(
            root: &mut FsEntry,
            index_path: impl IntoIterator<Item = usize>,
        ) -> &mut Vec<(String, FsEntry)> {
            let mut cur = root;
            for index in index_path {
                let FsEntry::Directory { entries } = cur else {
                    panic!("intermediate path entry not a directory");
                };
                cur = &mut entries[index].1;
            }
            let FsEntry::Directory { entries } = cur else {
                panic!("entry not a directory");
            };
            entries
        }

        fn append_entry_to_directory(
            root: &mut FsEntry,
            directory_index_path: impl IntoIterator<Item = usize>,
            name: &OsStr,
            entry: FsEntry,
        ) {
            let directory_entries =
                Self::resolve_index_path_mut_as_directory(root, directory_index_path);
            directory_entries.push((name.to_str().unwrap().to_owned(), entry));
        }

        fn adjust_one_index_path_for_removal_of_other(
            to_keep: &mut Vec<usize>,
            to_remove: &Vec<usize>,
        ) {
            let to_remove_len = to_remove.len();
            for (i, (to_keep_index, to_remove_index)) in
                to_keep.iter_mut().zip(to_remove.iter()).enumerate()
            {
                if i + 1 == to_remove_len {
                    if *to_remove_index < *to_keep_index {
                        *to_keep_index -= 1;
                    }
                    return;
                } else if *to_keep_index != *to_remove_index {
                    return;
                }
            }
        }

        fn remove_leaf_from_index_path(
            root: &mut FsEntry,
            index_path: &Vec<usize>,
            to_keep_index_path: &mut Vec<usize>,
        ) -> FsEntry {
            assert!(!index_path.is_empty());
            Self::adjust_one_index_path_for_removal_of_other(to_keep_index_path, index_path);
            let mut cur = root;
            let index_path_len = index_path.len();
            for (i, index) in index_path.into_iter().enumerate() {
                let FsEntry::Directory { entries } = cur else {
                    panic!("intermediate path entry not a directory");
                };
                if i + 1 == index_path_len {
                    return entries.remove(*index).1;
                } else {
                    cur = &mut entries[*index].1;
                }
            }
            unreachable!();
        }

        fn pop_index_path(root: &FsEntry, mut index_path: Vec<usize>) -> (&FsEntry, Vec<usize>) {
            index_path.pop();
            (
                Self::resolve_index_path(root, index_path.iter().copied()),
                index_path,
            )
        }

        fn is_descendant_of(
            potential_descendant_index_path: &Vec<usize>,
            potential_ancestor_index_path: &Vec<usize>,
        ) -> bool {
            potential_descendant_index_path
                .iter()
                .zip(potential_ancestor_index_path.iter())
                .take_while(|(l, r)| *l == *r)
                .count()
                == potential_ancestor_index_path.len()
        }

        fn lookup_index_path<'state, 'path>(
            root: &'state FsEntry,
            mut cur: &'state FsEntry,
            mut index_path: Vec<usize>,
            path: &'path Path,
        ) -> LookupIndexPath<'state> {
            let num_path_components = path.components().count();
            for (component_index, component) in path.components().enumerate() {
                let last_component = component_index + 1 == num_path_components;
                match component {
                    Component::Prefix(_) => {
                        unimplemented!("prefix components don't occur in Unix")
                    }
                    Component::RootDir => {
                        cur = root;
                        index_path = vec![];
                    }
                    Component::CurDir => {}
                    Component::ParentDir => {
                        (cur, index_path) = Self::pop_index_path(root, index_path);
                    }
                    Component::Normal(name) => loop {
                        match cur {
                            FsEntry::Directory { entries } => {
                                let entry_index = entries
                                    .into_iter()
                                    .position(|(entry_name, _)| name.eq(OsStr::new(&entry_name)));
                                match entry_index {
                                    Some(entry_index) => {
                                        cur = &entries[entry_index].1;
                                        index_path.push(entry_index);
                                        break;
                                    }
                                    None => {
                                        if last_component {
                                            return LookupIndexPath::FoundParent(cur, index_path);
                                        } else {
                                            return LookupIndexPath::NotFound;
                                        }
                                    }
                                }
                            }
                            FsEntry::File { .. } => {
                                return LookupIndexPath::FileAncestor;
                            }
                            FsEntry::Symlink { target } => {
                                (cur, index_path) = Self::pop_index_path(root, index_path);
                                match Self::lookup_index_path(
                                    root,
                                    cur,
                                    index_path,
                                    Path::new(target),
                                ) {
                                    LookupIndexPath::Found(new_cur, new_index_path) => {
                                        cur = new_cur;
                                        index_path = new_index_path;
                                    }
                                    LookupIndexPath::FileAncestor => {
                                        return LookupIndexPath::FileAncestor;
                                    }
                                    _ => {
                                        return LookupIndexPath::DanglingSymlink;
                                    }
                                }
                            }
                        }
                    },
                }
            }
            LookupIndexPath::Found(cur, index_path)
        }
    }

    impl Fs for TestFs2 {
        type Error = TestFsError;

        type TempFile = TestTempFile;

        type TempDir = TestTempDir;

        fn rand_u64(&self) -> u64 {
            let mut state = self.state.borrow_mut();
            state.last_random_number += 1;
            state.last_random_number
        }

        fn rename(&self, source_path: &Path, dest_path: &Path) -> Result<(), TestFsError> {
            let mut state = self.state.borrow_mut();

            let (source_entry, mut source_index_path) =
                match Self::lookup_index_path(&state.root, &state.root, vec![], source_path) {
                    LookupIndexPath::FileAncestor => {
                        return Err(TestFsError::NotDir);
                    }
                    LookupIndexPath::DanglingSymlink
                    | LookupIndexPath::NotFound
                    | LookupIndexPath::FoundParent(_, _) => {
                        return Err(TestFsError::NoEnt);
                    }
                    LookupIndexPath::Found(source_entry, source_index_path) => {
                        (source_entry, source_index_path)
                    }
                };

            let mut dest_parent_index_path =
                match Self::lookup_index_path(&state.root, &state.root, vec![], dest_path) {
                    LookupIndexPath::FileAncestor => {
                        return Err(TestFsError::NotDir);
                    }
                    LookupIndexPath::DanglingSymlink | LookupIndexPath::NotFound => {
                        return Err(TestFsError::NoEnt);
                    }
                    LookupIndexPath::FoundParent(_, dest_parent_index_path) => {
                        if source_entry.is_directory()
                            && Self::is_descendant_of(&dest_parent_index_path, &source_index_path)
                        {
                            // We can't move a directory into one of its descendants, including itself.
                            return Err(TestFsError::Inval);
                        } else {
                            dest_parent_index_path
                        }
                    }
                    LookupIndexPath::Found(dest_entry, mut dest_index_path) => {
                        if source_entry.is_directory() {
                            if let FsEntry::Directory { entries } = dest_entry {
                                if !entries.is_empty() {
                                    // A directory can't be moved on top of a non-empty directory.
                                    return Err(TestFsError::NotEmpty);
                                } else if source_index_path == dest_index_path {
                                    // This is a weird edge case, but we allow it. We're moving an
                                    // empty directory on top of itself. It's unknown if this matches
                                    // Linux behavior, but it doesn't really matter as we don't
                                    // imagine ever seeing this in our cache code.
                                    return Ok(());
                                } else if Self::is_descendant_of(
                                    &dest_index_path,
                                    &source_index_path,
                                ) {
                                    // We can't move a directory into one of its descendants.
                                    return Err(TestFsError::Inval);
                                }
                            } else {
                                // A directory can't be moved on top of a non-directory.
                                return Err(TestFsError::NotDir);
                            }
                        } else if dest_entry.is_directory() {
                            // A non-directory can't be moved on top of a directory.
                            return Err(TestFsError::IsDir);
                        } else if source_index_path == dest_index_path {
                            // Moving something onto itself is an accepted edge case.
                            return Ok(());
                        }

                        // Remove the destination directory and proceed as if it wasn't there to begin
                        // with. We may have to adjust the source_index_path to account for the
                        // removal of the destination.
                        Self::remove_leaf_from_index_path(
                            &mut state.root,
                            &dest_index_path,
                            &mut source_index_path,
                        );
                        dest_index_path.pop();
                        dest_index_path
                    }
                };

            let source_entry = Self::remove_leaf_from_index_path(
                &mut state.root,
                &source_index_path,
                &mut dest_parent_index_path,
            );
            Self::append_entry_to_directory(
                &mut state.root,
                dest_parent_index_path,
                dest_path.file_name().unwrap(),
                source_entry,
            );

            Ok(())
        }

        fn remove(&self, path: &Path) -> Result<(), TestFsError> {
            let mut state = self.state.borrow_mut();
            match Self::lookup_index_path(&state.root, &state.root, vec![], path) {
                LookupIndexPath::FileAncestor => Err(TestFsError::NotDir),
                LookupIndexPath::DanglingSymlink
                | LookupIndexPath::NotFound
                | LookupIndexPath::FoundParent(_, _) => Err(TestFsError::NoEnt),
                LookupIndexPath::Found(FsEntry::Directory { .. }, _) => Err(TestFsError::IsDir),
                LookupIndexPath::Found(_, mut index_path) => {
                    let index = index_path.pop().unwrap();
                    Self::resolve_index_path_mut_as_directory(&mut state.root, index_path)
                        .remove(index);
                    Ok(())
                }
            }
        }

        fn rmdir_recursively_on_thread(&self, _path: PathBuf) -> Result<(), TestFsError> {
            unimplemented!()
            /*
            self.messages
                .borrow_mut()
                .push(RemoveRecursively(path.to_owned()));
                */
        }

        fn mkdir_recursively(&self, path: &Path) -> Result<(), TestFsError> {
            let mut state = self.state.borrow_mut();
            match Self::lookup_index_path(&state.root, &state.root, vec![], path) {
                LookupIndexPath::FileAncestor => Err(TestFsError::NotDir),
                LookupIndexPath::DanglingSymlink => Err(TestFsError::NoEnt),
                LookupIndexPath::Found(FsEntry::Directory { .. }, _) => Ok(()),
                LookupIndexPath::Found(_, _) => Err(TestFsError::Exists),
                LookupIndexPath::FoundParent(_, parent_index_path) => {
                    Self::append_entry_to_directory(
                        &mut state.root,
                        parent_index_path,
                        path.file_name().unwrap(),
                        FsEntry::directory([]),
                    );
                    Ok(())
                }
                LookupIndexPath::NotFound => {
                    drop(state);
                    self.mkdir_recursively(path.parent().unwrap())?;
                    self.mkdir_recursively(path)
                }
            }
        }

        fn read_dir(
            &self,
            path: &Path,
        ) -> Result<impl Iterator<Item = Result<(OsString, FileMetadata), TestFsError>>, TestFsError>
        {
            let state = self.state.borrow();
            match Self::lookup_index_path(&state.root, &state.root, vec![], path) {
                LookupIndexPath::Found(FsEntry::Directory { entries }, _) => Ok(entries
                    .iter()
                    .map(|(name, entry)| Ok((OsStr::new(name).to_owned(), entry.metadata())))
                    .collect_vec()
                    .into_iter()),
                LookupIndexPath::Found(_, _) | LookupIndexPath::FileAncestor => {
                    Err(TestFsError::NotDir)
                }
                LookupIndexPath::NotFound
                | LookupIndexPath::FoundParent(_, _)
                | LookupIndexPath::DanglingSymlink => Err(TestFsError::NoEnt),
            }
        }

        fn create_file(&self, path: &Path, contents: &[u8]) -> Result<(), TestFsError> {
            let mut state = self.state.borrow_mut();
            let parent_index_path =
                match Self::lookup_index_path(&state.root, &state.root, vec![], path) {
                    LookupIndexPath::FileAncestor => Err(TestFsError::NotDir),
                    LookupIndexPath::DanglingSymlink => Err(TestFsError::NoEnt),
                    LookupIndexPath::NotFound => Err(TestFsError::NoEnt),
                    LookupIndexPath::Found(_, _) => Err(TestFsError::Exists),
                    LookupIndexPath::FoundParent(_, index_path) => Ok(index_path),
                }?;
            Self::append_entry_to_directory(
                &mut state.root,
                parent_index_path,
                path.file_name().unwrap(),
                FsEntry::file(contents.len().try_into().unwrap()),
            );
            Ok(())
        }

        fn symlink(&self, target: &Path, link: &Path) -> Result<(), TestFsError> {
            let mut state = self.state.borrow_mut();
            let parent_index_path =
                match Self::lookup_index_path(&state.root, &state.root, vec![], target) {
                    LookupIndexPath::FileAncestor => Err(TestFsError::NotDir),
                    LookupIndexPath::DanglingSymlink => Err(TestFsError::NoEnt),
                    LookupIndexPath::NotFound => Err(TestFsError::NoEnt),
                    LookupIndexPath::Found(_, _) => Err(TestFsError::Exists),
                    LookupIndexPath::FoundParent(_, index_path) => Ok(index_path),
                }?;
            Self::append_entry_to_directory(
                &mut state.root,
                parent_index_path,
                target.file_name().unwrap(),
                FsEntry::symlink(link.to_str().unwrap()),
            );
            Ok(())
        }

        fn metadata(&self, path: &Path) -> Result<Option<FileMetadata>, TestFsError> {
            let state = self.state.borrow();
            match Self::lookup_index_path(&state.root, &state.root, vec![], path) {
                LookupIndexPath::Found(entry, _) => Ok(Some(entry.metadata())),
                LookupIndexPath::NotFound | LookupIndexPath::FoundParent(_, _) => Ok(None),
                LookupIndexPath::DanglingSymlink => Err(TestFsError::NoEnt),
                LookupIndexPath::FileAncestor => Err(TestFsError::NotDir),
            }
        }

        fn temp_file(&self, _parent: &Path) -> Self::TempFile {
            unimplemented!()
            /*
            let path = parent.join(format!("{:0>16x}", self.rand_u64()));
            self.messages.borrow_mut().push(TempFile(path.clone()));
            TestTempFile {
                path,
                messages: self.messages.clone(),
            }
            */
        }

        fn temp_dir(&self, _parent: &Path) -> Self::TempDir {
            unimplemented!()
            /*
            let path = parent.join(format!("{:0>16x}", self.rand_u64()));
            self.messages.borrow_mut().push(TempDir(path.clone()));
            TestTempDir {
                path,
                messages: self.messages.clone(),
            }
            */
        }
    }

    #[test]
    fn test_fs2_rand_u64() {
        let fs = TestFs2::new(fs! {});
        assert_eq!(fs.rand_u64(), 1);
        assert_eq!(fs.rand_u64(), 2);
        assert_eq!(fs.rand_u64(), 3);
    }

    #[test]
    fn test_fs2_metadata_empty() {
        let fs = TestFs2::new(fs! {});
        assert_eq!(
            fs.metadata(Path::new("/")),
            Ok(Some(FileMetadata::directory(0)))
        );
        assert_eq!(fs.metadata(Path::new("/foo")), Ok(None));
    }

    #[test]
    fn test_fs2_metadata() {
        let fs = TestFs2::new(fs! {
            foo(42),
            bar {
                baz -> "/target",
                root -> "/",
                a -> "b",
                b -> "../bar/c",
                c -> "/bar/d",
                d -> ".",
            },
        });
        assert_eq!(
            fs.metadata(Path::new("/")),
            Ok(Some(FileMetadata::directory(2)))
        );
        assert_eq!(
            fs.metadata(Path::new("/foo")),
            Ok(Some(FileMetadata::file(42)))
        );
        assert_eq!(
            fs.metadata(Path::new("/bar")),
            Ok(Some(FileMetadata::directory(6)))
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/baz")),
            Ok(Some(FileMetadata::symlink(7)))
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/root/foo")),
            Ok(Some(FileMetadata::file(42)))
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/root/bar/a")),
            Ok(Some(FileMetadata::symlink(1)))
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/a/baz")),
            Ok(Some(FileMetadata::symlink(7)))
        );

        assert_eq!(fs.metadata(Path::new("/foo2")), Ok(None));
        assert_eq!(
            fs.metadata(Path::new("/bar/baz/foo")),
            Err(TestFsError::NoEnt)
        );
        assert_eq!(fs.metadata(Path::new("/bar/baz2")), Ok(None));
        assert_eq!(fs.metadata(Path::new("/bar/baz2/blah")), Ok(None));
        assert_eq!(fs.metadata(Path::new("/foo/bar")), Err(TestFsError::NotDir));
    }

    #[test]
    fn test_fs2_create_file() {
        let fs = TestFs2::new(fs! {
            foo(42),
            bar {
                baz -> "/target",
                root -> "/",
                a -> "b",
                b -> "../bar/c",
                c -> "/bar/d",
                d -> ".",
            },
        });

        assert_eq!(fs.create_file(Path::new("/new_file"), b"contents"), Ok(()));
        assert_eq!(
            fs.metadata(Path::new("/new_file")),
            Ok(Some(FileMetadata::file(8)))
        );

        assert_eq!(
            fs.create_file(Path::new("/bar/root/bar/a/new_file"), b"contents-2"),
            Ok(())
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/new_file")),
            Ok(Some(FileMetadata::file(10)))
        );

        assert_eq!(
            fs.create_file(Path::new("/new_file"), b"contents-3"),
            Err(TestFsError::Exists)
        );
        assert_eq!(
            fs.create_file(Path::new("/foo"), b"contents-3"),
            Err(TestFsError::Exists)
        );
        assert_eq!(
            fs.create_file(Path::new("/blah/new_file"), b"contents-3"),
            Err(TestFsError::NoEnt)
        );
        assert_eq!(
            fs.create_file(Path::new("/foo/new_file"), b"contents-3"),
            Err(TestFsError::NotDir)
        );
        assert_eq!(
            fs.create_file(Path::new("/bar/baz/new_file"), b"contents-3"),
            Err(TestFsError::NoEnt)
        );
    }

    #[test]
    fn test_fs2_symlink() {
        let fs = TestFs2::new(fs! {
            foo(42),
            bar {
                baz -> "/target",
                root -> "/",
                a -> "b",
                b -> "../bar/c",
                c -> "/bar/d",
                d -> ".",
            },
        });

        assert_eq!(
            fs.symlink(Path::new("/new_symlink"), Path::new("new-target")),
            Ok(())
        );
        assert_eq!(
            fs.metadata(Path::new("/new_symlink")),
            Ok(Some(FileMetadata::symlink(10)))
        );

        assert_eq!(
            fs.symlink(
                Path::new("/bar/root/bar/a/new_symlink"),
                Path::new("new-target-2")
            ),
            Ok(())
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/new_symlink")),
            Ok(Some(FileMetadata::symlink(12)))
        );

        assert_eq!(
            fs.symlink(Path::new("/new_symlink"), Path::new("new-target-3")),
            Err(TestFsError::Exists)
        );
        assert_eq!(
            fs.symlink(Path::new("/foo"), Path::new("new-target-3")),
            Err(TestFsError::Exists)
        );
        assert_eq!(
            fs.symlink(Path::new("/blah/new_symlink"), Path::new("new-target-3")),
            Err(TestFsError::NoEnt)
        );
        assert_eq!(
            fs.symlink(Path::new("/foo/new_symlink"), Path::new("new-target-3")),
            Err(TestFsError::NotDir)
        );
        assert_eq!(
            fs.symlink(Path::new("/bar/baz/new_symlink"), Path::new("new-target-3")),
            Err(TestFsError::NoEnt)
        );
    }

    #[test]
    fn test_fs2_mkdir_recursively() {
        let fs = TestFs2::new(fs! {
            foo(42),
            bar {
                baz -> "/target",
                root -> "/",
                a -> "b",
                b -> "../bar/c",
                c -> "/bar/d",
                d -> ".",
            },
        });

        assert_eq!(fs.mkdir_recursively(Path::new("/")), Ok(()));
        assert_eq!(fs.mkdir_recursively(Path::new("/bar")), Ok(()));

        assert_eq!(
            fs.mkdir_recursively(Path::new("/new/directory/and/subdirectory")),
            Ok(())
        );
        assert_eq!(
            fs.metadata(Path::new("/new")),
            Ok(Some(FileMetadata::directory(1)))
        );
        assert_eq!(
            fs.metadata(Path::new("/new/directory")),
            Ok(Some(FileMetadata::directory(1)))
        );
        assert_eq!(
            fs.metadata(Path::new("/new/directory/and")),
            Ok(Some(FileMetadata::directory(1)))
        );
        assert_eq!(
            fs.metadata(Path::new("/new/directory/and/subdirectory")),
            Ok(Some(FileMetadata::directory(0)))
        );

        assert_eq!(
            fs.mkdir_recursively(Path::new("/bar/root/bar/a/new/directory")),
            Ok(())
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/new")),
            Ok(Some(FileMetadata::directory(1)))
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/new/directory")),
            Ok(Some(FileMetadata::directory(0)))
        );

        assert_eq!(
            fs.mkdir_recursively(Path::new("/foo")),
            Err(TestFsError::Exists)
        );
        assert_eq!(
            fs.mkdir_recursively(Path::new("/foo/baz")),
            Err(TestFsError::NotDir)
        );
        assert_eq!(
            fs.mkdir_recursively(Path::new("/bar/baz/new/directory")),
            Err(TestFsError::NoEnt)
        );
    }

    #[test]
    fn test_fs2_remove() {
        let fs = TestFs2::new(fs! {
            foo(42),
            bar {
                baz -> "/target",
                root -> "/",
                a -> "b",
                b -> "../bar/c",
                c -> "/bar/d",
                d -> ".",
            },
        });

        assert_eq!(fs.remove(Path::new("/")), Err(TestFsError::IsDir));
        assert_eq!(fs.remove(Path::new("/bar")), Err(TestFsError::IsDir));
        assert_eq!(fs.remove(Path::new("/foo/baz")), Err(TestFsError::NotDir));
        assert_eq!(
            fs.remove(Path::new("/bar/baz/blah")),
            Err(TestFsError::NoEnt)
        );

        assert_eq!(fs.remove(Path::new("/bar/baz")), Ok(()));
        assert_eq!(fs.metadata(Path::new("/bar/baz")), Ok(None));

        assert_eq!(fs.remove(Path::new("/bar/a")), Ok(()));
        assert_eq!(fs.metadata(Path::new("/bar/a")), Ok(None));
        assert_eq!(
            fs.metadata(Path::new("/bar/b")),
            Ok(Some(FileMetadata::symlink(8)))
        );

        assert_eq!(fs.remove(Path::new("/foo")), Ok(()));
        assert_eq!(fs.metadata(Path::new("/foo")), Ok(None));
    }

    #[test]
    fn test_fs2_read_dir() {
        let fs = TestFs2::new(fs! {
            foo(42),
            bar {
                baz -> "/target",
                root -> "/",
                subdir {},
            },
        });
        assert_eq!(
            fs.read_dir(Path::new("/"))
                .unwrap()
                .map(Result::unwrap)
                .collect_vec(),
            vec![
                ("foo".into(), FileMetadata::file(42)),
                ("bar".into(), FileMetadata::directory(3)),
            ],
        );
        assert_eq!(
            fs.read_dir(Path::new("/bar"))
                .unwrap()
                .map(Result::unwrap)
                .collect_vec(),
            vec![
                ("baz".into(), FileMetadata::symlink(7)),
                ("root".into(), FileMetadata::symlink(1)),
                ("subdir".into(), FileMetadata::directory(0)),
            ],
        );
        assert_eq!(
            fs.read_dir(Path::new("/bar/root/bar/subdir"))
                .unwrap()
                .map(Result::unwrap)
                .collect_vec(),
            vec![],
        );

        assert!(matches!(
            fs.read_dir(Path::new("/foo")),
            Err(TestFsError::NotDir)
        ));
        assert!(matches!(
            fs.read_dir(Path::new("/foo/foo")),
            Err(TestFsError::NotDir)
        ));
        assert!(matches!(
            fs.read_dir(Path::new("/bar/baz")),
            Err(TestFsError::NotDir)
        ));
        assert!(matches!(
            fs.read_dir(Path::new("/bar/baz/foo")),
            Err(TestFsError::NoEnt)
        ));
        assert!(matches!(
            fs.read_dir(Path::new("/blah")),
            Err(TestFsError::NoEnt)
        ));
        assert!(matches!(
            fs.read_dir(Path::new("/blah/blah")),
            Err(TestFsError::NoEnt)
        ));
    }

    #[test]
    fn test_fs2_rename_source_path_with_file_ancestor() {
        let expected = fs! { foo(42) };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo/bar"), Path::new("/bar")),
            Err(TestFsError::NotDir)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_source_path_with_dangling_symlink() {
        let expected = fs! { foo -> "/dangle" };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo/bar"), Path::new("/bar")),
            Err(TestFsError::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_source_path_not_found() {
        let expected = fs! {};
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar")),
            Err(TestFsError::NoEnt)
        );
        assert_eq!(
            fs.rename(Path::new("/foo/bar"), Path::new("/bar")),
            Err(TestFsError::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_destination_path_with_file_ancestor() {
        let expected = fs! { foo(42), bar(43) };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar/baz")),
            Err(TestFsError::NotDir)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_destination_path_with_dangling_symlink() {
        let expected = fs! { foo(42), bar -> "dangle" };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar/baz")),
            Err(TestFsError::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_destination_path_not_found() {
        let expected = fs! { foo(42) };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar/baz")),
            Err(TestFsError::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_directory_into_new_entry_in_itself() {
        let expected = fs! { foo { bar { baz {} } } };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/foo/bar/baz/a")),
            Err(TestFsError::Inval)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_directory_into_new_entry_in_descendant_of_itself() {
        let expected = fs! { dir {} };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir"), Path::new("/dir/a")),
            Err(TestFsError::Inval)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_directory_onto_nonempty_directory() {
        let expected = fs! { dir1 {}, dir2 { foo(42) } };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir1"), Path::new("/dir2")),
            Err(TestFsError::NotEmpty)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_nonempty_directory_onto_itself() {
        let expected = fs! { dir { foo(42) } };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir"), Path::new("/dir")),
            Err(TestFsError::NotEmpty)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_nonempty_root_onto_itself() {
        let expected = fs! { foo(42) };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/"), Path::new("/")),
            Err(TestFsError::NotEmpty)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_directory_onto_descendant() {
        let expected = fs! { dir1 { dir2 {} } };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir1"), Path::new("/dir1/dir2")),
            Err(TestFsError::Inval)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_directory_onto_nondirectory() {
        let expected = fs! { dir {}, file(42), symlink -> "file" };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir"), Path::new("/file")),
            Err(TestFsError::NotDir)
        );
        assert_eq!(
            fs.rename(Path::new("/dir"), Path::new("/symlink")),
            Err(TestFsError::NotDir)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_nondirectory_onto_directory() {
        let expected = fs! { dir {}, file(42), symlink -> "file" };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/file"), Path::new("/dir")),
            Err(TestFsError::IsDir)
        );
        assert_eq!(
            fs.rename(Path::new("/symlink"), Path::new("/dir")),
            Err(TestFsError::IsDir)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_empty_root_onto_itself() {
        let fs = TestFs2::new(fs! {});
        assert_eq!(fs.rename(Path::new("/"), Path::new("/")), Ok(()));
        fs.assert_tree(fs! {});
    }

    #[test]
    fn test_fs2_rename_empty_directory_onto_itself() {
        let expected = fs! {
            dir {},
            root -> "/",
            root2 -> ".",
        };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(fs.rename(Path::new("/dir"), Path::new("/dir")), Ok(()));
        fs.assert_tree(expected.clone());
        assert_eq!(fs.rename(Path::new("/dir"), Path::new("/root/dir")), Ok(()));
        fs.assert_tree(expected.clone());
        assert_eq!(fs.rename(Path::new("/root/dir"), Path::new("/dir")), Ok(()));
        fs.assert_tree(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/root/root2/dir"), Path::new("/root2/root/dir")),
            Ok(())
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_file_onto_itself() {
        let expected = fs! {
            file(42),
            root -> "/",
            root2 -> ".",
        };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(fs.rename(Path::new("/file"), Path::new("/file")), Ok(()));
        fs.assert_tree(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/file"), Path::new("/root/file")),
            Ok(())
        );
        fs.assert_tree(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/root/file"), Path::new("/file")),
            Ok(())
        );
        fs.assert_tree(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/root/root2/file"), Path::new("/root2/root/file")),
            Ok(())
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_symlink_onto_itself() {
        let expected = fs! {
            symlink -> "dangle",
            root -> "/",
            root2 -> ".",
        };
        let fs = TestFs2::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/symlink"), Path::new("/symlink")),
            Ok(())
        );
        fs.assert_tree(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/symlink"), Path::new("/root/symlink")),
            Ok(())
        );
        fs.assert_tree(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/root/symlink"), Path::new("/symlink")),
            Ok(())
        );
        fs.assert_tree(expected.clone());
        assert_eq!(
            fs.rename(
                Path::new("/root/root2/symlink"),
                Path::new("/root2/root/symlink")
            ),
            Ok(())
        );
        fs.assert_tree(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/root"), Path::new("/root2/root")),
            Ok(())
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_file_onto_file() {
        let fs = TestFs2::new(fs! { foo(42), bar(43) });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar(42) });
    }

    #[test]
    fn test_fs2_rename_file_onto_symlink() {
        let fs = TestFs2::new(fs! { foo(42), bar -> "foo" });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar(42) });
    }

    #[test]
    fn test_fs2_rename_symlink_onto_file() {
        let fs = TestFs2::new(fs! { foo -> "bar", bar(43) });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar -> "bar" });
    }

    #[test]
    fn test_fs2_rename_symlink_onto_symlink() {
        let fs = TestFs2::new(fs! { foo -> "bar", bar -> "foo" });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar -> "bar" });
    }

    #[test]
    fn test_fs2_rename_into_new_entries_in_directory() {
        let fs = TestFs2::new(fs! {
            a {
                file(42),
                symlink -> "/dangle",
                dir { c(42), d -> "c", e {} },
            },
            b {},
        });
        assert_eq!(fs.rename(Path::new("/a/file"), Path::new("/b/xile")), Ok(()));
        assert_eq!(fs.rename(Path::new("/a/symlink"), Path::new("/b/xymlink")), Ok(()));
        assert_eq!(fs.rename(Path::new("/a/dir"), Path::new("/b/xir")), Ok(()));
        fs.assert_tree(fs! {
            a {},
            b {
                xile(42),
                xymlink -> "/dangle",
                xir { c(42), d -> "c", e {} },
            },
        });
    }

    #[test]
    fn test_fs2_rename_directory() {
        let fs = TestFs2::new(fs! {
            a {
                b {
                    c(42),
                    d -> "c",
                    e {},
                },
            },
            f {},
        });
        assert_eq!(fs.rename(Path::new("/a/b"), Path::new("/f/g")), Ok(()));
        fs.assert_tree(fs! {
            a {},
            f {
                g {
                    c(42),
                    d -> "c",
                    e {},
                },
            },
        });
    }

    #[test]
    fn test_fs2_rename_destination_in_ancestor_of_source() {
        let fs = TestFs2::new(fs! {
            before(41),
            target(42),
            dir1 {
                dir2 {
                    source(43),
                },
            },
            after(44),
        });
        assert_eq!(fs.rename(Path::new("/dir1/dir2/source"), Path::new("/target")), Ok(()));
        fs.assert_tree(fs! {
            before(41),
            dir1 {
                dir2 {},
            },
            after(44),
            target(43),
        });
    }

    #[test]
    fn test_fs2_rename_source_in_ancestor_of_target() {
        let fs = TestFs2::new(fs! {
            before(41),
            source(42),
            dir1 {
                dir2 {
                    target(43),
                },
            },
            after(44),
        });
        assert_eq!(fs.rename(Path::new("/source"), Path::new("/dir1/dir2/target")), Ok(()));
        fs.assert_tree(fs! {
            before(41),
            dir1 {
                dir2 {
                    target(42),
                },
            },
            after(44),
        });
    }
}
