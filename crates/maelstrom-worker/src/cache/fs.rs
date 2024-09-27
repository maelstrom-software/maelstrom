use std::{
    error,
    ffi::OsString,
    fmt::Debug,
    fs::{self, File},
    io::{self, ErrorKind, Write as _},
    os::unix::fs as unix_fs,
    path::{Path, PathBuf},
    thread,
};
use strum::Display;
use tempfile::{NamedTempFile, TempDir, TempPath};

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
    pub type_: FileType,
    pub size: u64,
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

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use std::{cell::RefCell, cmp::Ordering, ffi::OsStr, path::Component, rc::Rc};

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

    #[derive(Debug)]
    struct TestTempFile {
        path: PathBuf,
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

        fn persist(self, _target: &Path) {
            todo!()
        }
    }

    #[derive(Debug)]
    struct TestTempDir {
        path: PathBuf,
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

        fn persist(self, _target: &Path) {
            todo!()
        }
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
        assert_eq!(
            fs.rename(Path::new("/a/file"), Path::new("/b/xile")),
            Ok(())
        );
        assert_eq!(
            fs.rename(Path::new("/a/symlink"), Path::new("/b/xymlink")),
            Ok(())
        );
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
        assert_eq!(
            fs.rename(Path::new("/dir1/dir2/source"), Path::new("/target")),
            Ok(())
        );
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
        assert_eq!(
            fs.rename(Path::new("/source"), Path::new("/dir1/dir2/target")),
            Ok(())
        );
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
