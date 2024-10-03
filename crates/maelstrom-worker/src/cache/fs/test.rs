use super::Metadata;
use itertools::{Itertools, Position};
use maelstrom_util::ext::{BoolExt as _, OptionExt as _};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    error,
    ffi::{OsStr, OsString},
    fmt::Debug,
    path::{Component, Path, PathBuf},
    rc::Rc,
    slice, vec,
};
use strum::Display;

/// This macro is used to create a file-system tree to be passed to various functions in this
/// module. It expands to an [`Entry::Directory`].
///
/// Here is an example that illustrates most of the features:
/// ```
/// fs! {
///     subdir {
///         file_in_subdir(b"123"),
///         symlink_in_subdir -> "../subdir2",
///         nested_subdir {
///             file_in_nested_subdir(43),
///         },
///     },
///     "subdir2" {},
///     "file"(b"456"),
///     "symlink" -> "subdir",
/// }
/// ```
///
/// More specifically, this macro expects an "entry list". An entry list consists of zero or more
/// "entries", separated by ","s, with an optional trailing ",".
///
/// Each entry must be one of:
///   - A file entry, which consists of the file name, plus the contents of the file in parenthesis.
///     The file name can either be a bare word, if it's a valid identifier, or a quoted string.
///   - A symlink entry, which consists of the symlink name, the token "->", then the target of the
///     link in quotes. The symlink name can either be a bare word, if it's a valid identifier, or
///     a quoted string.
///   - A directory entry, which consists of the directory name, plus an entry list enclosed in
///     curly braces. The directory name can either be a bare word, if it's a valid identifier, or
///     a quoted string.
macro_rules! fs {
    (@expand [] -> [$($expanded:tt)*]) => {
        [$($expanded)*]
    };
    (@expand [$name:ident($contents:expr) $(,$($tail:tt)*)?] -> [$($($expanded:tt)+)?]) => {
        fs!(
            @expand
            [$($($tail)*)?] -> [
                $($($expanded)+,)?
                (stringify!($name), $crate::cache::fs::test::Entry::file($contents))
            ]
        )
    };
    (@expand [$name:literal($contents:expr) $(,$($tail:tt)*)?] -> [$($($expanded:tt)+)?]) => {
        fs!(
            @expand
            [$($($tail)*)?] -> [
                $($($expanded)+,)?
                ($name, $crate::cache::fs::test::Entry::file($contents))
            ]
        )
    };
    (@expand [$name:ident -> $target:literal $(,$($tail:tt)*)?] -> [$($($expanded:tt)+)?]) => {
        fs!(
            @expand
            [$($($tail)*)?] -> [
                $($($expanded)+,)?
                (stringify!($name), $crate::cache::fs::test::Entry::symlink($target))
            ]
        )
    };
    (@expand [$name:literal -> $target:literal $(,$($tail:tt)*)?] -> [$($($expanded:tt)+)?]) => {
        fs!(
            @expand
            [$($($tail)*)?] -> [
                $($($expanded)+,)?
                ($name, $crate::cache::fs::test::Entry::symlink($target))
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
    (@expand [$name:literal { $($dirents:tt)* } $(,$($tail:tt)*)?] -> [$($($expanded:tt)+)?]) => {
        fs!(
            @expand
            [$($($tail)*)?] -> [
                $($($expanded)+,)?
                ($name, fs!($($dirents)*))
            ]
        )
    };
    ($($body:tt)*) => ($crate::cache::fs::test::Entry::directory(fs!(@expand [$($body)*] -> [])));
}

pub(crate) use fs;

#[derive(Debug)]
pub struct Fs {
    state: RefCell<State>,
}

impl Fs {
    pub fn new(root: Entry) -> Self {
        assert!(root.is_directory());
        Self {
            state: RefCell::new(State {
                root,
                last_random_number: 0,
                recursive_rmdirs: Default::default(),
            }),
        }
    }

    #[track_caller]
    pub fn assert_tree(&self, expected: Entry) {
        self.assert_entry(Path::new("/"), expected);
    }

    #[track_caller]
    pub fn assert_entry(&self, path: &Path, expected: Entry) {
        let root = &self.state.borrow().root;
        let LookupComponentPath::Found(entry, _) = root.lookup_component_path(path) else {
            panic!("couldn't reolve {path:?}");
        };
        assert_eq!(entry, &expected);
    }

    #[track_caller]
    pub fn assert_recursive_rmdirs(&self, expected: HashSet<String>) {
        assert_eq!(self.state.borrow().recursive_rmdirs, expected);
    }

    pub fn complete_recursive_rmdir(&self, path: &str) {
        let mut state = self.state.borrow_mut();

        state.recursive_rmdirs.remove(path).assert_is_true();

        match state.root.lookup_component_path(Path::new(path)) {
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::DanglingSymlink
            | LookupComponentPath::NotFound
            | LookupComponentPath::FoundParent(_) => Err(Error::NoEnt),
            LookupComponentPath::Found(Entry::Directory { .. }, mut component_path) => {
                let component = component_path.pop().unwrap();
                state
                    .root
                    .resolve_component_path_mut(&component_path)
                    .directory_entries_mut()
                    .remove(&component)
                    .assert_is_some();
                Ok(())
            }
            LookupComponentPath::Found(_, _) => Err(Error::NotDir),
        }
        .unwrap();
    }
}

impl super::Fs for Fs {
    type Error = Error;

    fn rand_u64(&self) -> u64 {
        self.state.borrow_mut().rand_u64()
    }

    fn metadata(&self, path: &Path) -> Result<Option<Metadata>, Error> {
        self.state.borrow().metadata(path)
    }

    fn read_file(&self, path: &Path, contents_out: &mut [u8]) -> Result<usize, Error> {
        self.state.borrow().read_file(path, contents_out)
    }

    fn read_dir(
        &self,
        path: &Path,
    ) -> Result<impl Iterator<Item = Result<(OsString, Metadata), Error>>, Error> {
        self.state.borrow().read_dir(path)
    }

    fn create_file(&self, path: &Path, contents: &[u8]) -> Result<(), Error> {
        self.state.borrow_mut().create_file(path, contents)
    }

    fn symlink(&self, target: &Path, link: &Path) -> Result<(), Error> {
        self.state.borrow_mut().symlink(target, link)
    }

    fn mkdir(&self, path: &Path) -> Result<(), Error> {
        self.state.borrow_mut().mkdir(path)
    }

    fn mkdir_recursively(&self, path: &Path) -> Result<(), Error> {
        self.state.borrow_mut().mkdir_recursively(path)
    }

    fn remove(&self, path: &Path) -> Result<(), Error> {
        self.state.borrow_mut().remove(path)
    }

    fn rmdir_recursively_on_thread(&self, path: PathBuf) -> Result<(), Error> {
        self.state.borrow_mut().rmdir_recursively_on_thread(path)
    }

    fn rename(&self, source_path: &Path, dest_path: &Path) -> Result<(), Error> {
        self.state.borrow_mut().rename(source_path, dest_path)
    }

    type TempFile = TempFile;

    fn temp_file(&self, parent: &Path) -> Result<TempFile, Error> {
        self.state.borrow_mut().temp_file(parent)
    }

    fn persist_temp_file(&self, temp_file: TempFile, target: &Path) -> Result<(), Error> {
        self.state.borrow_mut().persist_temp_file(temp_file, target)
    }

    type TempDir = TempDir;

    fn temp_dir(&self, parent: &Path) -> Result<TempDir, Error> {
        self.state.borrow_mut().temp_dir(parent)
    }

    fn persist_temp_dir(&self, temp_dir: TempDir, target: &Path) -> Result<(), Error> {
        self.state.borrow_mut().persist_temp_dir(temp_dir, target)
    }
}

/// The potential errors that can happen in a file-system operation. These are modelled off of
/// errnos.
#[derive(Debug, Display, PartialEq)]
pub enum Error {
    Exists,
    IsDir,
    Inval,
    NoEnt,
    NotDir,
    NotEmpty,
}

impl error::Error for Error {}

#[derive(Debug)]
pub struct TempFile(PathBuf);

impl super::TempFile for TempFile {
    fn path(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug)]
pub struct TempDir(PathBuf);

impl super::TempDir for TempDir {
    fn path(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug)]
struct State {
    root: Entry,
    last_random_number: u64,
    recursive_rmdirs: HashSet<String>,
}

impl State {
    fn rand_u64(&mut self) -> u64 {
        self.last_random_number += 1;
        self.last_random_number
    }

    fn metadata(&self, path: &Path) -> Result<Option<Metadata>, Error> {
        match self.root.lookup_component_path(path) {
            LookupComponentPath::Found(entry, _) => Ok(Some(entry.metadata())),
            LookupComponentPath::FoundParent(_) | LookupComponentPath::NotFound => Ok(None),
            LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
        }
    }

    fn read_file(&self, path: &Path, contents_out: &mut [u8]) -> Result<usize, Error> {
        match self.root.lookup_leaf(path)? {
            ExpandSymlinks::FoundDirectory(_, _) => Err(Error::IsDir),
            ExpandSymlinks::DanglingSymlink => Err(Error::NoEnt),
            ExpandSymlinks::FileAncestor => Err(Error::NotDir),
            ExpandSymlinks::FoundFile(contents) => {
                let to_read = contents.len().min(contents_out.len());
                contents_out[..to_read].copy_from_slice(&contents[..to_read]);
                Ok(to_read)
            }
        }
    }

    fn read_dir(
        &self,
        path: &Path,
    ) -> Result<impl Iterator<Item = Result<(OsString, Metadata), Error>>, Error> {
        match self.root.lookup_leaf(path)? {
            ExpandSymlinks::FoundFile(_) | ExpandSymlinks::FileAncestor => Err(Error::NotDir),
            ExpandSymlinks::DanglingSymlink => Err(Error::NoEnt),
            ExpandSymlinks::FoundDirectory(entries, _) => Ok(entries
                .iter()
                .map(|(name, entry)| Ok((name.into(), entry.metadata())))
                .collect_vec()
                .into_iter()),
        }
    }

    fn create_file(&mut self, path: &Path, contents: &[u8]) -> Result<(), Error> {
        let parent_component_path = self.root.lookup_parent(path)?;
        self.root.append_entry_to_directory(
            &parent_component_path,
            path.file_name().unwrap(),
            Entry::file(contents),
        );
        Ok(())
    }

    fn symlink(&mut self, target: &Path, link: &Path) -> Result<(), Error> {
        let parent_component_path = self.root.lookup_parent(link)?;
        self.root.append_entry_to_directory(
            &parent_component_path,
            link.file_name().unwrap(),
            Entry::symlink(target.to_str().unwrap()),
        );
        Ok(())
    }

    fn mkdir(&mut self, path: &Path) -> Result<(), Error> {
        let parent_component_path = self.root.lookup_parent(path)?;
        self.root.append_entry_to_directory(
            &parent_component_path,
            path.file_name().unwrap(),
            Entry::directory([]),
        );
        Ok(())
    }

    fn mkdir_recursively(&mut self, path: &Path) -> Result<(), Error> {
        match self.root.lookup_component_path(path) {
            LookupComponentPath::Found(Entry::Directory { .. }, _) => Ok(()),
            LookupComponentPath::Found(_, _) => Err(Error::Exists),
            LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::FoundParent(_) => self.mkdir(path),
            LookupComponentPath::NotFound => {
                self.mkdir_recursively(path.parent().unwrap())?;
                self.mkdir(path)
            }
        }
    }

    fn remove(&mut self, path: &Path) -> Result<(), Error> {
        match self.root.lookup_component_path(path) {
            LookupComponentPath::Found(Entry::Directory { .. }, _) => Err(Error::IsDir),
            LookupComponentPath::FoundParent(_)
            | LookupComponentPath::NotFound
            | LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::Found(
                Entry::File { .. } | Entry::Symlink { .. },
                mut component_path,
            ) => {
                let component = component_path.pop().unwrap();
                self.root
                    .resolve_component_path_mut(&component_path)
                    .directory_entries_mut()
                    .remove(&component)
                    .assert_is_some();
                Ok(())
            }
        }
    }

    fn rmdir_recursively_on_thread(&mut self, path: PathBuf) -> Result<(), Error> {
        match self.root.lookup_component_path(&path) {
            LookupComponentPath::Found(Entry::File { .. } | Entry::Symlink { .. }, _) => {
                Err(Error::NotDir)
            }
            LookupComponentPath::FoundParent(_)
            | LookupComponentPath::NotFound
            | LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::Found(Entry::Directory { .. }, component_path) => {
                if component_path.is_empty() {
                    // Can't rmdir /.
                    Err(Error::Inval)
                } else {
                    self.recursive_rmdirs
                        .insert(path.into_os_string().into_string().unwrap())
                        .assert_is_true();
                    Ok(())
                }
            }
        }
    }

    fn rename(&mut self, source_path: &Path, dest_path: &Path) -> Result<(), Error> {
        let (source_entry, source_component_path) =
            match self.root.lookup_component_path(source_path) {
                LookupComponentPath::FoundParent(_)
                | LookupComponentPath::NotFound
                | LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
                LookupComponentPath::FileAncestor => Err(Error::NotDir),
                LookupComponentPath::Found(source_entry, source_component_path) => {
                    Ok((source_entry, source_component_path))
                }
            }?;

        let dest_parent_component_path = match self.root.lookup_component_path(dest_path) {
            LookupComponentPath::NotFound | LookupComponentPath::DanglingSymlink => {
                return Err(Error::NoEnt);
            }
            LookupComponentPath::FileAncestor => {
                return Err(Error::NotDir);
            }
            LookupComponentPath::Found(dest_entry, dest_component_path) => {
                if source_entry.is_directory() {
                    if let Entry::Directory { entries } = dest_entry {
                        if !entries.is_empty() {
                            // A directory can't be moved on top of a non-empty directory.
                            return Err(Error::NotEmpty);
                        } else if source_component_path == dest_component_path {
                            // This is a weird edge case, but we allow it. We're moving an
                            // empty directory on top of itself. It's unknown if this matches
                            // Linux behavior, but it doesn't really matter as we don't
                            // imagine ever seeing this in our cache code.
                            return Ok(());
                        } else if dest_component_path.is_descendant_of(&source_component_path) {
                            // We can't move a directory into one of its descendants.
                            return Err(Error::Inval);
                        }
                    } else {
                        // A directory can't be moved on top of a non-directory.
                        return Err(Error::NotDir);
                    }
                } else if dest_entry.is_directory() {
                    // A non-directory can't be moved on top of a directory.
                    return Err(Error::IsDir);
                } else if source_component_path == dest_component_path {
                    // Moving something onto itself is an accepted edge case.
                    return Ok(());
                }

                // Remove the destination entry and proceed like it wasn't there to begin with.
                self.root.remove_component_path(dest_component_path).1
            }
            LookupComponentPath::FoundParent(dest_parent_component_path) => {
                if source_entry.is_directory()
                    && dest_parent_component_path.is_descendant_of(&source_component_path)
                {
                    // We can't move a directory into one of its descendants, including itself.
                    return Err(Error::Inval);
                } else {
                    dest_parent_component_path
                }
            }
        };

        let (source_entry, _) = self.root.remove_component_path(source_component_path);
        self.root.append_entry_to_directory(
            &dest_parent_component_path,
            dest_path.file_name().unwrap(),
            source_entry,
        );

        Ok(())
    }

    fn temp_file(&mut self, parent: &Path) -> Result<TempFile, Error> {
        for i in 0.. {
            let path = parent.join(format!("{i:03}"));
            match self.create_file(&path, b"") {
                Ok(()) => {
                    return Ok(TempFile(path));
                }
                Err(Error::Exists) => {
                    continue;
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
        unreachable!();
    }

    fn persist_temp_file(&mut self, temp_file: TempFile, target: &Path) -> Result<(), Error> {
        self.rename(&temp_file.0, target)
    }

    fn temp_dir(&mut self, parent: &Path) -> Result<TempDir, Error> {
        for i in 0.. {
            let path = parent.join(format!("{i:03}"));
            match self.mkdir(&path) {
                Ok(()) => {
                    return Ok(TempDir(path));
                }
                Err(Error::Exists) => {
                    continue;
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
        unreachable!();
    }

    fn persist_temp_dir(&mut self, temp_dir: TempDir, target: &Path) -> Result<(), Error> {
        self.rename(&temp_dir.0, target)
    }
}

/// A file-system entry. There's no notion of hard links or things like sockets, devices, etc. in
/// this test file system.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Entry {
    File { contents: Box<[u8]> },
    Directory { entries: BTreeMap<String, Entry> },
    Symlink { target: String },
}

impl Entry {
    /// Create a new [`Entry::File`] of the given `size`.
    pub fn file<'a>(contents: impl IntoIterator<Item = &'a u8>) -> Self {
        Self::File {
            contents: contents
                .into_iter()
                .copied()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    /// Create a new [`Entry::Directory`] with the given `entries`.
    pub fn directory<const N: usize>(entries: [(&str, Self); N]) -> Self {
        Self::Directory {
            entries: entries
                .into_iter()
                .map(|(name, entry)| (name.to_owned(), entry))
                .collect(),
        }
    }

    /// Create a new [`Entry::Symlink`] that points to `target`.
    pub fn symlink(target: impl ToString) -> Self {
        Self::Symlink {
            target: target.to_string(),
        }
    }

    /// Return the [`FileMetadata`] for an [`Entry`].
    fn metadata(&self) -> Metadata {
        match self {
            Self::File { contents } => Metadata::file(contents.len().try_into().unwrap()),
            Self::Symlink { target } => Metadata::symlink(target.len().try_into().unwrap()),
            Self::Directory { entries } => Metadata::directory(entries.len().try_into().unwrap()),
        }
    }

    /// Return true iff [`Entry`] is a directory.
    fn is_directory(&self) -> bool {
        matches!(self, Self::Directory { .. })
    }

    /// Assume [`Entry`] is a directory, and return a reference to its entries.
    fn directory_entries(&self) -> &BTreeMap<String, Self> {
        let Self::Directory { entries } = self else {
            panic!("expected entry {self:?} to be a directory");
        };
        entries
    }

    /// Assume [`Entry`] is a directory, and return a mutable reference to its entries.
    fn directory_entries_mut(&mut self) -> &mut BTreeMap<String, Self> {
        let Self::Directory { entries } = self else {
            panic!("expected entry {self:?} to be a directory");
        };
        entries
    }

    /// Treating `self` as the root of the file system, attempt to resolve `path`.
    fn lookup_component_path<'state, 'path>(
        &'state self,
        path: &'path Path,
    ) -> LookupComponentPath<'state> {
        self.lookup_component_path_helper(self, Default::default(), path)
    }

    /// Treating `self` as the root of the file system, resolve `path`. `cur` indicates the
    /// directory the resolution should be done relative to, while `component_path` represents the
    /// component path to `cur`.
    fn lookup_component_path_helper<'state, 'path>(
        &'state self,
        mut cur: &'state Self,
        mut component_path: ComponentPath,
        path: &'path Path,
    ) -> LookupComponentPath<'state> {
        for component in path.components().with_position() {
            let is_last_component = matches!(component, Position::Last(_) | Position::Only(_));
            let component = component.into_inner();
            match component {
                Component::Prefix(_) => {
                    unimplemented!("prefix components don't occur in Unix")
                }
                Component::RootDir => {
                    cur = self;
                    component_path = Default::default();
                }
                Component::CurDir => {}
                Component::ParentDir => {
                    cur = self.pop_component_path(&mut component_path);
                }
                Component::Normal(name) => match self.expand_symlinks(cur, component_path) {
                    ExpandSymlinks::FoundDirectory(entries, new_component_path) => {
                        let name = name.to_str().unwrap();
                        match entries.get(name) {
                            Some(entry) => {
                                cur = entry;
                                component_path = new_component_path.push(name);
                            }
                            None => {
                                if is_last_component {
                                    return LookupComponentPath::FoundParent(new_component_path);
                                } else {
                                    return LookupComponentPath::NotFound;
                                }
                            }
                        }
                    }
                    ExpandSymlinks::FoundFile(_) | ExpandSymlinks::FileAncestor => {
                        return LookupComponentPath::FileAncestor;
                    }
                    ExpandSymlinks::DanglingSymlink => {
                        return LookupComponentPath::DanglingSymlink;
                    }
                },
            }
        }
        LookupComponentPath::Found(cur, component_path)
    }

    /// Treating `self` as the root of the file system, resolve `cur` and `component` by expanding
    /// repeatedly expanding `cur` if it's a symlink.
    fn expand_symlinks<'state>(
        &'state self,
        mut cur: &'state Self,
        mut component_path: ComponentPath,
    ) -> ExpandSymlinks<'state> {
        loop {
            match cur {
                Self::Directory { entries } => {
                    return ExpandSymlinks::FoundDirectory(entries, component_path);
                }
                Self::File { contents } => {
                    return ExpandSymlinks::FoundFile(contents);
                }
                Self::Symlink { target } => {
                    match self.lookup_component_path_helper(
                        self.pop_component_path(&mut component_path),
                        component_path,
                        Path::new(target),
                    ) {
                        LookupComponentPath::Found(new_cur, new_component_path) => {
                            cur = new_cur;
                            component_path = new_component_path;
                        }
                        LookupComponentPath::FoundParent(_)
                        | LookupComponentPath::DanglingSymlink
                        | LookupComponentPath::NotFound => {
                            return ExpandSymlinks::DanglingSymlink;
                        }
                        LookupComponentPath::FileAncestor => {
                            return ExpandSymlinks::FileAncestor;
                        }
                    }
                }
            }
        }
    }

    /// Treating `self` as the root of the file system, pop one component off of `component_path`.
    /// If `component_path` is empty, this will be a no-op. Return the [`Entry`] now pointed to by
    /// `component_path`.
    fn pop_component_path(&self, component_path: &mut ComponentPath) -> &Self {
        component_path.pop();
        self.resolve_component_path(&component_path)
    }

    /// Treating `self` as the root of the file system, resolve `component_path` to its
    /// corresponding [`Entry`].
    fn resolve_component_path(&self, component_path: &ComponentPath) -> &Self {
        let mut cur = self;
        for component in component_path {
            cur = cur.directory_entries().get(component).unwrap();
        }
        cur
    }

    /// Treating `self` as the root of the file system, resolve `component_path` to its
    /// corresponding [`Entry`], an return that entry as a mutable reference.
    fn resolve_component_path_mut(&mut self, component_path: &ComponentPath) -> &mut Self {
        let mut cur = self;
        for component in component_path {
            cur = cur.directory_entries_mut().get_mut(component).unwrap();
        }
        cur
    }

    /// Treating `self` as the root of the file system, resolve `directory_component_path` to its
    /// corresponding directory [`Entry`], then insert `entry` into the directory. The path
    /// component `name` must not already be in the directory.
    fn append_entry_to_directory(
        &mut self,
        directory_component_path: &ComponentPath,
        name: &OsStr,
        entry: Self,
    ) {
        self.resolve_component_path_mut(directory_component_path)
            .directory_entries_mut()
            .insert(name.to_str().unwrap().to_owned(), entry)
            .assert_is_none();
    }

    /// Treating `self` as the root of the file system, resolve `component_path` to its
    /// corresponding directory [`Entry`], then remove that entry from its parent and return the
    /// entry. `component_path` must not be empty: you can't remove the root from its parent.
    fn remove_component_path(
        &mut self,
        mut component_path: ComponentPath,
    ) -> (Self, ComponentPath) {
        assert!(!component_path.is_empty());
        let mut cur = self;
        let mut entry = None;
        for component in component_path.iter().with_position() {
            let entries = cur.directory_entries_mut();
            if let Position::Last(component) | Position::Only(component) = component {
                entry = Some(entries.remove(component).unwrap());
                break;
            } else {
                cur = entries.get_mut(component.into_inner()).unwrap();
            }
        }
        component_path.pop();
        (entry.unwrap(), component_path)
    }

    /// Treating `self` as the root of the file system, resolve `path` to its parent directory.
    /// Return an error if `path` exists, or if the parent directory can't be found.
    fn lookup_parent(&self, path: &Path) -> Result<ComponentPath, Error> {
        match self.lookup_component_path(path) {
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::NotFound => Err(Error::NoEnt),
            LookupComponentPath::Found(_, _) => Err(Error::Exists),
            LookupComponentPath::FoundParent(component_path) => Ok(component_path),
        }
    }

    /// Treating `self` as the root of the file system, resolve `path`. If `path` resolves to a
    /// symlink, expand it recursively until a non-symlink is found.
    fn lookup_leaf(&self, path: &Path) -> Result<ExpandSymlinks, Error> {
        match self.lookup_component_path(path) {
            LookupComponentPath::FoundParent(_)
            | LookupComponentPath::NotFound
            | LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::Found(entry, component_path) => {
                Ok(self.expand_symlinks(entry, component_path))
            }
        }
    }
}

enum LookupComponentPath<'state> {
    /// The path resolved to an actual entry. This contains the entry and the component path to it.
    Found(&'state Entry, ComponentPath),

    /// There was entry at the given path, but the parent directory exists. This contains the
    /// component path to the parent, which is guaranteed to be a directory.
    FoundParent(ComponentPath),

    /// There was no entry at the given path, nor does its parent directory exist. However, all
    /// ancestors that do exist are directories.
    NotFound,

    /// A symlink in the path didn't resolve.
    DanglingSymlink,

    /// An ancestor in the path was a file.
    FileAncestor,
}

enum ExpandSymlinks<'state> {
    /// The symlink resolved to a file entry. This contains the entry and the path to it.
    FoundFile(&'state Box<[u8]>),

    /// The symlink resolved to a directory entry. This contains the entry and the path to it.
    FoundDirectory(&'state BTreeMap<String, Entry>, ComponentPath),

    /// A symlink in the path didn't resolve.
    DanglingSymlink,

    /// An ancestor in the path was a file.
    FileAncestor,
}

/// A component path represents a path to an [`Entry`] that can be reached only through
/// directories. No symlinks are allowed. Put another way, this is the canonical path to an entry
/// in a file-system tree. Every entry in the component path, except for the last, must correspond
/// to a directory.
///
/// The motivation for using this struct is to keep the Rust code safe in modifying operations. We
/// will generally resolve a [`Path`] into a [`ComponentPath`] at the beginning of an operation,
/// and then operate using the [`ComponentPath`] for the rest of the operation. The assumption is
/// that one we've got it, we can always resolve a [`ComponentPath`] into an [`Entry`].
#[derive(Default, PartialEq)]
struct ComponentPath(Vec<String>);

impl ComponentPath {
    fn push(mut self, component: &str) -> Self {
        self.0.push(component.to_owned());
        self
    }

    fn pop(&mut self) -> Option<String> {
        self.0.pop()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn iter(&self) -> impl Iterator<Item = &'_ String> {
        self.0.iter()
    }

    /// Return true iff `potential_ancestor` is an actual ancestor of this component path. What
    /// that means in practice is that `potential_ancestor` is a prefix of this component path.
    fn is_descendant_of(&self, potential_ancestor: &ComponentPath) -> bool {
        self.iter()
            .zip(potential_ancestor)
            .take_while(|(l, r)| *l == *r)
            .count()
            == potential_ancestor.len()
    }
}

impl IntoIterator for ComponentPath {
    type Item = String;
    type IntoIter = vec::IntoIter<String>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a ComponentPath {
    type Item = &'a String;
    type IntoIter = slice::Iter<'a, String>;
    fn into_iter(self) -> Self::IntoIter {
        (&self.0).into_iter()
    }
}

impl super::Fs for Rc<Fs> {
    type Error = <Fs as super::Fs>::Error;
    fn rand_u64(&self) -> u64 {
        (**self).rand_u64()
    }
    fn metadata(&self, path: &Path) -> Result<Option<Metadata>, Error> {
        (**self).metadata(path)
    }
    fn read_file(&self, path: &Path, contents: &mut [u8]) -> Result<usize, Error> {
        (**self).read_file(path, contents)
    }
    fn read_dir(
        &self,
        path: &Path,
    ) -> Result<impl Iterator<Item = Result<(OsString, Metadata), Error>>, Error> {
        (**self).read_dir(path)
    }
    fn create_file(&self, path: &Path, contents: &[u8]) -> Result<(), Error> {
        (**self).create_file(path, contents)
    }
    fn symlink(&self, target: &Path, link: &Path) -> Result<(), Error> {
        (**self).symlink(target, link)
    }
    fn mkdir(&self, path: &Path) -> Result<(), Error> {
        (**self).mkdir(path)
    }
    fn mkdir_recursively(&self, path: &Path) -> Result<(), Error> {
        (**self).mkdir_recursively(path)
    }
    fn remove(&self, path: &Path) -> Result<(), Error> {
        (**self).remove(path)
    }
    fn rmdir_recursively_on_thread(&self, path: PathBuf) -> Result<(), Error> {
        (**self).rmdir_recursively_on_thread(path)
    }
    fn rename(&self, source_path: &Path, dest_path: &Path) -> Result<(), Error> {
        (**self).rename(source_path, dest_path)
    }
    type TempFile = <Fs as super::Fs>::TempFile;
    fn temp_file(&self, parent: &Path) -> Result<Self::TempFile, Error> {
        (**self).temp_file(parent)
    }
    fn persist_temp_file(&self, temp_file: Self::TempFile, target: &Path) -> Result<(), Error> {
        (**self).persist_temp_file(temp_file, target)
    }
    type TempDir = <Fs as super::Fs>::TempDir;
    fn temp_dir(&self, parent: &Path) -> Result<Self::TempDir, Error> {
        (**self).temp_dir(parent)
    }
    fn persist_temp_dir(&self, temp_dir: Self::TempDir, target: &Path) -> Result<(), Error> {
        (**self).persist_temp_dir(temp_dir, target)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Fs as _, TempDir as _, TempFile as _};
    use super::*;

    mod fs_macro {
        use super::*;

        #[test]
        fn empty() {
            assert_eq!(fs! {}, Entry::directory([]));
        }

        #[test]
        fn one_file_no_comma() {
            assert_eq!(
                fs! {
                    foo(b"abcd")
                },
                Entry::directory([("foo", Entry::file(b"abcd"))])
            );
        }

        #[test]
        fn one_file_comma() {
            assert_eq!(
                fs! {
                    foo(b"abcd"),
                },
                Entry::directory([("foo", Entry::file(b"abcd"))])
            );
        }

        #[test]
        fn one_symlink_no_comma() {
            assert_eq!(
                fs! {
                    foo -> "/target"
                },
                Entry::directory([("foo", Entry::symlink("/target"))])
            );
        }

        #[test]
        fn one_symlink_comma() {
            assert_eq!(
                fs! {
                    foo -> "/target",
                },
                Entry::directory([("foo", Entry::symlink("/target"))])
            );
        }

        #[test]
        fn one_directory_no_comma() {
            assert_eq!(
                fs! {
                    foo {}
                },
                Entry::directory([("foo", Entry::directory([]))])
            );
        }

        #[test]
        fn one_directory_comma() {
            assert_eq!(
                fs! {
                    foo {},
                },
                Entry::directory([("foo", Entry::directory([]))])
            );
        }

        #[test]
        fn one_directory_with_no_files() {
            assert_eq!(
                fs! {
                    foo {},
                },
                Entry::directory([("foo", Entry::directory([]))])
            );
        }

        #[test]
        fn non_identifier_keys() {
            assert_eq!(
                fs! {
                    "foo"(b"abcd"),
                    "bar" -> "/target/1",
                    "baz" { "empty" {} },
                },
                Entry::directory([
                    ("foo", Entry::file(b"abcd")),
                    ("bar", Entry::symlink("/target/1")),
                    ("baz", Entry::directory([("empty", Entry::directory([]))])),
                ])
            );
        }

        #[test]
        fn kitchen_sink() {
            assert_eq!(
                fs! {
                    foo(b"abcd"),
                    bar -> "/target/1",
                    baz {
                        zero {},
                        one {
                            foo(b"abcde")
                        },
                        two {
                            foo(b"abcdef"),
                            bar -> "/target/2"
                        },
                    }
                },
                Entry::directory([
                    ("foo", Entry::file(b"abcd")),
                    ("bar", Entry::symlink("/target/1")),
                    (
                        "baz",
                        Entry::directory([
                            ("zero", Entry::directory([])),
                            ("one", Entry::directory([("foo", Entry::file(b"abcde"))])),
                            (
                                "two",
                                Entry::directory([
                                    ("foo", Entry::file(b"abcdef")),
                                    ("bar", Entry::symlink("/target/2")),
                                ])
                            ),
                        ])
                    )
                ])
            );
        }
    }

    #[test]
    fn rand_u64() {
        let fs = Fs::new(fs! {});
        assert_eq!(fs.rand_u64(), 1);
        assert_eq!(fs.rand_u64(), 2);
        assert_eq!(fs.rand_u64(), 3);
    }

    #[test]
    fn metadata_empty() {
        let fs = Fs::new(fs! {});
        assert_eq!(
            fs.metadata(Path::new("/")),
            Ok(Some(Metadata::directory(0)))
        );
        assert_eq!(fs.metadata(Path::new("/foo")), Ok(None));
    }

    #[test]
    fn metadata() {
        let fs = Fs::new(fs! {
            foo(b"contents"),
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
            Ok(Some(Metadata::directory(2)))
        );
        assert_eq!(fs.metadata(Path::new("/foo")), Ok(Some(Metadata::file(8))));
        assert_eq!(
            fs.metadata(Path::new("/bar")),
            Ok(Some(Metadata::directory(6)))
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/baz")),
            Ok(Some(Metadata::symlink(7)))
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/root/foo")),
            Ok(Some(Metadata::file(8)))
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/root/bar/a")),
            Ok(Some(Metadata::symlink(1)))
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/a/baz")),
            Ok(Some(Metadata::symlink(7)))
        );

        assert_eq!(fs.metadata(Path::new("/foo2")), Ok(None));
        assert_eq!(fs.metadata(Path::new("/bar/baz/foo")), Err(Error::NoEnt));
        assert_eq!(fs.metadata(Path::new("/bar/baz2")), Ok(None));
        assert_eq!(fs.metadata(Path::new("/bar/baz2/blah")), Ok(None));
        assert_eq!(fs.metadata(Path::new("/foo/bar")), Err(Error::NotDir));
    }

    #[test]
    fn read_file() {
        let fs = Fs::new(fs! {
            foo(b"foo"),
            empty(b""),
            subdir {
                subdir {
                    root -> "../dotdot",
                },
                dotdot -> "..",
            },
            symlink -> "symlink2",
            symlink2 -> "subdir/subdir/root/foo",
            bad_symlink -> "dangle",
        });

        let mut buf = [0u8; 10];

        assert_eq!(fs.read_file(Path::new("/foo"), &mut buf[..]), Ok(3));
        assert_eq!(&buf[..3], b"foo");
        assert_eq!(fs.read_file(Path::new("/foo"), &mut buf[..1]), Ok(1));
        assert_eq!(&buf[..1], b"f");
        assert_eq!(fs.read_file(Path::new("/foo"), &mut buf[..0]), Ok(0));

        assert_eq!(fs.read_file(Path::new("/empty"), &mut buf[..]), Ok(0));

        assert_eq!(
            fs.read_file(Path::new("/subdir/subdir/root/foo"), &mut buf[..]),
            Ok(3)
        );
        assert_eq!(&buf[..3], b"foo");

        assert_eq!(fs.read_file(Path::new("/symlink"), &mut buf[..]), Ok(3));
        assert_eq!(&buf[..3], b"foo");

        assert_eq!(
            fs.read_file(Path::new("/missing"), &mut buf[..]),
            Err(Error::NoEnt)
        );
        assert_eq!(
            fs.read_file(Path::new("/subdir/missing"), &mut buf[..]),
            Err(Error::NoEnt)
        );
        assert_eq!(
            fs.read_file(Path::new("/dangle/missing"), &mut buf[..]),
            Err(Error::NoEnt)
        );
        assert_eq!(
            fs.read_file(Path::new("/foo/bar"), &mut buf[..]),
            Err(Error::NotDir)
        );
        assert_eq!(
            fs.read_file(Path::new("/subdir"), &mut buf[..]),
            Err(Error::IsDir)
        );
        assert_eq!(
            fs.read_file(Path::new("/subdir/dotdot"), &mut buf[..]),
            Err(Error::IsDir)
        );
        assert_eq!(
            fs.read_file(Path::new("/bad_symlink"), &mut buf[..]),
            Err(Error::NoEnt)
        );
    }

    #[test]
    fn read_dir() {
        let fs = Fs::new(fs! {
            foo(b"abcd"),
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
                .collect::<HashMap<_, _>>(),
            HashMap::from([
                ("foo".into(), Metadata::file(4)),
                ("bar".into(), Metadata::directory(3)),
            ]),
        );
        assert_eq!(
            fs.read_dir(Path::new("/bar"))
                .unwrap()
                .map(Result::unwrap)
                .collect::<HashMap<_, _>>(),
            HashMap::from([
                ("baz".into(), Metadata::symlink(7)),
                ("root".into(), Metadata::symlink(1)),
                ("subdir".into(), Metadata::directory(0)),
            ]),
        );
        assert_eq!(
            fs.read_dir(Path::new("/bar/root/bar/subdir"))
                .unwrap()
                .map(Result::unwrap)
                .collect::<HashMap<_, _>>(),
            HashMap::default(),
        );
        assert_eq!(
            fs.read_dir(Path::new("/bar/root"))
                .unwrap()
                .map(Result::unwrap)
                .collect::<HashMap<_, _>>(),
            HashMap::from([
                ("foo".into(), Metadata::file(4)),
                ("bar".into(), Metadata::directory(3)),
            ]),
        );

        assert!(matches!(fs.read_dir(Path::new("/foo")), Err(Error::NotDir)));
        assert!(matches!(
            fs.read_dir(Path::new("/foo/foo")),
            Err(Error::NotDir)
        ));
        assert!(matches!(
            fs.read_dir(Path::new("/bar/baz")),
            Err(Error::NoEnt)
        ));
        assert!(matches!(
            fs.read_dir(Path::new("/bar/baz/foo")),
            Err(Error::NoEnt)
        ));
        assert!(matches!(fs.read_dir(Path::new("/blah")), Err(Error::NoEnt)));
        assert!(matches!(
            fs.read_dir(Path::new("/blah/blah")),
            Err(Error::NoEnt)
        ));
    }

    #[test]
    fn create_file() {
        let fs = Fs::new(fs! {
            foo(b"abcd"),
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
            Ok(Some(Metadata::file(8)))
        );

        assert_eq!(
            fs.create_file(Path::new("/bar/root/bar/a/new_file"), b"contents-2"),
            Ok(())
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/new_file")),
            Ok(Some(Metadata::file(10)))
        );

        assert_eq!(
            fs.create_file(Path::new("/new_file"), b"contents-3"),
            Err(Error::Exists)
        );
        assert_eq!(
            fs.create_file(Path::new("/foo"), b"contents-3"),
            Err(Error::Exists)
        );
        assert_eq!(
            fs.create_file(Path::new("/blah/new_file"), b"contents-3"),
            Err(Error::NoEnt)
        );
        assert_eq!(
            fs.create_file(Path::new("/foo/new_file"), b"contents-3"),
            Err(Error::NotDir)
        );
        assert_eq!(
            fs.create_file(Path::new("/bar/baz/new_file"), b"contents-3"),
            Err(Error::NoEnt)
        );
    }

    #[test]
    fn symlink() {
        let fs = Fs::new(fs! {
            foo(b"abcd"),
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
            fs.symlink(Path::new("new-target"), Path::new("/new_symlink")),
            Ok(())
        );
        assert_eq!(
            fs.metadata(Path::new("/new_symlink")),
            Ok(Some(Metadata::symlink(10)))
        );

        assert_eq!(
            fs.symlink(
                Path::new("new-target-2"),
                Path::new("/bar/root/bar/a/new_symlink"),
            ),
            Ok(())
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/new_symlink")),
            Ok(Some(Metadata::symlink(12)))
        );

        assert_eq!(
            fs.symlink(Path::new("new-target-3"), Path::new("/new_symlink")),
            Err(Error::Exists)
        );
        assert_eq!(
            fs.symlink(Path::new("new-target-3"), Path::new("/foo")),
            Err(Error::Exists)
        );
        assert_eq!(
            fs.symlink(Path::new("new-target-3"), Path::new("/blah/new_symlink")),
            Err(Error::NoEnt)
        );
        assert_eq!(
            fs.symlink(Path::new("new-target-3"), Path::new("/foo/new_symlink")),
            Err(Error::NotDir)
        );
        assert_eq!(
            fs.symlink(Path::new("new-target-3"), Path::new("/bar/baz/new_symlink")),
            Err(Error::NoEnt)
        );
    }

    #[test]
    fn mkdir() {
        let fs = Fs::new(fs! {
            foo(b"abcd"),
            bar {
                baz -> "/target",
                root -> "/",
            },
        });

        assert_eq!(fs.mkdir(Path::new("/")), Err(Error::Exists));
        assert_eq!(fs.mkdir(Path::new("/bar/blah/foo")), Err(Error::NoEnt));
        assert_eq!(fs.mkdir(Path::new("/bar/baz/blah")), Err(Error::NoEnt));
        assert_eq!(fs.mkdir(Path::new("/blah/baz")), Err(Error::NoEnt));
        assert_eq!(fs.mkdir(Path::new("/foo/bar/baz")), Err(Error::NotDir));

        assert_eq!(fs.mkdir(Path::new("/bar/blah")), Ok(()));
        assert_eq!(fs.mkdir(Path::new("/bar/root/dir1")), Ok(()));
        assert_eq!(fs.mkdir(Path::new("/bar/root/dir1/dir2")), Ok(()));

        fs.assert_tree(fs! {
            foo(b"abcd"),
            bar {
                baz -> "/target",
                root -> "/",
                blah {},
            },
            dir1 {
                dir2 {},
            },
        });
    }

    #[test]
    fn mkdir_recursively() {
        let fs = Fs::new(fs! {
            foo(b"abcd"),
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
            Ok(Some(Metadata::directory(1)))
        );
        assert_eq!(
            fs.metadata(Path::new("/new/directory")),
            Ok(Some(Metadata::directory(1)))
        );
        assert_eq!(
            fs.metadata(Path::new("/new/directory/and")),
            Ok(Some(Metadata::directory(1)))
        );
        assert_eq!(
            fs.metadata(Path::new("/new/directory/and/subdirectory")),
            Ok(Some(Metadata::directory(0)))
        );

        assert_eq!(
            fs.mkdir_recursively(Path::new("/bar/root/bar/a/new/directory")),
            Ok(())
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/new")),
            Ok(Some(Metadata::directory(1)))
        );
        assert_eq!(
            fs.metadata(Path::new("/bar/new/directory")),
            Ok(Some(Metadata::directory(0)))
        );

        assert_eq!(fs.mkdir_recursively(Path::new("/foo")), Err(Error::Exists));
        assert_eq!(
            fs.mkdir_recursively(Path::new("/foo/baz")),
            Err(Error::NotDir)
        );
        assert_eq!(
            fs.mkdir_recursively(Path::new("/bar/baz/new/directory")),
            Err(Error::NoEnt)
        );
    }

    #[test]
    fn remove() {
        let fs = Fs::new(fs! {
            foo(b"abcd"),
            bar {
                baz -> "/target",
                root -> "/",
                a -> "b",
                b -> "../bar/c",
                c -> "/bar/d",
                d -> ".",
            },
        });

        assert_eq!(fs.remove(Path::new("/")), Err(Error::IsDir));
        assert_eq!(fs.remove(Path::new("/bar")), Err(Error::IsDir));
        assert_eq!(fs.remove(Path::new("/foo/baz")), Err(Error::NotDir));
        assert_eq!(fs.remove(Path::new("/bar/baz/blah")), Err(Error::NoEnt));

        assert_eq!(fs.remove(Path::new("/bar/baz")), Ok(()));
        assert_eq!(fs.metadata(Path::new("/bar/baz")), Ok(None));

        assert_eq!(fs.remove(Path::new("/bar/a")), Ok(()));
        assert_eq!(fs.metadata(Path::new("/bar/a")), Ok(None));
        assert_eq!(
            fs.metadata(Path::new("/bar/b")),
            Ok(Some(Metadata::symlink(8)))
        );

        assert_eq!(fs.remove(Path::new("/foo")), Ok(()));
        assert_eq!(fs.metadata(Path::new("/foo")), Ok(None));
    }

    #[test]
    fn rmdir_recursively_on_thread() {
        let fs = Fs::new(fs! {
            dir1 {
                dir2 {
                    file1(b"1"),
                    symlink1 -> "file1",
                },
            },
            dir3 {
                dir4 {
                    file2(b"2"),
                    symlink2 -> "file2",
                },
            },
        });
        assert_eq!(fs.rmdir_recursively_on_thread("/dir1".into()), Ok(()));
        assert_eq!(fs.rmdir_recursively_on_thread("/dir3".into()), Ok(()));
        fs.assert_tree(fs! {
            dir1 {
                dir2 {
                    file1(b"1"),
                    symlink1 -> "file1",
                },
            },
            dir3 {
                dir4 {
                    file2(b"2"),
                    symlink2 -> "file2",
                },
            },
        });
        fs.assert_recursive_rmdirs(HashSet::from(["/dir1".into(), "/dir3".into()]));
        fs.complete_recursive_rmdir("/dir1");
        fs.assert_tree(fs! {
            dir3 {
                dir4 {
                    file2(b"2"),
                    symlink2 -> "file2",
                },
            },
        });
        fs.complete_recursive_rmdir("/dir3");
        fs.assert_tree(fs! {});
    }

    #[test]
    fn rmdir_recursively_on_thread_errors() {
        let fs = Fs::new(fs! {
            foo(b"a"),
            bar {
                baz -> "/target",
            },
        });

        assert_eq!(
            fs.rmdir_recursively_on_thread("/".into()),
            Err(Error::Inval)
        );
        assert_eq!(
            fs.rmdir_recursively_on_thread("/foo".into()),
            Err(Error::NotDir)
        );
        assert_eq!(
            fs.rmdir_recursively_on_thread("/bar/baz".into()),
            Err(Error::NotDir)
        );
        assert_eq!(
            fs.rmdir_recursively_on_thread("/bar/frob".into()),
            Err(Error::NoEnt)
        );
        assert_eq!(
            fs.rmdir_recursively_on_thread("/bar/frob/blah".into()),
            Err(Error::NoEnt)
        );
        assert_eq!(
            fs.rmdir_recursively_on_thread("/bar/baz/blah".into()),
            Err(Error::NoEnt)
        );
    }

    #[test]
    fn rename_source_path_with_file_ancestor() {
        let expected = fs! { foo(b"abcd") };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo/bar"), Path::new("/bar")),
            Err(Error::NotDir)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_source_path_with_dangling_symlink() {
        let expected = fs! { foo -> "/dangle" };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo/bar"), Path::new("/bar")),
            Err(Error::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_source_path_not_found() {
        let expected = fs! {};
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar")),
            Err(Error::NoEnt)
        );
        assert_eq!(
            fs.rename(Path::new("/foo/bar"), Path::new("/bar")),
            Err(Error::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_destination_path_with_file_ancestor() {
        let expected = fs! { foo(b"a"), bar(b"b") };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar/baz")),
            Err(Error::NotDir)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_destination_path_with_dangling_symlink() {
        let expected = fs! { foo(b"a"), bar -> "dangle" };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar/baz")),
            Err(Error::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_destination_path_not_found() {
        let expected = fs! { foo(b"a") };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar/baz")),
            Err(Error::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_directory_into_new_entry_in_itself() {
        let expected = fs! { foo { bar { baz {} } } };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/foo/bar/baz/a")),
            Err(Error::Inval)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_directory_into_new_entry_in_descendant_of_itself() {
        let expected = fs! { dir {} };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir"), Path::new("/dir/a")),
            Err(Error::Inval)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_directory_onto_nonempty_directory() {
        let expected = fs! { dir1 {}, dir2 { foo(b"a") } };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir1"), Path::new("/dir2")),
            Err(Error::NotEmpty)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_nonempty_directory_onto_itself() {
        let expected = fs! { dir { foo(b"a") } };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir"), Path::new("/dir")),
            Err(Error::NotEmpty)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_nonempty_root_onto_itself() {
        let expected = fs! { foo(b"a") };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/"), Path::new("/")),
            Err(Error::NotEmpty)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_directory_onto_descendant() {
        let expected = fs! { dir1 { dir2 {} } };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir1"), Path::new("/dir1/dir2")),
            Err(Error::Inval)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_directory_onto_nondirectory() {
        let expected = fs! { dir {}, file(b"a"), symlink -> "file" };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir"), Path::new("/file")),
            Err(Error::NotDir)
        );
        assert_eq!(
            fs.rename(Path::new("/dir"), Path::new("/symlink")),
            Err(Error::NotDir)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_nondirectory_onto_directory() {
        let expected = fs! { dir {}, file(b"a"), symlink -> "file" };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/file"), Path::new("/dir")),
            Err(Error::IsDir)
        );
        assert_eq!(
            fs.rename(Path::new("/symlink"), Path::new("/dir")),
            Err(Error::IsDir)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_empty_root_onto_itself() {
        let fs = Fs::new(fs! {});
        assert_eq!(fs.rename(Path::new("/"), Path::new("/")), Ok(()));
        fs.assert_tree(fs! {});
    }

    #[test]
    fn rename_empty_directory_onto_itself() {
        let expected = fs! {
            dir {},
            root -> "/",
            root2 -> ".",
        };
        let fs = Fs::new(expected.clone());
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
    fn rename_file_onto_itself() {
        let expected = fs! {
            file(b"a"),
            root -> "/",
            root2 -> ".",
        };
        let fs = Fs::new(expected.clone());
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
    fn rename_symlink_onto_itself() {
        let expected = fs! {
            symlink -> "dangle",
            root -> "/",
            root2 -> ".",
        };
        let fs = Fs::new(expected.clone());
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
    fn rename_file_onto_file() {
        let fs = Fs::new(fs! { foo(b"a"), bar(b"b") });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar(b"a") });
    }

    #[test]
    fn rename_file_onto_symlink() {
        let fs = Fs::new(fs! { foo(b"a"), bar -> "foo" });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar(b"a") });
    }

    #[test]
    fn rename_symlink_onto_file() {
        let fs = Fs::new(fs! { foo -> "bar", bar(b"a") });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar -> "bar" });
    }

    #[test]
    fn rename_symlink_onto_symlink() {
        let fs = Fs::new(fs! { foo -> "bar", bar -> "foo" });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar -> "bar" });
    }

    #[test]
    fn rename_into_new_entries_in_directory() {
        let fs = Fs::new(fs! {
            a {
                file(b"a"),
                symlink -> "/dangle",
                dir { c(b"b"), d -> "c", e {} },
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
                xile(b"a"),
                xymlink -> "/dangle",
                xir { c(b"b"), d -> "c", e {} },
            },
        });
    }

    #[test]
    fn rename_directory() {
        let fs = Fs::new(fs! {
            a {
                b {
                    c(b"a"),
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
                    c(b"a"),
                    d -> "c",
                    e {},
                },
            },
        });
    }

    #[test]
    fn rename_destination_in_ancestor_of_source() {
        let fs = Fs::new(fs! {
            before(b"a"),
            target(b"b"),
            dir1 {
                dir2 {
                    source(b"c"),
                },
            },
            after(b"d"),
        });
        assert_eq!(
            fs.rename(Path::new("/dir1/dir2/source"), Path::new("/target")),
            Ok(())
        );
        fs.assert_tree(fs! {
            before(b"a"),
            dir1 {
                dir2 {},
            },
            after(b"d"),
            target(b"c"),
        });
    }

    #[test]
    fn rename_source_in_ancestor_of_target() {
        let fs = Fs::new(fs! {
            before(b"a"),
            source(b"b"),
            dir1 {
                dir2 {
                    target(b"c"),
                },
            },
            after(b"d"),
        });
        assert_eq!(
            fs.rename(Path::new("/source"), Path::new("/dir1/dir2/target")),
            Ok(())
        );
        fs.assert_tree(fs! {
            before(b"a"),
            dir1 {
                dir2 {
                    target(b"b"),
                },
            },
            after(b"d"),
        });
    }

    /******/

    #[test]
    fn temp_file() {
        let fs = Fs::new(fs! {
            "000"(b"a"),
            dir {
                "000"(b"b"),
                "001" -> "000",
                "002" {
                },
            },
            persist {
            },
        });

        let temp_file_1 = fs.temp_file(Path::new("/")).unwrap();
        assert_eq!(temp_file_1.path().to_str().unwrap(), "/001");

        let temp_file_2 = fs.temp_file(Path::new("/dir")).unwrap();
        assert_eq!(temp_file_2.path().to_str().unwrap(), "/dir/003");

        let temp_file_3 = fs.temp_file(Path::new("/dir/002")).unwrap();
        assert_eq!(temp_file_3.path().to_str().unwrap(), "/dir/002/000");

        fs.assert_tree(fs! {
            "000"(b"a"),
            "001"(b""),
            dir {
                "000"(b"b"),
                "001" -> "000",
                "002" {
                    "000"(b""),
                },
                "003"(b""),
            },
            persist {
            },
        });

        fs.persist_temp_file(temp_file_1, Path::new("/persist/1"))
            .unwrap();
        fs.assert_tree(fs! {
            "000"(b"a"),
            dir {
                "000"(b"b"),
                "001" -> "000",
                "002" {
                    "000"(b""),
                },
                "003"(b""),
            },
            persist {
                "1"(b""),
            },
        });

        fs.persist_temp_file(temp_file_2, Path::new("/persist/2"))
            .unwrap();
        fs.assert_tree(fs! {
            "000"(b"a"),
            dir {
                "000"(b"b"),
                "001" -> "000",
                "002" {
                    "000"(b""),
                },
            },
            persist {
                "1"(b""),
                "2"(b""),
            },
        });

        fs.persist_temp_file(temp_file_3, Path::new("/persist/3"))
            .unwrap();
        fs.assert_tree(fs! {
            "000"(b"a"),
            dir {
                "000"(b"b"),
                "001" -> "000",
                "002" {},
            },
            persist {
                "1"(b""),
                "2"(b""),
                "3"(b""),
            },
        });

        assert_eq!(
            fs.temp_file(Path::new("/nonexistent")).unwrap_err(),
            Error::NoEnt
        );
    }

    #[test]
    fn persist_temp_file_error() {
        let fs = Fs::new(fs! {
            tmp {},
            file(b"a"),
            bad_symlink -> "/foo",
        });

        let temp_file_1 = fs.temp_file(Path::new("/tmp")).unwrap();
        let temp_file_2 = fs.temp_file(Path::new("/tmp")).unwrap();
        let temp_file_3 = fs.temp_file(Path::new("/tmp")).unwrap();

        assert_eq!(
            fs.persist_temp_file(temp_file_1, Path::new("/foo/bar")),
            Err(Error::NoEnt)
        );
        assert_eq!(
            fs.persist_temp_file(temp_file_2, Path::new("/file/file")),
            Err(Error::NotDir)
        );
        assert_eq!(
            fs.persist_temp_file(temp_file_3, Path::new("/bad_symlink/bar")),
            Err(Error::NoEnt)
        );
    }

    #[test]
    fn temp_dir() {
        let fs = Fs::new(fs! {
            "000"(b"a"),
            dir {
                "000"(b"b"),
                "001" -> "000",
                "002" {
                },
            },
            persist {
            },
        });

        let temp_dir_1 = fs.temp_dir(Path::new("/")).unwrap();
        assert_eq!(temp_dir_1.path().to_str().unwrap(), "/001");

        let temp_dir_2 = fs.temp_dir(Path::new("/dir")).unwrap();
        assert_eq!(temp_dir_2.path().to_str().unwrap(), "/dir/003");

        let temp_dir_3 = fs.temp_dir(Path::new("/dir/002")).unwrap();
        assert_eq!(temp_dir_3.path().to_str().unwrap(), "/dir/002/000");

        fs.assert_tree(fs! {
            "000"(b"a"),
            "001" {},
            dir {
                "000"(b"b"),
                "001" -> "000",
                "002" {
                    "000" {},
                },
                "003" {},
            },
            persist {
            },
        });

        fs.persist_temp_dir(temp_dir_1, Path::new("/persist/1"))
            .unwrap();
        fs.assert_tree(fs! {
            "000"(b"a"),
            dir {
                "000"(b"b"),
                "001" -> "000",
                "002" {
                    "000" {},
                },
                "003" {},
            },
            persist {
                "1" {},
            },
        });

        fs.persist_temp_dir(temp_dir_2, Path::new("/persist/2"))
            .unwrap();
        fs.assert_tree(fs! {
            "000"(b"a"),
            dir {
                "000"(b"b"),
                "001" -> "000",
                "002" {
                    "000" {},
                },
            },
            persist {
                "1" {},
                "2" {},
            },
        });

        fs.persist_temp_dir(temp_dir_3, Path::new("/persist/3"))
            .unwrap();
        fs.assert_tree(fs! {
            "000"(b"a"),
            dir {
                "000"(b"b"),
                "001" -> "000",
                "002" {},
            },
            persist {
                "1" {},
                "2" {},
                "3" {},
            },
        });

        assert_eq!(
            fs.temp_dir(Path::new("/nonexistent")).unwrap_err(),
            Error::NoEnt,
        );
    }

    #[test]
    fn persist_temp_dir_error() {
        let fs = Fs::new(fs! {
            tmp {},
            file(b"a"),
            bad_symlink -> "/foo",
        });

        let temp_dir_1 = fs.temp_dir(Path::new("/tmp")).unwrap();
        let temp_dir_2 = fs.temp_dir(Path::new("/tmp")).unwrap();
        let temp_dir_3 = fs.temp_dir(Path::new("/tmp")).unwrap();

        assert_eq!(
            fs.persist_temp_dir(temp_dir_1, Path::new("/foo/bar")),
            Err(Error::NoEnt)
        );
        assert_eq!(
            fs.persist_temp_dir(temp_dir_2, Path::new("/file/file")),
            Err(Error::NotDir)
        );
        assert_eq!(
            fs.persist_temp_dir(temp_dir_3, Path::new("/bad_symlink/bar")),
            Err(Error::NoEnt)
        );
    }
}
