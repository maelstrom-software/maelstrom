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
    fn push(&mut self, component: String) {
        self.0.push(component);
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

/// A file-system entry. There's no notion of hard links or things like sockets, devices, etc. in
/// this test file system.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Entry {
    File { size: u64 },
    Directory { entries: BTreeMap<String, Entry> },
    Symlink { target: String },
}

impl Entry {
    /// Create a new [`Entry::File`] of the given `size`.
    pub fn file(size: u64) -> Self {
        Self::File { size }
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
            Self::File { size } => Metadata::file(*size),
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
                Component::Normal(name) => match self.expand_to_directory(cur, component_path) {
                    LookupComponentPath::Found(new_cur, new_component_path) => {
                        cur = new_cur;
                        component_path = new_component_path;
                        let name = name.to_str().unwrap();
                        match cur.directory_entries().get(name) {
                            Some(entry) => {
                                cur = entry;
                                component_path.push(name.to_owned());
                            }
                            None => {
                                if is_last_component {
                                    return LookupComponentPath::FoundParent(cur, component_path);
                                } else {
                                    return LookupComponentPath::NotFound;
                                }
                            }
                        }
                    }
                    LookupComponentPath::FoundParent(_, _) => {
                        unreachable!();
                    }
                    result @ _ => {
                        return result;
                    }
                },
            }
        }
        LookupComponentPath::Found(cur, component_path)
    }

    /// Treating `self` as the root of the file system, resolve `cur` and `component` into a
    /// directory by recursively resolving any symlinks encountered.
    fn expand_to_directory<'state>(
        &'state self,
        mut cur: &'state Self,
        mut component_path: ComponentPath,
    ) -> LookupComponentPath<'state> {
        loop {
            match cur {
                Self::Directory { .. } => {
                    return LookupComponentPath::Found(cur, component_path);
                }
                Self::File { .. } => {
                    return LookupComponentPath::FileAncestor;
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
                        LookupComponentPath::FileAncestor => {
                            return LookupComponentPath::FileAncestor;
                        }
                        _ => {
                            return LookupComponentPath::DanglingSymlink;
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
            LookupComponentPath::FoundParent(_, component_path) => Ok(component_path),
        }
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
                (stringify!($name), $crate::cache::fs::test::Entry::file($size))
            ]
        )
    };
    (@expand [$name:literal($size:literal) $(,$($tail:tt)*)?] -> [$($($expanded:tt)+)?]) => {
        fs!(
            @expand
            [$($($tail)*)?] -> [
                $($($expanded)+,)?
                ($name, $crate::cache::fs::test::Entry::file($size))
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

#[derive(Debug)]
struct State {
    root: Entry,
    last_random_number: u64,
    recursive_rmdirs: HashSet<String>,
}

enum LookupComponentPath<'state> {
    /// The path resolved to an actual entry. This contains the entry and the path to it.
    Found(&'state Entry, ComponentPath),

    /// There was entry at the given path, but the parent directory exists. This contains the
    /// parent entry (which is guaranteed to be a directory) and the path to it.
    #[expect(dead_code)]
    FoundParent(&'state Entry, ComponentPath),

    /// There was no entry at the given path, nor does its parent directory exist. However, all
    /// ancestors that do exist are directories.
    NotFound,

    /// A symlink in the path didn't resolve.
    DanglingSymlink,

    /// An ancestor in the path was a file.
    FileAncestor,
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
        assert_eq!(self.state.borrow().root, expected);
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
            | LookupComponentPath::FoundParent(_, _) => Err(Error::NoEnt),
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

    type TempFile = TempFile;

    type TempDir = TempDir;

    fn rand_u64(&self) -> u64 {
        let mut state = self.state.borrow_mut();
        state.last_random_number += 1;
        state.last_random_number
    }

    fn metadata(&self, path: &Path) -> Result<Option<Metadata>, Error> {
        match self.state.borrow().root.lookup_component_path(path) {
            LookupComponentPath::Found(entry, _) => Ok(Some(entry.metadata())),
            LookupComponentPath::NotFound | LookupComponentPath::FoundParent(_, _) => Ok(None),
            LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
        }
    }

    fn read_dir(
        &self,
        path: &Path,
    ) -> Result<impl Iterator<Item = Result<(OsString, Metadata), Error>>, Error> {
        match self.state.borrow().root.lookup_component_path(path) {
            LookupComponentPath::Found(Entry::Directory { entries }, _) => Ok(entries
                .iter()
                .map(|(name, entry)| Ok((name.into(), entry.metadata())))
                .collect_vec()
                .into_iter()),
            LookupComponentPath::Found(_, _) | LookupComponentPath::FileAncestor => {
                Err(Error::NotDir)
            }
            LookupComponentPath::NotFound
            | LookupComponentPath::FoundParent(_, _)
            | LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
        }
    }

    fn create_file(&self, path: &Path, contents: &[u8]) -> Result<(), Error> {
        let root = &mut self.state.borrow_mut().root;
        let parent_component_path = root.lookup_parent(path)?;
        root.append_entry_to_directory(
            &parent_component_path,
            path.file_name().unwrap(),
            Entry::file(contents.len().try_into().unwrap()),
        );
        Ok(())
    }

    fn symlink(&self, target: &Path, link: &Path) -> Result<(), Error> {
        let root = &mut self.state.borrow_mut().root;
        let parent_component_path = root.lookup_parent(link)?;
        root.append_entry_to_directory(
            &parent_component_path,
            link.file_name().unwrap(),
            Entry::symlink(target.to_str().unwrap()),
        );
        Ok(())
    }

    fn mkdir(&self, path: &Path) -> Result<(), Error> {
        let root = &mut self.state.borrow_mut().root;
        let parent_component_path = root.lookup_parent(path)?;
        root.append_entry_to_directory(
            &parent_component_path,
            path.file_name().unwrap(),
            Entry::directory([]),
        );
        Ok(())
    }

    fn mkdir_recursively(&self, path: &Path) -> Result<(), Error> {
        let state = self.state.borrow();
        match state.root.lookup_component_path(path) {
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::Found(Entry::Directory { .. }, _) => Ok(()),
            LookupComponentPath::Found(_, _) => Err(Error::Exists),
            LookupComponentPath::FoundParent(_, _) => {
                drop(state);
                self.mkdir(path)
            }
            LookupComponentPath::NotFound => {
                drop(state);
                self.mkdir_recursively(path.parent().unwrap())?;
                self.mkdir(path)
            }
        }
    }

    fn remove(&self, path: &Path) -> Result<(), Error> {
        let root = &mut self.state.borrow_mut().root;
        match root.lookup_component_path(path) {
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::DanglingSymlink
            | LookupComponentPath::NotFound
            | LookupComponentPath::FoundParent(_, _) => Err(Error::NoEnt),
            LookupComponentPath::Found(Entry::Directory { .. }, _) => Err(Error::IsDir),
            LookupComponentPath::Found(_, mut component_path) => {
                let component = component_path.pop().unwrap();
                root.resolve_component_path_mut(&component_path)
                    .directory_entries_mut()
                    .remove(&component)
                    .assert_is_some();
                Ok(())
            }
        }
    }

    fn rmdir_recursively_on_thread(&self, path: PathBuf) -> Result<(), Error> {
        let state = &mut self.state.borrow_mut();
        match state.root.lookup_component_path(&path) {
            LookupComponentPath::Found(Entry::Directory { .. }, component_path) => {
                if component_path.is_empty() {
                    // Can't rmdir /.
                    Err(Error::Inval)
                } else {
                    state
                        .recursive_rmdirs
                        .insert(path.into_os_string().into_string().unwrap())
                        .assert_is_true();
                    Ok(())
                }
            }
            LookupComponentPath::Found(_, _) => Err(Error::NotDir),
            LookupComponentPath::NotFound
            | LookupComponentPath::FoundParent(_, _)
            | LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
        }
    }

    fn rename(&self, source_path: &Path, dest_path: &Path) -> Result<(), Error> {
        let root = &mut self.state.borrow_mut().root;
        let (source_entry, source_component_path) = match root.lookup_component_path(source_path) {
            LookupComponentPath::FileAncestor => {
                return Err(Error::NotDir);
            }
            LookupComponentPath::DanglingSymlink
            | LookupComponentPath::NotFound
            | LookupComponentPath::FoundParent(_, _) => {
                return Err(Error::NoEnt);
            }
            LookupComponentPath::Found(source_entry, source_component_path) => {
                (source_entry, source_component_path)
            }
        };

        let dest_parent_component_path = match root.lookup_component_path(dest_path) {
            LookupComponentPath::FileAncestor => {
                return Err(Error::NotDir);
            }
            LookupComponentPath::DanglingSymlink | LookupComponentPath::NotFound => {
                return Err(Error::NoEnt);
            }
            LookupComponentPath::FoundParent(_, dest_parent_component_path) => {
                if source_entry.is_directory()
                    && dest_parent_component_path.is_descendant_of(&source_component_path)
                {
                    // We can't move a directory into one of its descendants, including itself.
                    return Err(Error::Inval);
                } else {
                    dest_parent_component_path
                }
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
                root.remove_component_path(dest_component_path).1
            }
        };

        let (source_entry, _) = root.remove_component_path(source_component_path);
        root.append_entry_to_directory(
            &dest_parent_component_path,
            dest_path.file_name().unwrap(),
            source_entry,
        );

        Ok(())
    }

    fn temp_file(&self, parent: &Path) -> Result<Self::TempFile, Error> {
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

    fn persist_temp_file(&self, temp_file: Self::TempFile, target: &Path) -> Result<(), Error> {
        self.rename(&temp_file.0, target)
    }

    fn temp_dir(&self, parent: &Path) -> Result<Self::TempDir, Error> {
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

    fn persist_temp_dir(&self, temp_dir: Self::TempDir, target: &Path) -> Result<(), Error> {
        self.rename(&temp_dir.0, target)
    }
}

impl super::Fs for Rc<Fs> {
    type Error = <Fs as super::Fs>::Error;
    type TempFile = <Fs as super::Fs>::TempFile;
    type TempDir = <Fs as super::Fs>::TempDir;

    fn rand_u64(&self) -> u64 {
        (**self).rand_u64()
    }
    fn rename(&self, source_path: &Path, dest_path: &Path) -> Result<(), Error> {
        (**self).rename(source_path, dest_path)
    }
    fn remove(&self, path: &Path) -> Result<(), Error> {
        (**self).remove(path)
    }
    fn rmdir_recursively_on_thread(&self, path: PathBuf) -> Result<(), Error> {
        (**self).rmdir_recursively_on_thread(path)
    }
    fn mkdir(&self, path: &Path) -> Result<(), Error> {
        (**self).mkdir(path)
    }
    fn mkdir_recursively(&self, path: &Path) -> Result<(), Error> {
        (**self).mkdir_recursively(path)
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
    fn metadata(&self, path: &Path) -> Result<Option<Metadata>, Error> {
        (**self).metadata(path)
    }
    fn temp_file(&self, parent: &Path) -> Result<Self::TempFile, Error> {
        (**self).temp_file(parent)
    }
    fn persist_temp_file(&self, temp_file: Self::TempFile, target: &Path) -> Result<(), Error> {
        (**self).persist_temp_file(temp_file, target)
    }
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
                    foo(42)
                },
                Entry::directory([("foo", Entry::file(42))])
            );
        }

        #[test]
        fn one_file_comma() {
            assert_eq!(
                fs! {
                    foo(42),
                },
                Entry::directory([("foo", Entry::file(42))])
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
                    "foo"(42),
                    "bar" -> "/target/1",
                    "baz" { "empty" {} },
                },
                Entry::directory([
                    ("foo", Entry::file(42)),
                    ("bar", Entry::symlink("/target/1")),
                    ("baz", Entry::directory([("empty", Entry::directory([]))])),
                ])
            );
        }

        #[test]
        fn kitchen_sink() {
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
                Entry::directory([
                    ("foo", Entry::file(42)),
                    ("bar", Entry::symlink("/target/1")),
                    (
                        "baz",
                        Entry::directory([
                            ("zero", Entry::directory([])),
                            ("one", Entry::directory([("foo", Entry::file(43))])),
                            (
                                "two",
                                Entry::directory([
                                    ("foo", Entry::file(44)),
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
            Ok(Some(Metadata::directory(2)))
        );
        assert_eq!(fs.metadata(Path::new("/foo")), Ok(Some(Metadata::file(42))));
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
            Ok(Some(Metadata::file(42)))
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
    fn create_file() {
        let fs = Fs::new(fs! {
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
            foo(42),
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
            foo(42),
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
    fn read_dir() {
        let fs = Fs::new(fs! {
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
                .collect::<HashMap<_, _>>(),
            HashMap::from([
                ("foo".into(), Metadata::file(42)),
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

        assert!(matches!(fs.read_dir(Path::new("/foo")), Err(Error::NotDir)));
        assert!(matches!(
            fs.read_dir(Path::new("/foo/foo")),
            Err(Error::NotDir)
        ));
        assert!(matches!(
            fs.read_dir(Path::new("/bar/baz")),
            Err(Error::NotDir)
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
    fn rename_source_path_with_file_ancestor() {
        let expected = fs! { foo(42) };
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
        let expected = fs! { foo(42), bar(43) };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar/baz")),
            Err(Error::NotDir)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_destination_path_with_dangling_symlink() {
        let expected = fs! { foo(42), bar -> "dangle" };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar/baz")),
            Err(Error::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_destination_path_not_found() {
        let expected = fs! { foo(42) };
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
        let expected = fs! { dir1 {}, dir2 { foo(42) } };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir1"), Path::new("/dir2")),
            Err(Error::NotEmpty)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_nonempty_directory_onto_itself() {
        let expected = fs! { dir { foo(42) } };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir"), Path::new("/dir")),
            Err(Error::NotEmpty)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn rename_nonempty_root_onto_itself() {
        let expected = fs! { foo(42) };
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
        let expected = fs! { dir {}, file(42), symlink -> "file" };
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
        let expected = fs! { dir {}, file(42), symlink -> "file" };
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
            file(42),
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
        let fs = Fs::new(fs! { foo(42), bar(43) });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar(42) });
    }

    #[test]
    fn rename_file_onto_symlink() {
        let fs = Fs::new(fs! { foo(42), bar -> "foo" });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar(42) });
    }

    #[test]
    fn rename_symlink_onto_file() {
        let fs = Fs::new(fs! { foo -> "bar", bar(43) });
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
    fn rename_directory() {
        let fs = Fs::new(fs! {
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
    fn rename_destination_in_ancestor_of_source() {
        let fs = Fs::new(fs! {
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
    fn rename_source_in_ancestor_of_target() {
        let fs = Fs::new(fs! {
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

    #[test]
    fn rmdir_recursively_on_thread() {
        let fs = Fs::new(fs! {
            dir1 {
                dir2 {
                    file1(43),
                    symlink1 -> "file1",
                },
            },
            dir3 {
                dir4 {
                    file2(43),
                    symlink2 -> "file2",
                },
            },
        });
        assert_eq!(fs.rmdir_recursively_on_thread("/dir1".into()), Ok(()));
        assert_eq!(fs.rmdir_recursively_on_thread("/dir3".into()), Ok(()));
        fs.assert_tree(fs! {
            dir1 {
                dir2 {
                    file1(43),
                    symlink1 -> "file1",
                },
            },
            dir3 {
                dir4 {
                    file2(43),
                    symlink2 -> "file2",
                },
            },
        });
        fs.assert_recursive_rmdirs(HashSet::from(["/dir1".into(), "/dir3".into()]));
        fs.complete_recursive_rmdir("/dir1");
        fs.assert_tree(fs! {
            dir3 {
                dir4 {
                    file2(43),
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
            foo(42),
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
    fn temp_file() {
        let fs = Fs::new(fs! {
            "000"(42),
            dir {
                "000"(43),
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
            "000"(42),
            "001"(0),
            dir {
                "000"(43),
                "001" -> "000",
                "002" {
                    "000"(0),
                },
                "003"(0),
            },
            persist {
            },
        });

        fs.persist_temp_file(temp_file_1, Path::new("/persist/1"))
            .unwrap();
        fs.assert_tree(fs! {
            "000"(42),
            dir {
                "000"(43),
                "001" -> "000",
                "002" {
                    "000"(0),
                },
                "003"(0),
            },
            persist {
                "1"(0),
            },
        });

        fs.persist_temp_file(temp_file_2, Path::new("/persist/2"))
            .unwrap();
        fs.assert_tree(fs! {
            "000"(42),
            dir {
                "000"(43),
                "001" -> "000",
                "002" {
                    "000"(0),
                },
            },
            persist {
                "1"(0),
                "2"(0),
            },
        });

        fs.persist_temp_file(temp_file_3, Path::new("/persist/3"))
            .unwrap();
        fs.assert_tree(fs! {
            "000"(42),
            dir {
                "000"(43),
                "001" -> "000",
                "002" {},
            },
            persist {
                "1"(0),
                "2"(0),
                "3"(0),
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
            file(42),
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
            "000"(42),
            dir {
                "000"(43),
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
            "000"(42),
            "001" {},
            dir {
                "000"(43),
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
            "000"(42),
            dir {
                "000"(43),
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
            "000"(42),
            dir {
                "000"(43),
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
            "000"(42),
            dir {
                "000"(43),
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
            file(42),
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
