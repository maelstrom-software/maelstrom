use super::FileMetadata;
use itertools::{Itertools, Position};
use std::{
    cell::RefCell,
    collections::HashMap,
    error,
    ffi::{OsStr, OsString},
    fmt::Debug,
    path::{Component, Path, PathBuf},
    rc::Rc,
    slice, vec,
};
use strum::Display;

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
pub struct TempFile {
    path: PathBuf,
}

impl super::FsTempFile for TempFile {
    fn path(&self) -> &Path {
        &self.path
    }

    fn persist(self, _target: &Path) {
        todo!()
    }
}

#[derive(Debug)]
pub struct TempDir {
    path: PathBuf,
}

impl super::FsTempDir for TempDir {
    fn path(&self) -> &Path {
        &self.path
    }

    fn persist(self, _target: &Path) {
        todo!()
    }
}

#[derive(Default, PartialEq)]
struct ComponentPath(Vec<String>);

impl ComponentPath {
    fn clear(&mut self) {
        *self = Default::default()
    }

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Entry {
    File { size: u64 },
    Directory { entries: HashMap<String, Entry> },
    Symlink { target: String },
}

impl Entry {
    pub fn file(size: u64) -> Self {
        Self::File { size }
    }

    pub fn directory<const N: usize>(entries: [(&str, Entry); N]) -> Self {
        Self::Directory {
            entries: entries
                .into_iter()
                .map(|(name, entry)| (name.to_owned(), entry))
                .collect(),
        }
    }

    pub fn symlink(target: impl ToString) -> Self {
        Self::Symlink {
            target: target.to_string(),
        }
    }

    fn metadata(&self) -> FileMetadata {
        match self {
            Entry::File { size } => FileMetadata::file(*size),
            Entry::Symlink { target } => FileMetadata::symlink(target.len().try_into().unwrap()),
            Entry::Directory { entries } => {
                FileMetadata::directory(entries.len().try_into().unwrap())
            }
        }
    }

    fn is_directory(&self) -> bool {
        matches!(self, Self::Directory { .. })
    }

    fn lookup_component_path<'state, 'path>(
        &'state self,
        path: &'path Path,
    ) -> LookupComponentPath<'state> {
        self.lookup_component_path_helper(self, Default::default(), path)
    }

    fn lookup_component_path_helper<'state, 'path>(
        &'state self,
        mut cur: &'state Entry,
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
                    component_path.clear();
                }
                Component::CurDir => {}
                Component::ParentDir => {
                    (cur, component_path) = Self::pop_component_path(self, component_path);
                }
                Component::Normal(name) => loop {
                    match cur {
                        Entry::Directory { entries } => {
                            let name = name.to_str().unwrap();
                            match entries.get(name) {
                                Some(entry) => {
                                    cur = entry;
                                    component_path.push(name.to_owned());
                                    break;
                                }
                                None => {
                                    if is_last_component {
                                        return LookupComponentPath::FoundParent(
                                            cur,
                                            component_path,
                                        );
                                    } else {
                                        return LookupComponentPath::NotFound;
                                    }
                                }
                            }
                        }
                        Entry::File { .. } => {
                            return LookupComponentPath::FileAncestor;
                        }
                        Entry::Symlink { target } => {
                            (cur, component_path) = Self::pop_component_path(self, component_path);
                            match self.lookup_component_path_helper(
                                cur,
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
                },
            }
        }
        LookupComponentPath::Found(cur, component_path)
    }

    fn pop_component_path(&self, mut component_path: ComponentPath) -> (&Entry, ComponentPath) {
        component_path.pop();
        (self.resolve_component_path(&component_path), component_path)
    }

    /// Resolve `path` starting at this entry.
    fn resolve_component_path(&self, component_path: &ComponentPath) -> &Entry {
        let mut cur = self;
        for component in component_path {
            let Entry::Directory { entries } = cur else {
                panic!("intermediate path entry not a directory");
            };
            cur = entries.get(component).unwrap();
        }
        cur
    }

    fn resolve_component_path_mut_as_directory(
        &mut self,
        component_path: &ComponentPath,
    ) -> &mut HashMap<String, Entry> {
        let mut cur = self;
        for component in component_path {
            let Entry::Directory { entries } = cur else {
                panic!("intermediate path entry not a directory");
            };
            cur = entries.get_mut(component).unwrap();
        }
        let Entry::Directory { entries } = cur else {
            panic!("entry not a directory");
        };
        entries
    }

    fn append_entry_to_directory(
        &mut self,
        directory_component_path: &ComponentPath,
        name: &OsStr,
        entry: Entry,
    ) {
        self.resolve_component_path_mut_as_directory(directory_component_path)
            .insert(name.to_str().unwrap().to_owned(), entry);
    }

    fn remove_leaf_from_component_path(&mut self, component_path: &ComponentPath) -> Entry {
        assert!(!component_path.is_empty());
        let mut cur = self;
        for component in component_path.iter().with_position() {
            let Entry::Directory { entries } = cur else {
                panic!("intermediate path entry not a directory");
            };
            if let Position::Last(component) | Position::Only(component) = component {
                return entries.remove(component).unwrap();
            } else {
                cur = entries.get_mut(component.into_inner()).unwrap();
            }
        }
        unreachable!();
    }
}

#[macro_export]
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
    (@expand [$name:ident -> $target:literal $(,$($tail:tt)*)?] -> [$($($expanded:tt)+)?]) => {
        fs!(
            @expand
            [$($($tail)*)?] -> [
                $($($expanded)+,)?
                (stringify!($name), $crate::cache::fs::test::Entry::symlink($target))
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
    ($($body:tt)*) => ($crate::cache::fs::test::Entry::directory(fs!(@expand [$($body)*] -> [])));
}

#[derive(Debug)]
struct State {
    root: Entry,
    last_random_number: u64,
}

#[derive(Debug)]
pub struct Fs {
    state: Rc<RefCell<State>>,
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
            state: Rc::new(RefCell::new(State {
                root,
                last_random_number: 0,
            })),
        }
    }

    #[track_caller]
    pub fn assert_tree(&self, expected: Entry) {
        assert_eq!(self.state.borrow().root, expected);
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

    fn rename(&self, source_path: &Path, dest_path: &Path) -> Result<(), Error> {
        let mut state = self.state.borrow_mut();

        let (source_entry, source_component_path) =
            match state.root.lookup_component_path(source_path) {
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

        let dest_parent_component_path = match state.root.lookup_component_path(dest_path) {
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
            LookupComponentPath::Found(dest_entry, mut dest_component_path) => {
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

                // Remove the destination directory and proceed as if it wasn't there to begin
                // with. We may have to adjust the source_component_path to account for the
                // removal of the destination.
                state
                    .root
                    .remove_leaf_from_component_path(&dest_component_path);
                dest_component_path.pop();
                dest_component_path
            }
        };

        let source_entry = state
            .root
            .remove_leaf_from_component_path(&source_component_path);
        state.root.append_entry_to_directory(
            &dest_parent_component_path,
            dest_path.file_name().unwrap(),
            source_entry,
        );

        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<(), Error> {
        let mut state = self.state.borrow_mut();
        match state.root.lookup_component_path(path) {
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::DanglingSymlink
            | LookupComponentPath::NotFound
            | LookupComponentPath::FoundParent(_, _) => Err(Error::NoEnt),
            LookupComponentPath::Found(Entry::Directory { .. }, _) => Err(Error::IsDir),
            LookupComponentPath::Found(_, mut component_path) => {
                let component = component_path.pop().unwrap();
                state
                    .root
                    .resolve_component_path_mut_as_directory(&component_path)
                    .remove(&component);
                Ok(())
            }
        }
    }

    fn rmdir_recursively_on_thread(&self, _path: PathBuf) -> Result<(), Error> {
        unimplemented!()
        /*
        self.messages
            .borrow_mut()
            .push(RemoveRecursively(path.to_owned()));
            */
    }

    fn mkdir_recursively(&self, path: &Path) -> Result<(), Error> {
        let mut state = self.state.borrow_mut();
        match state.root.lookup_component_path(path) {
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::Found(Entry::Directory { .. }, _) => Ok(()),
            LookupComponentPath::Found(_, _) => Err(Error::Exists),
            LookupComponentPath::FoundParent(_, parent_component_path) => {
                state.root.append_entry_to_directory(
                    &parent_component_path,
                    path.file_name().unwrap(),
                    Entry::directory([]),
                );
                Ok(())
            }
            LookupComponentPath::NotFound => {
                drop(state);
                self.mkdir_recursively(path.parent().unwrap())?;
                self.mkdir_recursively(path)
            }
        }
    }

    fn read_dir(
        &self,
        path: &Path,
    ) -> Result<impl Iterator<Item = Result<(OsString, FileMetadata), Error>>, Error> {
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
        let mut state = self.state.borrow_mut();
        let parent_component_path = match state.root.lookup_component_path(path) {
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::NotFound => Err(Error::NoEnt),
            LookupComponentPath::Found(_, _) => Err(Error::Exists),
            LookupComponentPath::FoundParent(_, component_path) => Ok(component_path),
        }?;
        state.root.append_entry_to_directory(
            &parent_component_path,
            path.file_name().unwrap(),
            Entry::file(contents.len().try_into().unwrap()),
        );
        Ok(())
    }

    fn symlink(&self, target: &Path, link: &Path) -> Result<(), Error> {
        let mut state = self.state.borrow_mut();
        let parent_component_path = match state.root.lookup_component_path(target) {
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
            LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::NotFound => Err(Error::NoEnt),
            LookupComponentPath::Found(_, _) => Err(Error::Exists),
            LookupComponentPath::FoundParent(_, component_path) => Ok(component_path),
        }?;
        state.root.append_entry_to_directory(
            &parent_component_path,
            target.file_name().unwrap(),
            Entry::symlink(link.to_str().unwrap()),
        );
        Ok(())
    }

    fn metadata(&self, path: &Path) -> Result<Option<FileMetadata>, Error> {
        match self.state.borrow().root.lookup_component_path(path) {
            LookupComponentPath::Found(entry, _) => Ok(Some(entry.metadata())),
            LookupComponentPath::NotFound | LookupComponentPath::FoundParent(_, _) => Ok(None),
            LookupComponentPath::DanglingSymlink => Err(Error::NoEnt),
            LookupComponentPath::FileAncestor => Err(Error::NotDir),
        }
    }

    fn temp_file(&self, _parent: &Path) -> Self::TempFile {
        unimplemented!()
        /*
        let path = parent.join(format!("{:0>16x}", self.rand_u64()));
        self.messages.borrow_mut().push(TempFile(path.clone()));
        TempFile {
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
        TempDir {
            path,
            messages: self.messages.clone(),
        }
        */
    }
}

#[cfg(test)]
mod tests {
    use super::super::Fs as _;
    use super::*;

    #[test]
    fn fs_empty() {
        assert_eq!(fs! {}, Entry::directory([]));
    }

    #[test]
    fn fs_one_file_no_comma() {
        assert_eq!(
            fs! {
                foo(42)
            },
            Entry::directory([("foo", Entry::file(42))])
        );
    }

    #[test]
    fn fs_one_file_comma() {
        assert_eq!(
            fs! {
                foo(42),
            },
            Entry::directory([("foo", Entry::file(42))])
        );
    }

    #[test]
    fn fs_one_symlink_no_comma() {
        assert_eq!(
            fs! {
                foo -> "/target"
            },
            Entry::directory([("foo", Entry::symlink("/target"))])
        );
    }

    #[test]
    fn fs_one_symlink_comma() {
        assert_eq!(
            fs! {
                foo -> "/target",
            },
            Entry::directory([("foo", Entry::symlink("/target"))])
        );
    }

    #[test]
    fn fs_one_directory_no_comma() {
        assert_eq!(
            fs! {
                foo {}
            },
            Entry::directory([("foo", Entry::directory([]))])
        );
    }

    #[test]
    fn fs_one_directory_comma() {
        assert_eq!(
            fs! {
                foo {},
            },
            Entry::directory([("foo", Entry::directory([]))])
        );
    }

    #[test]
    fn fs_one_directory_with_no_files() {
        assert_eq!(
            fs! {
                foo {},
            },
            Entry::directory([("foo", Entry::directory([]))])
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

    #[test]
    fn test_fs2_rand_u64() {
        let fs = Fs::new(fs! {});
        assert_eq!(fs.rand_u64(), 1);
        assert_eq!(fs.rand_u64(), 2);
        assert_eq!(fs.rand_u64(), 3);
    }

    #[test]
    fn test_fs2_metadata_empty() {
        let fs = Fs::new(fs! {});
        assert_eq!(
            fs.metadata(Path::new("/")),
            Ok(Some(FileMetadata::directory(0)))
        );
        assert_eq!(fs.metadata(Path::new("/foo")), Ok(None));
    }

    #[test]
    fn test_fs2_metadata() {
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
        assert_eq!(fs.metadata(Path::new("/bar/baz/foo")), Err(Error::NoEnt));
        assert_eq!(fs.metadata(Path::new("/bar/baz2")), Ok(None));
        assert_eq!(fs.metadata(Path::new("/bar/baz2/blah")), Ok(None));
        assert_eq!(fs.metadata(Path::new("/foo/bar")), Err(Error::NotDir));
    }

    #[test]
    fn test_fs2_create_file() {
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
    fn test_fs2_symlink() {
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
            Err(Error::Exists)
        );
        assert_eq!(
            fs.symlink(Path::new("/foo"), Path::new("new-target-3")),
            Err(Error::Exists)
        );
        assert_eq!(
            fs.symlink(Path::new("/blah/new_symlink"), Path::new("new-target-3")),
            Err(Error::NoEnt)
        );
        assert_eq!(
            fs.symlink(Path::new("/foo/new_symlink"), Path::new("new-target-3")),
            Err(Error::NotDir)
        );
        assert_eq!(
            fs.symlink(Path::new("/bar/baz/new_symlink"), Path::new("new-target-3")),
            Err(Error::NoEnt)
        );
    }

    #[test]
    fn test_fs2_mkdir_recursively() {
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
    fn test_fs2_remove() {
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
            Ok(Some(FileMetadata::symlink(8)))
        );

        assert_eq!(fs.remove(Path::new("/foo")), Ok(()));
        assert_eq!(fs.metadata(Path::new("/foo")), Ok(None));
    }

    #[test]
    fn test_fs2_read_dir() {
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
                ("foo".into(), FileMetadata::file(42)),
                ("bar".into(), FileMetadata::directory(3)),
            ]),
        );
        assert_eq!(
            fs.read_dir(Path::new("/bar"))
                .unwrap()
                .map(Result::unwrap)
                .collect::<HashMap<_, _>>(),
            HashMap::from([
                ("baz".into(), FileMetadata::symlink(7)),
                ("root".into(), FileMetadata::symlink(1)),
                ("subdir".into(), FileMetadata::directory(0)),
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
    fn test_fs2_rename_source_path_with_file_ancestor() {
        let expected = fs! { foo(42) };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo/bar"), Path::new("/bar")),
            Err(Error::NotDir)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_source_path_with_dangling_symlink() {
        let expected = fs! { foo -> "/dangle" };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo/bar"), Path::new("/bar")),
            Err(Error::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_source_path_not_found() {
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
    fn test_fs2_rename_destination_path_with_file_ancestor() {
        let expected = fs! { foo(42), bar(43) };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar/baz")),
            Err(Error::NotDir)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_destination_path_with_dangling_symlink() {
        let expected = fs! { foo(42), bar -> "dangle" };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar/baz")),
            Err(Error::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_destination_path_not_found() {
        let expected = fs! { foo(42) };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/bar/baz")),
            Err(Error::NoEnt)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_directory_into_new_entry_in_itself() {
        let expected = fs! { foo { bar { baz {} } } };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/foo"), Path::new("/foo/bar/baz/a")),
            Err(Error::Inval)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_directory_into_new_entry_in_descendant_of_itself() {
        let expected = fs! { dir {} };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir"), Path::new("/dir/a")),
            Err(Error::Inval)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_directory_onto_nonempty_directory() {
        let expected = fs! { dir1 {}, dir2 { foo(42) } };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir1"), Path::new("/dir2")),
            Err(Error::NotEmpty)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_nonempty_directory_onto_itself() {
        let expected = fs! { dir { foo(42) } };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir"), Path::new("/dir")),
            Err(Error::NotEmpty)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_nonempty_root_onto_itself() {
        let expected = fs! { foo(42) };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/"), Path::new("/")),
            Err(Error::NotEmpty)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_directory_onto_descendant() {
        let expected = fs! { dir1 { dir2 {} } };
        let fs = Fs::new(expected.clone());
        assert_eq!(
            fs.rename(Path::new("/dir1"), Path::new("/dir1/dir2")),
            Err(Error::Inval)
        );
        fs.assert_tree(expected);
    }

    #[test]
    fn test_fs2_rename_directory_onto_nondirectory() {
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
    fn test_fs2_rename_nondirectory_onto_directory() {
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
    fn test_fs2_rename_empty_root_onto_itself() {
        let fs = Fs::new(fs! {});
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
    fn test_fs2_rename_file_onto_itself() {
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
    fn test_fs2_rename_symlink_onto_itself() {
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
    fn test_fs2_rename_file_onto_file() {
        let fs = Fs::new(fs! { foo(42), bar(43) });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar(42) });
    }

    #[test]
    fn test_fs2_rename_file_onto_symlink() {
        let fs = Fs::new(fs! { foo(42), bar -> "foo" });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar(42) });
    }

    #[test]
    fn test_fs2_rename_symlink_onto_file() {
        let fs = Fs::new(fs! { foo -> "bar", bar(43) });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar -> "bar" });
    }

    #[test]
    fn test_fs2_rename_symlink_onto_symlink() {
        let fs = Fs::new(fs! { foo -> "bar", bar -> "foo" });
        assert_eq!(fs.rename(Path::new("/foo"), Path::new("/bar")), Ok(()));
        fs.assert_tree(fs! { bar -> "bar" });
    }

    #[test]
    fn test_fs2_rename_into_new_entries_in_directory() {
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
    fn test_fs2_rename_directory() {
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
    fn test_fs2_rename_destination_in_ancestor_of_source() {
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
    fn test_fs2_rename_source_in_ancestor_of_target() {
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
}
