use super::FileMetadata;
use itertools::Itertools;
use std::{
    cell::RefCell,
    error,
    ffi::{OsStr, OsString},
    fmt::Debug,
    path::{Component, Path, PathBuf},
    rc::Rc,
};
use strum::Display;

#[derive(Debug, Display, PartialEq)]
enum Error {
    Exists,
    IsDir,
    Inval,
    NoEnt,
    NotDir,
    NotEmpty,
}

impl error::Error for Error {}

#[derive(Debug)]
struct TempFile {
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
struct TempDir {
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum Entry {
    File { size: u64 },
    Directory { entries: Vec<(String, Entry)> },
    Symlink { target: String },
}

impl Entry {
    fn file(size: u64) -> Self {
        Self::File { size }
    }

    fn directory<const N: usize>(entries: [(&str, Entry); N]) -> Self {
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
                    (stringify!($name), Entry::file($size))
                ]
            )
        };
        (@expand [$name:ident -> $target:literal $(,$($tail:tt)*)?] -> [$($($expanded:tt)+)?]) => {
            fs!(
                @expand
                [$($($tail)*)?] -> [
                    $($($expanded)+,)?
                    (stringify!($name), Entry::symlink($target))
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
        ($($body:tt)*) => (Entry::directory(fs!(@expand [$($body)*] -> [])));
    }

#[derive(Debug)]
struct State {
    root: Entry,
    last_random_number: u64,
}

#[derive(Debug)]
struct Fs {
    state: Rc<RefCell<State>>,
}

enum LookupIndexPath<'state> {
    /// The path resolved to an actual entry. This contains the entry and the path to it.
    Found(&'state Entry, Vec<usize>),

    /// There was entry at the given path, but the parent directory exists. This contains the
    /// parent entry (which is guaranteed to be a directory) and the path to it.
    #[expect(dead_code)]
    FoundParent(&'state Entry, Vec<usize>),

    /// There was no entry at the given path, nor does its parent directory exist. However, all
    /// ancestors that do exist are directories.
    NotFound,

    /// A symlink in the path didn't resolve.
    DanglingSymlink,

    /// An ancestor in the path was a file.
    FileAncestor,
}

impl Fs {
    fn new(root: Entry) -> Self {
        assert!(root.is_directory());
        Self {
            state: Rc::new(RefCell::new(State {
                root,
                last_random_number: 0,
            })),
        }
    }

    #[track_caller]
    fn assert_tree(&self, expected: Entry) {
        assert_eq!(self.state.borrow().root, expected);
    }

    fn resolve_index_path(root: &Entry, index_path: impl IntoIterator<Item = usize>) -> &Entry {
        let mut cur = root;
        for index in index_path {
            let Entry::Directory { entries } = cur else {
                panic!("intermediate path entry not a directory");
            };
            cur = &entries[index].1;
        }
        cur
    }

    fn resolve_index_path_mut_as_directory(
        root: &mut Entry,
        index_path: impl IntoIterator<Item = usize>,
    ) -> &mut Vec<(String, Entry)> {
        let mut cur = root;
        for index in index_path {
            let Entry::Directory { entries } = cur else {
                panic!("intermediate path entry not a directory");
            };
            cur = &mut entries[index].1;
        }
        let Entry::Directory { entries } = cur else {
            panic!("entry not a directory");
        };
        entries
    }

    fn append_entry_to_directory(
        root: &mut Entry,
        directory_index_path: impl IntoIterator<Item = usize>,
        name: &OsStr,
        entry: Entry,
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
        root: &mut Entry,
        index_path: &Vec<usize>,
        to_keep_index_path: &mut Vec<usize>,
    ) -> Entry {
        assert!(!index_path.is_empty());
        Self::adjust_one_index_path_for_removal_of_other(to_keep_index_path, index_path);
        let mut cur = root;
        let index_path_len = index_path.len();
        for (i, index) in index_path.into_iter().enumerate() {
            let Entry::Directory { entries } = cur else {
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

    fn pop_index_path(root: &Entry, mut index_path: Vec<usize>) -> (&Entry, Vec<usize>) {
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
        root: &'state Entry,
        mut cur: &'state Entry,
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
                        Entry::Directory { entries } => {
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
                        Entry::File { .. } => {
                            return LookupIndexPath::FileAncestor;
                        }
                        Entry::Symlink { target } => {
                            (cur, index_path) = Self::pop_index_path(root, index_path);
                            match Self::lookup_index_path(root, cur, index_path, Path::new(target))
                            {
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

        let (source_entry, mut source_index_path) =
            match Self::lookup_index_path(&state.root, &state.root, vec![], source_path) {
                LookupIndexPath::FileAncestor => {
                    return Err(Error::NotDir);
                }
                LookupIndexPath::DanglingSymlink
                | LookupIndexPath::NotFound
                | LookupIndexPath::FoundParent(_, _) => {
                    return Err(Error::NoEnt);
                }
                LookupIndexPath::Found(source_entry, source_index_path) => {
                    (source_entry, source_index_path)
                }
            };

        let mut dest_parent_index_path =
            match Self::lookup_index_path(&state.root, &state.root, vec![], dest_path) {
                LookupIndexPath::FileAncestor => {
                    return Err(Error::NotDir);
                }
                LookupIndexPath::DanglingSymlink | LookupIndexPath::NotFound => {
                    return Err(Error::NoEnt);
                }
                LookupIndexPath::FoundParent(_, dest_parent_index_path) => {
                    if source_entry.is_directory()
                        && Self::is_descendant_of(&dest_parent_index_path, &source_index_path)
                    {
                        // We can't move a directory into one of its descendants, including itself.
                        return Err(Error::Inval);
                    } else {
                        dest_parent_index_path
                    }
                }
                LookupIndexPath::Found(dest_entry, mut dest_index_path) => {
                    if source_entry.is_directory() {
                        if let Entry::Directory { entries } = dest_entry {
                            if !entries.is_empty() {
                                // A directory can't be moved on top of a non-empty directory.
                                return Err(Error::NotEmpty);
                            } else if source_index_path == dest_index_path {
                                // This is a weird edge case, but we allow it. We're moving an
                                // empty directory on top of itself. It's unknown if this matches
                                // Linux behavior, but it doesn't really matter as we don't
                                // imagine ever seeing this in our cache code.
                                return Ok(());
                            } else if Self::is_descendant_of(&dest_index_path, &source_index_path) {
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

    fn remove(&self, path: &Path) -> Result<(), Error> {
        let mut state = self.state.borrow_mut();
        match Self::lookup_index_path(&state.root, &state.root, vec![], path) {
            LookupIndexPath::FileAncestor => Err(Error::NotDir),
            LookupIndexPath::DanglingSymlink
            | LookupIndexPath::NotFound
            | LookupIndexPath::FoundParent(_, _) => Err(Error::NoEnt),
            LookupIndexPath::Found(Entry::Directory { .. }, _) => Err(Error::IsDir),
            LookupIndexPath::Found(_, mut index_path) => {
                let index = index_path.pop().unwrap();
                Self::resolve_index_path_mut_as_directory(&mut state.root, index_path)
                    .remove(index);
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
        match Self::lookup_index_path(&state.root, &state.root, vec![], path) {
            LookupIndexPath::FileAncestor => Err(Error::NotDir),
            LookupIndexPath::DanglingSymlink => Err(Error::NoEnt),
            LookupIndexPath::Found(Entry::Directory { .. }, _) => Ok(()),
            LookupIndexPath::Found(_, _) => Err(Error::Exists),
            LookupIndexPath::FoundParent(_, parent_index_path) => {
                Self::append_entry_to_directory(
                    &mut state.root,
                    parent_index_path,
                    path.file_name().unwrap(),
                    Entry::directory([]),
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
    ) -> Result<impl Iterator<Item = Result<(OsString, FileMetadata), Error>>, Error> {
        let state = self.state.borrow();
        match Self::lookup_index_path(&state.root, &state.root, vec![], path) {
            LookupIndexPath::Found(Entry::Directory { entries }, _) => Ok(entries
                .iter()
                .map(|(name, entry)| Ok((OsStr::new(name).to_owned(), entry.metadata())))
                .collect_vec()
                .into_iter()),
            LookupIndexPath::Found(_, _) | LookupIndexPath::FileAncestor => Err(Error::NotDir),
            LookupIndexPath::NotFound
            | LookupIndexPath::FoundParent(_, _)
            | LookupIndexPath::DanglingSymlink => Err(Error::NoEnt),
        }
    }

    fn create_file(&self, path: &Path, contents: &[u8]) -> Result<(), Error> {
        let mut state = self.state.borrow_mut();
        let parent_index_path =
            match Self::lookup_index_path(&state.root, &state.root, vec![], path) {
                LookupIndexPath::FileAncestor => Err(Error::NotDir),
                LookupIndexPath::DanglingSymlink => Err(Error::NoEnt),
                LookupIndexPath::NotFound => Err(Error::NoEnt),
                LookupIndexPath::Found(_, _) => Err(Error::Exists),
                LookupIndexPath::FoundParent(_, index_path) => Ok(index_path),
            }?;
        Self::append_entry_to_directory(
            &mut state.root,
            parent_index_path,
            path.file_name().unwrap(),
            Entry::file(contents.len().try_into().unwrap()),
        );
        Ok(())
    }

    fn symlink(&self, target: &Path, link: &Path) -> Result<(), Error> {
        let mut state = self.state.borrow_mut();
        let parent_index_path =
            match Self::lookup_index_path(&state.root, &state.root, vec![], target) {
                LookupIndexPath::FileAncestor => Err(Error::NotDir),
                LookupIndexPath::DanglingSymlink => Err(Error::NoEnt),
                LookupIndexPath::NotFound => Err(Error::NoEnt),
                LookupIndexPath::Found(_, _) => Err(Error::Exists),
                LookupIndexPath::FoundParent(_, index_path) => Ok(index_path),
            }?;
        Self::append_entry_to_directory(
            &mut state.root,
            parent_index_path,
            target.file_name().unwrap(),
            Entry::symlink(link.to_str().unwrap()),
        );
        Ok(())
    }

    fn metadata(&self, path: &Path) -> Result<Option<FileMetadata>, Error> {
        let state = self.state.borrow();
        match Self::lookup_index_path(&state.root, &state.root, vec![], path) {
            LookupIndexPath::Found(entry, _) => Ok(Some(entry.metadata())),
            LookupIndexPath::NotFound | LookupIndexPath::FoundParent(_, _) => Ok(None),
            LookupIndexPath::DanglingSymlink => Err(Error::NoEnt),
            LookupIndexPath::FileAncestor => Err(Error::NotDir),
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
    use itertools::Itertools;

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
