pub mod std;
#[cfg(test)]
pub mod test;

use ::std::{
    error,
    ffi::OsString,
    fmt::Debug,
    fs::{self},
    path::{Path, PathBuf},
};
use strum::Display;

/// A type used to represent a temporary file. The assumption is that the implementer may want to
/// make the type [`Drop`] so that the temporary file is cleaned up if it isn't consumed.
pub trait TempFile: Debug {
    /// Return the path to the temporary file. Can be used to open the file to write into it.
    fn path(&self) -> &Path;
}

/// A type used to represent a temporary directory. The assumption is that the implementer may want
/// to make the type [`Drop`] so that the temporary directory is cleaned up if it isn't consumed.
pub trait TempDir: Debug {
    /// Return the path to the temporary directory. Can be used to create files in the directory
    /// before it is made persistent.
    fn path(&self) -> &Path;
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
pub struct Metadata {
    pub type_: FileType,
    pub size: u64,
}

impl Metadata {
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

impl From<fs::Metadata> for Metadata {
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

    /// Create an empty directory with given `path`.
    fn mkdir(&self, path: &Path) -> Result<(), Self::Error>;

    /// Ensure `path` exists and is a directory. If it doesn't exist, recusively ensure its parent exists,
    /// then create it. Panic on file system error or if `path` or any of its ancestors aren't
    /// directories.
    fn mkdir_recursively(&self, path: &Path) -> Result<(), Self::Error>;

    /// Return and iterator that will yield all of the children of a directory. Panic on file
    /// system error or if `path` doesn't exist or isn't a directory.
    fn read_dir(
        &self,
        path: &Path,
    ) -> Result<impl Iterator<Item = Result<(OsString, Metadata), Self::Error>>, Self::Error>;

    /// Create a file with given `path` and `contents`. Panic on file system error, including if
    /// the file already exists.
    fn create_file(&self, path: &Path, contents: &[u8]) -> Result<(), Self::Error>;

    /// Create a symlink at `link` that points to `target`. Panic on file system error.
    fn symlink(&self, target: &Path, link: &Path) -> Result<(), Self::Error>;

    /// Get the metadata of the file at `path`. Panic on file system error. If `path` doesn't
    /// exist, return None. If the last component is a symlink, return the metadata about the
    /// symlink instead of trying to resolve it.
    fn metadata(&self, path: &Path) -> Result<Option<Metadata>, Self::Error>;

    /// The type returned by the [`Self::temp_file`] method. Some implementations may make this
    /// type [`Drop`] so that the temporary file can be cleaned up when it is closed.
    type TempFile: TempFile;

    /// Create a new temporary file in the directory `parent`. Panic on file system error or if
    /// `parent` isn't a directory.
    fn temp_file(&self, parent: &Path) -> Result<Self::TempFile, Self::Error>;

    /// Make `temp_file` exist no more, by moving it to `target`.
    fn persist_temp_file(
        &self,
        temp_file: Self::TempFile,
        target: &Path,
    ) -> Result<(), Self::Error>;

    /// The type returned by the [`Self::temp_dir`] method. Some implementations may make this
    /// type [`Drop`] so that the temporary directory can be cleaned up when it is closed.
    type TempDir: TempDir;

    /// Create a new temporary directory in the directory `parent`. Panic on file system error or
    /// if `parent` isn't a directory.
    fn temp_dir(&self, parent: &Path) -> Result<Self::TempDir, Self::Error>;

    /// Make `temp_dir` exist no more, by moving it to `target`.
    fn persist_temp_dir(&self, temp_dir: Self::TempDir, target: &Path) -> Result<(), Self::Error>;
}
