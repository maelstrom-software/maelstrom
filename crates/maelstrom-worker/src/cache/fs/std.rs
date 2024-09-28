use super::FileMetadata;
use std::{
    ffi::OsString,
    fmt::Debug,
    fs::{self, File},
    io::{self, ErrorKind, Write as _},
    os::unix::fs as unix_fs,
    path::{Path, PathBuf},
    thread,
};
use tempfile::{NamedTempFile, TempDir, TempPath};

#[derive(Debug)]
pub struct StdTempFile(TempPath);

impl super::FsTempFile for StdTempFile {
    fn path(&self) -> &Path {
        &self.0
    }

    fn persist(self, target: &Path) {
        self.0.persist(target).unwrap();
    }
}

#[derive(Debug)]
pub struct StdTempDir(TempDir);

impl super::FsTempDir for StdTempDir {
    fn path(&self) -> &Path {
        self.0.path()
    }

    fn persist(self, target: &Path) {
        fs::rename(self.0.into_path(), target).unwrap();
    }
}

/// The standard implementation of CacheFs that uses [std] and [rand].
pub struct StdFs;

impl super::Fs for StdFs {
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
