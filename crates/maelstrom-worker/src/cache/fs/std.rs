use super::Metadata;
use std::{
    ffi::OsString,
    fmt::Debug,
    fs::{self, File},
    io::{self, ErrorKind, Read as _, Write as _},
    os::unix::fs as unix_fs,
    path::{Path, PathBuf},
    thread,
};
use tempfile::{self, NamedTempFile};

#[derive(Debug)]
pub struct TempFile(tempfile::TempPath);

impl super::TempFile for TempFile {
    fn path(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug)]
pub struct TempDir(tempfile::TempDir);

impl super::TempDir for TempDir {
    fn path(&self) -> &Path {
        self.0.path()
    }
}

/// The standard implementation of CacheFs that uses [std] and [rand].
pub struct Fs;

impl super::Fs for Fs {
    type Error = io::Error;

    fn rand_u64(&self) -> u64 {
        rand::random()
    }

    fn metadata(&self, path: &Path) -> io::Result<Option<Metadata>> {
        match fs::symlink_metadata(path) {
            Ok(metadata) => Ok(Some(metadata.into())),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn read_file(&self, path: &Path, contents: &mut [u8]) -> io::Result<usize> {
        File::open(path)?.read(contents)
    }

    fn read_dir(
        &self,
        path: &Path,
    ) -> io::Result<impl Iterator<Item = io::Result<(OsString, Metadata)>>> {
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

    fn mkdir(&self, path: &Path) -> io::Result<()> {
        fs::create_dir(path)
    }

    fn mkdir_recursively(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
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

    fn rename(&self, source: &Path, destination: &Path) -> io::Result<()> {
        fs::rename(source, destination)
    }

    type TempFile = TempFile;

    fn temp_file(&self, parent: &Path) -> io::Result<Self::TempFile> {
        Ok(TempFile(NamedTempFile::new_in(parent)?.into_temp_path()))
    }

    fn persist_temp_file(&self, temp_file: Self::TempFile, target: &Path) -> io::Result<()> {
        Ok(temp_file.0.persist(target)?)
    }

    type TempDir = TempDir;

    fn temp_dir(&self, parent: &Path) -> io::Result<Self::TempDir> {
        Ok(TempDir(tempfile::TempDir::new_in(parent)?))
    }

    fn persist_temp_dir(&self, temp_dir: Self::TempDir, target: &Path) -> io::Result<()> {
        fs::rename(temp_dir.0.into_path(), target)
    }
}
