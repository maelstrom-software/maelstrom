use anyhow::{Context as _, Result};
use std::io::{self};
use std::path::{Path, PathBuf};

pub struct Fs;

impl Fs {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self
    }
}

macro_rules! fs_trampoline {
    ($f:ident, $p:ident) => {{
        let path = $p.as_ref();
        std::fs::$f(path).with_context(|| format!("{}({})", stringify!($f), path.display()))
    }};
    ($f:ident, $p1:ident, $p2:ident) => {{
        let path1 = $p1.as_ref();
        let path2 = $p2.as_ref();
        std::fs::$f(path1, path2).with_context(|| {
            format!(
                "{}({}, {})",
                stringify!($f),
                path1.display(),
                path2.display()
            )
        })
    }};
}

macro_rules! fs_inner_trampoline {
    ($self:expr, $f:ident, $($args:tt),*) => {{
        $self
            .inner
            .$f($($args),*)
            .with_context(|| format!("{}({})", stringify!($f), $self.path.display()))
    }};
    ($self:expr, $f:ident) => {
        fs_inner_trampoline!($self, $f, )
    };
}

pub struct ReadDir {
    inner: std::fs::ReadDir,
    path: PathBuf,
}

impl Iterator for ReadDir {
    type Item = Result<DirEntry>;

    fn next(&mut self) -> Option<Result<DirEntry>> {
        self.inner.next().map(|entry| {
            entry
                .map(|inner| DirEntry { inner })
                .with_context(|| format!("read_dir({})", self.path.display()))
        })
    }
}

pub struct DirEntry {
    inner: std::fs::DirEntry,
}

impl DirEntry {
    pub fn path(&self) -> PathBuf {
        self.inner.path()
    }

    pub fn metadata(&self) -> Result<Metadata> {
        self.inner
            .metadata()
            .map(|inner| Metadata {
                inner,
                path: self.path(),
            })
            .with_context(|| format!("metadata({})", self.path().display()))
    }
}

pub struct Metadata {
    inner: std::fs::Metadata,
    path: PathBuf,
}

impl Metadata {
    pub fn file_type(&self) -> std::fs::FileType {
        self.inner.file_type()
    }

    pub fn is_dir(&self) -> bool {
        self.inner.is_dir()
    }

    pub fn is_file(&self) -> bool {
        self.inner.is_file()
    }

    pub fn is_symlink(&self) -> bool {
        self.inner.is_symlink()
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    pub fn permissions(&self) -> std::fs::Permissions {
        self.inner.permissions()
    }

    pub fn modified(&self) -> Result<std::time::SystemTime> {
        fs_inner_trampoline!(self, modified)
    }

    pub fn accessed(&self) -> Result<std::time::SystemTime> {
        fs_inner_trampoline!(self, accessed)
    }

    pub fn created(&self) -> Result<std::time::SystemTime> {
        fs_inner_trampoline!(self, created)
    }
}

impl Fs {
    pub fn create_dir<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(create_dir, path)
    }

    pub fn create_dir_all<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(create_dir_all, path)
    }

    pub fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata> {
        fs_trampoline!(metadata, path).map(|inner| Metadata {
            inner,
            path: path.as_ref().into(),
        })
    }

    pub fn rename<P: AsRef<Path>, Q: AsRef<Path>>(&self, from: P, to: Q) -> Result<()> {
        fs_trampoline!(rename, from, to)
    }

    pub fn remove_dir<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(remove_dir, path)
    }

    pub fn remove_dir_all<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(remove_dir_all, path)
    }

    pub fn remove_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(remove_file, path)
    }

    pub fn write<P: AsRef<Path>, C: AsRef<[u8]>>(&self, path: P, contents: C) -> Result<()> {
        let path = path.as_ref();
        std::fs::write(path, contents).with_context(|| format!("write({})", path.display()))
    }

    pub fn read_to_string<P: AsRef<Path>>(&self, path: P) -> Result<String> {
        fs_trampoline!(read_to_string, path)
    }

    pub fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<ReadDir> {
        fs_trampoline!(read_dir, path).map(|inner| ReadDir {
            inner,
            path: path.as_ref().into(),
        })
    }

    pub fn open_file<P: AsRef<Path>>(&self, path: P) -> Result<File<'_>> {
        let path = path.as_ref();
        Ok(File {
            inner: std::fs::File::open(path)
                .with_context(|| format!("open({})", path.display()))?,
            path: path.into(),
            fs: self,
        })
    }

    pub fn create_file<P: AsRef<Path>>(&self, path: P) -> Result<File<'_>> {
        let path = path.as_ref();
        Ok(File {
            inner: std::fs::File::create(path)
                .with_context(|| format!("create({})", path.display()))?,
            path: path.into(),
            fs: self,
        })
    }

    pub fn canonicalize<P: AsRef<Path>>(&self, path: P) -> Result<PathBuf> {
        fs_trampoline!(canonicalize, path)
    }
}

pub struct File<'fs> {
    inner: std::fs::File,
    path: PathBuf,
    #[allow(dead_code)]
    fs: &'fs Fs,
}

impl<'fs> File<'fs> {
    pub fn into_inner(self) -> std::fs::File {
        self.inner
    }

    pub fn set_len(&self, size: u64) -> Result<()> {
        fs_inner_trampoline!(self, set_len, size)
    }

    pub fn metadata(&self) -> Result<Metadata> {
        fs_inner_trampoline!(self, metadata).map(|inner| Metadata {
            inner,
            path: self.path.clone(),
        })
    }

    pub fn set_permissions(&self, perm: std::fs::Permissions) -> Result<()> {
        fs_inner_trampoline!(self, set_permissions, perm)
    }
}

impl io::Write for File<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.inner.write_vectored(bufs)
    }
}

impl<'a> io::Write for &'a File<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&self.inner).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&self.inner).flush()
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        (&self.inner).write_vectored(bufs)
    }
}

impl<'a> io::Read for &'a File<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&self.inner).read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        (&self.inner).read_vectored(bufs)
    }
}

impl io::Read for File<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.inner.read_vectored(bufs)
    }
}

impl io::Seek for File<'_> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl<'a> io::Seek for &'a File<'_> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        (&self.inner).seek(pos)
    }
}
