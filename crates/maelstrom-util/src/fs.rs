use anyhow::{Context as _, Result};
use fs2::FileExt as _;
use std::{
    fmt,
    io::{self},
    os::fd::{AsRawFd, RawFd},
    path::{Path, PathBuf},
};

pub trait GetPath {
    fn path(&self) -> &Path;
}

impl<'a, T: GetPath> GetPath for &'a mut T {
    fn path(&self) -> &Path {
        let self_: &T = self;
        self_.path()
    }
}

impl<'a, T: GetPath> GetPath for &'a T {
    fn path(&self) -> &Path {
        let self_: &T = self;
        self_.path()
    }
}

impl<T> GetPath for io::BufReader<T>
where
    T: GetPath + io::Read,
{
    fn path(&self) -> &Path {
        self.get_ref().path()
    }
}

impl<T> GetPath for io::BufWriter<T>
where
    T: GetPath + io::Write,
{
    fn path(&self) -> &Path {
        self.get_ref().path()
    }
}

impl<T> GetPath for crate::io::BufferedStream<T>
where
    T: GetPath,
{
    fn path(&self) -> &Path {
        self.get_ref().path()
    }
}

pub struct Fs;

impl Fs {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self
    }
}

macro_rules! fs_trampoline {
    ($f:path, $p:ident) => {{
        let path = $p.as_ref();
        $f(path).with_context(|| format!("{}(\"{}\")", stringify!($f), path.display()))
    }};
    ($f:path, $p1:ident, $p2:ident) => {{
        let path1 = $p1.as_ref();
        let path2 = $p2.as_ref();
        $f(path1, path2).with_context(|| {
            format!(
                "{}(\"{}\", \"{}\")",
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
            .with_context(|| format!("{}(\"{}\")", stringify!($f), $self.path.display()))
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
                .with_context(|| format!("read_dir(\"{}\")", self.path.display()))
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
            .with_context(|| format!("metadata(\"{}\")", self.path().display()))
    }
}

impl fmt::Debug for DirEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

pub struct Metadata {
    pub(crate) inner: std::fs::Metadata,
    pub(crate) path: PathBuf,
}

impl Metadata {
    pub fn into_inner(self) -> std::fs::Metadata {
        self.inner
    }

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

#[cfg(unix)]
impl std::os::unix::fs::MetadataExt for Metadata {
    fn dev(&self) -> u64 {
        self.inner.dev()
    }
    fn ino(&self) -> u64 {
        self.inner.ino()
    }
    fn mode(&self) -> u32 {
        self.inner.mode()
    }
    fn nlink(&self) -> u64 {
        self.inner.nlink()
    }
    fn uid(&self) -> u32 {
        self.inner.uid()
    }
    fn gid(&self) -> u32 {
        self.inner.gid()
    }
    fn rdev(&self) -> u64 {
        self.inner.rdev()
    }
    fn size(&self) -> u64 {
        self.inner.size()
    }
    fn atime(&self) -> i64 {
        self.inner.atime()
    }
    fn atime_nsec(&self) -> i64 {
        self.inner.atime_nsec()
    }
    fn mtime(&self) -> i64 {
        self.inner.mtime()
    }
    fn mtime_nsec(&self) -> i64 {
        self.inner.mtime_nsec()
    }
    fn ctime(&self) -> i64 {
        self.inner.ctime()
    }
    fn ctime_nsec(&self) -> i64 {
        self.inner.ctime_nsec()
    }
    fn blksize(&self) -> u64 {
        self.inner.blksize()
    }
    fn blocks(&self) -> u64 {
        self.inner.blocks()
    }
}

fn is_not_found_err(err: &anyhow::Error) -> bool {
    let std_err = err.root_cause().downcast_ref::<std::io::Error>();
    matches!(std_err, Some(e) if e.kind() == std::io::ErrorKind::NotFound)
}

impl Fs {
    pub fn create_dir<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(std::fs::create_dir, path)
    }

    pub fn create_dir_all<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(std::fs::create_dir_all, path)
    }

    pub fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata> {
        fs_trampoline!(std::fs::metadata, path).map(|inner| Metadata {
            inner,
            path: path.as_ref().into(),
        })
    }

    pub fn symlink_metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata> {
        fs_trampoline!(std::fs::symlink_metadata, path).map(|inner| Metadata {
            inner,
            path: path.as_ref().into(),
        })
    }

    pub fn rename<P: AsRef<Path>, Q: AsRef<Path>>(&self, from: P, to: Q) -> Result<()> {
        fs_trampoline!(std::fs::rename, from, to)
    }

    pub fn remove_dir<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(std::fs::remove_dir, path)
    }

    pub fn remove_dir_all<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(std::fs::remove_dir_all, path)
    }

    pub fn remove_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(std::fs::remove_file, path)
    }

    pub fn write<P: AsRef<Path>, C: AsRef<[u8]>>(&self, path: P, contents: C) -> Result<()> {
        let path = path.as_ref();
        std::fs::write(path, contents).with_context(|| format!("write(\"{}\")", path.display()))
    }

    pub fn read_to_string<P: AsRef<Path>>(&self, path: P) -> Result<String> {
        fs_trampoline!(std::fs::read_to_string, path)
    }

    pub fn read_to_string_if_exists<P: AsRef<Path>>(&self, path: P) -> Result<Option<String>> {
        match fs_trampoline!(std::fs::read_to_string, path) {
            Ok(contents) => Ok(Some(contents)),
            Err(err) if is_not_found_err(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<ReadDir> {
        fs_trampoline!(std::fs::read_dir, path).map(|inner| ReadDir {
            inner,
            path: path.as_ref().into(),
        })
    }

    pub fn read_link<P: AsRef<Path>>(&self, path: P) -> Result<PathBuf> {
        fs_trampoline!(std::fs::read_link, path)
    }

    pub fn open_file<P: AsRef<Path>>(&self, path: P) -> Result<File<'_>> {
        let path = path.as_ref();
        Ok(File {
            inner: std::fs::File::open(path)
                .with_context(|| format!("open(\"{}\")", path.display()))?,
            path: path.into(),
            fs: self,
        })
    }

    pub fn open_or_create_file<P: AsRef<Path>>(&self, path: P) -> Result<File<'_>> {
        let path = path.as_ref();
        Ok(File {
            inner: std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .with_context(|| format!("open_or_create(\"{}\")", path.display()))?,
            path: path.into(),
            fs: self,
        })
    }

    pub fn create_file<P: AsRef<Path>>(&self, path: P) -> Result<File<'_>> {
        let path = path.as_ref();
        Ok(File {
            inner: std::fs::File::create(path)
                .with_context(|| format!("create(\"{}\")", path.display()))?,
            path: path.into(),
            fs: self,
        })
    }

    pub fn canonicalize<P: AsRef<Path>>(&self, path: P) -> Result<PathBuf> {
        fs_trampoline!(std::fs::canonicalize, path)
    }

    pub fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
        path.as_ref().exists()
    }

    pub fn glob_walk<'fs, 'glob, P: AsRef<Path>>(
        &'fs self,
        path: P,
        glob: &'glob globset::GlobSet,
    ) -> GlobWalker<'fs, 'glob> {
        GlobWalker::new(self, path.as_ref(), glob)
    }
}

pub struct GlobWalker<'fs, 'glob> {
    start_path: PathBuf,
    fs_walker: walkdir::IntoIter,
    glob: &'glob globset::GlobSet,
    #[allow(dead_code)]
    fs: &'fs Fs,
}

impl<'fs, 'glob> GlobWalker<'fs, 'glob> {
    fn new(fs: &'fs Fs, path: &Path, glob: &'glob globset::GlobSet) -> Self {
        Self {
            start_path: path.to_owned(),
            fs_walker: walkdir::WalkDir::new(path).into_iter(),
            glob,
            fs,
        }
    }

    fn matches(&self, path: &Path) -> bool {
        self.glob.is_match_candidate(&globset::Candidate::new(
            path.strip_prefix(&self.start_path).unwrap(),
        ))
    }
}

impl<'fs, 'glob> Iterator for GlobWalker<'fs, 'glob> {
    type Item = Result<PathBuf>;

    fn next(&mut self) -> Option<Result<PathBuf>> {
        loop {
            match self.fs_walker.next()? {
                Ok(entry) if self.matches(entry.path()) => return Some(Ok(entry.into_path())),
                Err(err) => return Some(Err(err.into())),
                _ => continue,
            }
        }
    }
}

#[cfg(test)]
fn glob_walker_test(glob: &str, input: Vec<&str>, expected: Vec<&str>) {
    use globset::{Glob, GlobSet};

    let temp_dir = tempfile::tempdir().unwrap();
    let fs = Fs::new();
    for p in input {
        let path = temp_dir.path().join(p);
        fs.create_dir_all(path.parent().unwrap()).unwrap();
        fs.write(path, b"").unwrap();
    }

    let mut builder = GlobSet::builder();
    builder.add(Glob::new(glob).unwrap());
    let glob = builder.build().unwrap();

    let paths = Vec::from_iter(fs.glob_walk(temp_dir.path(), &glob).map(|e| e.unwrap()));

    let expected: Vec<_> = expected
        .into_iter()
        .map(|e| temp_dir.path().join(e))
        .collect();
    assert_eq!(paths, expected);
}

#[test]
fn glob_walker_basic() {
    glob_walker_test("*.txt", vec!["a.txt", "b.bin"], vec!["a.txt"]);
    glob_walker_test("foo/*", vec!["foo/a", "bar/b"], vec!["foo/a"]);
    glob_walker_test(
        "foo/**",
        vec!["foo/bar/baz", "bar/b"],
        vec!["foo/bar", "foo/bar/baz"],
    );
}

#[cfg(unix)]
impl Fs {
    pub fn symlink<P: AsRef<Path>, Q: AsRef<Path>>(&self, original: P, link: Q) -> Result<()> {
        fs_trampoline!(std::os::unix::fs::symlink, original, link)
    }
}

pub struct File<'fs> {
    inner: std::fs::File,
    path: PathBuf,
    #[allow(dead_code)]
    fs: &'fs Fs,
}

impl GetPath for File<'_> {
    fn path(&self) -> &Path {
        self.path()
    }
}

impl AsRawFd for File<'_> {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl<'fs> File<'fs> {
    pub fn path(&self) -> &Path {
        &self.path
    }

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

    pub fn lock_shared(&self) -> Result<()> {
        fs_inner_trampoline!(self, lock_shared)
    }

    pub fn lock_exclusive(&self) -> Result<()> {
        fs_inner_trampoline!(self, lock_exclusive)
    }

    pub fn try_lock_shared(&self) -> Result<()> {
        fs_inner_trampoline!(self, try_lock_shared)
    }

    pub fn try_lock_exclusive(&self) -> Result<()> {
        fs_inner_trampoline!(self, try_lock_exclusive)
    }

    pub fn unlock(&self) -> Result<()> {
        fs_inner_trampoline!(self, unlock)
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
