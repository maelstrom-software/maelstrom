pub use crate::fs::Metadata;
use anyhow::{Context as _, Result};
use fs2::FileExt as _;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::{pin, Pin};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

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
        $f(path)
            .await
            .with_context(|| format!("{}(\"{}\")", stringify!($f), path.display()))
    }};
    ($f:path, $p1:ident, $p2:ident) => {{
        let path1 = $p1.as_ref();
        let path2 = $p2.as_ref();
        $f(path1, path2).await.with_context(|| {
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
            .await
            .with_context(|| format!("{}(\"{}\")", stringify!($f), $self.path.display()))
    }};
    ($self:expr, $f:ident) => {
        fs_inner_trampoline!($self, $f, )
    };
}

pub struct ReadDir {
    inner: tokio::fs::ReadDir,
    path: PathBuf,
}

impl ReadDir {
    pub async fn next_entry(&mut self) -> Result<Option<DirEntry>> {
        let entry = self
            .inner
            .next_entry()
            .await
            .with_context(|| format!("read_dir(\"{}\")", self.path.display()))?;
        Ok(entry.map(|inner| DirEntry { inner }))
    }
}

pub struct DirEntry {
    inner: tokio::fs::DirEntry,
}

impl DirEntry {
    pub fn path(&self) -> PathBuf {
        self.inner.path()
    }

    pub async fn metadata(&self) -> Result<Metadata> {
        self.inner
            .metadata()
            .await
            .map(|inner| Metadata {
                inner,
                path: self.path(),
            })
            .with_context(|| format!("metadata(\"{}\")", self.path().display()))
    }
}

fn is_not_found_err(err: &anyhow::Error) -> bool {
    let std_err = err.root_cause().downcast_ref::<std::io::Error>();
    matches!(std_err, Some(e) if e.kind() == std::io::ErrorKind::NotFound)
}

impl Fs {
    pub async fn create_dir<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(tokio::fs::create_dir, path)
    }

    pub async fn create_dir_all<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(tokio::fs::create_dir_all, path)
    }

    pub async fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata> {
        fs_trampoline!(tokio::fs::metadata, path).map(|inner| Metadata {
            inner,
            path: path.as_ref().into(),
        })
    }

    pub async fn symlink_metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata> {
        fs_trampoline!(tokio::fs::symlink_metadata, path).map(|inner| Metadata {
            inner,
            path: path.as_ref().into(),
        })
    }

    pub async fn rename<P: AsRef<Path>, Q: AsRef<Path>>(&self, from: P, to: Q) -> Result<()> {
        fs_trampoline!(tokio::fs::rename, from, to)
    }

    pub async fn remove_dir<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(tokio::fs::remove_dir, path)
    }

    pub async fn remove_dir_all<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(tokio::fs::remove_dir_all, path)
    }

    pub async fn remove_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        fs_trampoline!(tokio::fs::remove_file, path)
    }

    pub async fn write<P: AsRef<Path>, C: AsRef<[u8]>>(&self, path: P, contents: C) -> Result<()> {
        let path = path.as_ref();
        tokio::fs::write(path, contents)
            .await
            .with_context(|| format!("write(\"{}\")", path.display()))
    }

    pub async fn read_to_string<P: AsRef<Path>>(&self, path: P) -> Result<String> {
        fs_trampoline!(tokio::fs::read_to_string, path)
    }

    pub async fn read_to_string_if_exists<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Option<String>> {
        match fs_trampoline!(tokio::fs::read_to_string, path) {
            Ok(contents) => Ok(Some(contents)),
            Err(err) if is_not_found_err(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn read_link<P: AsRef<Path>>(&self, path: P) -> Result<PathBuf> {
        fs_trampoline!(tokio::fs::read_link, path)
    }

    pub async fn open_file<P: AsRef<Path>>(&self, path: P) -> Result<File<'_>> {
        let path = path.as_ref();
        Ok(File {
            inner: tokio::fs::File::open(path)
                .await
                .with_context(|| format!("open(\"{}\")", path.display()))?,
            path: path.into(),
            fs: self,
        })
    }

    pub async fn open_or_create_file<P: AsRef<Path>>(&self, path: P) -> Result<File<'_>> {
        let path = path.as_ref();
        Ok(File {
            inner: tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .await
                .with_context(|| format!("open_or_create(\"{}\")", path.display()))?,
            path: path.into(),
            fs: self,
        })
    }

    pub async fn create_file<P: AsRef<Path>>(&self, path: P) -> Result<File<'_>> {
        let path = path.as_ref();
        Ok(File {
            inner: tokio::fs::File::create(path)
                .await
                .with_context(|| format!("create(\"{}\")", path.display()))?,
            path: path.into(),
            fs: self,
        })
    }

    pub async fn canonicalize<P: AsRef<Path>>(&self, path: P) -> Result<PathBuf> {
        fs_trampoline!(tokio::fs::canonicalize, path)
    }

    pub async fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
        path.as_ref().exists()
    }

    pub async fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<ReadDir> {
        fs_trampoline!(tokio::fs::read_dir, path).map(|inner| ReadDir {
            inner,
            path: path.as_ref().into(),
        })
    }
}

#[cfg(unix)]
impl Fs {
    pub async fn symlink<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        original: P,
        link: Q,
    ) -> Result<()> {
        fs_trampoline!(tokio::fs::symlink, original, link)
    }
}

pub struct File<'fs> {
    inner: tokio::fs::File,
    path: PathBuf,
    #[allow(dead_code)]
    fs: &'fs Fs,
}

impl<'fs> File<'fs> {
    pub fn into_inner(self) -> tokio::fs::File {
        self.inner
    }

    pub async fn set_len(&self, size: u64) -> Result<()> {
        fs_inner_trampoline!(self, set_len, size)
    }

    pub async fn metadata(&self) -> Result<Metadata> {
        fs_inner_trampoline!(self, metadata).map(|inner| Metadata {
            inner,
            path: self.path.clone(),
        })
    }

    pub async fn set_permissions(&self, perm: std::fs::Permissions) -> Result<()> {
        fs_inner_trampoline!(self, set_permissions, perm)
    }

    pub async fn lock_shared(&self) -> Result<()> {
        let f = self.inner.try_clone().await?;
        let std_f = f.into_std().await;
        tokio::task::spawn_blocking(move || std_f.lock_shared())
            .await?
            .with_context(|| format!("lock_shared(\"{}\")", self.path.display()))
    }

    pub async fn lock_exclusive(&self) -> Result<()> {
        let f = self.inner.try_clone().await?;
        let std_f = f.into_std().await;
        tokio::task::spawn_blocking(move || std_f.lock_exclusive())
            .await?
            .with_context(|| format!("lock_exclusive(\"{}\")", self.path.display()))
    }

    pub async fn try_lock_shared(&self) -> Result<()> {
        let f = self.inner.try_clone().await?;
        let std_f = f.into_std().await;
        std_f
            .try_lock_shared()
            .with_context(|| format!("try_lock_shared(\"{}\")", self.path.display()))
    }

    pub async fn try_lock_exclusive(&self) -> Result<()> {
        let f = self.inner.try_clone().await?;
        let std_f = f.into_std().await;
        std_f
            .try_lock_exclusive()
            .with_context(|| format!("try_lock_exclusive(\"{}\")", self.path.display()))
    }

    pub async fn unlock(&self) -> Result<()> {
        let f = self.inner.try_clone().await?;
        let std_f = f.into_std().await;
        tokio::task::spawn_blocking(move || std_f.unlock())
            .await?
            .with_context(|| format!("unlock(\"{}\")", self.path.display()))
    }
}

impl AsyncRead for File<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        AsyncRead::poll_read(pin!(&mut me.inner), cx, dst)
    }
}

impl AsyncWrite for File<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        src: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        AsyncWrite::poll_write(pin!(&mut me.inner), cx, src)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let me = self.get_mut();
        AsyncWrite::poll_flush(pin!(&mut me.inner), cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let me = self.get_mut();
        AsyncWrite::poll_shutdown(pin!(&mut me.inner), cx)
    }
}

impl AsyncSeek for File<'_> {
    fn start_seek(self: Pin<&mut Self>, pos: io::SeekFrom) -> io::Result<()> {
        let me = self.get_mut();
        AsyncSeek::start_seek(pin!(&mut me.inner), pos)
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let me = self.get_mut();
        AsyncSeek::poll_complete(pin!(&mut me.inner), cx)
    }
}