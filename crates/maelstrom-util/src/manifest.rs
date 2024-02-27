use crate::async_fs::{self, Fs};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use maelstrom_base::{
    manifest::{
        ManifestEntry, ManifestEntryData, ManifestEntryMetadata, ManifestVersion, Mode,
        UnixTimestamp,
    },
    proto, Sha256Digest, Utf8PathBuf,
};
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

pub struct ManifestReader<ReadT> {
    r: ReadT,
    stream_end: u64,
}

impl<ReadT: io::Read + io::Seek> ManifestReader<ReadT> {
    pub fn new(mut r: ReadT) -> io::Result<Self> {
        let stream_start = r.stream_position()?;
        r.seek(io::SeekFrom::End(0))?;
        let stream_end = r.stream_position()?;
        r.seek(io::SeekFrom::Start(stream_start))?;

        let version: ManifestVersion =
            proto::deserialize_from(&mut r).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if version != ManifestVersion::default() {
            return Err(io::Error::new(io::ErrorKind::Other, "bad manifest version"));
        }

        Ok(Self { r, stream_end })
    }

    fn next_inner(&mut self) -> io::Result<Option<ManifestEntry>> {
        if self.r.stream_position()? == self.stream_end {
            return Ok(None);
        }
        Ok(Some(
            proto::deserialize_from(&mut self.r)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        ))
    }
}

impl<ReadT: io::Read + io::Seek> Iterator for ManifestReader<ReadT> {
    type Item = io::Result<ManifestEntry>;

    fn next(&mut self) -> Option<io::Result<ManifestEntry>> {
        self.next_inner().transpose()
    }
}

pub struct ManifestWriter<WriteT> {
    w: WriteT,
}

impl<WriteT: io::Write> ManifestWriter<WriteT> {
    pub fn new(mut w: WriteT) -> io::Result<Self> {
        proto::serialize_into(&mut w, &ManifestVersion::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(Self { w })
    }

    pub fn write_entry(&mut self, entry: &ManifestEntry) -> io::Result<()> {
        proto::serialize_into(&mut self.w, entry)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn write_entries<'entry>(
        &mut self,
        entries: impl IntoIterator<Item = &'entry ManifestEntry>,
    ) -> io::Result<()> {
        for entry in entries {
            self.write_entry(entry)?
        }
        Ok(())
    }
}

pub struct AsyncManifestWriter<WriteT> {
    w: WriteT,
}

impl<WriteT: AsyncWrite + Unpin> AsyncManifestWriter<WriteT> {
    pub async fn new(mut w: WriteT) -> io::Result<Self> {
        let mut buf = vec![];
        proto::serialize_into(&mut buf, &ManifestVersion::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        w.write_all(&buf).await?;
        Ok(Self { w })
    }

    pub async fn write_entry(&mut self, entry: &ManifestEntry) -> io::Result<()> {
        let mut buf = vec![];
        proto::serialize_into(&mut buf, entry)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.w.write_all(&buf).await?;
        Ok(())
    }

    pub async fn write_entries<'entry>(
        &mut self,
        entries: impl IntoIterator<Item = &'entry ManifestEntry>,
    ) -> io::Result<()> {
        for entry in entries {
            self.write_entry(entry).await?
        }
        Ok(())
    }
}

fn to_utf8_path(path: impl AsRef<Path>) -> Utf8PathBuf {
    path.as_ref().to_owned().try_into().unwrap()
}

fn convert_metadata(meta: &async_fs::Metadata) -> ManifestEntryMetadata {
    ManifestEntryMetadata {
        size: meta.is_file().then(|| meta.size()).unwrap_or(0),
        mode: Mode(meta.mode()),
        mtime: UnixTimestamp(meta.mtime()),
    }
}

#[async_trait]
pub trait DataUpload: Send {
    async fn upload(&mut self, path: &Path) -> Result<Sha256Digest>;
}

pub struct ManifestBuilder<'cb, WriteT> {
    fs: Fs,
    writer: AsyncManifestWriter<WriteT>,
    follow_symlinks: bool,
    data_upload: Box<dyn DataUpload + 'cb>,
}

impl<'cb, WriteT: AsyncWrite + Unpin> ManifestBuilder<'cb, WriteT> {
    pub async fn new(
        writer: WriteT,
        follow_symlinks: bool,
        data_upload: impl DataUpload + 'cb,
    ) -> io::Result<Self> {
        Ok(Self {
            fs: Fs::new(),
            writer: AsyncManifestWriter::new(writer).await?,
            data_upload: Box::new(data_upload),
            follow_symlinks,
        })
    }

    async fn add_entry(
        &mut self,
        meta: &async_fs::Metadata,
        path: impl AsRef<Path>,
        data: ManifestEntryData,
    ) -> Result<()> {
        let entry = ManifestEntry {
            path: to_utf8_path(path),
            metadata: convert_metadata(meta),
            data,
        };
        self.writer.write_entry(&entry).await?;
        Ok(())
    }

    pub async fn add_file(
        &mut self,
        source: impl AsRef<Path>,
        dest: impl AsRef<Path>,
    ) -> Result<()> {
        let meta = if self.follow_symlinks {
            self.fs.metadata(source.as_ref()).await?
        } else {
            self.fs.symlink_metadata(source.as_ref()).await?
        };
        if meta.is_file() {
            let data = if meta.size() > 0 {
                Some(self.data_upload.upload(source.as_ref()).await?)
            } else {
                None
            };
            self.add_entry(&meta, dest, ManifestEntryData::File(data))
                .await
        } else if meta.is_dir() {
            self.add_entry(&meta, dest, ManifestEntryData::Directory)
                .await
        } else if meta.is_symlink() {
            let data = self.fs.read_link(source.as_ref()).await?;
            self.add_entry(
                &meta,
                dest,
                ManifestEntryData::Symlink(data.into_os_string().into_encoded_bytes()),
            )
            .await
        } else {
            Err(anyhow!("unknown file type {}", source.as_ref().display()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::path::PathBuf;
    use std::pin::Pin;
    use tempfile::{tempdir, TempDir};

    struct Fixture {
        fs: Fs,
        temp_dir: TempDir,
        input_path: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let temp_dir = tempdir().unwrap();
            Fixture {
                fs: Fs::new(),
                input_path: temp_dir.path().join("input_entry"),
                temp_dir,
            }
        }
    }

    struct TestDataUpload;

    #[async_trait]
    impl DataUpload for TestDataUpload {
        async fn upload(&mut self, _path: &Path) -> Result<Sha256Digest> {
            Ok(42u64.into())
        }
    }

    async fn assert_entry<BuildT>(
        build: BuildT,
        follow_symlinks: bool,
        expected_path: &str,
        expected_size: u64,
        data: ManifestEntryData,
    ) where
        BuildT: for<'a> FnOnce(
            &'a mut Fixture,
            &'a mut ManifestBuilder<&mut Vec<u8>>,
        ) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
    {
        let mut buffer = vec![];
        let mut builder = ManifestBuilder::new(&mut buffer, follow_symlinks, TestDataUpload)
            .await
            .unwrap();

        let mut fixture = Fixture::new();
        build(&mut fixture, &mut builder).await;

        let actual_entries: Vec<_> = ManifestReader::new(io::Cursor::new(buffer))
            .unwrap()
            .map(|e| e.unwrap())
            .collect();

        let input_meta = if follow_symlinks {
            fixture.fs.metadata(&fixture.input_path).await.unwrap()
        } else {
            fixture
                .fs
                .symlink_metadata(&fixture.input_path)
                .await
                .unwrap()
        };
        assert_eq!(
            actual_entries,
            vec![ManifestEntry {
                path: to_utf8_path(expected_path),
                metadata: ManifestEntryMetadata {
                    size: expected_size,
                    ..convert_metadata(&input_meta)
                },
                data,
            }]
        );
    }

    #[tokio::test]
    async fn builder_file() {
        assert_entry(
            |fixture, builder| {
                Box::pin(async {
                    fixture
                        .fs
                        .write(&fixture.input_path, b"foobar")
                        .await
                        .unwrap();
                    builder
                        .add_file(&fixture.input_path, "foo/bar.txt")
                        .await
                        .unwrap();
                })
            },
            false, /* follow_symlinks */
            "foo/bar.txt",
            6,
            ManifestEntryData::File(Some(42u64.into())),
        )
        .await;
    }

    #[tokio::test]
    async fn builder_directory() {
        assert_entry(
            |fixture, builder| {
                Box::pin(async {
                    fixture.fs.create_dir(&fixture.input_path).await.unwrap();
                    builder
                        .add_file(&fixture.input_path, "foo/bar")
                        .await
                        .unwrap();
                })
            },
            false, /* follow_symlinks */
            "foo/bar",
            0,
            ManifestEntryData::Directory,
        )
        .await;
    }

    #[tokio::test]
    async fn builder_symlink() {
        assert_entry(
            |fixture, builder| {
                Box::pin(async {
                    fixture
                        .fs
                        .symlink("../baz", &fixture.input_path)
                        .await
                        .unwrap();
                    builder
                        .add_file(&fixture.input_path, "foo/bar")
                        .await
                        .unwrap();
                })
            },
            false, /* follow_symlinks */
            "foo/bar",
            0,
            ManifestEntryData::Symlink(b"../baz".to_vec()),
        )
        .await;
    }

    #[tokio::test]
    async fn builder_follows_symlinks() {
        assert_entry(
            |fixture, builder| {
                Box::pin(async {
                    let real_file = fixture.temp_dir.path().join("real");
                    fixture.fs.write(&real_file, b"foobar").await.unwrap();
                    fixture
                        .fs
                        .symlink(&real_file, &fixture.input_path)
                        .await
                        .unwrap();
                    builder
                        .add_file(&fixture.input_path, "foo/bar.txt")
                        .await
                        .unwrap();
                })
            },
            true, /* follow_symlinks */
            "foo/bar.txt",
            6,
            ManifestEntryData::File(Some(42u64.into())),
        )
        .await;
    }
}
