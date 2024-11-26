use crate::async_fs::{self, Fs};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use maelstrom_base::{
    manifest::{
        ManifestEntry, ManifestEntryData, ManifestEntryMetadata, ManifestFileData, ManifestVersion,
        Mode, UnixTimestamp,
    },
    proto, Sha256Digest, Utf8PathBuf,
};
use serde::{de::DeserializeOwned, Serialize};
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;
use tokio::io::{
    AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _, AsyncWrite, AsyncWriteExt as _,
};

pub async fn decode_async<T: DeserializeOwned>(
    mut stream: impl AsyncRead + Unpin,
) -> io::Result<T> {
    let len = stream.read_u64().await?;
    let mut buffer = vec![0; len as usize];
    stream.read_exact(&mut buffer).await?;
    proto::deserialize(&buffer).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

pub async fn encode_async<T: Serialize>(
    mut stream: impl AsyncWrite + Unpin,
    t: &T,
) -> io::Result<()> {
    let mut buffer = vec![0; 8];
    proto::serialize_into(&mut buffer, t).unwrap();
    let len = buffer.len() as u64 - 8;
    std::io::Cursor::new(&mut buffer[..8])
        .write_u64(len)
        .await
        .unwrap();
    stream.write_all(&buffer).await?;
    Ok(())
}

pub fn decode<T: DeserializeOwned>(mut stream: impl io::Read) -> io::Result<T> {
    use byteorder::{BigEndian, ReadBytesExt as _};

    let len = stream.read_u64::<BigEndian>()?;
    let mut buffer = vec![0; len as usize];
    stream.read_exact(&mut buffer)?;
    proto::deserialize(&buffer).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

pub fn encode<T: Serialize>(mut stream: impl io::Write, t: &T) -> io::Result<()> {
    use byteorder::{BigEndian, WriteBytesExt};

    let mut buffer = vec![0; 8];
    proto::serialize_into(&mut buffer, t).unwrap();
    let len = buffer.len() as u64 - 8;
    WriteBytesExt::write_u64::<BigEndian>(&mut &mut buffer[..8], len).unwrap();
    stream.write_all(&buffer)?;
    Ok(())
}

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

        let version: ManifestVersion = decode(&mut r)?;
        if version != ManifestVersion::default() {
            return Err(io::Error::new(io::ErrorKind::Other, "bad manifest version"));
        }

        Ok(Self { r, stream_end })
    }

    fn next_inner(&mut self) -> io::Result<Option<ManifestEntry>> {
        if self.r.stream_position()? == self.stream_end {
            return Ok(None);
        }
        Ok(Some(decode(&mut self.r)?))
    }
}

impl<ReadT: io::Read + io::Seek> Iterator for ManifestReader<ReadT> {
    type Item = io::Result<ManifestEntry>;

    fn next(&mut self) -> Option<io::Result<ManifestEntry>> {
        self.next_inner().transpose()
    }
}

pub struct AsyncManifestReader<ReadT>(tokio::io::Take<ReadT>);

impl<ReadT: AsyncRead + AsyncSeek + Unpin> AsyncManifestReader<ReadT> {
    pub async fn new(mut r: ReadT) -> io::Result<Self> {
        let stream_start = r.stream_position().await?;
        r.seek(io::SeekFrom::End(0)).await?;
        let stream_end = r.stream_position().await?;
        r.seek(io::SeekFrom::Start(stream_start)).await?;

        Self::new_with_size(r, stream_end).await
    }
}

impl<ReadT: AsyncRead + Unpin> AsyncManifestReader<ReadT> {
    pub async fn new_with_size(r: ReadT, size: u64) -> io::Result<Self> {
        let mut r = r.take(size);
        let version: ManifestVersion = decode_async(&mut r).await?;
        if version != ManifestVersion::default() {
            return Err(io::Error::new(io::ErrorKind::Other, "bad manifest version"));
        }

        Ok(Self(r))
    }

    pub async fn next(&mut self) -> io::Result<Option<ManifestEntry>> {
        if self.0.limit() == 0 {
            return Ok(None);
        }
        Ok(Some(decode_async(&mut self.0).await?))
    }
}

pub struct ManifestWriter<WriteT> {
    w: WriteT,
}

impl<WriteT: io::Write> ManifestWriter<WriteT> {
    pub fn new(mut w: WriteT) -> io::Result<Self> {
        encode(&mut w, &ManifestVersion::default())?;
        Ok(Self { w })
    }

    pub fn write_entry(&mut self, entry: &ManifestEntry) -> io::Result<()> {
        encode(&mut self.w, entry)?;
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
        encode_async(&mut w, &ManifestVersion::default()).await?;
        Ok(Self { w })
    }

    pub async fn write_entry(&mut self, entry: &ManifestEntry) -> io::Result<()> {
        encode_async(&mut self.w, entry).await?;
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
    inline_limit: u64,
}

impl<'cb, WriteT: AsyncWrite + Unpin> ManifestBuilder<'cb, WriteT> {
    pub async fn new(
        writer: WriteT,
        follow_symlinks: bool,
        data_upload: impl DataUpload + 'cb,
        inline_limit: u64,
    ) -> io::Result<Self> {
        Ok(Self {
            fs: Fs::new(),
            writer: AsyncManifestWriter::new(writer).await?,
            data_upload: Box::new(data_upload),
            follow_symlinks,
            inline_limit,
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
            let file_size = meta.size();
            let data = if file_size <= self.inline_limit {
                ManifestFileData::Inline(self.fs.read(source.as_ref()).await?)
            } else if file_size > 0 {
                ManifestFileData::Digest(self.data_upload.upload(source.as_ref()).await?)
            } else {
                ManifestFileData::Empty
            };
            self.add_entry(&meta, dest, ManifestEntryData::File(data))
                .await
        } else if meta.is_dir() {
            self.add_entry(&meta, dest, ManifestEntryData::Directory { opaque: false })
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
        let mut builder = ManifestBuilder::new(&mut buffer, follow_symlinks, TestDataUpload, 5)
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
            ManifestEntryData::File(ManifestFileData::Digest(42u64.into())),
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
            ManifestEntryData::Directory { opaque: false },
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
            ManifestEntryData::File(ManifestFileData::Digest(42u64.into())),
        )
        .await;
    }
}
