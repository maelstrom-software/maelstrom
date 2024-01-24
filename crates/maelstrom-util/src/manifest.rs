use crate::fs::{self, Fs};
use anyhow::{anyhow, Result};
use maelstrom_base::{
    manifest::{
        Identity, ManifestEntry, ManifestEntryData, ManifestEntryMetadata, ManifestWriter, Mode,
        UnixTimestamp,
    },
    Sha256Digest, Utf8PathBuf,
};
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;

fn to_utf8_path(path: impl AsRef<Path>) -> Utf8PathBuf {
    path.as_ref().to_owned().try_into().unwrap()
}

fn convert_metadata(meta: &fs::Metadata) -> ManifestEntryMetadata {
    ManifestEntryMetadata {
        size: meta.is_file().then(|| meta.size()).unwrap_or(0),
        mode: Mode(meta.mode()),
        user: Identity::Id(meta.uid() as u64),
        group: Identity::Id(meta.gid() as u64),
        mtime: UnixTimestamp(meta.mtime()),
    }
}

type DataUploadCb<'cb> = Box<dyn FnMut(&Path) -> Result<Sha256Digest> + 'cb>;

pub struct ManifestBuilder<'cb, WriteT> {
    fs: Fs,
    writer: ManifestWriter<WriteT>,
    follow_symlinks: bool,
    data_upload: DataUploadCb<'cb>,
}

impl<'cb, WriteT: io::Write> ManifestBuilder<'cb, WriteT> {
    pub fn new(
        writer: WriteT,
        follow_symlinks: bool,
        data_upload: impl FnMut(&Path) -> Result<Sha256Digest> + 'cb,
    ) -> io::Result<Self> {
        Ok(Self {
            fs: Fs::new(),
            writer: ManifestWriter::new(writer)?,
            data_upload: Box::new(data_upload),
            follow_symlinks,
        })
    }

    fn add_entry(
        &mut self,
        meta: &fs::Metadata,
        path: impl AsRef<Path>,
        data: ManifestEntryData,
    ) -> Result<()> {
        let entry = ManifestEntry {
            path: to_utf8_path(path),
            metadata: convert_metadata(meta),
            data,
        };
        self.writer.write_entry(&entry)?;
        Ok(())
    }

    pub fn add_file(&mut self, source: impl AsRef<Path>, dest: impl AsRef<Path>) -> Result<()> {
        let meta = if self.follow_symlinks {
            self.fs.metadata(source.as_ref())?
        } else {
            self.fs.symlink_metadata(source.as_ref())?
        };
        if meta.is_file() {
            let data = (meta.size() > 0)
                .then(|| (self.data_upload)(source.as_ref()))
                .transpose()?;
            self.add_entry(&meta, dest, ManifestEntryData::File(data))
        } else if meta.is_dir() {
            self.add_entry(&meta, dest, ManifestEntryData::Directory)
        } else if meta.is_symlink() {
            let data = self.fs.read_link(source.as_ref())?;
            self.add_entry(
                &meta,
                dest,
                ManifestEntryData::Symlink(data.into_os_string().into_encoded_bytes()),
            )
        } else {
            Err(anyhow!("unknown file type {}", source.as_ref().display()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::Fs;
    use maelstrom_base::manifest::ManifestReader;
    use maelstrom_test::*;
    use std::path::PathBuf;
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

    fn assert_entry(
        build: impl FnOnce(&mut Fixture, &mut ManifestBuilder<&mut Vec<u8>>),
        follow_symlinks: bool,
        expected_path: &str,
        expected_size: u64,
        data: ManifestEntryData,
    ) {
        let mut buffer = vec![];
        let mut builder =
            ManifestBuilder::new(&mut buffer, follow_symlinks, |_| Ok(digest![42])).unwrap();

        let mut fixture = Fixture::new();
        build(&mut fixture, &mut builder);

        let actual_entries: Vec<_> = ManifestReader::new(io::Cursor::new(buffer))
            .unwrap()
            .map(|e| e.unwrap())
            .collect();
        let input_meta = if follow_symlinks {
            fixture.fs.metadata(&fixture.input_path).unwrap()
        } else {
            fixture.fs.symlink_metadata(&fixture.input_path).unwrap()
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

    #[test]
    fn builder_file() {
        assert_entry(
            |fixture, builder| {
                fixture.fs.write(&fixture.input_path, b"foobar").unwrap();
                builder
                    .add_file(&fixture.input_path, "foo/bar.txt")
                    .unwrap();
            },
            false, /* follow_symlinks */
            "foo/bar.txt",
            6,
            ManifestEntryData::File(Some(digest![42])),
        );
    }

    #[test]
    fn builder_directory() {
        assert_entry(
            |fixture, builder| {
                fixture.fs.create_dir(&fixture.input_path).unwrap();
                builder.add_file(&fixture.input_path, "foo/bar").unwrap();
            },
            false, /* follow_symlinks */
            "foo/bar",
            0,
            ManifestEntryData::Directory,
        );
    }

    #[test]
    fn builder_symlink() {
        assert_entry(
            |fixture, builder| {
                fixture.fs.symlink("../baz", &fixture.input_path).unwrap();
                builder.add_file(&fixture.input_path, "foo/bar").unwrap();
            },
            false, /* follow_symlinks */
            "foo/bar",
            0,
            ManifestEntryData::Symlink(b"../baz".to_vec()),
        );
    }

    #[test]
    fn builder_follows_symlinks() {
        assert_entry(
            |fixture, builder| {
                let real_file = fixture.temp_dir.path().join("real");
                fixture.fs.write(&real_file, b"foobar").unwrap();
                fixture.fs.symlink(&real_file, &fixture.input_path).unwrap();
                builder
                    .add_file(&fixture.input_path, "foo/bar.txt")
                    .unwrap();
            },
            true, /* follow_symlinks */
            "foo/bar.txt",
            6,
            ManifestEntryData::File(Some(digest![42])),
        );
    }
}
