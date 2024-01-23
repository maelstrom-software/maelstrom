use crate::fs;
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
        size: meta.size(),
        mode: Mode(meta.mode()),
        user: Identity::Id(meta.uid() as u64),
        group: Identity::Id(meta.gid() as u64),
        mtime: UnixTimestamp(meta.mtime()),
    }
}

pub struct ManifestBuilder<WriteT> {
    writer: ManifestWriter<WriteT>,
}

impl<WriteT: io::Write> ManifestBuilder<WriteT> {
    pub fn new(writer: WriteT) -> io::Result<Self> {
        Ok(Self {
            writer: ManifestWriter::new(writer)?,
        })
    }

    fn add_entry(
        &mut self,
        meta: &fs::Metadata,
        path: impl AsRef<Path>,
        data: ManifestEntryData,
    ) -> io::Result<()> {
        let entry = ManifestEntry {
            path: to_utf8_path(path),
            metadata: convert_metadata(meta),
            data,
        };
        self.writer.write_entry(&entry)?;
        Ok(())
    }
    pub fn add_file(
        &mut self,
        meta: &fs::Metadata,
        path: impl AsRef<Path>,
        data: Option<Sha256Digest>,
    ) -> io::Result<()> {
        self.add_entry(meta, path, ManifestEntryData::File(data))
    }

    pub fn add_directory(&mut self, meta: &fs::Metadata, path: impl AsRef<Path>) -> io::Result<()> {
        self.add_entry(meta, path, ManifestEntryData::Directory)
    }

    pub fn add_symlink(
        &mut self,
        meta: &fs::Metadata,
        path: impl AsRef<Path>,
        data: impl Into<Vec<u8>>,
    ) -> io::Result<()> {
        self.add_entry(meta, path, ManifestEntryData::Symlink(data.into()))
    }

    pub fn add_hardlink(
        &mut self,
        meta: &fs::Metadata,
        target: impl AsRef<Path>,
        source: impl AsRef<Path>,
    ) -> io::Result<()> {
        self.add_entry(
            meta,
            target,
            ManifestEntryData::Hardlink(to_utf8_path(source)),
        )
    }
}

#[test]
fn builder_file() {
    use maelstrom_base::manifest::ManifestReader;
    use maelstrom_test::*;

    let mut buffer = vec![];
    let mut builder = ManifestBuilder::new(&mut buffer).unwrap();

    let tmp_dir = tempfile::tempdir().unwrap();
    let fs = fs::Fs::new();
    let foo_path = tmp_dir.path().join("foo.txt");
    fs.write(&foo_path, b"foobar").unwrap();
    builder
        .add_file(
            &fs.metadata(&foo_path).unwrap(),
            "foo/bar.txt",
            Some(digest![42]),
        )
        .unwrap();

    let entries: Vec<_> = ManifestReader::new(io::Cursor::new(buffer))
        .unwrap()
        .map(|e| e.unwrap())
        .collect();
    assert_eq!(
        entries,
        vec![ManifestEntry {
            path: to_utf8_path("foo/bar.txt"),
            metadata: ManifestEntryMetadata {
                size: 6,
                ..convert_metadata(&fs.metadata(&foo_path).unwrap())
            },
            data: ManifestEntryData::File(Some(digest![42]))
        }]
    );
}
