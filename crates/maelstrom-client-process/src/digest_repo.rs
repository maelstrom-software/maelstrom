use anyhow::Result;
use chrono::{DateTime, Utc};
use maelstrom_base::Sha256Digest;
use maelstrom_util::async_fs::Fs;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_with::{serde_as, DisplayFromStr};
use std::{
    collections::HashMap,
    io::SeekFrom,
    path::{Path, PathBuf},
    time::SystemTime,
};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u32)]
enum DigestRepositoryVersion {
    #[default]
    V0 = 0,
}

#[serde_as]
#[derive(Deserialize, Serialize)]
struct DigestRepositoryEntry {
    #[serde_as(as = "DisplayFromStr")]
    digest: Sha256Digest,
    mtime: DateTime<Utc>,
}

#[derive(Default, Deserialize, Serialize)]
struct DigestRepositoryContents {
    version: DigestRepositoryVersion,
    digests: HashMap<PathBuf, DigestRepositoryEntry>,
}

impl DigestRepositoryContents {
    fn from_str(s: &str) -> Result<Self> {
        Ok(toml::from_str(s)?)
    }

    fn to_pretty_string(&self) -> String {
        toml::to_string_pretty(self).unwrap()
    }
}

const CACHED_IMAGE_FILE_NAME: &str = "cached-digests.toml";

pub struct DigestRepository {
    fs: Fs,
    path: PathBuf,
    cache: Option<DigestRepositoryContents>,
}

impl DigestRepository {
    pub fn new(path: &Path) -> Self {
        Self {
            fs: Fs::new(),
            path: path.into(),
            cache: None,
        }
    }

    pub async fn add(
        &mut self,
        path: PathBuf,
        mtime: SystemTime,
        digest: Sha256Digest,
    ) -> Result<()> {
        self.fs.create_dir_all(&self.path).await?;
        let mut file = self
            .fs
            .open_or_create_file(self.path.join(CACHED_IMAGE_FILE_NAME))
            .await?;
        file.lock_exclusive().await?;

        let mut contents = String::new();
        file.read_to_string(&mut contents).await?;
        let mut digests = DigestRepositoryContents::from_str(&contents).unwrap_or_default();
        digests.digests.insert(
            path,
            DigestRepositoryEntry {
                mtime: mtime.into(),
                digest,
            },
        );

        file.seek(SeekFrom::Start(0)).await?;
        file.set_len(0).await?;
        file.write_all(digests.to_pretty_string().as_bytes())
            .await?;

        self.cache = Some(digests);

        Ok(())
    }

    pub async fn get(&mut self, path: &PathBuf) -> Result<Option<Sha256Digest>> {
        if self.cache.is_none() {
            let Some(contents) = self
                .fs
                .read_to_string_if_exists(self.path.join(CACHED_IMAGE_FILE_NAME))
                .await?
            else {
                return Ok(None);
            };
            self.cache = Some(DigestRepositoryContents::from_str(&contents).unwrap_or_default());
        }

        let Some(entry) = self.cache.as_ref().unwrap().digests.get(path) else {
            return Ok(None);
        };
        let current_mtime: DateTime<Utc> = self.fs.metadata(path).await?.modified()?.into();
        Ok((current_mtime == entry.mtime).then_some(entry.digest.clone()))
    }
}

#[tokio::test]
async fn digest_repository_simple_add_get() {
    let fs = Fs::new();
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut repo = DigestRepository::new(tmp_dir.path());

    let foo_path = tmp_dir.path().join("foo.tar");
    fs.write(&foo_path, "foo").await.unwrap();
    let (mtime, digest) = crate::calculate_digest(&foo_path).await.unwrap();
    repo.add(foo_path.clone(), mtime, digest.clone())
        .await
        .unwrap();

    assert_eq!(repo.get(&foo_path).await.unwrap(), Some(digest));
}

#[tokio::test]
async fn digest_repository_simple_add_get_after_modify() {
    let fs = Fs::new();
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut repo = DigestRepository::new(tmp_dir.path());

    let foo_path = tmp_dir.path().join("foo.tar");
    fs.write(&foo_path, "foo").await.unwrap();
    let (mtime, digest) = crate::calculate_digest(&foo_path).await.unwrap();
    repo.add(foo_path.clone(), mtime, digest.clone())
        .await
        .unwrap();

    // apparently depending on the file-system mtime can have up to a 10ms granularity
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    fs.write(&foo_path, "bar").await.unwrap();

    assert_eq!(repo.get(&foo_path).await.unwrap(), None);
}
