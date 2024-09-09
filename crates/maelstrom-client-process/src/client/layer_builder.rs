mod shared_libraries;

use anyhow::{anyhow, Result};
use futures::StreamExt as _;
use itertools::Itertools as _;
use maelstrom_base::{
    manifest::{
        ManifestEntry, ManifestEntryData, ManifestEntryMetadata, ManifestFileData, Mode,
        UnixTimestamp,
    },
    ArtifactType, Sha256Digest, Utf8Path, Utf8PathBuf,
};
use maelstrom_client_base::{
    spec::{LayerSpec, PrefixOptions, SymlinkSpec},
    CacheDir, ProjectDir, MANIFEST_DIR, SO_LISTINGS_DIR, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR,
};
use maelstrom_util::{
    async_fs,
    manifest::{AsyncManifestWriter, DataUpload, ManifestBuilder},
    root::RootBuf,
};
use sha2::{Digest as _, Sha256};
use shared_libraries::get_shared_library_dependencies;
use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::pin;
use tokio::io::AsyncWriteExt as _;

/// Having some deterministic time-stamp for files we create in manifests is useful for testing and
/// potentially caching.
/// I picked this time arbitrarily 2024-1-11 11:11:11
const ARBITRARY_TIME: UnixTimestamp = UnixTimestamp(1705000271);

#[derive(Default)]
struct PathHasher {
    hasher: Sha256,
}

impl PathHasher {
    fn new() -> Self {
        Self::default()
    }

    fn hash_path(&mut self, path: &Utf8Path) {
        self.hasher.update(path.as_str().as_bytes());
    }

    fn finish(self) -> Sha256Digest {
        Sha256Digest::new(self.hasher.finalize().into())
    }
}

fn calculate_manifest_entry_path(
    path: &Utf8Path,
    root: &Path,
    prefix_options: &PrefixOptions,
) -> Result<Utf8PathBuf> {
    let mut path = path.to_owned();
    if prefix_options.canonicalize {
        let mut input = path.into_std_path_buf();
        if input.is_relative() {
            input = root.join(input);
        }
        path = Utf8PathBuf::try_from(input.canonicalize()?)?;
    }
    if let Some(prefix) = &prefix_options.strip_prefix {
        if let Ok(new_path) = path.strip_prefix(prefix) {
            path = new_path.to_owned();
        }
    }
    if let Some(prefix) = &prefix_options.prepend_prefix {
        if path.is_absolute() {
            path = prefix.join(path.strip_prefix("/").unwrap());
        } else {
            path = prefix.join(path);
        }
    }
    Ok(path)
}

fn expand_braces(expr: &str) -> Result<Vec<String>> {
    if expr.contains('{') {
        bracoxide::explode(expr).map_err(|e| anyhow!("{e}"))
    } else {
        Ok(vec![expr.to_owned()])
    }
}

pub struct LayerBuilder {
    cache_dir: RootBuf<CacheDir>,
    project_dir: RootBuf<ProjectDir>,
    inline_limit: u64,
}

impl LayerBuilder {
    pub fn new(
        cache_dir: RootBuf<CacheDir>,
        project_dir: RootBuf<ProjectDir>,
        inline_limit: u64,
    ) -> Self {
        Self {
            cache_dir,
            project_dir,
            inline_limit,
        }
    }

    fn build_manifest_path(&self, name: &impl fmt::Display) -> PathBuf {
        (**self.cache_dir)
            .join(MANIFEST_DIR)
            .join(format!("{name}.manifest"))
    }

    async fn temp_manifest_path(&self) -> Result<PathBuf> {
        let manifest_dir = (**self.cache_dir).join(MANIFEST_DIR);
        tokio::task::spawn_blocking(move || {
            let (_, p) = tempfile::Builder::new()
                .prefix(".tmp")
                .tempfile_in(manifest_dir)?
                .keep()?;
            Ok(p)
        })
        .await?
    }

    fn build_stub_manifest_path(&self, name: &impl fmt::Display) -> PathBuf {
        (**self.cache_dir)
            .join(STUB_MANIFEST_DIR)
            .join(format!("{name}.manifest"))
    }

    fn build_symlink_manifest_path(&self, name: &impl fmt::Display) -> PathBuf {
        (**self.cache_dir)
            .join(SYMLINK_MANIFEST_DIR)
            .join(format!("{name}.manifest"))
    }

    async fn build_manifest(
        &self,
        mut paths: impl futures::stream::Stream<Item = Result<impl AsRef<Path>>>,
        prefix_options: PrefixOptions,
        data_upload: impl DataUpload,
    ) -> Result<PathBuf> {
        let fs = async_fs::Fs::new();
        let tmp_file_path = self.temp_manifest_path().await?;
        let mut manifest_file = fs.create_file(&tmp_file_path).await?;
        let follow_symlinks = prefix_options.follow_symlinks;
        let mut builder = ManifestBuilder::new(
            &mut manifest_file,
            follow_symlinks,
            data_upload,
            self.inline_limit,
        )
        .await?;
        let mut path_hasher = PathHasher::new();
        let mut pinned_paths = pin!(paths);
        while let Some(maybe_path) = pinned_paths.next().await {
            let mut path = maybe_path?.as_ref().to_owned();
            let input_path_relative = path.is_relative();
            if input_path_relative {
                path = (**self.project_dir).join(path);
            }
            let utf8_path = Utf8Path::from_path(&path).ok_or_else(|| anyhow!("non-utf8 path"))?;
            path_hasher.hash_path(utf8_path);

            let entry_path = if input_path_relative {
                utf8_path.strip_prefix(&self.project_dir).unwrap()
            } else {
                utf8_path
            };
            let dest =
                calculate_manifest_entry_path(entry_path, &self.project_dir, &prefix_options)?;
            builder.add_file(utf8_path, dest).await?;
        }
        drop(builder);
        manifest_file.flush().await?;

        let manifest_path = self.build_manifest_path(&path_hasher.finish());
        fs.rename(tmp_file_path, &manifest_path).await?;
        Ok(manifest_path)
    }

    async fn build_stub_manifest(&self, stubs: Vec<String>) -> Result<PathBuf> {
        let fs = async_fs::Fs::new();
        let tmp_file_path = self.temp_manifest_path().await?;
        let mut file = fs.create_file(&tmp_file_path).await?;
        let mut writer = AsyncManifestWriter::new(&mut file).await?;
        let mut path_hasher = PathHasher::new();
        for maybe_stub in stubs.iter().map(|s| expand_braces(s)).flatten_ok() {
            let stub = Utf8PathBuf::from(maybe_stub?);
            path_hasher.hash_path(&stub);
            let is_dir = stub.as_str().ends_with('/');
            let data = if is_dir {
                ManifestEntryData::Directory { opaque: false }
            } else {
                ManifestEntryData::File(ManifestFileData::Empty)
            };
            let metadata = ManifestEntryMetadata {
                size: 0,
                mode: Mode(0o444 | if is_dir { 0o111 } else { 0 }),
                mtime: ARBITRARY_TIME,
            };
            let entry = ManifestEntry {
                path: stub,
                metadata,
                data,
            };
            writer.write_entry(&entry).await?;
        }
        file.flush().await?;

        let manifest_path = self.build_stub_manifest_path(&path_hasher.finish());
        fs.rename(tmp_file_path, &manifest_path).await?;
        Ok(manifest_path)
    }

    async fn build_symlink_manifest(&self, symlinks: Vec<SymlinkSpec>) -> Result<PathBuf> {
        let fs = async_fs::Fs::new();
        let tmp_file_path = self.temp_manifest_path().await?;
        let mut file = fs.create_file(&tmp_file_path).await?;
        let mut writer = AsyncManifestWriter::new(&mut file).await?;
        let mut path_hasher = PathHasher::new();
        for SymlinkSpec { link, target } in symlinks {
            path_hasher.hash_path(&link);
            path_hasher.hash_path(&target);
            let data = ManifestEntryData::Symlink(target.into_string().into_bytes());
            let metadata = ManifestEntryMetadata {
                size: 0,
                mode: Mode(0o444),
                mtime: ARBITRARY_TIME,
            };
            let entry = ManifestEntry {
                path: link,
                metadata,
                data,
            };
            writer.write_entry(&entry).await?;
        }
        file.flush().await?;

        let manifest_path = self.build_symlink_manifest_path(&path_hasher.finish());
        fs.rename(tmp_file_path, &manifest_path).await?;
        Ok(manifest_path)
    }

    pub async fn build_layer(
        &self,
        layer: LayerSpec,
        mut data_upload: impl DataUpload,
    ) -> Result<(PathBuf, ArtifactType)> {
        Ok(match layer {
            LayerSpec::Tar { path } => (path.into_std_path_buf(), ArtifactType::Tar),
            LayerSpec::Paths {
                paths,
                prefix_options,
            } => {
                let manifest_path = self
                    .build_manifest(
                        futures::stream::iter(paths.iter().map(Ok)),
                        prefix_options,
                        data_upload,
                    )
                    .await?;
                (manifest_path, ArtifactType::Manifest)
            }
            LayerSpec::Glob {
                glob,
                prefix_options,
            } => {
                let mut glob_builder = globset::GlobSet::builder();
                glob_builder.add(globset::Glob::new(&glob)?);
                let fs = async_fs::Fs::new();
                let project_dir = self.project_dir.clone();
                let manifest_path = self
                    .build_manifest(
                        fs.glob_walk(&self.project_dir, &glob_builder.build()?)
                            .as_stream()
                            .map(|p| p.map(|p| p.strip_prefix(&project_dir).unwrap().to_owned())),
                        prefix_options,
                        data_upload,
                    )
                    .await?;
                (manifest_path, ArtifactType::Manifest)
            }
            LayerSpec::Stubs { stubs } => {
                let manifest_path = self.build_stub_manifest(stubs).await?;
                (manifest_path, ArtifactType::Manifest)
            }
            LayerSpec::Symlinks { symlinks } => {
                let manifest_path = self.build_symlink_manifest(symlinks).await?;
                (manifest_path, ArtifactType::Manifest)
            }
            LayerSpec::SharedLibraryDependencies {
                binary_paths,
                prefix_options,
            } => {
                let paths = get_shared_library_dependencies(
                    &self.project_dir,
                    &(**self.cache_dir).join(SO_LISTINGS_DIR),
                    &mut data_upload,
                    &binary_paths,
                )
                .await?;
                let manifest_path = self
                    .build_manifest(
                        futures::stream::iter(paths.iter().map(Ok)),
                        prefix_options,
                        data_upload,
                    )
                    .await?;
                (manifest_path, ArtifactType::Manifest)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use maelstrom_test::utf8_path_buf;
    use maelstrom_util::manifest::AsyncManifestReader;
    use maplit::hashmap;
    use std::collections::HashMap;
    use std::os::unix::fs::MetadataExt as _;
    use tempfile::tempdir;

    fn hash_data(data: &[u8]) -> Sha256Digest {
        let mut hasher = Sha256::new();
        hasher.update(data);
        Sha256Digest::new(hasher.finalize().into())
    }

    struct TestUploader {
        artifact_dir: PathBuf,
    }

    impl TestUploader {
        fn new(artifact_dir: PathBuf) -> Self {
            Self { artifact_dir }
        }
    }

    #[async_trait]
    impl<'a> DataUpload for &'a TestUploader {
        async fn upload(&mut self, path: &Path) -> Result<Sha256Digest> {
            let fs = async_fs::Fs::new();
            let data = fs.read(&path).await?;
            let digest = hash_data(&data);
            fs.copy(path, self.artifact_dir.join(digest.to_string()))
                .await?;
            Ok(digest)
        }
    }

    #[derive(Debug)]
    struct ExpectedManifestEntry {
        pub path: Utf8PathBuf,
        pub mode: Mode,
        pub data: ManifestEntryData,
    }

    impl ExpectedManifestEntry {
        fn new(path: &str, mode: u32, data: ManifestEntryData) -> Self {
            Self {
                path: Utf8PathBuf::from(path),
                mode: Mode(mode),
                data,
            }
        }
    }

    async fn verify_manifest(manifest_path: &Path, expected: Vec<ExpectedManifestEntry>) {
        let fs = async_fs::Fs::new();
        let mut entry_iter = AsyncManifestReader::new(fs.open_file(manifest_path).await.unwrap())
            .await
            .unwrap();
        let mut actual = vec![];
        while let Some(entry) = entry_iter.next().await.unwrap() {
            actual.push(entry);
        }
        assert_eq!(
            actual.len(),
            expected.len(),
            "expected = {expected:?}, actual = {actual:?}"
        );

        let actual_map: HashMap<&Utf8Path, &ManifestEntry> =
            actual.iter().map(|e| (e.path.as_path(), e)).collect();
        for ExpectedManifestEntry { path, mode, data } in expected {
            let actual_data = actual_map
                .get(path.as_path())
                .unwrap_or_else(|| panic!("{path:?} not found in {actual_map:?}"));
            assert_eq!(mode, actual_data.metadata.mode);
            assert_eq!(&data, &actual_data.data);
        }
    }

    async fn verify_single_entry_manifest(
        manifest_path: &Path,
        expected_entry_path: &Path,
        expected_entry_data: ManifestEntryData,
    ) {
        let fs = async_fs::Fs::new();
        let mut entry_iter = AsyncManifestReader::new(fs.open_file(manifest_path).await.unwrap())
            .await
            .unwrap();
        let mut entries = vec![];
        while let Some(entry) = entry_iter.next().await.unwrap() {
            entries.push(entry)
        }
        assert_eq!(entries.len(), 1, "{entries:?}");
        let entry = &entries[0];

        assert_eq!(expected_entry_data, entry.data);
        assert_eq!(entry.path, expected_entry_path);
    }

    async fn verify_empty_manifest(manifest_path: &Path) {
        let fs = async_fs::Fs::new();
        let mut entry_iter = AsyncManifestReader::new(fs.open_file(manifest_path).await.unwrap())
            .await
            .unwrap();
        let mut entries = vec![];
        while let Some(entry) = entry_iter.next().await.unwrap() {
            entries.push(entry);
        }
        assert_eq!(entries, vec![]);
    }

    struct Fixture {
        _temp_dir: tempfile::TempDir,
        builder: LayerBuilder,
        uploader: TestUploader,
        artifact_dir: PathBuf,
        fs: async_fs::Fs,
    }

    impl Fixture {
        async fn new() -> Self {
            let temp_dir = tempdir().unwrap();
            let artifact_dir = temp_dir.path().join("artifacts");
            let cache_dir = temp_dir.path().join("cache");
            let fs = async_fs::Fs::new();
            fs.create_dir_all(&artifact_dir).await.unwrap();
            fs.create_dir_all(&cache_dir).await.unwrap();

            for sub_dir in [MANIFEST_DIR, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR] {
                fs.create_dir_all(cache_dir.join(sub_dir)).await.unwrap();
            }

            let uploader = TestUploader::new(artifact_dir.clone());
            let builder = LayerBuilder::new(
                RootBuf::<CacheDir>::new(cache_dir),
                RootBuf::<ProjectDir>::new(artifact_dir.clone()),
                5,
            );
            Self {
                _temp_dir: temp_dir,
                builder,
                uploader,
                artifact_dir,
                fs,
            }
        }

        async fn build_layer(&self, layer: LayerSpec) -> PathBuf {
            let is_tar = matches!(layer, LayerSpec::Tar { .. });

            let (artifact_path, artifact_type) = self
                .builder
                .build_layer(layer, &self.uploader)
                .await
                .unwrap();
            if is_tar {
                assert_eq!(artifact_type, ArtifactType::Tar);
            } else {
                assert_eq!(artifact_type, ArtifactType::Manifest);
            }

            artifact_path
        }

        async fn hash_file(&self, path: &Path) -> Sha256Digest {
            let data = self.fs.read(path).await.unwrap();
            hash_data(&data)
        }
    }

    #[tokio::test]
    async fn paths_layer() {
        let fix = Fixture::new().await;
        let test_artifact = fix.artifact_dir.join("test_artifact");
        fix.fs.write(&test_artifact, b"hello world").await.unwrap();

        let manifest = fix
            .build_layer(LayerSpec::Paths {
                paths: vec![test_artifact.try_into().unwrap()],
                prefix_options: Default::default(),
            })
            .await;
        verify_single_entry_manifest(
            &manifest,
            &fix.artifact_dir.join("test_artifact"),
            ManifestEntryData::File(ManifestFileData::Digest(hash_data(b"hello world"))),
        )
        .await;
    }

    #[tokio::test]
    async fn paths_layer_inline_data() {
        let fix = Fixture::new().await;
        let test_artifact = fix.artifact_dir.join("test_artifact");
        fix.fs.write(&test_artifact, b"hi").await.unwrap();

        let manifest = fix
            .build_layer(LayerSpec::Paths {
                paths: vec![test_artifact.try_into().unwrap()],
                prefix_options: Default::default(),
            })
            .await;
        verify_single_entry_manifest(
            &manifest,
            &fix.artifact_dir.join("test_artifact"),
            ManifestEntryData::File(ManifestFileData::Inline(b"hi".to_vec())),
        )
        .await;
    }

    async fn paths_and_prefix_options_test(
        input_path_factory: impl FnOnce(&Path) -> PathBuf,
        prefix_options_factory: impl FnOnce(&Path) -> PrefixOptions,
        expected_path_factory: impl FnOnce(&Path) -> PathBuf,
    ) {
        let fix = Fixture::new().await;
        let input_path = input_path_factory(&fix.artifact_dir);
        let mut artifact_path = input_path.clone();
        if artifact_path.is_relative() {
            artifact_path = fix.artifact_dir.join(artifact_path);
        }
        fix.fs
            .create_dir_all(artifact_path.parent().unwrap())
            .await
            .unwrap();
        fix.fs.write(&artifact_path, b"hello world").await.unwrap();

        let manifest = fix
            .build_layer(LayerSpec::Paths {
                paths: vec![input_path.try_into().unwrap()],
                prefix_options: prefix_options_factory(&fix.artifact_dir),
            })
            .await;
        verify_single_entry_manifest(
            &manifest,
            &expected_path_factory(&fix.artifact_dir),
            ManifestEntryData::File(ManifestFileData::Digest(hash_data(b"hello world"))),
        )
        .await;
    }

    #[tokio::test]
    async fn paths_prefix_strip_and_prepend_absolute() {
        paths_and_prefix_options_test(
            |artifact_dir| artifact_dir.join("test_artifact"),
            |artifact_dir| PrefixOptions {
                strip_prefix: Some(artifact_dir.to_owned().try_into().unwrap()),
                prepend_prefix: Some("foo/".into()),
                ..Default::default()
            },
            |_| Path::new("foo/test_artifact").to_owned(),
        )
        .await;
    }

    #[tokio::test]
    async fn paths_prefix_strip_and_prepend_relative() {
        paths_and_prefix_options_test(
            |_| Path::new("bar/test_artifact").to_owned(),
            |_| PrefixOptions {
                strip_prefix: Some("bar".into()),
                prepend_prefix: Some("foo/".into()),
                ..Default::default()
            },
            |_| Path::new("foo/test_artifact").to_owned(),
        )
        .await;
    }

    #[tokio::test]
    async fn paths_prefix_strip_not_found_absolute() {
        paths_and_prefix_options_test(
            |artifact_dir| artifact_dir.join("test_artifact"),
            |_| PrefixOptions {
                strip_prefix: Some("not_there/".into()),
                ..Default::default()
            },
            |artifact_dir| artifact_dir.join("test_artifact"),
        )
        .await;
    }

    #[tokio::test]
    async fn paths_prefix_strip_not_found_relative() {
        paths_and_prefix_options_test(
            |_| Path::new("test_artifact").to_owned(),
            |_| PrefixOptions {
                strip_prefix: Some("not_there/".into()),
                ..Default::default()
            },
            |_| Path::new("test_artifact").to_owned(),
        )
        .await;
    }

    #[tokio::test]
    async fn paths_prefix_prepend_absolute() {
        paths_and_prefix_options_test(
            |artifact_dir| artifact_dir.join("test_artifact"),
            |_| PrefixOptions {
                prepend_prefix: Some("foo/bar".into()),
                ..Default::default()
            },
            |artifact_dir| {
                Path::new("foo/bar")
                    .join(artifact_dir.strip_prefix("/").unwrap())
                    .join("test_artifact")
            },
        )
        .await;
    }

    #[tokio::test]
    async fn paths_prefix_prepend_relative() {
        paths_and_prefix_options_test(
            |_| Path::new("test_artifact").to_owned(),
            |_| PrefixOptions {
                prepend_prefix: Some("foo/bar".into()),
                ..Default::default()
            },
            |_| Path::new("foo/bar/test_artifact").to_owned(),
        )
        .await;
    }

    #[tokio::test]
    async fn paths_prefix_canonicalize_relative() {
        paths_and_prefix_options_test(
            |_| Path::new("test_artifact").to_owned(),
            |_| PrefixOptions {
                canonicalize: true,
                ..Default::default()
            },
            |artifact_dir| artifact_dir.join("test_artifact"),
        )
        .await;
    }

    #[tokio::test]
    async fn paths_prefix_canonicalize_absolute() {
        paths_and_prefix_options_test(
            |artifact_dir| artifact_dir.join("test_artifact"),
            |_| PrefixOptions {
                canonicalize: true,
                ..Default::default()
            },
            |artifact_dir| artifact_dir.join("test_artifact"),
        )
        .await
    }

    async fn glob_and_prefix_options_test(
        glob_factory: impl FnOnce(&Path) -> String,
        input_files: HashMap<&str, &str>,
        prefix_options_factory: impl FnOnce(&Path) -> PrefixOptions,
        expected_path: &Path,
    ) {
        let fix = Fixture::new().await;
        for (path, contents) in input_files {
            let artifact = fix.artifact_dir.join(path);
            fix.fs
                .create_dir_all(artifact.parent().unwrap())
                .await
                .unwrap();
            fix.fs.write(artifact, contents.as_bytes()).await.unwrap();
        }

        let manifest = fix
            .build_layer(LayerSpec::Glob {
                glob: glob_factory(&fix.artifact_dir),
                prefix_options: prefix_options_factory(&fix.artifact_dir),
            })
            .await;
        verify_single_entry_manifest(
            &manifest,
            expected_path,
            ManifestEntryData::File(ManifestFileData::Digest(hash_data(b"hello world"))),
        )
        .await;
    }

    #[tokio::test]
    async fn glob_basic_relative() {
        glob_and_prefix_options_test(
            |_| "*.txt".into(),
            hashmap! {
                "foo.txt" => "hello world",
                "bar.bin" => "hello world",
            },
            |_| PrefixOptions::default(),
            Path::new("foo.txt"),
        )
        .await;
    }

    #[tokio::test]
    async fn glob_strip_and_prepend_prefix_relative() {
        glob_and_prefix_options_test(
            |_| "*.txt".into(),
            hashmap! {
                "foo.txt" => "hello world",
                "bar.bin" => "hello world",
            },
            |artifact_dir| PrefixOptions {
                strip_prefix: Some(artifact_dir.to_owned().try_into().unwrap()),
                prepend_prefix: Some("foo/bar".into()),
                ..Default::default()
            },
            Path::new("foo/bar/foo.txt"),
        )
        .await;
    }

    #[tokio::test]
    async fn glob_strip_prefix_relative() {
        glob_and_prefix_options_test(
            |_| "*.txt".into(),
            hashmap! {
                "foo.txt" => "hello world",
                "bar.bin" => "hello world",
            },
            |artifact_dir| PrefixOptions {
                strip_prefix: Some(artifact_dir.to_owned().try_into().unwrap()),
                ..Default::default()
            },
            Path::new("foo.txt"),
        )
        .await;
    }

    #[tokio::test]
    async fn glob_prepend_prefix_relative() {
        glob_and_prefix_options_test(
            |_| "*.txt".into(),
            hashmap! {
                "foo.txt" => "hello world",
                "bar.bin" => "hello world",
            },
            |_| PrefixOptions {
                prepend_prefix: Some("foo/".into()),
                ..Default::default()
            },
            Path::new("foo/foo.txt"),
        )
        .await;
    }

    #[tokio::test]
    async fn glob_sub_dir_relative() {
        glob_and_prefix_options_test(
            |_| "foo/*".into(),
            hashmap! {
                "foo/bar.txt" => "hello world",
                "bar.bin" => "hello world",
            },
            |_| PrefixOptions::default(),
            Path::new("foo/bar.txt"),
        )
        .await;
    }

    #[tokio::test]
    async fn glob_no_files_relative() {
        let fix = Fixture::new().await;
        let manifest = fix
            .build_layer(LayerSpec::Glob {
                glob: "*.txt".into(),
                prefix_options: Default::default(),
            })
            .await;
        verify_empty_manifest(&manifest).await;
    }

    async fn stubs_test(path: &str, expected: Vec<ExpectedManifestEntry>) {
        let fix = Fixture::new().await;
        let manifest = fix
            .build_layer(LayerSpec::Stubs {
                stubs: vec![path.to_owned()],
            })
            .await;
        verify_manifest(&manifest, expected).await;
    }

    #[tokio::test]
    async fn stub_file_test() {
        stubs_test(
            "/foo",
            vec![ExpectedManifestEntry::new(
                "/foo",
                0o444,
                ManifestEntryData::File(ManifestFileData::Empty),
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn stub_expanded_file_test() {
        stubs_test(
            "/foo/{bar,baz}",
            vec![
                ExpectedManifestEntry::new(
                    "/foo/bar",
                    0o444,
                    ManifestEntryData::File(ManifestFileData::Empty),
                ),
                ExpectedManifestEntry::new(
                    "/foo/baz",
                    0o444,
                    ManifestEntryData::File(ManifestFileData::Empty),
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn stub_expanded_file_with_empty_test() {
        stubs_test(
            "/foo/{bar,baz,}",
            vec![
                ExpectedManifestEntry::new(
                    "/foo/bar",
                    0o444,
                    ManifestEntryData::File(ManifestFileData::Empty),
                ),
                ExpectedManifestEntry::new(
                    "/foo/baz",
                    0o444,
                    ManifestEntryData::File(ManifestFileData::Empty),
                ),
                ExpectedManifestEntry::new(
                    "/foo/",
                    0o555,
                    ManifestEntryData::Directory { opaque: false },
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn stub_dir_test() {
        stubs_test(
            "/foo/",
            vec![ExpectedManifestEntry::new(
                "/foo/",
                0o555,
                ManifestEntryData::Directory { opaque: false },
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn stub_expanded_dir_test() {
        stubs_test(
            "/foo/{bar,baz}/",
            vec![
                ExpectedManifestEntry::new(
                    "/foo/bar/",
                    0o555,
                    ManifestEntryData::Directory { opaque: false },
                ),
                ExpectedManifestEntry::new(
                    "/foo/baz/",
                    0o555,
                    ManifestEntryData::Directory { opaque: false },
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn symlink_test() {
        let fix = Fixture::new().await;
        let manifest = fix
            .build_layer(LayerSpec::Symlinks {
                symlinks: vec![SymlinkSpec {
                    link: utf8_path_buf!("/foo"),
                    target: utf8_path_buf!("/bar"),
                }],
            })
            .await;
        verify_manifest(
            &manifest,
            vec![ExpectedManifestEntry::new(
                "/foo",
                0o444,
                ManifestEntryData::Symlink(b"/bar".to_vec()),
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn shared_library_dependencies_test() {
        let fix = Fixture::new().await;

        let manifest_dir: &Utf8Path = env!("CARGO_MANIFEST_DIR").into();
        let so_test_files = manifest_dir.join("src/client/layer_builder/so_test_files");
        let test_binary = so_test_files.join("main");

        let liba_path = so_test_files.join("liba.so");
        let libb_path = so_test_files.join("libb.so");
        let ld_linux_path: &Utf8Path = "/lib64/ld-linux-x86-64.so.2".into();

        let liba_hash = fix.hash_file(liba_path.as_std_path()).await;
        let libb_hash = fix.hash_file(libb_path.as_std_path()).await;
        let ld_linux_hash = fix.hash_file(ld_linux_path.as_std_path()).await;

        let liba_mode = fix.fs.metadata(&liba_path).await.unwrap().mode();
        let libb_mode = fix.fs.metadata(&libb_path).await.unwrap().mode();
        let ld_linux_mode = fix.fs.metadata(&ld_linux_path).await.unwrap().mode();

        let manifest = fix
            .build_layer(LayerSpec::SharedLibraryDependencies {
                binary_paths: vec![test_binary.into()],
                prefix_options: PrefixOptions {
                    follow_symlinks: true,
                    ..Default::default()
                },
            })
            .await;
        verify_manifest(
            &manifest,
            vec![
                ExpectedManifestEntry::new(
                    liba_path.as_str(),
                    liba_mode,
                    ManifestEntryData::File(ManifestFileData::Digest(liba_hash)),
                ),
                ExpectedManifestEntry::new(
                    libb_path.as_str(),
                    libb_mode,
                    ManifestEntryData::File(ManifestFileData::Digest(libb_hash)),
                ),
                ExpectedManifestEntry::new(
                    ld_linux_path.as_str(),
                    ld_linux_mode,
                    ManifestEntryData::File(ManifestFileData::Digest(ld_linux_hash)),
                ),
            ],
        )
        .await;
    }
}
