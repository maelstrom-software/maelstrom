use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use futures::stream::TryStreamExt as _;
use indicatif::ProgressBar;
use meticulous_util::fs::Fs;
use oci_spec::image::{Descriptor, ImageConfiguration, ImageIndex, ImageManifest, Platform};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWrite;
use tokio::task;
use tokio_util::compat::FuturesAsyncReadCompatExt as _;

#[derive(Deserialize, Debug, Clone)]
#[serde(transparent)]
struct AuthToken(String);

impl fmt::Display for AuthToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Deserialize, Debug)]
struct AuthResponse {
    token: AuthToken,
    #[allow(dead_code)]
    expires_in: u32,
    #[allow(dead_code)]
    issued_at: String,
}

async fn get_token(client: &reqwest::Client, pkg: &str) -> Result<AuthToken> {
    let url = format!(
        "https://auth.docker.io/\
        token?service=registry.docker.io&scope=repository:library/{pkg}:pull"
    );
    let res: AuthResponse = client.get(&url).send().await?.json().await?;
    Ok(res.token)
}

async fn get_image_index(
    client: &reqwest::Client,
    token: &AuthToken,
    pkg: &str,
    tag_or_digest: &str,
) -> Result<ImageIndex> {
    Ok(client
        .get(&format!(
            "https://registry-1.docker.io/v2/library/{pkg}/manifests/{tag_or_digest}"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.list.v2+json",
        )
        .send()
        .await?
        .json()
        .await?)
}

fn find_manifest_for_platform<'a>(
    mut manifests: impl Iterator<Item = &'a Descriptor>,
) -> &'a Descriptor {
    let current_platform = Platform::default();
    manifests
        .find(|des| {
            des.platform()
                .as_ref()
                .is_some_and(|p| p == &current_platform)
        })
        .unwrap()
}

async fn get_image_manifest(
    client: &reqwest::Client,
    token: &AuthToken,
    pkg: &str,
    manifest_digest: &str,
) -> Result<ImageManifest> {
    Ok(client
        .get(&format!(
            "https://registry-1.docker.io/v2/library/{pkg}/manifests/{manifest_digest}"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .send()
        .await?
        .json()
        .await?)
}

async fn get_image_config(
    client: &reqwest::Client,
    token: &AuthToken,
    pkg: &str,
    config_digest: &str,
) -> Result<ImageConfiguration> {
    Ok(client
        .get(&format!(
            "https://registry-1.docker.io/v2/library/{pkg}/blobs/{config_digest}"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header(
            "Accept",
            "sha256:a416a98b71e224a31ee99cff8e16063554498227d2b696152a9c3e0aa65e5824",
        )
        .send()
        .await?
        .json()
        .await?)
}

async fn download_layer(
    client: &reqwest::Client,
    token: &AuthToken,
    pkg: &str,
    digest: &str,
    prog: ProgressBar,
    mut out: impl AsyncWrite + Unpin,
) -> Result<()> {
    let tar_stream = client
        .get(&format!(
            "https://registry-1.docker.io/v2/library/{pkg}/blobs/{digest}"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?
        .error_for_status()?;
    let mut d = GzipDecoder::new(tokio::io::BufReader::new(
        prog.wrap_async_read(
            tar_stream
                .bytes_stream()
                .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                .into_async_read()
                .compat(),
        ),
    ));
    tokio::io::copy(&mut d, &mut out).await?;
    Ok(())
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u32)]
pub enum ContainerImageVersion {
    V0 = 0,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerImage {
    pub version: ContainerImageVersion,
    pub name: String,
    pub digest: String,
    pub config: ImageConfiguration,
    pub layers: Vec<PathBuf>,
}

impl ContainerImage {
    fn from_path(fs: &Fs, path: impl AsRef<Path>) -> Result<Self> {
        let c = fs.read_to_string(path)?;
        Ok(serde_json::from_str(&c)?)
    }

    fn from_dir(fs: &Fs, path: impl AsRef<Path>) -> Option<Self> {
        ContainerImage::from_path(fs, path.as_ref().join("config.json")).ok()
    }

    pub fn env(&self) -> Option<&Vec<String>> {
        self.config.config().as_ref().and_then(|c| c.env().as_ref())
    }
}

fn download_layer_on_task(
    client: reqwest::Client,
    layer_digest: String,
    pkg: String,
    token: AuthToken,
    path: PathBuf,
    prog: ProgressBar,
) -> task::JoinHandle<Result<()>> {
    task::spawn(async move {
        let mut file = tokio::fs::File::create(&path).await?;
        download_layer(&client, &token, &pkg, &layer_digest, prog, &mut file).await?;
        Ok(())
    })
}

pub async fn resolve_tag(client: &reqwest::Client, name: &str, tag: &str) -> Result<String> {
    let token = get_token(client, name).await?;

    let index = get_image_index(client, &token, name, tag).await?;
    let manifest = find_manifest_for_platform(index.manifests().iter());
    Ok(manifest.digest().clone())
}

pub async fn download_image(
    client: &reqwest::Client,
    name: &str,
    tag_or_digest: &str,
    layer_dir: impl AsRef<Path>,
    prog: ProgressBar,
) -> Result<ContainerImage> {
    let token = get_token(client, name).await?;

    let manifest_digest: String = if !tag_or_digest.starts_with("sha256:") {
        let index = get_image_index(client, &token, name, tag_or_digest).await?;
        let manifest = find_manifest_for_platform(index.manifests().iter());
        manifest.digest().into()
    } else {
        tag_or_digest.into()
    };

    let image = get_image_manifest(client, &token, name, &manifest_digest).await?;

    let config = get_image_config(client, &token, name, image.config().digest()).await?;

    let total_size: i64 = image.layers().iter().map(|l| l.size()).sum();
    prog.set_length(total_size as u64);

    let mut task_handles = vec![];
    let mut layers = vec![];
    for (i, layer) in image.layers().iter().enumerate() {
        let path = layer_dir.as_ref().join(format!("layer_{i}.tar"));
        let handle = download_layer_on_task(
            client.clone(),
            layer.digest().clone(),
            name.to_owned(),
            token.clone(),
            path.clone(),
            prog.clone(),
        );
        task_handles.push(handle);
        layers.push(path);
    }

    for handle in task_handles {
        handle.await??;
    }

    Ok(ContainerImage {
        version: ContainerImageVersion::V0,
        name: name.into(),
        digest: manifest_digest,
        config,
        layers,
    })
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u32)]
pub enum LockedContainerImageTagsVersion {
    #[default]
    V0 = 0,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
struct LockedContainerImageTags {
    version: LockedContainerImageTagsVersion,
    #[serde(flatten)]
    map: BTreeMap<String, HashMap<String, String>>,
}

impl LockedContainerImageTags {
    fn from_path(fs: &Fs, path: impl AsRef<Path>) -> Result<Self> {
        let c = fs.read_to_string(path)?;
        Ok(toml::from_str(&c)?)
    }

    fn get(&self, name: &str, tag: &str) -> Option<&String> {
        let tags = self.map.get(name)?;
        tags.get(tag)
    }

    fn add(&mut self, name: String, tag: String, digest: String) {
        self.map.entry(name).or_default().insert(tag, digest);
    }
}

pub trait ContainerImageDepotOps {
    fn resolve_tag(&self, name: &str, tag: &str) -> Result<String>;
    fn download_image(
        &self,
        name: &str,
        digest: &str,
        layer_dir: &Path,
        prog: ProgressBar,
    ) -> Result<ContainerImage>;
}

pub struct DefaultContainerImageDepotOps {
    client: reqwest::Client,
}

impl DefaultContainerImageDepotOps {
    fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl ContainerImageDepotOps for DefaultContainerImageDepotOps {
    fn resolve_tag(&self, name: &str, tag: &str) -> Result<String> {
        resolve_tag_sync(&self.client, name, tag)
    }

    fn download_image(
        &self,
        name: &str,
        digest: &str,
        layer_dir: &Path,
        prog: ProgressBar,
    ) -> Result<ContainerImage> {
        download_image_sync(&self.client, name, digest, layer_dir, prog)
    }
}

pub struct ContainerImageDepot<ContainerImageDepotOpsT = DefaultContainerImageDepotOps> {
    locked_tags: LockedContainerImageTags,
    cached_images: HashMap<String, ContainerImage>,
    cache_dir: PathBuf,
    project_dir: PathBuf,
    ops: ContainerImageDepotOpsT,
}

impl ContainerImageDepot<DefaultContainerImageDepotOps> {
    pub fn new(project_dir: impl AsRef<Path>) -> Result<Self> {
        Self::new_with(
            project_dir,
            directories::BaseDirs::new()
                .expect("failed to find cache dir")
                .cache_dir()
                .join("meticulous")
                .join("containers"),
            DefaultContainerImageDepotOps::new(),
        )
    }
}

const TAG_FILE_NAME: &str = "meticulous-container-tags.lock";

impl<ContainerImageDepotOpsT: ContainerImageDepotOps> ContainerImageDepot<ContainerImageDepotOpsT> {
    fn new_with(
        project_dir: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
        ops: ContainerImageDepotOpsT,
    ) -> Result<Self> {
        let fs = Fs::new();
        let project_dir = project_dir.as_ref();
        let cache_dir = cache_dir.as_ref();

        if !cache_dir.exists() {
            fs.create_dir_all(cache_dir)?;
        }

        let locked_tags = LockedContainerImageTags::from_path(&fs, project_dir.join(TAG_FILE_NAME))
            .unwrap_or_default();
        let mut cached_images = HashMap::new();
        for d in fs.read_dir(cache_dir)? {
            let d = d?;
            if let Some(container_image) = ContainerImage::from_dir(&fs, d.path()) {
                cached_images.insert(container_image.digest.clone(), container_image);
            }
        }
        Ok(Self {
            locked_tags,
            cached_images,
            project_dir: project_dir.to_owned(),
            cache_dir: cache_dir.to_owned(),
            ops,
        })
    }

    fn write_lock_file(&self) -> Result<()> {
        let fs = Fs::new();
        fs.write(
            self.project_dir.join(TAG_FILE_NAME),
            toml::to_string_pretty(&self.locked_tags)
                .unwrap()
                .as_bytes(),
        )?;
        Ok(())
    }

    pub fn get_container_image(
        &mut self,
        name: &str,
        tag: &str,
        prog: ProgressBar,
    ) -> Result<ContainerImage> {
        let fs = Fs::new();
        let digest = if let Some(digest) = self.locked_tags.get(name, tag) {
            digest.into()
        } else {
            let digest = self.ops.resolve_tag(name, tag)?;
            self.locked_tags
                .add(name.into(), tag.into(), digest.clone());
            digest
        };
        if let Some(img) = self.cached_images.get(&digest) {
            self.write_lock_file()?;
            return Ok(img.clone());
        }

        let output_dir = self.cache_dir.join(&digest);
        if output_dir.exists() {
            fs.remove_dir_all(&output_dir)?;
        }
        fs.create_dir(&output_dir)?;

        let img = self.ops.download_image(name, &digest, &output_dir, prog)?;
        fs.write(
            output_dir.join("config.json"),
            serde_json::to_vec(&img).unwrap(),
        )?;

        self.write_lock_file()?;
        self.cached_images.insert(img.digest.clone(), img.clone());
        Ok(img)
    }
}

#[cfg(test)]
struct PanicContainerImageDepotOps;

#[cfg(test)]
impl ContainerImageDepotOps for PanicContainerImageDepotOps {
    fn resolve_tag(&self, _name: &str, _tag: &str) -> Result<String> {
        panic!()
    }

    fn download_image(
        &self,
        _name: &str,
        _digest: &str,
        _layer_dir: &Path,
        _prog: ProgressBar,
    ) -> Result<ContainerImage> {
        panic!()
    }
}

#[cfg(test)]
#[derive(Clone)]
struct FakeContainerImageDepotOps(HashMap<String, String>);

#[cfg(test)]
impl ContainerImageDepotOps for FakeContainerImageDepotOps {
    fn resolve_tag(&self, name: &str, tag: &str) -> Result<String> {
        Ok(self.0.get(&format!("{name}-{tag}")).unwrap().clone())
    }

    fn download_image(
        &self,
        name: &str,
        digest: &str,
        _layer_dir: &Path,
        _prog: ProgressBar,
    ) -> Result<ContainerImage> {
        Ok(ContainerImage {
            version: ContainerImageVersion::V0,
            name: name.into(),
            digest: digest.into(),
            config: ImageConfiguration::default(),
            layers: vec![],
        })
    }
}

#[cfg(test)]
fn sorted_dir_listing(fs: &Fs, path: impl AsRef<Path>) -> Vec<String> {
    let mut listing: Vec<String> = fs
        .read_dir(path)
        .unwrap()
        .map(|d| {
            d.unwrap()
                .path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .into()
        })
        .collect();
    listing.sort();
    listing
}

#[test]
fn container_image_depot_download_dir_structure() {
    let fs = Fs::new();
    let project_dir = tempfile::tempdir().unwrap();
    let image_dir = tempfile::tempdir().unwrap();

    let mut depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();

    assert_eq!(
        fs.read_to_string(project_dir.path().join(TAG_FILE_NAME))
            .unwrap(),
        "\
            version = 0\n\
            \n\
            [foo]\n\
            latest = \"sha256:abcdef\"\n\
        "
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir.path()),
        vec!["sha256:abcdef"]
    );
}

#[test]
fn container_image_depot_download_then_reload() {
    let project_dir = tempfile::tempdir().unwrap();
    let image_dir = tempfile::tempdir().unwrap();

    let mut depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    let img1 = depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();
    drop(depot);

    let mut depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        PanicContainerImageDepotOps,
    )
    .unwrap();
    let img2 = depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();

    assert_eq!(img1, img2);
}

#[test]
fn container_image_depot_redownload_corrupt() {
    let fs = Fs::new();
    let project_dir = tempfile::tempdir().unwrap();
    let image_dir = tempfile::tempdir().unwrap();

    let mut depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();
    drop(depot);
    fs.remove_file(image_dir.path().join("sha256:abcdef").join("config.json"))
        .unwrap();

    let mut depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();

    assert_eq!(
        sorted_dir_listing(&fs, project_dir.path()),
        vec![TAG_FILE_NAME]
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir.path()),
        vec!["sha256:abcdef"]
    );
}

#[test]
fn container_image_depot_update_image() {
    let fs = Fs::new();
    let project_dir = tempfile::tempdir().unwrap();
    let image_dir = tempfile::tempdir().unwrap();

    let mut depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
            "bar-latest".into() => "sha256:ghijk".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();
    depot
        .get_container_image("bar", "latest", ProgressBar::hidden())
        .unwrap();
    drop(depot);
    fs.remove_file(project_dir.path().join(TAG_FILE_NAME))
        .unwrap();
    let bar_meta_before = fs.metadata(image_dir.path().join("sha256:ghijk")).unwrap();

    let mut depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:lmnop".into(),
            "bar-latest".into() => "sha256:ghijk".into(),
        }),
    )
    .unwrap();
    let foo = depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();
    depot
        .get_container_image("bar", "latest", ProgressBar::hidden())
        .unwrap();

    // ensure we get new foo
    assert_eq!(foo.digest, "sha256:lmnop");

    let bar_meta_after = fs.metadata(image_dir.path().join("sha256:ghijk")).unwrap();

    // ensure we didn't re-download bar
    assert_eq!(
        bar_meta_before.modified().unwrap(),
        bar_meta_after.modified().unwrap()
    );

    assert_eq!(
        sorted_dir_listing(&fs, project_dir.path()),
        vec![TAG_FILE_NAME]
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir.path()),
        vec!["sha256:abcdef", "sha256:ghijk", "sha256:lmnop",]
    );
}

#[test]
fn container_image_depot_update_image_but_nothing_to_do() {
    let fs = Fs::new();

    let project_dir = tempfile::tempdir().unwrap();
    let image_dir = tempfile::tempdir().unwrap();

    let ops = FakeContainerImageDepotOps(maplit::hashmap! {
        "foo-latest".into() => "sha256:abcdef".into(),
        "bar-latest".into() => "sha256:ghijk".into(),
    });
    let mut depot =
        ContainerImageDepot::new_with(project_dir.path(), image_dir.path(), ops.clone()).unwrap();
    depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();
    depot
        .get_container_image("bar", "latest", ProgressBar::hidden())
        .unwrap();
    drop(depot);
    fs.remove_file(project_dir.path().join(TAG_FILE_NAME))
        .unwrap();

    let mut depot =
        ContainerImageDepot::new_with(project_dir.path(), image_dir.path(), ops).unwrap();
    depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();
    depot
        .get_container_image("bar", "latest", ProgressBar::hidden())
        .unwrap();

    assert_eq!(
        fs.read_to_string(project_dir.path().join(TAG_FILE_NAME))
            .unwrap(),
        "\
            version = 0\n\
            \n\
            [bar]\n\
            latest = \"sha256:ghijk\"\n\
            \n\
            [foo]\n\
            latest = \"sha256:abcdef\"\n\
        "
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir.path()),
        vec!["sha256:abcdef", "sha256:ghijk"]
    );
}

#[tokio::main]
pub async fn resolve_tag_sync(client: &reqwest::Client, name: &str, tag: &str) -> Result<String> {
    resolve_tag(client, name, tag).await
}

#[tokio::main]
pub async fn download_image_sync(
    client: &reqwest::Client,
    name: &str,
    tag_or_digest: &str,
    layer_dir: impl AsRef<Path>,
    prog: ProgressBar,
) -> Result<ContainerImage> {
    download_image(client, name, tag_or_digest, layer_dir, prog).await
}
