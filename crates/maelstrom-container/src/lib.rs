pub use oci_spec::image::{Arch, Os};

use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use core::task::Poll;
use futures::stream::TryStreamExt as _;
use maelstrom_util::async_fs::{self as fs, Fs};
use oci_spec::image::{Descriptor, ImageIndex, ImageManifest, Platform};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::future::Future;
use std::pin::Pin;
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    io::{self, SeekFrom},
    path::{Path, PathBuf},
};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio::sync::Mutex;
use tokio::{io::AsyncWrite, task};
use tokio_util::compat::FuturesAsyncReadCompatExt as _;
use xdg::BaseDirectories;

#[derive(Serialize, Deserialize, PartialEq, Eq, Default, Debug, Clone)]
pub struct Config {
    pub user: Option<String>,
    pub exposed_ports: Vec<String>,
    pub env: Vec<String>,
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub volumes: Vec<String>,
    pub working_dir: Option<String>,
    pub labels: HashMap<String, String>,
    pub stop_signal: Option<String>,
}

impl From<oci_spec::image::Config> for Config {
    fn from(other: oci_spec::image::Config) -> Self {
        Self {
            user: other.user().clone(),
            exposed_ports: other.exposed_ports().clone().unwrap_or_default(),
            env: other.env().clone().unwrap_or_default(),
            entrypoint: other.entrypoint().clone().unwrap_or_default(),
            cmd: other.cmd().clone().unwrap_or_default(),
            volumes: other.volumes().clone().unwrap_or_default(),
            working_dir: other.working_dir().clone(),
            labels: other.labels().clone().unwrap_or_default(),
            stop_signal: other.stop_signal().clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Default, PartialEq, Serialize)]
pub struct RootFs {
    pub r#type: String,
    pub diff_ids: Vec<String>,
}

impl From<oci_spec::image::RootFs> for RootFs {
    fn from(other: oci_spec::image::RootFs) -> Self {
        Self {
            r#type: other.typ().clone(),
            diff_ids: other.diff_ids().clone(),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Default, Debug, Clone)]
pub struct ImageConfiguration {
    pub created: Option<String>,
    pub author: Option<String>,
    pub architecture: Arch,
    pub os: Os,
    pub os_version: Option<String>,
    pub os_features: Vec<String>,
    pub variant: Option<String>,
    pub config: Option<Config>,
    pub rootfs: RootFs,
}

impl From<oci_spec::image::ImageConfiguration> for ImageConfiguration {
    fn from(other: oci_spec::image::ImageConfiguration) -> Self {
        Self {
            created: other.created().clone(),
            author: other.author().clone(),
            architecture: other.architecture().clone(),
            os: other.os().clone(),
            os_version: other.os_version().clone(),
            os_features: other.os_features().clone().unwrap_or_default(),
            variant: other.variant().clone(),
            config: other.config().clone().map(Into::into),
            rootfs: other.rootfs().clone().into(),
        }
    }
}

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
    let config: oci_spec::image::ImageConfiguration = client
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
        .await?;
    Ok(config.into())
}

pub trait ProgressTracker: Clone + Unpin + Send + 'static {
    fn set_length(&self, length: u64);
    fn inc(&self, v: u64);
}

#[derive(Debug, Copy, Clone)]
pub struct NullProgressTracker;

impl ProgressTracker for NullProgressTracker {
    fn set_length(&self, _length: u64) {}
    fn inc(&self, _v: u64) {}
}

impl ProgressTracker for indicatif::ProgressBar {
    fn set_length(&self, length: u64) {
        indicatif::ProgressBar::set_length(self, length)
    }

    fn inc(&self, v: u64) {
        indicatif::ProgressBar::inc(self, v)
    }
}

struct ProgressTrackerStream<ProgressT, ReadT> {
    prog: ProgressT,
    inner: ReadT,
}

impl<ProgressT, ReadT> ProgressTrackerStream<ProgressT, ReadT> {
    fn new(prog: ProgressT, inner: ReadT) -> Self {
        Self { prog, inner }
    }
}

impl<ProgressT: ProgressTracker, ReadT: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead
    for ProgressTrackerStream<ProgressT, ReadT>
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let prev_len = buf.filled().len() as u64;
        if let Poll::Ready(e) = Pin::new(&mut self.inner).poll_read(cx, buf) {
            self.prog.inc(buf.filled().len() as u64 - prev_len);
            Poll::Ready(e)
        } else {
            Poll::Pending
        }
    }
}

async fn download_layer(
    client: &reqwest::Client,
    token: &AuthToken,
    pkg: &str,
    digest: &str,
    prog: impl ProgressTracker,
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
    let mut d = GzipDecoder::new(tokio::io::BufReader::new(ProgressTrackerStream::new(
        prog,
        tar_stream
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read()
            .compat(),
    )));
    tokio::io::copy(&mut d, &mut out).await?;
    Ok(())
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u32)]
pub enum ContainerImageVersion {
    V0 = 0,
    #[default]
    V1 = 1,
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
    async fn from_path(fs: &Fs, path: impl AsRef<Path>) -> Result<Self> {
        let c = fs.read_to_string(path).await?;
        Ok(serde_json::from_str(&c)?)
    }

    async fn from_dir(fs: &Fs, path: impl AsRef<Path>) -> Option<Self> {
        let i = ContainerImage::from_path(fs, path.as_ref().join("config.json"))
            .await
            .ok()?;
        (i.version == ContainerImageVersion::default()).then_some(i)
    }

    pub fn env(&self) -> Option<&Vec<String>> {
        self.config.config.as_ref().map(|c| &c.env)
    }

    pub fn working_dir(&self) -> Option<&String> {
        self.config
            .config
            .as_ref()
            .and_then(|c| c.working_dir.as_ref())
    }
}

fn download_layer_on_task(
    client: reqwest::Client,
    layer_digest: String,
    pkg: String,
    token: AuthToken,
    path: PathBuf,
    prog: impl ProgressTracker,
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
    prog: impl ProgressTracker,
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
        version: ContainerImageVersion::default(),
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
    fn from_str(s: &str) -> Result<Self> {
        Ok(toml::from_str(s)?)
    }

    fn get(&self, name: &str, tag: &str) -> Option<&String> {
        let tags = self.map.get(name)?;
        tags.get(tag)
    }

    fn add(&mut self, name: String, tag: String, digest: String) {
        self.map.entry(name).or_default().insert(tag, digest);
    }
}

#[async_trait]
pub trait ContainerImageDepotOps {
    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<String>;
    async fn download_image(
        &self,
        name: &str,
        digest: &str,
        layer_dir: &Path,
        prog: impl ProgressTracker,
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

#[async_trait]
impl ContainerImageDepotOps for DefaultContainerImageDepotOps {
    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<String> {
        resolve_tag(&self.client, name, tag).await
    }

    async fn download_image(
        &self,
        name: &str,
        digest: &str,
        layer_dir: &Path,
        prog: impl ProgressTracker,
    ) -> Result<ContainerImage> {
        download_image(&self.client, name, digest, layer_dir, prog).await
    }
}

pub struct ContainerImageDepot<ContainerImageDepotOpsT = DefaultContainerImageDepotOps> {
    fs: Fs,
    cache_dir: PathBuf,
    project_dir: PathBuf,
    ops: ContainerImageDepotOpsT,
    cache: Mutex<HashMap<(String, String), ContainerImage>>,
}

impl ContainerImageDepot<DefaultContainerImageDepotOps> {
    pub fn new(project_dir: impl AsRef<Path>) -> Result<Self> {
        Self::new_with(
            project_dir,
            BaseDirectories::with_prefix("maelstrom/container")
                .expect("failed to find cache dir")
                .get_cache_file(""),
            DefaultContainerImageDepotOps::new(),
        )
    }
}

const TAG_FILE_NAME: &str = "maelstrom-container-tags.lock";

struct LockedTagsHandle<'fs> {
    locked_tags: LockedContainerImageTags,
    lock_file: fs::File<'fs>,
}

impl<'fs> LockedTagsHandle<'fs> {
    async fn write(mut self) -> Result<()> {
        self.lock_file.seek(SeekFrom::Start(0)).await?;
        self.lock_file.set_len(0).await?;
        self.lock_file
            .write_all(
                toml::to_string_pretty(&self.locked_tags)
                    .unwrap()
                    .as_bytes(),
            )
            .await?;
        self.lock_file.flush().await?;
        Ok(())
    }
}

impl<ContainerImageDepotOpsT: ContainerImageDepotOps> ContainerImageDepot<ContainerImageDepotOpsT> {
    fn new_with(
        project_dir: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
        ops: ContainerImageDepotOpsT,
    ) -> Result<Self> {
        let fs = Fs::new();
        let project_dir = project_dir.as_ref();
        let cache_dir = cache_dir.as_ref();

        Ok(Self {
            fs,
            project_dir: project_dir.to_owned(),
            cache_dir: cache_dir.to_owned(),
            cache: Default::default(),
            ops,
        })
    }

    async fn get_image_digest(
        &self,
        locked_tags: &mut LockedContainerImageTags,
        name: &str,
        tag: &str,
    ) -> Result<String> {
        Ok(if let Some(digest) = locked_tags.get(name, tag) {
            digest.into()
        } else {
            let digest = self.ops.resolve_tag(name, tag).await?;
            locked_tags.add(name.into(), tag.into(), digest.clone());
            digest
        })
    }

    async fn get_cached_image(&self, digest: &str) -> Option<ContainerImage> {
        ContainerImage::from_dir(&self.fs, self.cache_dir.join(digest)).await
    }

    async fn download_image(
        &self,
        name: &str,
        digest: &str,
        prog: impl ProgressTracker,
    ) -> Result<ContainerImage> {
        let output_dir = self.cache_dir.join(digest);
        if output_dir.exists() {
            self.fs.remove_dir_all(&output_dir).await?;
        }
        self.fs.create_dir(&output_dir).await?;

        let img = self
            .ops
            .download_image(name, digest, &output_dir, prog)
            .await?;
        self.fs
            .write(
                output_dir.join("config.json"),
                serde_json::to_vec(&img).unwrap(),
            )
            .await?;

        Ok(img)
    }

    async fn with_cache_lock<RetT>(
        &self,
        digest: &str,
        body: impl Future<Output = Result<RetT>>,
    ) -> Result<RetT> {
        let lock_file_path = self.cache_dir.join(format!(".{digest}.flock"));
        let lock_file = self.fs.create_file(lock_file_path).await?;
        lock_file.lock_exclusive().await?;
        body.await
    }

    async fn lock_tags(&self) -> Result<LockedTagsHandle<'_>> {
        let mut lock_file = self
            .fs
            .open_or_create_file(self.project_dir.join(TAG_FILE_NAME))
            .await?;
        lock_file.lock_exclusive().await?;

        let mut contents = String::new();
        lock_file.read_to_string(&mut contents).await?;
        let locked_tags = LockedContainerImageTags::from_str(&contents).unwrap_or_default();
        Ok(LockedTagsHandle {
            locked_tags,
            lock_file,
        })
    }

    pub async fn get_container_image(
        &self,
        name: &str,
        tag: &str,
        prog: impl ProgressTracker,
    ) -> Result<ContainerImage> {
        self.fs.create_dir_all(&self.cache_dir).await?;

        let cache_key = (name.into(), tag.into());
        if let Some(img) = self.cache.lock().await.get(&cache_key) {
            return Ok(img.clone());
        }

        let mut tags = self.lock_tags().await?;
        let digest = self
            .get_image_digest(&mut tags.locked_tags, name, tag)
            .await?;

        let img = self
            .with_cache_lock(&digest, async {
                Ok(if let Some(img) = self.get_cached_image(&digest).await {
                    img
                } else {
                    self.download_image(name, &digest, prog).await?
                })
            })
            .await?;
        tags.write().await?;

        self.cache.lock().await.insert(cache_key, img.clone());
        Ok(img)
    }
}

#[cfg(test)]
struct PanicContainerImageDepotOps;

#[cfg(test)]
#[async_trait]
impl ContainerImageDepotOps for PanicContainerImageDepotOps {
    async fn resolve_tag(&self, _name: &str, _tag: &str) -> Result<String> {
        panic!()
    }

    async fn download_image(
        &self,
        _name: &str,
        _digest: &str,
        _layer_dir: &Path,
        _prog: impl ProgressTracker,
    ) -> Result<ContainerImage> {
        panic!()
    }
}

#[cfg(test)]
#[derive(Clone)]
struct FakeContainerImageDepotOps(HashMap<String, String>);

#[cfg(test)]
#[async_trait]
impl ContainerImageDepotOps for FakeContainerImageDepotOps {
    async fn resolve_tag(&self, name: &str, tag: &str) -> Result<String> {
        Ok(self.0.get(&format!("{name}-{tag}")).unwrap().clone())
    }

    async fn download_image(
        &self,
        name: &str,
        digest: &str,
        _layer_dir: &Path,
        _prog: impl ProgressTracker,
    ) -> Result<ContainerImage> {
        Ok(ContainerImage {
            version: ContainerImageVersion::default(),
            name: name.into(),
            digest: digest.into(),
            config: ImageConfiguration::default(),
            layers: vec![],
        })
    }
}

#[cfg(test)]
async fn sorted_dir_listing(fs: &Fs, path: impl AsRef<Path>) -> Vec<String> {
    let mut listing = vec![];
    let mut read_dir = fs.read_dir(path).await.unwrap();
    while let Some(entry) = read_dir.next_entry().await.unwrap() {
        let name = entry
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        if !name.starts_with('.') {
            listing.push(name);
        }
    }
    listing.sort();
    listing
}

#[tokio::test]
async fn container_image_depot_download_dir_structure() {
    let fs = Fs::new();
    let project_dir = tempfile::tempdir().unwrap();
    let image_dir = tempfile::tempdir().unwrap();

    let depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("foo", "latest", NullProgressTracker)
        .await
        .unwrap();

    assert_eq!(
        fs.read_to_string(project_dir.path().join(TAG_FILE_NAME))
            .await
            .unwrap(),
        "\
            version = 0\n\
            \n\
            [foo]\n\
            latest = \"sha256:abcdef\"\n\
        "
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir.path()).await,
        vec!["sha256:abcdef"]
    );
}

#[tokio::test]
async fn container_image_depot_download_then_reload() {
    let project_dir = tempfile::tempdir().unwrap();
    let image_dir = tempfile::tempdir().unwrap();

    let depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    let img1 = depot
        .get_container_image("foo", "latest", NullProgressTracker)
        .await
        .unwrap();
    drop(depot);

    let depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        PanicContainerImageDepotOps,
    )
    .unwrap();
    let img2 = depot
        .get_container_image("foo", "latest", NullProgressTracker)
        .await
        .unwrap();

    assert_eq!(img1, img2);
}

#[tokio::test]
async fn container_image_depot_redownload_corrupt() {
    let fs = Fs::new();
    let project_dir = tempfile::tempdir().unwrap();
    let image_dir = tempfile::tempdir().unwrap();

    let depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("foo", "latest", NullProgressTracker)
        .await
        .unwrap();
    drop(depot);
    fs.remove_file(image_dir.path().join("sha256:abcdef").join("config.json"))
        .await
        .unwrap();

    let depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("foo", "latest", NullProgressTracker)
        .await
        .unwrap();

    assert_eq!(
        sorted_dir_listing(&fs, project_dir.path()).await,
        vec![TAG_FILE_NAME]
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir.path()).await,
        vec!["sha256:abcdef"]
    );
}

#[tokio::test]
async fn container_image_depot_update_image() {
    let fs = Fs::new();
    let project_dir = tempfile::tempdir().unwrap();
    let image_dir = tempfile::tempdir().unwrap();

    let depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
            "bar-latest".into() => "sha256:ghijk".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("foo", "latest", NullProgressTracker)
        .await
        .unwrap();
    depot
        .get_container_image("bar", "latest", NullProgressTracker)
        .await
        .unwrap();
    drop(depot);
    fs.remove_file(project_dir.path().join(TAG_FILE_NAME))
        .await
        .unwrap();
    let bar_meta_before = fs
        .metadata(image_dir.path().join("sha256:ghijk"))
        .await
        .unwrap();

    let depot = ContainerImageDepot::new_with(
        project_dir.path(),
        image_dir.path(),
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:lmnop".into(),
            "bar-latest".into() => "sha256:ghijk".into(),
        }),
    )
    .unwrap();
    #[allow(clippy::disallowed_names)]
    let foo = depot
        .get_container_image("foo", "latest", NullProgressTracker)
        .await
        .unwrap();
    depot
        .get_container_image("bar", "latest", NullProgressTracker)
        .await
        .unwrap();

    // ensure we get new foo
    assert_eq!(foo.digest, "sha256:lmnop");

    let bar_meta_after = fs
        .metadata(image_dir.path().join("sha256:ghijk"))
        .await
        .unwrap();

    // ensure we didn't re-download bar
    assert_eq!(
        bar_meta_before.modified().unwrap(),
        bar_meta_after.modified().unwrap()
    );

    assert_eq!(
        sorted_dir_listing(&fs, project_dir.path()).await,
        vec![TAG_FILE_NAME]
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir.path()).await,
        vec!["sha256:abcdef", "sha256:ghijk", "sha256:lmnop",]
    );
}

#[tokio::test]
async fn container_image_depot_update_image_but_nothing_to_do() {
    let fs = Fs::new();

    let project_dir = tempfile::tempdir().unwrap();
    let image_dir = tempfile::tempdir().unwrap();

    let ops = FakeContainerImageDepotOps(maplit::hashmap! {
        "foo-latest".into() => "sha256:abcdef".into(),
        "bar-latest".into() => "sha256:ghijk".into(),
    });
    let depot =
        ContainerImageDepot::new_with(project_dir.path(), image_dir.path(), ops.clone()).unwrap();
    depot
        .get_container_image("foo", "latest", NullProgressTracker)
        .await
        .unwrap();
    depot
        .get_container_image("bar", "latest", NullProgressTracker)
        .await
        .unwrap();
    drop(depot);
    fs.remove_file(project_dir.path().join(TAG_FILE_NAME))
        .await
        .unwrap();

    let depot = ContainerImageDepot::new_with(project_dir.path(), image_dir.path(), ops).unwrap();
    depot
        .get_container_image("foo", "latest", NullProgressTracker)
        .await
        .unwrap();
    depot
        .get_container_image("bar", "latest", NullProgressTracker)
        .await
        .unwrap();

    assert_eq!(
        fs.read_to_string(project_dir.path().join(TAG_FILE_NAME))
            .await
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
        sorted_dir_listing(&fs, image_dir.path()).await,
        vec!["sha256:abcdef", "sha256:ghijk"]
    );
}
