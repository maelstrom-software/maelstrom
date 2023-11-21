use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use futures::stream::TryStreamExt as _;
use indicatif::ProgressBar;
use oci_spec::image::{Descriptor, ImageConfiguration, ImageIndex, ImageManifest, Platform};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
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
    fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let c = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&c)?)
    }

    fn from_dir(path: impl AsRef<Path>) -> Option<Self> {
        ContainerImage::from_path(path.as_ref().join("config.json")).ok()
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

pub async fn download_image(
    name: &str,
    tag_or_digest: &str,
    layer_dir: impl AsRef<Path>,
    prog: ProgressBar,
) -> Result<ContainerImage> {
    let client = reqwest::Client::new();
    let token = get_token(&client, name).await?;

    let manifest_digest: String = if !tag_or_digest.starts_with("sha256:") {
        let index = get_image_index(&client, &token, name, tag_or_digest).await?;
        let manifest = find_manifest_for_platform(index.manifests().iter());
        manifest.digest().into()
    } else {
        tag_or_digest.into()
    };

    let image = get_image_manifest(&client, &token, name, &manifest_digest).await?;

    let config = get_image_config(&client, &token, name, image.config().digest()).await?;

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
    map: HashMap<String, HashMap<String, String>>,
}

impl LockedContainerImageTags {
    fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let c = std::fs::read_to_string(path)?;
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

trait DownloadImageFn:
    Fn(&str, &str, &Path, ProgressBar) -> Result<ContainerImage> + Send + Sync
{
}

impl<T> DownloadImageFn for T where
    T: Fn(&str, &str, &Path, ProgressBar) -> Result<ContainerImage> + Send + Sync
{
}

pub struct ContainerImageDepot {
    locked_tags: LockedContainerImageTags,
    cached_images: HashMap<String, ContainerImage>,
    cache_dir: PathBuf,
    download_image_fn: Box<dyn DownloadImageFn>,
}

impl ContainerImageDepot {
    pub fn new() -> Result<Self> {
        Self::new_with(
            directories::BaseDirs::new()
                .expect("failed to find cache dir")
                .cache_dir()
                .join("meticulous")
                .join("containers"),
            Box::new(|pkg, tag_or_digest, path, prog| {
                download_image_sync(pkg, tag_or_digest, path, prog)
            }),
        )
    }

    fn new_with(
        cache_dir: impl AsRef<Path>,
        download_image_fn: Box<dyn DownloadImageFn>,
    ) -> Result<Self> {
        let cache_dir = cache_dir.as_ref();

        if !cache_dir.exists() {
            std::fs::create_dir_all(cache_dir)?;
        }

        let locked_tags =
            LockedContainerImageTags::from_path(cache_dir.join("tags.lock")).unwrap_or_default();
        let mut cached_images = HashMap::new();
        for d in std::fs::read_dir(cache_dir)? {
            let d = d?;
            if let Some(container_image) = ContainerImage::from_dir(d.path()) {
                cached_images.insert(container_image.digest.clone(), container_image);
            }
        }
        Ok(Self {
            locked_tags,
            cached_images,
            cache_dir: cache_dir.to_owned(),
            download_image_fn,
        })
    }

    pub fn get_container_image(
        &mut self,
        name: &str,
        tag: &str,
        prog: ProgressBar,
    ) -> Result<ContainerImage> {
        let tag_or_digest: String = if let Some(digest) = self.locked_tags.get(name, tag) {
            if let Some(img) = self.cached_images.get(digest) {
                return Ok(img.clone());
            }
            digest.into()
        } else {
            tag.into()
        };

        let output_dir = self.cache_dir.join(format!("{name}-{tag}.in-progress"));
        if output_dir.exists() {
            std::fs::remove_dir_all(&output_dir)?;
        }
        std::fs::create_dir(&output_dir)?;

        let mut img = (self.download_image_fn)(name, &tag_or_digest, &output_dir, prog)?;
        let new_path = self.cache_dir.join(&img.digest);
        img.layers = img
            .layers
            .into_iter()
            .map(|l| new_path.join(l.file_name().unwrap()))
            .collect();
        std::fs::write(
            output_dir.join("config.json"),
            serde_json::to_vec(&img).unwrap(),
        )?;

        std::fs::remove_dir_all(&new_path).ok();
        std::fs::rename(output_dir, new_path)?;

        self.locked_tags
            .add(name.into(), tag.into(), img.digest.clone());
        std::fs::write(
            self.cache_dir.join("tags.lock"),
            toml::to_string_pretty(&self.locked_tags)
                .unwrap()
                .as_bytes(),
        )?;
        self.cached_images.insert(img.digest.clone(), img.clone());
        Ok(img)
    }
}

#[cfg(test)]
fn fake_container_download(
    name: &str,
    tag_or_digest: &str,
    _dir: &Path,
    _prog: ProgressBar,
) -> Result<ContainerImage> {
    assert_eq!(name, "foo");
    assert!(tag_or_digest == "latest" || tag_or_digest == "sha256:abcdef");
    Ok(ContainerImage {
        version: ContainerImageVersion::V0,
        name: "foo".into(),
        digest: "sha256:abcdef".into(),
        config: ImageConfiguration::default(),
        layers: vec![],
    })
}

#[cfg(test)]
fn sorted_dir_listing(path: impl AsRef<Path>) -> Vec<String> {
    let mut listing: Vec<String> = std::fs::read_dir(path)
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
    let temp_dir = tempfile::tempdir().unwrap();
    let fake_container_download_fn = Box::new(fake_container_download);

    let mut depot =
        ContainerImageDepot::new_with(temp_dir.path(), fake_container_download_fn).unwrap();
    depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();

    assert_eq!(
        sorted_dir_listing(temp_dir.path()),
        vec!["sha256:abcdef", "tags.lock"]
    );
}

#[test]
fn container_image_depot_download_then_reload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fake_container_download_fn = Box::new(fake_container_download);

    let mut depot =
        ContainerImageDepot::new_with(temp_dir.path(), fake_container_download_fn).unwrap();
    let img1 = depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();
    drop(depot);

    let panic_container_download_fn = Box::new(|_: &str, _: &str, _: &Path, _| panic!());
    let mut depot =
        ContainerImageDepot::new_with(temp_dir.path(), panic_container_download_fn).unwrap();
    let img2 = depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();

    assert_eq!(img1, img2);
}

#[test]
fn container_image_depot_redownload_corrupt() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fake_container_download_fn = Box::new(fake_container_download);

    let mut depot =
        ContainerImageDepot::new_with(temp_dir.path(), fake_container_download_fn.clone()).unwrap();
    depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();
    drop(depot);
    std::fs::remove_file(temp_dir.path().join("sha256:abcdef").join("config.json")).unwrap();

    let mut depot =
        ContainerImageDepot::new_with(temp_dir.path(), fake_container_download_fn).unwrap();
    depot
        .get_container_image("foo", "latest", ProgressBar::hidden())
        .unwrap();

    assert_eq!(
        sorted_dir_listing(temp_dir.path()),
        vec!["sha256:abcdef", "tags.lock"]
    );
}

#[tokio::main]
pub async fn download_image_sync(
    pkg: &str,
    tag_or_digest: &str,
    layer_dir: impl AsRef<Path>,
    prog: ProgressBar,
) -> Result<ContainerImage> {
    download_image(pkg, tag_or_digest, layer_dir, prog).await
}
