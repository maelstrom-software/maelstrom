use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use futures::stream::TryStreamExt as _;
use oci_spec::image::{Descriptor, ImageConfiguration, ImageIndex, ImageManifest, Platform};
use serde::Deserialize;
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
    version: &str,
) -> Result<ImageIndex> {
    Ok(client
        .get(&format!(
            "https://registry-1.docker.io/v2/library/{pkg}/manifests/{version}"
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
    let mut d = GzipDecoder::new(
        tar_stream
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read()
            .compat(),
    );
    tokio::io::copy(&mut d, &mut out).await?;
    Ok(())
}

#[derive(Debug)]
pub struct ContainerImage {
    pub config: ImageConfiguration,
    pub layers: Vec<PathBuf>,
}

impl ContainerImage {
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
) -> task::JoinHandle<Result<()>> {
    task::spawn(async move {
        let mut file = tokio::fs::File::create(&path).await?;
        download_layer(&client, &token, &pkg, &layer_digest, &mut file).await?;
        Ok(())
    })
}

pub async fn download_image(
    pkg: &str,
    version: &str,
    layer_dir: impl AsRef<Path>,
) -> Result<ContainerImage> {
    let client = reqwest::Client::new();
    let token = get_token(&client, pkg).await?;

    let index = get_image_index(&client, &token, pkg, version).await?;
    let manifest = find_manifest_for_platform(index.manifests().iter());

    let image = get_image_manifest(&client, &token, pkg, manifest.digest()).await?;

    let config = get_image_config(&client, &token, pkg, image.config().digest()).await?;

    let mut task_handles = vec![];
    let mut layers = vec![];
    for (i, layer) in image.layers().iter().enumerate() {
        let path = layer_dir.as_ref().join(format!("layer_{i}.tar"));
        let handle = download_layer_on_task(
            client.clone(),
            layer.digest().clone(),
            pkg.to_owned(),
            token.clone(),
            path.clone(),
        );
        task_handles.push(handle);
        layers.push(path);
    }

    for handle in task_handles {
        handle.await??;
    }

    Ok(ContainerImage { config, layers })
}

#[tokio::main]
pub async fn download_image_sync(
    pkg: &str,
    version: &str,
    layer_dir: impl AsRef<Path>,
) -> Result<ContainerImage> {
    download_image(pkg, version, layer_dir).await
}
