pub mod image_name;
pub mod local_registry;

pub use image_name::{DockerReference, ImageName};
pub use oci_spec::{
    distribution::ErrorResponse,
    image::{Arch, Os},
};

use anyhow::{anyhow, bail, Result};
use anyhow_trace::anyhow_trace;
use async_compression::tokio::bufread::GzipDecoder;
use combine::{
    between, many, many1,
    parser::char::{spaces, string},
    satisfy, sep_by, token, Parser, Stream,
};
use futures::stream::TryStreamExt as _;
use maelstrom_util::{
    async_fs::{self as fs, Fs},
    io::Sha256Stream,
    root::{Root, RootBuf},
};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive as _;
use oci_spec::image::{Descriptor, ImageIndex, ImageManifest, Platform};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    future::Future,
    io::{self, SeekFrom},
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::Poll,
};
use tokio::{
    io::AsyncWrite,
    io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _},
    sync::{Mutex, MutexGuard},
    task,
};
use tokio_util::compat::FuturesAsyncReadCompatExt as _;

#[macro_export]
macro_rules! parse_str {
    ($ty:ty, $input:expr) => {{
        use combine::{EasyParser as _, Parser as _};
        <$ty>::parser()
            .skip(combine::eof())
            .easy_parse(combine::stream::position::Stream::new($input))
            .map(|x| x.0)
    }};
}

struct DigestDir;
struct ContainerConfigFile;
struct ContainerTagFile;
pub struct ContainerImageDepotDir;
pub struct ProjectDir;

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
}

pub trait ProgressTracker: Unpin + Send + 'static {
    fn set_length(&self, length: u64);
    fn inc(&self, v: u64);
}

#[derive(Debug, Copy, Clone)]
pub struct NullProgressTracker;

impl ProgressTracker for NullProgressTracker {
    fn set_length(&self, _length: u64) {}
    fn inc(&self, _v: u64) {}
}

impl<T: ProgressTracker + Sync> ProgressTracker for std::sync::Arc<T> {
    fn set_length(&self, length: u64) {
        <T as ProgressTracker>::set_length(self, length)
    }

    fn inc(&self, v: u64) {
        <T as ProgressTracker>::inc(self, v)
    }
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
    #[anyhow_trace]
    async fn from_path(fs: &Fs, path: impl AsRef<Root<ContainerConfigFile>>) -> Result<Self> {
        let c = fs.read_to_string(path.as_ref()).await?;
        let value = serde_json::from_str(&c)?;
        Ok(value)
    }

    async fn from_dir(fs: &Fs, path: impl AsRef<Root<DigestDir>>) -> Option<Self> {
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

async fn get_json_error(response: reqwest::Response) -> Option<String> {
    let errors: ErrorResponse = response.json().await.ok()?;
    let mut s = String::new();
    for d in errors.detail() {
        if !s.is_empty() {
            s += "; "
        }
        s += &format!("{:?}", d.code());
        if let Some(message) = d.message() {
            s += "; ";
            s += message;
        }
        if let Some(detail) = d.detail() {
            s += "; ";
            s += detail;
        }
    }
    Some(s)
}

#[anyhow_trace]
async fn check_for_error(name: &str, response: reqwest::Response) -> Result<reqwest::Response> {
    let status_code = response.status();
    if !status_code.is_success() {
        let message =
            format!("container repository error: {status_code}; are you sure {name:?} exists?");

        if response.status().is_client_error() {
            if let Some(api_errors) = get_json_error(response).await {
                bail!("{message}; {api_errors}");
            }
        }

        bail!("{message}");
    }
    Ok(response)
}

#[anyhow_trace]
async fn decode_and_check_for_error<T: DeserializeOwned>(
    name: &str,
    response: reqwest::Response,
) -> Result<T> {
    let response = check_for_error(name, response).await?;
    let v = response.json().await?;
    Ok(v)
}

fn find_manifest_for_platform(manifests: &[Descriptor]) -> Result<&Descriptor> {
    if manifests.is_empty() {
        bail!("empty image index");
    }
    let current_platform = Platform::default();
    if let Some(manifest) = manifests.iter().find(|des| {
        des.platform()
            .as_ref()
            .is_some_and(|p| p == &current_platform)
    }) {
        return Ok(manifest);
    }
    if manifests.len() != 1 {
        bail!("no manifest found for the current platform");
    }

    Ok(&manifests[0])
}

#[derive(Debug, PartialEq, Eq)]
struct WwwAuthenticate {
    realm: Option<String>,
    service: Option<String>,
    scopes: Vec<String>,
}

// This is similar to as described from RFC-6750 section 3.
impl WwwAuthenticate {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        let keyword = || many1(satisfy(|c| c != '='));
        let quoted = || between(token('"'), token('"'), many(satisfy(|c| c != '"')));
        string("Bearer ")
            .with(sep_by(
                (keyword().skip(token('=')), quoted()),
                (token(','), spaces()),
            ))
            .map(|kw_pairs: Vec<(String, String)>| {
                let mut values: HashMap<_, _> = kw_pairs.into_iter().collect();
                Self {
                    realm: values.remove("realm"),
                    service: values.remove("service"),
                    scopes: values
                        .remove("scope")
                        .map(|s| s.split(' ').map(|s| s.to_owned()).collect())
                        .unwrap_or_default(),
                }
            })
    }

    fn url(&self) -> Result<String> {
        let realm = self
            .realm
            .as_ref()
            .ok_or_else(|| anyhow!("missing realm from www-authenticate"))?;
        let mut url_params = vec![];

        if let Some(service) = &self.service {
            url_params.push(format!("service={service}"));
        }
        for scope in &self.scopes {
            url_params.push(format!("scope={scope}"));
        }

        let mut url = realm.clone();
        if !url_params.is_empty() {
            url += "?";
            url += &url_params.join("&");
        }
        Ok(url)
    }
}

#[test]
fn parse_www_authenticate_full() {
    let s =
        "Bearer realm=\"https://public.ecr.aws/token/\",service=\"public.ecr.aws\",scope=\"aws\"";
    assert_eq!(
        parse_str!(WwwAuthenticate, s).unwrap(),
        WwwAuthenticate {
            realm: Some("https://public.ecr.aws/token/".into()),
            service: Some("public.ecr.aws".into()),
            scopes: vec!["aws".into()],
        }
    );
}

#[test]
fn parse_www_authenticate_multiple_scopes() {
    let s = "Bearer realm=\"https://public.ecr.aws/token/\",scope=\"aws bws cws\"";
    assert_eq!(
        parse_str!(WwwAuthenticate, s).unwrap(),
        WwwAuthenticate {
            realm: Some("https://public.ecr.aws/token/".into()),
            service: None,
            scopes: vec!["aws".into(), "bws".into(), "cws".into()],
        }
    );
}

#[test]
fn www_authenticate_url() {
    let w = WwwAuthenticate {
        realm: Some("https://public.ecr.aws/token/".into()),
        service: Some("public.ecr.aws".into()),
        scopes: vec!["aws".into()],
    };
    assert_eq!(
        w.url().unwrap(),
        "https://public.ecr.aws/token/?service=public.ecr.aws&scope=aws"
    );

    let w = WwwAuthenticate {
        realm: Some("https://public.ecr.aws/token/".into()),
        service: Some("public.ecr.aws".into()),
        scopes: vec!["aws".into(), "bws".into()],
    };
    assert_eq!(
        w.url().unwrap(),
        "https://public.ecr.aws/token/?service=public.ecr.aws&scope=aws&scope=bws"
    );

    let w = WwwAuthenticate {
        realm: Some("https://public.ecr.aws/token/".into()),
        service: Some("public.ecr.aws".into()),
        scopes: vec![],
    };
    assert_eq!(
        w.url().unwrap(),
        "https://public.ecr.aws/token/?service=public.ecr.aws"
    );

    let w = WwwAuthenticate {
        realm: Some("https://public.ecr.aws/token/".into()),
        service: None,
        scopes: vec![],
    };
    assert_eq!(w.url().unwrap(), "https://public.ecr.aws/token/");

    let w = WwwAuthenticate {
        realm: None,
        service: None,
        scopes: vec![],
    };
    w.url().unwrap_err();
}

#[test]
fn parse_www_authenticate_partial() {
    let s = "Bearer realm=\"https://public.ecr.aws/token/\"";
    assert_eq!(
        parse_str!(WwwAuthenticate, s).unwrap(),
        WwwAuthenticate {
            realm: Some("https://public.ecr.aws/token/".into()),
            service: None,
            scopes: vec![]
        }
    );
}

impl FromStr for WwwAuthenticate {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        parse_str!(Self, s).map_err(|e| anyhow!("{e}: invalid www-authenticate: {s:?}"))
    }
}

pub struct ImageDownloader {
    client: reqwest::Client,
    token: Option<AuthToken>,
}

impl ImageDownloader {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            token: None,
        }
    }

    // See <https://distribution.github.io/distribution/spec/auth/token/> about how this works.
    #[anyhow_trace]
    async fn get_token(&mut self, www_authenticate: &str) -> Result<()> {
        let auth: WwwAuthenticate = www_authenticate.parse()?;
        let auth_url = auth.url()?;

        let req = self.client.get(&auth_url);
        let resp: AuthResponse = decode_and_check_for_error(&auth_url, req.send().await?).await?;
        self.token = Some(resp.token);
        Ok(())
    }

    #[anyhow_trace]
    async fn get_image_index_inner(&self, ref_: &DockerReference) -> Result<reqwest::Response> {
        let name = ref_.name();
        let base_url = ref_.host.base_url();
        let digest_or_tag = ref_.digest_or_tag();
        let mut req = self
            .client
            .get(format!("{base_url}/{name}/manifests/{digest_or_tag}"))
            .header(
                "Accept",
                "application/vnd.docker.distribution.manifest.list.v2+json",
            )
            .header("Accept", "application/vnd.oci.image.index.v1+json");
        if let Some(token) = &self.token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        let response = req.send().await?;
        Ok(response)
    }

    #[anyhow_trace]
    async fn get_image_index(&mut self, ref_: &DockerReference) -> Result<ImageIndex> {
        let mut response = self.get_image_index_inner(ref_).await?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            let www_authenticate = response
                .headers()
                .get("www-authenticate")
                .ok_or_else(|| anyhow!("UNAUTHORIZED with no www-authenticate header"))?
                .to_str()?;
            self.get_token(www_authenticate).await?;
            response = self.get_image_index_inner(ref_).await?;
        }
        decode_and_check_for_error(&ref_.to_string(), response).await
    }

    #[anyhow_trace]
    async fn get_image_manifest(
        &self,
        ref_: &DockerReference,
        manifest_digest: &str,
    ) -> Result<ImageManifest> {
        let name = ref_.name();
        let base_url = ref_.host.base_url();
        let mut req = self
            .client
            .get(format!("{base_url}/{name}/manifests/{manifest_digest}"))
            .header(
                "Accept",
                "application/vnd.docker.distribution.manifest.v2+json",
            )
            .header("Accept", "application/vnd.oci.image.manifest.v1+json");
        if let Some(token) = &self.token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        decode_and_check_for_error(&ref_.to_string(), req.send().await?).await
    }

    async fn get_image_config(
        &self,
        ref_: &DockerReference,
        config_digest: &str,
    ) -> Result<ImageConfiguration> {
        let name = ref_.name();
        let base_url = ref_.host.base_url();
        let mut req = self
            .client
            .get(format!("{base_url}/{name}/blobs/{config_digest}"));
        if let Some(token) = &self.token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        let config: oci_spec::image::ImageConfiguration =
            decode_and_check_for_error(&ref_.to_string(), req.send().await?).await?;
        Ok(config.into())
    }

    #[anyhow_trace]
    async fn download_layer(
        &self,
        ref_: &DockerReference,
        digest: &str,
        prog: impl ProgressTracker,
        mut out: impl AsyncWrite + Unpin,
    ) -> Result<()> {
        let base_url = ref_.host.base_url();
        let name = ref_.name();
        let mut req = self.client.get(format!("{base_url}/{name}/blobs/{digest}"));
        if let Some(token) = &self.token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        let tar_stream = req.send().await?.error_for_status()?;
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

    #[anyhow_trace]
    fn download_layer_on_task(
        self: Arc<Self>,
        layer_digest: String,
        ref_: DockerReference,
        path: PathBuf,
        prog: impl ProgressTracker,
    ) -> task::JoinHandle<Result<()>> {
        task::spawn(async move {
            let mut file = tokio::fs::File::create(&path).await?;
            self.download_layer(&ref_, &layer_digest, prog, &mut file)
                .await?;
            Ok(())
        })
    }

    #[anyhow_trace]
    async fn resolve_tag_inner(&self, ref_: &DockerReference) -> Result<reqwest::Response> {
        if ref_.digest().is_some() {
            bail!("image name has digest")
        }

        let name = ref_.name();
        let base_url = ref_.host.base_url();
        let tag = ref_.tag();

        let mut req = self
            .client
            .get(format!("{base_url}/{name}/manifests/{tag}"))
            .header(
                "Accept",
                "application/vnd.docker.distribution.manifest.list.v2+json",
            )
            .header("Accept", "application/vnd.oci.image.index.v1+json");
        if let Some(token) = &self.token {
            req = req.header("Authorization", format!("Bearer {token}"))
        };
        let response = req.send().await?;
        Ok(response)
    }

    #[anyhow_trace]
    pub async fn resolve_tag(&mut self, ref_: &DockerReference) -> Result<String> {
        let mut response = self.resolve_tag_inner(ref_).await?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            let www_authenticate = response
                .headers()
                .get("www-authenticate")
                .ok_or_else(|| anyhow!("UNAUTHORIZED with no www-authenticate header"))?
                .to_str()?;
            self.get_token(www_authenticate).await?;
            response = self.resolve_tag_inner(ref_).await?;
        }
        let response = check_for_error(&ref_.to_string(), response).await?;
        let response_str = response.text().await?;

        let mut hasher = Sha256Stream::new(tokio::io::sink());
        hasher.write_all(response_str.as_bytes()).await.unwrap();
        let (_, hash) = hasher.finalize();
        Ok(format!("sha256:{hash}"))
    }

    #[anyhow_trace]
    pub async fn inspect(
        mut self,
        ref_: &DockerReference,
    ) -> Result<(ImageManifest, ImageConfiguration)> {
        let index = self.get_image_index(ref_).await?;
        let manifest = find_manifest_for_platform(index.manifests())?;
        let manifest_digest = manifest.digest().clone();

        let image = self.get_image_manifest(ref_, &manifest_digest).await?;
        let config = self.get_image_config(ref_, image.config().digest()).await?;
        Ok((image, config))
    }

    #[anyhow_trace]
    pub async fn download_image(
        mut self,
        ref_: &DockerReference,
        layer_dir: impl AsRef<Path>,
        prog: impl ProgressTracker + Clone,
    ) -> Result<ContainerImage> {
        let index = self.get_image_index(ref_).await?;
        let manifest = find_manifest_for_platform(index.manifests())?;
        let manifest_digest = manifest.digest().clone();

        let image = self.get_image_manifest(ref_, &manifest_digest).await?;

        let config = self.get_image_config(ref_, image.config().digest()).await?;

        let total_size: i64 = image.layers().iter().map(|l| l.size()).sum();
        prog.set_length(total_size as u64);

        let self_ = Arc::new(self);
        let mut task_handles = vec![];
        let mut layers = vec![];
        for (i, layer) in image.layers().iter().enumerate() {
            let path = layer_dir.as_ref().join(format!("layer_{i}.tar"));
            let handle = self_.clone().download_layer_on_task(
                layer.digest().clone(),
                ref_.clone(),
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
            name: ref_.name().into(),
            digest: manifest_digest,
            config,
            layers,
        })
    }
}

#[derive(
    Copy, Clone, Default, Debug, FromPrimitive, PartialEq, Eq, Serialize_repr, Deserialize_repr,
)]
#[repr(u32)]
pub enum LockedContainerImageTagsVersion {
    V0 = 0,
    #[default]
    V1 = 1,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
struct LockedContainerImageTags {
    version: LockedContainerImageTagsVersion,
    #[serde(flatten)]
    map: BTreeMap<String, BTreeMap<String, String>>,
}

const MISSING_VERSION: &str = "missing version";
const VERSION_NOT_AN_INTEGER: &str = "version field is not an integer";

impl LockedContainerImageTags {
    fn from_str(contents: &str) -> Result<Self> {
        let mut table: toml::Table = toml::from_str(contents)?;
        let version = table
            .remove("version")
            .ok_or_else(|| anyhow!(MISSING_VERSION))?;
        let Some(version) = version.as_integer() else {
            bail!(VERSION_NOT_AN_INTEGER);
        };
        match LockedContainerImageTagsVersion::from_i64(version) {
            Some(LockedContainerImageTagsVersion::V0) => {
                Err(anyhow!("old version of container image tags file"))
            }
            Some(LockedContainerImageTagsVersion::V1) => Ok(toml::from_str::<Self>(contents)?),
            _ => Err(anyhow!("unknown version of container image tags file")),
        }
    }

    fn get(&self, name: &str, tag: &str) -> Option<&String> {
        let tags = self.map.get(name)?;
        tags.get(tag)
    }

    fn add(&mut self, name: String, tag: String, digest: String) {
        self.map.entry(name).or_default().insert(tag, digest);
    }
}

#[allow(async_fn_in_trait)]
pub trait ContainerImageDepotOps {
    async fn resolve_tag(&self, ref_: &DockerReference) -> Result<String>;
    async fn download_image(
        &self,
        ref_: &DockerReference,
        layer_dir: &Path,
        prog: impl ProgressTracker + Clone,
    ) -> Result<ContainerImage>;
}

pub struct DefaultContainerImageDepotOps {
    client: reqwest::Client,
}

impl DefaultContainerImageDepotOps {
    fn new(accept_invalid_certs: bool) -> Self {
        Self {
            client: reqwest::Client::builder()
                .danger_accept_invalid_certs(accept_invalid_certs)
                .build()
                .unwrap(),
        }
    }
}

impl ContainerImageDepotOps for DefaultContainerImageDepotOps {
    async fn resolve_tag(&self, ref_: &DockerReference) -> Result<String> {
        let mut downloader = ImageDownloader::new(self.client.clone());
        downloader.resolve_tag(ref_).await
    }

    async fn download_image(
        &self,
        ref_: &DockerReference,
        layer_dir: &Path,
        prog: impl ProgressTracker + Clone,
    ) -> Result<ContainerImage> {
        let downloader = ImageDownloader::new(self.client.clone());
        downloader.download_image(ref_, layer_dir, prog).await
    }
}

pub struct ContainerImageDepot<ContainerImageDepotOpsT = DefaultContainerImageDepotOps> {
    fs: Fs,
    cache_dir: RootBuf<ContainerImageDepotDir>,
    project_dir: RootBuf<ProjectDir>,
    ops: ContainerImageDepotOpsT,
    cache: Mutex<HashMap<ImageName, ContainerImage>>,
    // We use this lock to make sure only one thread is trying to fill the image cache at a time.
    // This is important to avoid self-contention on the file-locks.
    // Contending on a file-lock uses a file-descriptor, and we are only allowed so many.
    cache_fill_lock: Mutex<()>,
}

impl ContainerImageDepot<DefaultContainerImageDepotOps> {
    pub fn new(
        project_dir: impl AsRef<Root<ProjectDir>>,
        cache_dir: impl AsRef<Root<ContainerImageDepotDir>>,
        accept_invalid_certs: bool,
    ) -> Result<Self> {
        Self::new_with(
            project_dir,
            cache_dir,
            DefaultContainerImageDepotOps::new(accept_invalid_certs),
        )
    }
}

const TAG_FILE_NAME: &str = "maelstrom-container-tags.lock";

struct LockedTagsHandle<'a, 'b> {
    locked_tags: LockedContainerImageTags,
    lock_file: fs::File,
    // Holding this lock we ensure we don't contend with ourselves for the file-lock
    _cache_fill: &'a MutexGuard<'b, ()>,
}

impl<'a, 'b> LockedTagsHandle<'a, 'b> {
    #[anyhow_trace]
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
        project_dir: impl AsRef<Root<ProjectDir>>,
        cache_dir: impl AsRef<Root<ContainerImageDepotDir>>,
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
            cache_fill_lock: Mutex::new(()),
        })
    }

    #[anyhow_trace]
    async fn get_image_digest(
        &self,
        locked_tags: &mut LockedContainerImageTags,
        ref_: &DockerReference,
    ) -> Result<String> {
        if let Some(digest) = ref_.digest() {
            return Ok(digest.into());
        }

        let mut short_ref = ref_.clone();
        short_ref.tag = None;
        let short_ref_str = short_ref.to_string();

        Ok(
            if let Some(digest) = locked_tags.get(&short_ref_str, ref_.tag()) {
                digest.into()
            } else {
                let digest = self.ops.resolve_tag(ref_).await?;
                locked_tags.add(short_ref_str, ref_.tag().into(), digest.clone());
                digest
            },
        )
    }

    #[anyhow_trace]
    async fn get_cached_image(&self, digest: &str) -> Option<ContainerImage> {
        ContainerImage::from_dir(&self.fs, self.cache_dir.join(digest)).await
    }

    #[anyhow_trace]
    async fn download_image(
        &self,
        ref_: &DockerReference,
        output_dir: &Root<DigestDir>,
        prog: impl ProgressTracker + Clone,
    ) -> Result<ContainerImage> {
        if output_dir.exists() {
            self.fs.remove_dir_all(&output_dir).await?;
        }
        self.fs.create_dir(&output_dir).await?;

        let img = self.ops.download_image(ref_, output_dir, prog).await?;
        self.fs
            .write(
                output_dir.join::<ContainerConfigFile>("config.json"),
                serde_json::to_vec(&img).unwrap(),
            )
            .await?;

        Ok(img)
    }

    #[anyhow_trace]
    async fn with_cache_lock<RetT>(
        &self,
        digest: &str,
        // Holding this lock we ensure we don't contend with ourselves for the file-lock
        _cache_fill: &MutexGuard<'_, ()>,
        body: impl Future<Output = Result<RetT>>,
    ) -> Result<RetT> {
        struct DigestLockFile;
        let lock_file_path = self
            .cache_dir
            .join::<DigestLockFile>(format!(".{digest}.flock"));
        let lock_file = self.fs.create_file(lock_file_path).await?;
        lock_file.lock_exclusive().await?;
        body.await
    }

    #[anyhow_trace]
    async fn lock_tags<'a, 'b>(
        &self,
        cache_fill: &'a MutexGuard<'b, ()>,
    ) -> Result<LockedTagsHandle<'a, 'b>> {
        let mut lock_file = self
            .fs
            .open_or_create_file(self.project_dir.join::<ContainerTagFile>(TAG_FILE_NAME))
            .await?;
        lock_file.lock_exclusive().await?;

        let mut contents = String::new();
        lock_file.read_to_string(&mut contents).await?;
        let locked_tags = LockedContainerImageTags::from_str(&contents).unwrap_or_default();
        Ok(LockedTagsHandle {
            locked_tags,
            lock_file,
            _cache_fill: cache_fill,
        })
    }

    #[anyhow_trace]
    pub async fn get_container_image(
        &self,
        name: &str,
        prog: impl ProgressTracker + Clone,
    ) -> Result<ContainerImage> {
        self.fs.create_dir_all(&self.cache_dir).await?;

        let image_name: ImageName = name.parse()?;

        if let Some(img) = self.cache.lock().await.get(&image_name) {
            return Ok(img.clone());
        }

        let ImageName::Docker(ref_) = &image_name else {
            bail!("local image path not supported yet");
        };

        let cache_fill = self.cache_fill_lock.lock().await;
        let mut tags = self.lock_tags(&cache_fill).await?;
        let digest = self.get_image_digest(&mut tags.locked_tags, ref_).await?;

        let img = self
            .with_cache_lock(&digest, &cache_fill, async {
                Ok(if let Some(img) = self.get_cached_image(&digest).await {
                    img
                } else {
                    let output_dir = self.cache_dir.join::<DigestDir>(digest.clone());
                    let mut specific_ref = ref_.clone();
                    specific_ref.tag = None;
                    specific_ref.digest = Some(digest.clone());
                    self.download_image(&specific_ref, &output_dir, prog)
                        .await?
                })
            })
            .await?;
        tags.write().await?;
        drop(cache_fill);

        self.cache.lock().await.insert(image_name, img.clone());
        Ok(img)
    }
}

#[cfg(test)]
struct PanicContainerImageDepotOps;

#[cfg(test)]
impl ContainerImageDepotOps for PanicContainerImageDepotOps {
    async fn resolve_tag(&self, _ref: &DockerReference) -> Result<String> {
        panic!()
    }

    async fn download_image(
        &self,
        _ref: &DockerReference,
        _layer_dir: &Path,
        _prog: impl ProgressTracker + Clone,
    ) -> Result<ContainerImage> {
        panic!()
    }
}

#[cfg(test)]
#[derive(Clone)]
struct FakeContainerImageDepotOps(HashMap<String, String>);

#[cfg(test)]
impl ContainerImageDepotOps for FakeContainerImageDepotOps {
    async fn resolve_tag(&self, ref_: &DockerReference) -> Result<String> {
        let name = ref_.name();
        let tag = ref_.tag();
        Ok(self.0.get(&format!("{name}-{tag}")).unwrap().clone())
    }

    async fn download_image(
        &self,
        ref_: &DockerReference,
        _layer_dir: &Path,
        _prog: impl ProgressTracker,
    ) -> Result<ContainerImage> {
        Ok(ContainerImage {
            version: ContainerImageVersion::default(),
            name: ref_.name().into(),
            digest: ref_.digest().unwrap().into(),
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
    let project_dir = Root::<ProjectDir>::new(project_dir.path());
    let image_dir = tempfile::tempdir().unwrap();
    let image_dir = Root::<ContainerImageDepotDir>::new(image_dir.path());

    let depot = ContainerImageDepot::new_with(
        project_dir,
        image_dir,
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("docker://foo", NullProgressTracker)
        .await
        .unwrap();

    assert_eq!(
        fs.read_to_string(project_dir.join::<ContainerTagFile>(TAG_FILE_NAME))
            .await
            .unwrap(),
        "\
            version = 1\n\
            \n\
            [foo]\n\
            latest = \"sha256:abcdef\"\n\
        "
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir).await,
        vec!["sha256:abcdef"]
    );
}

#[tokio::test]
async fn container_image_depot_download_then_reload() {
    let project_dir = tempfile::tempdir().unwrap();
    let project_dir = Root::<ProjectDir>::new(project_dir.path());
    let image_dir = tempfile::tempdir().unwrap();
    let image_dir = Root::<ContainerImageDepotDir>::new(image_dir.path());

    let depot = ContainerImageDepot::new_with(
        project_dir,
        image_dir,
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    let img1 = depot
        .get_container_image("docker://foo", NullProgressTracker)
        .await
        .unwrap();
    drop(depot);

    let depot =
        ContainerImageDepot::new_with(project_dir, image_dir, PanicContainerImageDepotOps).unwrap();
    let img2 = depot
        .get_container_image("docker://foo", NullProgressTracker)
        .await
        .unwrap();

    assert_eq!(img1, img2);
}

#[tokio::test]
async fn container_image_depot_redownload_corrupt() {
    let fs = Fs::new();
    let project_dir = tempfile::tempdir().unwrap();
    let project_dir = Root::<ProjectDir>::new(project_dir.path());
    let image_dir = tempfile::tempdir().unwrap();
    let image_dir = Root::<ContainerImageDepotDir>::new(image_dir.path());

    let depot = ContainerImageDepot::new_with(
        project_dir,
        image_dir,
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("docker://foo", NullProgressTracker)
        .await
        .unwrap();
    drop(depot);
    fs.remove_file(
        image_dir
            .join::<DigestDir>("sha256:abcdef")
            .join::<ContainerConfigFile>("config.json"),
    )
    .await
    .unwrap();

    let depot = ContainerImageDepot::new_with(
        project_dir,
        image_dir,
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("docker://foo", NullProgressTracker)
        .await
        .unwrap();

    assert_eq!(
        sorted_dir_listing(&fs, project_dir).await,
        vec![TAG_FILE_NAME]
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir).await,
        vec!["sha256:abcdef"]
    );
}

#[tokio::test]
async fn container_image_depot_update_image() {
    let fs = Fs::new();
    let project_dir = tempfile::tempdir().unwrap();
    let project_dir = Root::<ProjectDir>::new(project_dir.path());
    let image_dir = tempfile::tempdir().unwrap();
    let image_dir = Root::<ContainerImageDepotDir>::new(image_dir.path());

    let depot = ContainerImageDepot::new_with(
        project_dir,
        image_dir,
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:abcdef".into(),
            "bar-latest".into() => "sha256:ghijk".into(),
        }),
    )
    .unwrap();
    depot
        .get_container_image("docker://foo", NullProgressTracker)
        .await
        .unwrap();
    depot
        .get_container_image("docker://bar", NullProgressTracker)
        .await
        .unwrap();
    drop(depot);
    fs.remove_file(project_dir.join::<ContainerTagFile>(TAG_FILE_NAME))
        .await
        .unwrap();
    let bar_meta_before = fs
        .metadata(image_dir.join::<DigestDir>("sha256:ghijk"))
        .await
        .unwrap();

    let depot = ContainerImageDepot::new_with(
        project_dir,
        image_dir,
        FakeContainerImageDepotOps(maplit::hashmap! {
            "foo-latest".into() => "sha256:lmnop".into(),
            "bar-latest".into() => "sha256:ghijk".into(),
        }),
    )
    .unwrap();
    #[allow(clippy::disallowed_names)]
    let foo = depot
        .get_container_image("docker://foo", NullProgressTracker)
        .await
        .unwrap();
    depot
        .get_container_image("docker://bar", NullProgressTracker)
        .await
        .unwrap();

    // ensure we get new foo
    assert_eq!(foo.digest, "sha256:lmnop");

    let bar_meta_after = fs
        .metadata(image_dir.join::<DigestDir>("sha256:ghijk"))
        .await
        .unwrap();

    // ensure we didn't re-download bar
    assert_eq!(
        bar_meta_before.modified().unwrap(),
        bar_meta_after.modified().unwrap()
    );

    assert_eq!(
        sorted_dir_listing(&fs, project_dir).await,
        vec![TAG_FILE_NAME]
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir).await,
        vec!["sha256:abcdef", "sha256:ghijk", "sha256:lmnop",]
    );
}

#[tokio::test]
async fn container_image_depot_update_image_but_nothing_to_do() {
    let fs = Fs::new();
    let project_dir = tempfile::tempdir().unwrap();
    let project_dir = Root::<ProjectDir>::new(project_dir.path());
    let image_dir = tempfile::tempdir().unwrap();
    let image_dir = Root::<ContainerImageDepotDir>::new(image_dir.path());

    let ops = FakeContainerImageDepotOps(maplit::hashmap! {
        "foo-latest".into() => "sha256:abcdef".into(),
        "bar-latest".into() => "sha256:ghijk".into(),
    });
    let depot = ContainerImageDepot::new_with(project_dir, image_dir, ops.clone()).unwrap();
    depot
        .get_container_image("docker://foo", NullProgressTracker)
        .await
        .unwrap();
    depot
        .get_container_image("docker://bar", NullProgressTracker)
        .await
        .unwrap();
    drop(depot);
    fs.remove_file(project_dir.join::<ContainerTagFile>(TAG_FILE_NAME))
        .await
        .unwrap();

    let depot = ContainerImageDepot::new_with(project_dir, image_dir, ops).unwrap();
    depot
        .get_container_image("docker://foo", NullProgressTracker)
        .await
        .unwrap();
    depot
        .get_container_image("docker://bar", NullProgressTracker)
        .await
        .unwrap();

    assert_eq!(
        fs.read_to_string(project_dir.join::<ContainerTagFile>(TAG_FILE_NAME))
            .await
            .unwrap(),
        "\
            version = 1\n\
            \n\
            [bar]\n\
            latest = \"sha256:ghijk\"\n\
            \n\
            [foo]\n\
            latest = \"sha256:abcdef\"\n\
        "
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir).await,
        vec!["sha256:abcdef", "sha256:ghijk"]
    );
}

#[tokio::test]
async fn container_image_depot_local_registry() {
    let fs = Fs::new();
    let project_dir = tempfile::tempdir().unwrap();
    let project_dir = Root::<ProjectDir>::new(project_dir.path());
    let image_dir = tempfile::tempdir().unwrap();
    let image_dir = Root::<ContainerImageDepotDir>::new(image_dir.path());

    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let log = maelstrom_util::log::test_logger();
    let address = local_registry::LocalRegistry::run(manifest_dir.join("src"), log)
        .await
        .unwrap();

    let ops = DefaultContainerImageDepotOps::new(true /* accept_invalid_certs */);
    let depot = ContainerImageDepot::new_with(project_dir, image_dir, ops).unwrap();
    depot
        .get_container_image(&format!("docker://{address}/busybox"), NullProgressTracker)
        .await
        .unwrap();

    assert_eq!(
        fs.read_to_string(project_dir.join::<ContainerTagFile>(TAG_FILE_NAME))
            .await
            .unwrap(),
        format!(
            "\
            version = 1\n\
            \n\
            [\"{address}/busybox\"]\n\
            latest = \"sha256:0d3f3db50eadc1930aa204eef3d21966037b797cdbef2c7446bbdf10541bda4b\"\n\
        "
        )
    );
    assert_eq!(
        sorted_dir_listing(&fs, image_dir).await,
        vec!["sha256:0d3f3db50eadc1930aa204eef3d21966037b797cdbef2c7446bbdf10541bda4b"]
    );
}
