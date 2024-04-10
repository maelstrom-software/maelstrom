mod artifact_upload;
mod digest_repo;
mod dispatcher;
mod driver;
mod rpc;
mod test;

use anyhow::{anyhow, Error, Result};
use artifact_upload::ArtifactUploadTracker;
use async_trait::async_trait;
use digest_repo::DigestRepository;
use dispatcher::Message;
use driver::{new_driver, ClientDeps, ClientDriver, DispatcherAdapter};
use futures::StreamExt;
use itertools::Itertools as _;
use maelstrom_base::{
    manifest::{ManifestEntry, ManifestEntryData, ManifestEntryMetadata, Mode, UnixTimestamp},
    stats::JobStateCounts,
    ArtifactType, ClientJobId, JobOutcomeResult, JobSpec, Sha256Digest, Utf8Path, Utf8PathBuf,
};
use maelstrom_client_base::{
    spec::{Layer, PrefixOptions, SymlinkSpec},
    ArtifactUploadProgress, ClientDriverMode, ClientMessageKind, MANIFEST_DIR, STUB_MANIFEST_DIR,
    SYMLINK_MANIFEST_DIR,
};
use maelstrom_container::{ContainerImage, ContainerImageDepot, ProgressTracker};
use maelstrom_util::{
    async_fs,
    config::common::BrokerAddr,
    io::Sha256Stream,
    manifest::{AsyncManifestWriter, ManifestBuilder},
};
pub use rpc::client_process_main;
use sha2::{Digest as _, Sha256};
use slog::Drain as _;
use std::pin::pin;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::{Path, PathBuf},
    time::SystemTime,
};
use tokio::sync::{mpsc::Sender, oneshot, Mutex};

async fn calculate_digest(path: &Path) -> Result<(SystemTime, Sha256Digest)> {
    let fs = async_fs::Fs::new();
    let mut f = fs.open_file(path).await?;
    let mut hasher = Sha256Stream::new(tokio::io::sink());
    tokio::io::copy(&mut f, &mut hasher).await?;
    let mtime = f.metadata().await?.modified()?;

    Ok((mtime, hasher.finalize().1))
}

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

#[async_trait]
impl<'a> maelstrom_util::manifest::DataUpload for &'a Client {
    async fn upload(&mut self, path: &Path) -> Result<Sha256Digest> {
        self.add_artifact(path).await
    }
}

/// Having some deterministic time-stamp for files we create in manifests is useful for testing and
/// potentially caching.
/// I picked this time arbitrarily 2024-1-11 11:11:11
const ARBITRARY_TIME: UnixTimestamp = UnixTimestamp(1705000271);

async fn default_log(fs: &async_fs::Fs, cache_dir: &Path) -> Result<slog::Logger> {
    let log_file = fs
        .open_or_create_file(cache_dir.join("cargo-maelstrom.log"))
        .await?;
    let decorator = slog_term::PlainDecorator::new(log_file.into_inner().into_std().await);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    Ok(slog::Logger::root(drain, slog::o!()))
}

struct ClientState {
    digest_repo: DigestRepository,
    processed_artifact_paths: HashSet<PathBuf>,
    cached_layers: HashMap<Layer, (Sha256Digest, ArtifactType)>,
}

struct Client {
    dispatcher_sender: Sender<Message<DispatcherAdapter>>,
    cache_dir: PathBuf,
    project_dir: PathBuf,
    upload_tracker: ArtifactUploadTracker,
    container_image_depot: ContainerImageDepot,
    driver: Box<dyn ClientDriver + Send + Sync>,
    log: slog::Logger,
    state: Mutex<ClientState>,
}

impl Client {
    async fn new(
        driver_mode: ClientDriverMode,
        broker_addr: BrokerAddr,
        project_dir: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
        log: Option<slog::Logger>,
    ) -> Result<Self> {
        let fs = async_fs::Fs::new();
        for d in [MANIFEST_DIR, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR] {
            fs.create_dir_all(cache_dir.as_ref().join(d)).await?;
        }

        let log = match log {
            Some(l) => l,
            None => default_log(&fs, cache_dir.as_ref()).await?,
        };

        slog::debug!(log, "client starting");
        let driver = new_driver(driver_mode);
        let upload_tracker = ArtifactUploadTracker::default();
        let deps = ClientDeps::new(broker_addr, upload_tracker.clone()).await?;
        let dispatcher_sender = deps.dispatcher_sender.clone();
        driver.drive(deps).await;

        slog::debug!(log, "client connected to broker"; "broker_addr" => ?broker_addr);

        Ok(Client {
            dispatcher_sender,
            cache_dir: cache_dir.as_ref().to_owned(),
            project_dir: project_dir.as_ref().to_owned(),
            upload_tracker,
            container_image_depot: ContainerImageDepot::new(project_dir.as_ref())?,
            driver,
            log,
            state: Mutex::new(ClientState {
                digest_repo: DigestRepository::new(cache_dir.as_ref()),
                processed_artifact_paths: HashSet::default(),
                cached_layers: HashMap::new(),
            }),
        })
    }

    async fn add_artifact(&self, path: &Path) -> Result<Sha256Digest> {
        slog::debug!(self.log, "add_artifact"; "path" => ?path);

        let fs = async_fs::Fs::new();
        let path = fs.canonicalize(path).await?;

        let mut state = self.state.lock().await;
        let digest = if let Some(digest) = state.digest_repo.get(&path).await? {
            digest
        } else {
            let (mtime, digest) = calculate_digest(&path).await?;
            state
                .digest_repo
                .add(path.clone(), mtime, digest.clone())
                .await?;
            digest
        };
        if !state.processed_artifact_paths.contains(&path) {
            state.processed_artifact_paths.insert(path.clone());
            self.dispatcher_sender
                .send(Message::AddArtifact(path, digest.clone()))
                .await?;
        }
        Ok(digest)
    }

    fn build_manifest_path(&self, name: &impl fmt::Display) -> PathBuf {
        self.cache_dir
            .join(MANIFEST_DIR)
            .join(format!("{name}.manifest"))
    }

    fn build_stub_manifest_path(&self, name: &impl fmt::Display) -> PathBuf {
        self.cache_dir
            .join(STUB_MANIFEST_DIR)
            .join(format!("{name}.manifest"))
    }

    fn build_symlink_manifest_path(&self, name: &impl fmt::Display) -> PathBuf {
        self.cache_dir
            .join(SYMLINK_MANIFEST_DIR)
            .join(format!("{name}.manifest"))
    }

    async fn build_manifest(
        &self,
        mut paths: impl futures::stream::Stream<Item = Result<impl AsRef<Path>>>,
        prefix_options: PrefixOptions,
    ) -> Result<PathBuf> {
        let fs = async_fs::Fs::new();
        let project_dir = self.project_dir.clone();
        let tmp_file_path = self.build_manifest_path(&".temp");
        let manifest_file = fs.create_file(&tmp_file_path).await?;
        let follow_symlinks = prefix_options.follow_symlinks;
        let mut builder = ManifestBuilder::new(manifest_file, follow_symlinks, self).await?;
        let mut path_hasher = PathHasher::new();
        let mut pinned_paths = pin!(paths);
        while let Some(maybe_path) = pinned_paths.next().await {
            let mut path = maybe_path?.as_ref().to_owned();
            let input_path_relative = path.is_relative();
            if input_path_relative {
                path = project_dir.join(path);
            }
            let utf8_path = Utf8Path::from_path(&path).ok_or_else(|| anyhow!("non-utf8 path"))?;
            path_hasher.hash_path(utf8_path);

            let entry_path = if input_path_relative {
                utf8_path.strip_prefix(&project_dir).unwrap()
            } else {
                utf8_path
            };
            let dest = calculate_manifest_entry_path(entry_path, &project_dir, &prefix_options)?;
            builder.add_file(utf8_path, dest).await?;
        }
        drop(builder);

        let manifest_path = self.build_manifest_path(&path_hasher.finish());
        fs.rename(tmp_file_path, &manifest_path).await?;
        Ok(manifest_path)
    }

    async fn build_stub_manifest(&self, stubs: Vec<String>) -> Result<PathBuf> {
        let fs = async_fs::Fs::new();
        let tmp_file_path = self.build_manifest_path(&".temp");
        let mut writer = AsyncManifestWriter::new(fs.create_file(&tmp_file_path).await?).await?;
        let mut path_hasher = PathHasher::new();
        for maybe_stub in stubs.iter().map(|s| expand_braces(s)).flatten_ok() {
            let stub = Utf8PathBuf::from(maybe_stub?);
            path_hasher.hash_path(&stub);
            let is_dir = stub.as_str().ends_with('/');
            let data = if is_dir {
                ManifestEntryData::Directory
            } else {
                ManifestEntryData::File(None)
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

        let manifest_path = self.build_stub_manifest_path(&path_hasher.finish());
        fs.rename(tmp_file_path, &manifest_path).await?;
        Ok(manifest_path)
    }

    async fn build_symlink_manifest(&self, symlinks: Vec<SymlinkSpec>) -> Result<PathBuf> {
        let fs = async_fs::Fs::new();
        let tmp_file_path = self.build_manifest_path(&".temp");
        let mut writer = AsyncManifestWriter::new(fs.create_file(&tmp_file_path).await?).await?;
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

        let manifest_path = self.build_symlink_manifest_path(&path_hasher.finish());
        fs.rename(tmp_file_path, &manifest_path).await?;
        Ok(manifest_path)
    }

    async fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        slog::debug!(self.log, "add_layer"; "layer" => ?layer);

        if let Some(l) = self.state.lock().await.cached_layers.get(&layer) {
            return Ok(l.clone());
        }

        let res = match layer.clone() {
            Layer::Tar { path } => (
                self.add_artifact(path.as_std_path()).await?,
                ArtifactType::Tar,
            ),
            Layer::Paths {
                paths,
                prefix_options,
            } => {
                let manifest_path = self
                    .build_manifest(futures::stream::iter(paths.iter().map(Ok)), prefix_options)
                    .await?;
                (
                    self.add_artifact(&manifest_path).await?,
                    ArtifactType::Manifest,
                )
            }
            Layer::Glob {
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
                    )
                    .await?;
                (
                    self.add_artifact(&manifest_path).await?,
                    ArtifactType::Manifest,
                )
            }
            Layer::Stubs { stubs } => {
                let manifest_path = self.build_stub_manifest(stubs).await?;
                (
                    self.add_artifact(&manifest_path).await?,
                    ArtifactType::Manifest,
                )
            }
            Layer::Symlinks { symlinks } => {
                let manifest_path = self.build_symlink_manifest(symlinks).await?;
                (
                    self.add_artifact(&manifest_path).await?,
                    ArtifactType::Manifest,
                )
            }
        };

        self.state
            .lock()
            .await
            .cached_layers
            .insert(layer, res.clone());
        Ok(res)
    }

    async fn get_container_image(
        &self,
        name: &str,
        tag: &str,
        prog: impl ProgressTracker,
    ) -> Result<ContainerImage> {
        slog::debug!(self.log, "get_container_image"; "name" => name, "tag" => tag);
        self.container_image_depot
            .get_container_image(name, tag, prog)
            .await
    }

    async fn run_job(&self, spec: JobSpec) -> Result<(ClientJobId, JobOutcomeResult)> {
        slog::debug!(self.log, "run_job"; "spec" => ?spec);

        let (sender, receiver) = oneshot::channel();

        self.dispatcher_sender
            .send(Message::AddJob(
                spec,
                Box::new(move |cjid: ClientJobId, res: JobOutcomeResult| {
                    let _ = sender.send((cjid, res));
                })
                    as Box<(dyn FnOnce(ClientJobId, JobOutcomeResult) + Send + Sync + 'static)>,
            ))
            .await?;

        receiver.await.map_err(Error::new)
    }

    async fn stop_accepting(&self) -> Result<()> {
        slog::debug!(self.log, "stop_accepting");
        self.dispatcher_sender.send(Message::Stop).await?;
        Ok(())
    }

    async fn wait_for_outstanding_jobs(&self) -> Result<()> {
        slog::debug!(self.log, "wait_for_outstanding_jobs");
        self.stop_accepting().await.ok();
        self.driver.stop().await?;
        Ok(())
    }

    async fn get_job_state_counts(&self) -> Result<JobStateCounts> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.dispatcher_sender
            .send(Message::GetJobStateCounts(sender))
            .await?;
        receiver.await.map_err(Error::new)
    }

    async fn get_artifact_upload_progress(&self) -> Vec<ArtifactUploadProgress> {
        self.upload_tracker.get_artifact_upload_progress().await
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    async fn process_broker_msg_single_threaded(&self, count: usize) {
        slog::debug!(self.log, "process_broker_msg_single_threaded");
        self.driver.process_broker_msg_single_threaded(count).await
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    async fn process_client_messages_single_threaded(&self, wanted: ClientMessageKind) {
        slog::debug!(self.log, "process_client_messages_single_threaded"; "wanted" => ?wanted);
        self.driver
            .process_client_messages_single_threaded(wanted)
            .await
    }

    /// Must only be called if created with `ClientDriverMode::SingleThreaded`
    async fn process_artifact_single_threaded(&self) {
        slog::debug!(self.log, "process_artifact_single_threaded");
        self.driver.process_artifact_single_threaded().await
    }
}
