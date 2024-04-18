use crate::{
    artifact_pusher::{self, ArtifactUploadTracker},
    digest_repo::DigestRepository,
    dispatcher, local_broker, SocketReader,
};
use anyhow::{anyhow, bail, Context as _, Error, Result};
use async_trait::async_trait;
use futures::StreamExt;
use itertools::Itertools as _;
use maelstrom_base::{
    manifest::{ManifestEntry, ManifestEntryData, ManifestEntryMetadata, Mode, UnixTimestamp},
    proto::Hello,
    stats::JobStateCounts,
    ArtifactType, ClientJobId, JobOutcomeResult, JobSpec, Sha256Digest, Utf8Path, Utf8PathBuf,
};
use maelstrom_client_base::{
    spec::{Layer, PrefixOptions, SymlinkSpec},
    ArtifactUploadProgress, MANIFEST_DIR, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR,
};
use maelstrom_container::{ContainerImage, ContainerImageDepot, NullProgressTracker};
use maelstrom_util::{
    async_fs,
    config::common::BrokerAddr,
    manifest::{AsyncManifestWriter, ManifestBuilder},
    net,
};
use sha2::{Digest as _, Sha256};
use slog::{debug, Drain as _};
use std::{
    cell::UnsafeCell,
    collections::{HashMap, HashSet},
    fmt,
    path::{Path, PathBuf},
    pin::pin,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
    task::{self, JoinSet},
};

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

/// Having some deterministic time-stamp for files we create in manifests is useful for testing and
/// potentially caching.
/// I picked this time arbitrarily 2024-1-11 11:11:11
const ARBITRARY_TIME: UnixTimestamp = UnixTimestamp(1705000271);

pub async fn default_log(fs: &async_fs::Fs, cache_dir: &Path) -> Result<slog::Logger> {
    let log_file = fs
        .open_or_create_file(cache_dir.join("cargo-maelstrom.log"))
        .await?;
    let decorator = slog_term::PlainDecorator::new(log_file.into_inner().into_std().await);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    Ok(slog::Logger::root(drain, slog::o!()))
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ClientState {
    NotYetStarted,
    Starting,
    Started,
    Failed,
}

struct ClientStateStarted {
    dispatcher_sender: dispatcher::Sender,
    cache_dir: PathBuf,
    project_dir: PathBuf,
    upload_tracker: ArtifactUploadTracker,
    container_image_depot: ContainerImageDepot,
    log: slog::Logger,
    locked: Mutex<ClientStateStartedLocked>,
}

#[async_trait]
impl<'a> maelstrom_util::manifest::DataUpload for &'a ClientStateStarted {
    async fn upload(&mut self, path: &Path) -> Result<Sha256Digest> {
        self.add_artifact(path).await
    }
}

impl ClientStateStarted {
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

    async fn add_artifact(&self, path: &Path) -> Result<Sha256Digest> {
        slog::debug!(self.log, "add_artifact"; "path" => ?path);

        let fs = async_fs::Fs::new();
        let path = fs.canonicalize(path).await?;

        let mut locked = self.locked.lock().await;
        let digest = if let Some(digest) = locked.digest_repo.get(&path).await? {
            digest
        } else {
            let (mtime, digest) = crate::calculate_digest(&path).await?;
            locked
                .digest_repo
                .add(path.clone(), mtime, digest.clone())
                .await?;
            digest
        };
        if !locked.processed_artifact_paths.contains(&path) {
            locked.processed_artifact_paths.insert(path.clone());
            self.dispatcher_sender
                .send(dispatcher::Message::AddArtifact(path, digest.clone()))?;
        }
        Ok(digest)
    }

    async fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        slog::debug!(self.log, "add_layer"; "layer" => ?layer);

        if let Some(l) = self.locked.lock().await.cached_layers.get(&layer) {
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

        self.locked
            .lock()
            .await
            .cached_layers
            .insert(layer, res.clone());
        Ok(res)
    }
}

struct ClientStateStartedLocked {
    digest_repo: DigestRepository,
    processed_artifact_paths: HashSet<PathBuf>,
    cached_layers: HashMap<Layer, (Sha256Digest, ArtifactType)>,
}

pub struct Client {
    keeper: ClientStateKeeper,
}

pub async fn start_tasks(
    broker_addr: BrokerAddr,
    log: slog::Logger,
) -> Result<(dispatcher::Sender, ArtifactUploadTracker)> {
    slog::debug!(log, "client starting");

    let mut broker_socket = TcpStream::connect(broker_addr.inner())
        .await
        .with_context(|| format!("failed to connect to {broker_addr}"))?;
    net::write_message_to_async_socket(&mut broker_socket, Hello::Client).await?;

    slog::debug!(log, "client connected to broker"; "broker_addr" => ?broker_addr);

    let (dispatcher_sender, dispatcher_receiver) = dispatcher::channel();
    let (local_broker_sender, local_broker_receiver) = local_broker::channel();
    let upload_tracker = ArtifactUploadTracker::default();
    let (artifact_pusher_sender, artifact_pusher_receiver) = artifact_pusher::channel();

    let (broker_socket_reader, broker_socket_writer) = broker_socket.into_split();
    let (broker_sender, broker_receiver) = mpsc::unbounded_channel();
    let mut socket_reader = SocketReader::new(broker_socket_reader, local_broker_sender.clone());

    let mut join_set = JoinSet::new();

    dispatcher::start_task(&mut join_set, dispatcher_receiver, local_broker_sender);
    local_broker::start_task(
        &mut join_set,
        local_broker_receiver,
        dispatcher_sender.clone(),
        broker_sender,
        artifact_pusher_sender,
    );
    artifact_pusher::start_task(
        &mut join_set,
        artifact_pusher_receiver,
        broker_addr,
        upload_tracker.clone(),
    );

    join_set.spawn(async move { while socket_reader.process_one().await {} });

    join_set.spawn(net::async_socket_writer(
        broker_receiver,
        broker_socket_writer,
        move |msg| {
            debug!(log, "sending broker message"; "msg" => ?msg);
        },
    ));

    task::spawn(async move {
        join_set.join_next().await;
    });

    Ok((dispatcher_sender, upload_tracker))
}

struct ClientStateKeeper {
    not_yet_started: UnsafeCell<Option<slog::Logger>>,
    started: UnsafeCell<Option<ClientStateStarted>>,
    failed: UnsafeCell<Option<String>>,
    sender: tokio::sync::watch::Sender<ClientState>,
}

unsafe impl Sync for ClientStateKeeper {}

impl ClientStateKeeper {
    fn new(log: Option<slog::Logger>) -> Self {
        Self {
            not_yet_started: UnsafeCell::new(log),
            started: UnsafeCell::new(None),
            failed: UnsafeCell::new(None),
            sender: tokio::sync::watch::Sender::new(ClientState::NotYetStarted),
        }
    }

    fn try_to_move_to_starting(&self) -> Result<Option<slog::Logger>> {
        if !self.sender.send_if_modified(|state| {
            if *state == ClientState::NotYetStarted {
                *state = ClientState::Starting;
                true
            } else {
                false
            }
        }) {
            bail!("client already started");
        }
        let log_ptr = self.not_yet_started.get();
        Ok(unsafe { &mut *log_ptr }.take())
    }

    fn move_to_started(&self, state: ClientStateStarted) {
        let ptr = self.started.get();
        unsafe { *ptr = Some(state) };
        let old = self.sender.send_replace(ClientState::Started);
        assert_eq!(old, ClientState::Starting);
    }

    fn fail_to_start(&self, err: String) {
        let ptr = self.failed.get();
        unsafe { *ptr = Some(err) };
        let old = self.sender.send_replace(ClientState::Failed);
        assert_eq!(old, ClientState::Starting);
    }

    fn started(&self) -> Result<&ClientStateStarted> {
        match *self.sender.borrow() {
            ClientState::NotYetStarted | ClientState::Starting => {
                Err(anyhow!("client not yet started"))
            }
            ClientState::Started => {
                let started_ptr = self.started.get();
                let state = unsafe { &*started_ptr }.as_ref().unwrap();
                Ok(state)
            }
            ClientState::Failed => Err(self._done_err()),
        }
    }

    fn started_with_watcher(&self) -> Result<(&ClientStateStarted, StateWatcher<'_>)> {
        let receiver = self.sender.subscribe();
        let idx = *receiver.borrow();
        match idx {
            ClientState::NotYetStarted | ClientState::Starting => {
                Err(anyhow!("client not yet started"))
            }
            ClientState::Started => {
                let started_ptr = self.started.get();
                let state = unsafe { &*started_ptr }.as_ref().unwrap();
                Ok((
                    state,
                    StateWatcher {
                        keeper: self,
                        receiver,
                    },
                ))
            }
            ClientState::Failed => Err(self._done_err()),
        }
    }

    fn _done_err(&self) -> Error {
        let done_ptr = self.failed.get();
        let err = unsafe { &*done_ptr }.as_ref().unwrap();
        anyhow!("client failed with error: {err}")
    }
}

struct StateWatcher<'a> {
    keeper: &'a ClientStateKeeper,
    receiver: tokio::sync::watch::Receiver<ClientState>,
}

impl<'a> StateWatcher<'a> {
    async fn recv<T>(mut self, receiver: tokio::sync::oneshot::Receiver<T>) -> Result<T> {
        tokio::select! {
            Ok(result) = receiver => {
                Ok(result)
            }
            _ = self.receiver.changed() => {
                assert_eq!(*self.receiver.borrow(), ClientState::Failed);
                Err(self.keeper._done_err())
            }
        }
    }
}

impl Client {
    pub fn new(log: Option<slog::Logger>) -> Self {
        Self {
            keeper: ClientStateKeeper::new(log),
        }
    }

    pub async fn start(
        &self,
        broker_addr: BrokerAddr,
        project_dir: PathBuf,
        cache_dir: PathBuf,
    ) -> Result<()> {
        let log = self.keeper.try_to_move_to_starting()?;

        async fn try_to_start(
            log: Option<slog::Logger>,
            broker_addr: BrokerAddr,
            project_dir: PathBuf,
            cache_dir: PathBuf,
        ) -> Result<ClientStateStarted> {
            let fs = async_fs::Fs::new();
            for d in [
                crate::MANIFEST_DIR,
                crate::STUB_MANIFEST_DIR,
                crate::SYMLINK_MANIFEST_DIR,
            ] {
                fs.create_dir_all(cache_dir.join(d)).await?;
            }

            let log = match log {
                Some(log) => log.clone(),
                None => default_log(&fs, cache_dir.as_ref()).await?,
            };

            let (dispatcher_sender, upload_tracker) = start_tasks(broker_addr, log.clone()).await?;

            let container_image_depot = ContainerImageDepot::new(&project_dir)?;
            let digest_repo = DigestRepository::new(&cache_dir);

            Ok(ClientStateStarted {
                dispatcher_sender,
                cache_dir,
                project_dir,
                upload_tracker,
                container_image_depot,
                log,
                locked: Mutex::new(ClientStateStartedLocked {
                    digest_repo,
                    processed_artifact_paths: HashSet::default(),
                    cached_layers: HashMap::new(),
                }),
            })
        }

        match try_to_start(log, broker_addr, project_dir, cache_dir).await {
            Ok(state) => {
                self.keeper.move_to_started(state);
                Ok(())
            }
            Err(err) => {
                self.keeper.fail_to_start(err.to_string());
                Err(err)
            }
        }
    }

    pub async fn add_artifact(&self, path: &Path) -> Result<Sha256Digest> {
        self.keeper.started()?.add_artifact(path).await
    }

    pub async fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        self.keeper.started()?.add_layer(layer).await
    }

    pub async fn get_container_image(&self, name: &str, tag: &str) -> Result<ContainerImage> {
        let state = self.keeper.started()?;
        slog::debug!(state.log, "get_container_image"; "name" => name, "tag" => tag);
        state
            .container_image_depot
            .get_container_image(name, tag, NullProgressTracker)
            .await
    }

    pub async fn run_job(&self, spec: JobSpec) -> Result<(ClientJobId, JobOutcomeResult)> {
        let (state, watcher) = self.keeper.started_with_watcher()?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        slog::debug!(state.log, "run_job"; "spec" => ?spec);
        state
            .dispatcher_sender
            .send(dispatcher::Message::AddJob(spec, sender))?;
        watcher.recv(receiver).await
    }

    pub async fn wait_for_outstanding_jobs(&self) -> Result<()> {
        let (state, watcher) = self.keeper.started_with_watcher()?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        slog::debug!(state.log, "wait_for_outstanding_jobs");
        state
            .dispatcher_sender
            .send(dispatcher::Message::NotifyWhenAllJobsComplete(sender))?;
        watcher.recv(receiver).await
    }

    pub async fn get_job_state_counts(&self) -> Result<JobStateCounts> {
        let (state, watcher) = self.keeper.started_with_watcher()?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        state
            .dispatcher_sender
            .send(dispatcher::Message::GetJobStateCounts(sender))?;
        watcher.recv(receiver).await
    }

    pub async fn get_artifact_upload_progress(&self) -> Result<Vec<ArtifactUploadProgress>> {
        Ok(self
            .keeper
            .started()?
            .upload_tracker
            .get_artifact_upload_progress()
            .await)
    }
}
