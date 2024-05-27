mod layer_builder;
mod state_machine;

use crate::{
    artifact_pusher,
    digest_repo::DigestRepository,
    progress::{LazyProgress, ProgressTracker},
    router,
};
use anyhow::{anyhow, bail, Context as _, Result};
use async_trait::async_trait;
use layer_builder::LayerBuilder;
use maelstrom_base::{
    proto::{Hello, WorkerToBroker},
    ArtifactType, ClientJobId, JobOutcomeResult, Sha256Digest,
};
use maelstrom_client_base::{
    spec::{environment_eval, std_env_lookup, ConvertedImage, ImageConfig, JobSpec, Layer},
    CacheDir, IntrospectResponse, ProjectDir, StateDir, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR,
};
use maelstrom_container::{
    self as container, ContainerImage, ContainerImageDepot, ContainerImageDepotDir,
};
use maelstrom_util::{
    async_fs,
    config::common::{BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots},
    ext::BoolExt,
    log::LoggerFactory,
    net,
    root::{Root, RootBuf},
    sync,
};
use maelstrom_worker::local_worker;
use slog::{debug, warn, Logger};
use state_machine::StateMachine;
use std::{
    collections::{HashMap, HashSet},
    mem,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
    task::{self, JoinSet},
};

pub struct Client {
    state_machine: Arc<StateMachine<LoggerFactory, ClientState>>,
}

struct ClientState {
    local_broker_sender: router::Sender,
    layer_builder: LayerBuilder,
    artifact_upload_tracker: ProgressTracker,
    image_download_tracker: ProgressTracker,
    container_image_depot: ContainerImageDepot,
    log: Logger,
    locked: Mutex<ClientStateLocked>,
}

struct ClientStateLocked {
    digest_repo: DigestRepository,
    processed_artifact_paths: HashSet<PathBuf>,
    cached_layers: HashMap<Layer, (Sha256Digest, ArtifactType)>,
}

#[async_trait]
impl<'a> maelstrom_util::manifest::DataUpload for &'a ClientState {
    async fn upload(&mut self, path: &Path) -> Result<Sha256Digest> {
        self.add_artifact(path).await
    }
}

impl ClientState {
    async fn add_artifact(&self, path: &Path) -> Result<Sha256Digest> {
        debug!(self.log, "add_artifact"; "path" => ?path);

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
            self.local_broker_sender
                .send(router::Message::AddArtifact(path, digest.clone()))?;
        }
        Ok(digest)
    }

    async fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        debug!(self.log, "add_layer"; "layer" => ?layer);

        if let Some(l) = self.locked.lock().await.cached_layers.get(&layer) {
            return Ok(l.clone());
        }

        let (artifact_path, artifact_type) =
            self.layer_builder.build_layer(layer.clone(), self).await?;
        let artifact_digest = self.add_artifact(&artifact_path).await?;
        let res = (artifact_digest, artifact_type);

        self.locked
            .lock()
            .await
            .cached_layers
            .insert(layer, res.clone());
        Ok(res)
    }

    async fn get_container_image(&self, name: &str, tag: &str) -> Result<ContainerImage> {
        let dl_name = format!("{name}:{tag}");

        let tracker = self.image_download_tracker.clone();
        let dl_name_clone = dl_name.clone();
        let prog = LazyProgress::new(move |size| tracker.new_task(&dl_name_clone, size));

        let res = self
            .container_image_depot
            .get_container_image(name, tag, prog)
            .await;
        self.image_download_tracker.remove_task(&dl_name);
        res
    }
}

impl Client {
    pub fn new(log: LoggerFactory) -> Self {
        Self {
            state_machine: Arc::new(StateMachine::new(log)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        &self,
        broker_addr: Option<BrokerAddr>,
        project_dir: RootBuf<ProjectDir>,
        state_dir: RootBuf<StateDir>,
        cache_dir: RootBuf<CacheDir>,
        container_image_depot_cache_dir: RootBuf<ContainerImageDepotDir>,
        cache_size: CacheSize,
        inline_limit: InlineLimit,
        slots: Slots,
    ) -> Result<()> {
        async fn file_logger(
            log_level: LogLevel,
            fs: &async_fs::Fs,
            state_dir: &Root<StateDir>,
        ) -> Result<Logger> {
            struct LogFile;
            let log_file = fs
                .open_or_create_file_append(state_dir.join::<LogFile>("client-process.log"))
                .await?;
            Ok(maelstrom_util::log::file_logger(
                log_level,
                log_file.into_inner().into_std().await,
            ))
        }

        async fn try_to_start(
            log: LoggerFactory,
            broker_addr: Option<BrokerAddr>,
            project_dir: RootBuf<ProjectDir>,
            state_dir: RootBuf<StateDir>,
            cache_dir: RootBuf<CacheDir>,
            container_image_depot_cache_dir: RootBuf<ContainerImageDepotDir>,
            cache_size: CacheSize,
            inline_limit: InlineLimit,
            slots: Slots,
        ) -> Result<(ClientState, JoinSet<Result<()>>)> {
            let fs = async_fs::Fs::new();

            // Make sure the state dir exists before we try to put a log file in.
            fs.create_dir_all(&state_dir).await?;

            // Ensure we have a logger. If this program was started by a user on the shell, then a
            // logger will have been provided. Otherwise, open a log file in the cache directory
            // and log there.
            let log = match log {
                LoggerFactory::FromLogger(log) => log,
                LoggerFactory::FromLevel(level) => file_logger(level, &fs, &state_dir).await?,
            };
            debug!(log, "client starting";
                "broker_addr" => ?broker_addr,
                "project_dir" => ?project_dir,
                "state_dir" => ?state_dir,
                "cache_dir" => ?cache_dir,
                "container_image_depot_cache_dir" => ?container_image_depot_cache_dir,
                "cache_size" => ?cache_size,
                "inline_limit" => ?inline_limit,
                "slots" => ?slots,
            );

            // Ensure all of the appropriate subdirectories have been created in the cache
            // directory.
            const LOCAL_WORKER_DIR: &str = "local-worker";
            for d in [STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR, LOCAL_WORKER_DIR] {
                fs.create_dir_all((**cache_dir).join(d)).await?;
            }

            // Create standalone sub-components.
            let container_image_depot = ContainerImageDepot::new(
                project_dir.transmute::<container::ProjectDir>(),
                container_image_depot_cache_dir,
            )?;
            let digest_repo = DigestRepository::new(&cache_dir);
            let artifact_upload_tracker = ProgressTracker::default();
            let image_download_tracker = ProgressTracker::default();

            // Create the JoinSet we're going to put tasks in. If we bail early from this function,
            // we'll cancel all tasks we have started thus far.
            let mut join_set = JoinSet::new();

            // Create all of the channels we're going to need to connect everything up.
            let (artifact_pusher_sender, artifact_pusher_receiver) = artifact_pusher::channel();
            let (broker_sender, broker_receiver) = mpsc::unbounded_channel();
            let (local_broker_sender, local_broker_receiver) = router::channel();
            let (local_worker_sender, local_worker_receiver) = mpsc::unbounded_channel();

            let standalone;
            if let Some(broker_addr) = broker_addr {
                // We have a broker_addr, which means we're not in standalone mode.
                standalone = false;

                // Connect to the broker.
                let (broker_socket_read_half, mut broker_socket_write_half) =
                    TcpStream::connect(broker_addr.inner())
                        .await
                        .with_context(|| format!("failed to connect to {broker_addr}"))?
                        .into_split();
                debug!(log, "client connected to broker"; "broker_addr" => ?broker_addr);

                // Send it a Hello message.
                net::write_message_to_async_socket(&mut broker_socket_write_half, Hello::Client)
                    .await?;

                // Spawn a task to read from the socket and write to the router's channel.
                let log_clone = log.clone();
                let local_broker_sender_clone = local_broker_sender.clone();
                join_set.spawn(async move {
                    net::async_socket_reader(
                        broker_socket_read_half,
                        local_broker_sender_clone,
                        |msg| {
                            debug!(log_clone, "received broker message"; "msg" => ?msg);
                            router::Message::Broker(msg)
                        },
                    )
                    .await
                    .inspect_err(
                        |err| debug!(log_clone, "error reading broker message"; "err" => ?err),
                    )
                    .context("reading from broker")
                });

                // Spawn a task to read from the broker's channel and write to the socket.
                let log_clone = log.clone();
                join_set.spawn(async move {
                    net::async_socket_writer(broker_receiver, broker_socket_write_half, |msg| {
                        debug!(log_clone, "sending broker message"; "msg" => ?msg);
                    })
                    .await
                    .inspect_err(
                        |err| debug!(log_clone, "error writing broker message"; "err" => ?err),
                    )
                    .context("writing to broker")
                });

                // Spawn a task for the artifact_pusher.
                artifact_pusher::start_task(
                    &mut join_set,
                    artifact_pusher_receiver,
                    broker_addr,
                    artifact_upload_tracker.clone(),
                );
            } else {
                // We don't have a broker_addr, which means we're in standalone mode.
                standalone = true;

                // Drop the receivers for the artifact_pusher and the broker. We're not going to be
                // sending messages to their corresponding senders (at least, we better not be!).
                drop(artifact_pusher_receiver);
                drop(broker_receiver);
            }

            // Spawn a task for the router.
            router::start_task(
                &mut join_set,
                standalone,
                slots,
                local_broker_receiver,
                broker_sender,
                artifact_pusher_sender,
                local_worker_sender.clone(),
            );

            // Start the local_worker.
            {
                let cache_root = cache_dir.join::<local_worker::WorkerCacheDir>(LOCAL_WORKER_DIR);
                let mount_dir = cache_root.join::<local_worker::MountDir>("mount");
                let tmpfs_dir = cache_root.join::<local_worker::TmpfsDir>("upper");
                let cache_root = cache_root.join::<local_worker::CacheDir>("artifacts");
                let blob_dir = cache_root.join::<local_worker::BlobDir>("blob/sha256");

                // Create the local_worker's cache. This is the same cache as the "real" worker
                // uses.
                let local_worker_cache = local_worker::Cache::new(
                    local_worker::StdFs,
                    cache_root,
                    cache_size,
                    log.clone(),
                );

                // Create the local_worker's deps. This the same adapter as the "real" worker uses.
                let local_worker_dispatcher_adapter = local_worker::DispatcherAdapter::new(
                    local_worker_sender,
                    inline_limit,
                    log.clone(),
                    mount_dir,
                    tmpfs_dir,
                    blob_dir,
                )?;

                // Create an ArtifactFetcher for the local_worker that just forwards requests to
                // the router.
                struct ArtifactFetcher(router::Sender);
                impl local_worker::ArtifactFetcher for ArtifactFetcher {
                    fn start_artifact_fetch(&mut self, digest: Sha256Digest, path: PathBuf) {
                        self.0
                            .send(router::Message::LocalWorkerStartArtifactFetch(digest, path))
                            .ok();
                    }
                }
                let local_worker_artifact_fetcher = ArtifactFetcher(local_broker_sender.clone());

                // Create a BrokerSender for the local_worker that just forwards messages to
                // the router.
                struct BrokerSender(router::Sender);
                impl local_worker::BrokerSender for BrokerSender {
                    fn send_message_to_broker(&mut self, msg: WorkerToBroker) {
                        self.0.send(router::Message::LocalWorker(msg)).ok();
                    }
                }
                let worker_broker_sender = BrokerSender(local_broker_sender.clone());

                // Create the actual local_worker.
                let mut worker_dispatcher = local_worker::Dispatcher::new(
                    local_worker_dispatcher_adapter,
                    local_worker_artifact_fetcher,
                    worker_broker_sender,
                    local_worker_cache,
                    slots,
                );

                // Spawn a task for the local_worker.
                join_set.spawn(sync::channel_reader(local_worker_receiver, move |msg| {
                    worker_dispatcher.receive_message(msg)
                }));
            }

            Ok((
                ClientState {
                    local_broker_sender,
                    layer_builder: LayerBuilder::new(cache_dir, project_dir),
                    artifact_upload_tracker,
                    image_download_tracker,
                    container_image_depot,
                    log,
                    locked: Mutex::new(ClientStateLocked {
                        digest_repo,
                        processed_artifact_paths: HashSet::default(),
                        cached_layers: HashMap::new(),
                    }),
                },
                join_set,
            ))
        }

        let (log, activation_handle) = self.state_machine.try_to_begin_activation()?;

        let result = try_to_start(
            log,
            broker_addr,
            project_dir,
            state_dir,
            cache_dir,
            container_image_depot_cache_dir,
            cache_size,
            inline_limit,
            slots,
        )
        .await;
        match result {
            Ok((state, mut join_set)) => {
                let log = state.log.clone();
                debug!(log, "client started successfully");
                activation_handle.activate(state);
                let state_machine_clone = self.state_machine.clone();
                task::spawn(async move {
                    while let Some(res) = join_set.join_next().await {
                        match res {
                            Err(err) => {
                                // This means that the task was either cancelled or it panicked.
                                warn!(log, "task join failed"; "err" => ?err);
                                state_machine_clone.fail(err.to_string()).assert_is_true();
                                return;
                            }
                            Ok(Err(err)) => {
                                // One of the tasks ran into an error. Log it and return.
                                debug!(log, "task completed with error"; "err" => ?err);
                                state_machine_clone.fail(err.to_string()).assert_is_true();
                                return;
                            }
                            Ok(Ok(())) => {
                                // We ignore Ok(_) because we expect to hear about the real error later.
                                continue;
                            }
                        }
                    }
                    // Somehow we didn't get a real error. That's not good!
                    warn!(log, "all tasks exited, but none completed with an error");
                    state_machine_clone
                        .fail("client unexpectedly exited prematurely".to_string())
                        .assert_is_true();
                });
                Ok(())
            }
            Err(err) => {
                activation_handle.fail(err.to_string());
                Err(err)
            }
        }
    }

    pub async fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        self.state_machine.active()?.add_layer(layer).await
    }

    pub async fn run_job(&self, spec: JobSpec) -> Result<(ClientJobId, JobOutcomeResult)> {
        let (state, watcher) = self.state_machine.active_with_watcher()?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        debug!(state.log, "run_job"; "spec" => ?spec);

        let mut layers = spec.layers;
        let mut initial_env = Default::default();
        let mut image_working_directory = None;
        if let Some(image_spec) = spec.image {
            let image = state
                .get_container_image(&image_spec.name, &image_spec.tag)
                .await?;
            let image_config = ImageConfig {
                layers: image.layers.clone(),
                environment: image.env().cloned(),
                working_directory: image.working_dir().map(From::from),
            };
            let image_name = format!("{}:{}", &image_spec.name, &image_spec.tag);
            let image = ConvertedImage::new(&image_name, image_config);
            if image_spec.use_layers {
                let end = mem::take(&mut layers);
                for layer in image.layers()? {
                    layers.push(state.add_layer(layer).await?);
                }
                layers.extend(end);
            }
            if image_spec.use_environment {
                initial_env = image.environment()?;
            }
            if image_spec.use_working_directory {
                image_working_directory = Some(image.working_directory()?);
            }
        }
        if image_working_directory.is_some() && spec.working_directory.is_some() {
            bail!("can't provide both `working_directory` and `image.use_working_directory`");
        }

        let working_directory = image_working_directory
            .or(spec.working_directory)
            .ok_or_else(|| anyhow!("no working_directory provided"))?;

        let spec = maelstrom_base::JobSpec {
            program: spec.program,
            arguments: spec.arguments,
            environment: environment_eval(initial_env, spec.environment, std_env_lookup)?,
            layers: layers.try_into().map_err(|_| anyhow!("missing layers"))?,
            devices: spec.devices,
            mounts: spec.mounts,
            network: spec.network,
            enable_writable_file_system: spec.enable_writable_file_system,
            working_directory,
            user: spec.user,
            group: spec.group,
            timeout: spec.timeout,
            estimated_duration: spec.estimated_duration,
        };
        state
            .local_broker_sender
            .send(router::Message::RunJob(spec, sender))?;
        watcher.wait(receiver).await
    }

    pub async fn introspect(&self) -> Result<IntrospectResponse> {
        let (state, watcher) = self.state_machine.active_with_watcher()?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        state
            .local_broker_sender
            .send(router::Message::GetJobStateCounts(sender))?;
        let job_state_counts = watcher.wait(receiver).await?;
        let artifact_uploads = state.artifact_upload_tracker.get_remote_progresses();
        let image_downloads = state.image_download_tracker.get_remote_progresses();
        Ok(IntrospectResponse {
            job_state_counts,
            artifact_uploads,
            image_downloads,
        })
    }
}
