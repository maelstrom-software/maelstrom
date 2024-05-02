mod layer_builder;
mod state_machine;

use crate::{
    artifact_pusher::{self, ArtifactUploadTracker},
    digest_repo::DigestRepository,
    router,
};
use anyhow::{Context as _, Result};
use async_trait::async_trait;
use layer_builder::LayerBuilder;
use maelstrom_base::{
    proto::{Hello, WorkerToBroker},
    stats::JobStateCounts,
    ArtifactType, ClientJobId, JobOutcomeResult, JobSpec, Sha256Digest,
};
use maelstrom_client_base::{
    spec::Layer, ArtifactUploadProgress, ProjectDir, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR,
};
use maelstrom_container::{ContainerImage, ContainerImageDepot, NullProgressTracker};
use maelstrom_util::{
    async_fs,
    config::common::{BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots},
    ext::BoolExt,
    log::LoggerFactory,
    net,
    root::{CacheDir, RootBuf},
    sync,
};
use maelstrom_worker::local_worker;
use slog::{debug, Logger};
use state_machine::StateMachine;
use std::{
    collections::{HashMap, HashSet},
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
    upload_tracker: ArtifactUploadTracker,
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
        cache_dir: RootBuf<CacheDir>,
        container_image_depot_cache_dir: PathBuf,
        cache_size: CacheSize,
        inline_limit: InlineLimit,
        slots: Slots,
    ) -> Result<()> {
        async fn file_logger(
            log_level: LogLevel,
            fs: &async_fs::Fs,
            cache_dir: &Path,
        ) -> Result<Logger> {
            let log_file = fs
                .open_or_create_file(cache_dir.join("client-process.log"))
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
            cache_dir: RootBuf<CacheDir>,
            container_image_depot_cache_dir: PathBuf,
            cache_size: CacheSize,
            inline_limit: InlineLimit,
            slots: Slots,
        ) -> Result<(ClientState, JoinSet<Result<()>>)> {
            let fs = async_fs::Fs::new();

            // Make sure the cache dir exists before we try to put a log file in.
            fs.create_dir_all(&cache_dir).await?;

            // Ensure we have a logger. If this program was started by a user on the shell, then a
            // logger will have been provided. Otherwise, open a log file in the cache directory
            // and log there.
            let log = match log {
                LoggerFactory::FromLogger(log) => log,
                LoggerFactory::FromLevel(level) => {
                    file_logger(level, &fs, cache_dir.as_ref()).await?
                }
            };
            debug!(log, "client starting";
                "broker_addr" => ?broker_addr,
                "project_dir" => ?project_dir.as_path(),
                "cache_dir" => ?cache_dir.as_path(),
                "container_image_depot_cache_dir" => ?container_image_depot_cache_dir,
                "cache_size" => ?cache_size,
                "inline_limit" => ?inline_limit,
                "slots" => ?slots,
            );

            // Ensure all of the appropriate subdirectories have been created in the cache
            // directory.
            const LOCAL_WORKER_DIR: &str = "local-worker";
            for d in [STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR, LOCAL_WORKER_DIR] {
                fs.create_dir_all(cache_dir.join(d)).await?;
            }

            // Create standalone sub-components.
            let container_image_depot =
                ContainerImageDepot::new(&project_dir, container_image_depot_cache_dir)?;
            let digest_repo = DigestRepository::new(&cache_dir);
            let upload_tracker = ArtifactUploadTracker::default();

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
                        move |msg| {
                            debug!(log_clone, "received broker message"; "msg" => ?msg);
                            router::Message::Broker(msg)
                        },
                    )
                    .await
                    .with_context(|| "Reading from broker")
                });

                // Spawn a task to read from the broker's channel and write to the socket.
                let log_clone = log.clone();
                join_set.spawn(async move {
                    net::async_socket_writer(
                        broker_receiver,
                        broker_socket_write_half,
                        move |msg| {
                            debug!(log_clone, "sending broker message"; "msg" => ?msg);
                        },
                    )
                    .await
                    .with_context(|| "Writing to broker")
                });

                // Spawn a task for the artifact_pusher.
                artifact_pusher::start_task(
                    &mut join_set,
                    artifact_pusher_receiver,
                    broker_addr,
                    upload_tracker.clone(),
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
                let cache_root = cache_dir.join(LOCAL_WORKER_DIR);
                let mount_dir = cache_root
                    .join("mount")
                    .transmute::<local_worker::MountDir>();
                let tmpfs_dir = cache_root
                    .join("upper")
                    .transmute::<local_worker::TmpfsDir>();
                let cache_root = cache_root.join("artifacts");
                let blob_dir = cache_root
                    .join("blob/sha256")
                    .transmute::<local_worker::BlobDir>();

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
                    upload_tracker,
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
            cache_dir,
            container_image_depot_cache_dir,
            cache_size,
            inline_limit,
            slots,
        )
        .await;
        match result {
            Ok((state, mut join_set)) => {
                activation_handle.activate(state);
                let state_machine_clone = self.state_machine.clone();
                task::spawn(async move {
                    while let Some(res) = join_set.join_next().await {
                        // We ignore Ok(_) because we expect to hear about the real error later.
                        if let Err(err) = res {
                            state_machine_clone.fail(err.to_string()).assert_is_true();
                            return;
                        }
                    }
                    // Somehow we didn't get a real error. That's not good!
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

    pub async fn add_artifact(&self, path: &Path) -> Result<Sha256Digest> {
        self.state_machine.active()?.add_artifact(path).await
    }

    pub async fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        self.state_machine.active()?.add_layer(layer).await
    }

    pub async fn get_container_image(&self, name: &str, tag: &str) -> Result<ContainerImage> {
        let state = self.state_machine.active()?;
        debug!(state.log, "get_container_image"; "name" => name, "tag" => tag);
        state
            .container_image_depot
            .get_container_image(name, tag, NullProgressTracker)
            .await
    }

    pub async fn run_job(&self, spec: JobSpec) -> Result<(ClientJobId, JobOutcomeResult)> {
        let (state, watcher) = self.state_machine.active_with_watcher()?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        debug!(state.log, "run_job"; "spec" => ?spec);
        state
            .local_broker_sender
            .send(router::Message::RunJob(spec, sender))?;
        watcher.wait(receiver).await
    }

    pub async fn wait_for_outstanding_jobs(&self) -> Result<()> {
        let (state, watcher) = self.state_machine.active_with_watcher()?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        debug!(state.log, "wait_for_outstanding_jobs");
        state
            .local_broker_sender
            .send(router::Message::NotifyWhenAllJobsComplete(sender))?;
        watcher.wait(receiver).await
    }

    pub async fn get_job_state_counts(&self) -> Result<JobStateCounts> {
        let (state, watcher) = self.state_machine.active_with_watcher()?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        state
            .local_broker_sender
            .send(router::Message::GetJobStateCounts(sender))?;
        watcher.wait(receiver).await
    }

    pub async fn get_artifact_upload_progress(&self) -> Result<Vec<ArtifactUploadProgress>> {
        Ok(self
            .state_machine
            .active()?
            .upload_tracker
            .get_artifact_upload_progress()
            .await)
    }
}
