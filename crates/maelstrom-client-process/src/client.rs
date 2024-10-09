mod layer_builder;
mod layer_cache;
mod state_machine;

use crate::{
    artifact_pusher,
    digest_repo::DigestRepository,
    progress::{LazyProgress, ProgressTracker},
    router,
};
use anyhow::{anyhow, bail, Context as _, Result};
use assert_matches::assert_matches;
use async_trait::async_trait;
use layer_builder::LayerBuilder;
use layer_cache::{CacheResult, LayerCache};
use maelstrom_base::{
    proto::{Hello, WorkerToBroker},
    ArtifactType, Sha256Digest,
};
use maelstrom_client_base::{
    spec::{
        environment_eval, std_env_lookup, ContainerRef, ContainerSpec, ConvertedImage, ImageConfig,
        JobSpec, LayerSpec,
    },
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, IntrospectResponse, JobStatus, ProjectDir,
    StateDir, MANIFEST_DIR, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR,
};
use maelstrom_container::{
    self as container, ContainerImage, ContainerImageDepot, ContainerImageDepotDir,
};
use maelstrom_util::{
    async_fs,
    config::common::{BrokerAddr, CacheSize, InlineLimit, Slots},
    net,
    root::RootBuf,
};
use maelstrom_worker::local_worker;
use slog::{debug, info, warn, Logger};
use state_machine::StateMachine;
use std::future::Future;
use std::pin::Pin;
use std::{
    collections::{HashMap, HashSet},
    mem,
    path::Path,
    sync::Arc,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
    task::{self, JoinSet},
};

#[derive(Clone)]
pub struct Client {
    state_machine: Arc<StateMachine<(), ClientState>>,
    clean_up: Arc<CleanUpWork>,
}

struct ClientState {
    local_broker_sender: router::Sender,
    layer_builder: Arc<LayerBuilder>,
    artifact_upload_tracker: ProgressTracker,
    image_download_tracker: ProgressTracker,
    container_image_depot: ContainerImageDepot,
    log: Logger,
    locked: Arc<Mutex<ClientStateLocked>>,
    layer_building_semaphore: Arc<tokio::sync::Semaphore>,
}

struct ClientStateLocked {
    digest_repo: DigestRepository,
    processed_artifact_digests: HashSet<Sha256Digest>,
    cached_layers: LayerCache,
    containers: HashMap<String, ContainerSpec>,
}

#[derive(Default)]
struct CleanUpWork {
    work: std::sync::Mutex<Vec<Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>>>,
}

impl CleanUpWork {
    async fn execute(&self) {
        let work = std::mem::take(&mut *self.work.lock().unwrap());
        for w in work {
            w.await;
        }
    }

    fn add_work(&self, fut: impl Future<Output = ()> + Send + Sync + 'static) {
        let mut work = self.work.lock().unwrap();
        work.push(Box::pin(fut));
    }
}

#[derive(Clone)]
struct Uploader {
    log: slog::Logger,
    local_broker_sender: router::Sender,
    locked: Arc<Mutex<ClientStateLocked>>,
}

impl Uploader {
    async fn upload(&self, path: &Path) -> Result<Sha256Digest> {
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
        if !locked.processed_artifact_digests.contains(&digest) {
            locked.processed_artifact_digests.insert(digest.clone());
            self.local_broker_sender
                .send(router::Message::AddArtifact(path, digest.clone()))?;
        }
        Ok(digest)
    }
}

#[async_trait]
impl<'a> maelstrom_util::manifest::DataUpload for &'a Uploader {
    async fn upload(&mut self, path: &Path) -> Result<Sha256Digest> {
        Uploader::upload(self, path).await
    }
}

impl ClientState {
    fn build_layer(&self, layer: LayerSpec) {
        let uploader = Uploader {
            log: self.log.clone(),
            local_broker_sender: self.local_broker_sender.clone(),
            locked: self.locked.clone(),
        };
        let layer_builder = self.layer_builder.clone();
        let sem = self.layer_building_semaphore.clone();
        let locked = self.locked.clone();
        tokio::task::spawn(async move {
            let build_fn = async {
                let _permit = sem.acquire().await.unwrap();
                let (artifact_path, artifact_type) =
                    layer_builder.build_layer(layer.clone(), &uploader).await?;
                let artifact_digest = uploader.upload(&artifact_path).await?;
                Result::<_>::Ok((artifact_digest, artifact_type))
            };
            let res = build_fn.await;
            locked.lock().await.cached_layers.fill(&layer, res);
        });
    }

    async fn get_layers(
        &self,
        layers: Vec<LayerSpec>,
    ) -> Result<Vec<(Sha256Digest, ArtifactType)>> {
        debug!(self.log, "get_layers"; "layers" => ?layers);

        let mut locked = self.locked.lock().await;
        let mut cache_results: Vec<_> = layers
            .iter()
            .map(|layer| locked.cached_layers.get(layer))
            .collect();
        drop(locked);

        // Kick off tasks to build any layers that need to be
        for (result, layer) in cache_results.iter_mut().zip(&layers) {
            if let CacheResult::Build(r) = result {
                *result = CacheResult::Wait(r.clone());
                self.build_layer(layer.clone());
            }
        }

        // Wait for any layers being built to complete
        for result in &mut cache_results {
            if let CacheResult::Wait(w) = result {
                let res = w.recv().await?;
                *result = CacheResult::Success(res);
            }
        }

        // Now we should have all the layers
        Ok(cache_results
            .into_iter()
            .map(|r| assert_matches!(r, CacheResult::Success(r) => r))
            .collect())
    }

    async fn clear_cached_layers(&self) -> Result<()> {
        debug!(self.log, "clear_cached_layers");

        let mut locked = self.locked.lock().await;
        locked.cached_layers.clear()
    }

    async fn get_container_image(&self, name: &str) -> Result<ContainerImage> {
        let dl_name = name.to_owned();

        let tracker = self.image_download_tracker.clone();
        let dl_name_clone = dl_name.clone();
        let prog = LazyProgress::new(move |size| tracker.new_task(&dl_name_clone, size));

        let res = self
            .container_image_depot
            .get_container_image(name, prog)
            .await;
        self.image_download_tracker.remove_task(&dl_name);
        res
    }
}

/// For files under this size, the data is stashed in the manifest rather than uploaded separately
const MANIFEST_INLINE_LIMIT: u64 = 200 * 1024;

/// Maximum number of layers to build simultaneously
const MAX_IN_FLIGHT_LAYER_BUILDS: usize = 10;

impl Client {
    pub fn new() -> Self {
        Self {
            state_machine: Arc::new(StateMachine::new(())),
            clean_up: Default::default(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        &self,
        log: Logger,
        broker_addr: Option<BrokerAddr>,
        project_dir: RootBuf<ProjectDir>,
        state_dir: RootBuf<StateDir>,
        cache_dir: RootBuf<CacheDir>,
        container_image_depot_cache_dir: RootBuf<ContainerImageDepotDir>,
        cache_size: CacheSize,
        inline_limit: InlineLimit,
        slots: Slots,
        accept_invalid_remote_container_tls_certs: AcceptInvalidRemoteContainerTlsCerts,
    ) -> Result<()> {
        async fn try_to_start(
            log: Logger,
            broker_addr: Option<BrokerAddr>,
            project_dir: RootBuf<ProjectDir>,
            state_dir: RootBuf<StateDir>,
            cache_dir: RootBuf<CacheDir>,
            container_image_depot_cache_dir: RootBuf<ContainerImageDepotDir>,
            cache_size: CacheSize,
            inline_limit: InlineLimit,
            slots: Slots,
            accept_invalid_remote_container_tls_certs: AcceptInvalidRemoteContainerTlsCerts,
        ) -> Result<(
            ClientState,
            JoinSet<Result<()>>,
            tokio::task::JoinHandle<()>,
        )> {
            let fs = async_fs::Fs::new();

            // Make sure the state dir exists before we try to put a log file in.
            fs.create_dir_all(&state_dir).await?;

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

            let extra = 1 /* rpc connection */ +
                /* 1 for the manifest, 1 for file we are reading, 1 for directory we are listing */
                MAX_IN_FLIGHT_LAYER_BUILDS * 3 +
                artifact_pusher::MAX_CLIENT_UPLOADS * 2; // 1 for the socket, 1 for the file.
            local_worker::check_open_file_limit(&log, slots, extra as u64)?;

            // We recreate all the manifests every time. We delete it here to clean-up unused
            // manifests and leaked temporary files.
            if fs.exists((**cache_dir).join(MANIFEST_DIR)).await {
                fs.remove_dir_all((**cache_dir).join(MANIFEST_DIR)).await?;
            }

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
                accept_invalid_remote_container_tls_certs.into_inner(),
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
            let (local_worker_sender, mut local_worker_receiver) = mpsc::unbounded_channel();

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
                local_broker_receiver,
                broker_sender,
                artifact_pusher_sender,
                local_worker_sender.clone(),
            );

            // Start the local_worker.
            let worker_handle = {
                let cache_root = cache_dir.join::<local_worker::WorkerCacheDir>(LOCAL_WORKER_DIR);
                let mount_dir = cache_root.join::<local_worker::MountDir>("mount");
                let tmpfs_dir = cache_root.join::<local_worker::TmpfsDir>("upper");
                let cache_root = cache_root.join::<local_worker::CacheDir>("artifacts");
                let blob_dir = cache_root.join::<local_worker::BlobDir>("sha256/blob");

                // Create the local_worker's cache. This is the same cache as the "real" worker
                // uses.
                let local_worker_cache =
                    local_worker::Cache::new(local_worker::Fs, cache_root, cache_size, log.clone());

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
                impl local_worker::ArtifactFetcher<local_worker::Fs> for ArtifactFetcher {
                    fn start_artifact_fetch(
                        &mut self,
                        _cache: &impl local_worker::CacheTrait<local_worker::Fs>,
                        digest: Sha256Digest,
                    ) {
                        self.0
                            .send(router::Message::LocalWorkerStartArtifactFetch(digest))
                            .ok();
                    }
                }
                let local_worker_artifact_fetcher = ArtifactFetcher(local_broker_sender.clone());

                // Create a BrokerSender for the local_worker that just forwards messages to
                // the router.
                struct BrokerSender(Option<router::Sender>);
                impl local_worker::BrokerSender for BrokerSender {
                    fn send_message_to_broker(&mut self, msg: WorkerToBroker) {
                        if let Some(sender) = self.0.as_ref() {
                            sender.send(router::Message::LocalWorker(msg)).ok();
                        }
                    }

                    fn close(&mut self) {
                        self.0 = None;
                    }
                }
                let worker_broker_sender = BrokerSender(Some(local_broker_sender.clone()));

                // Create the actual local_worker.
                let mut worker_dispatcher = local_worker::Dispatcher::new(
                    local_worker_dispatcher_adapter,
                    local_worker_artifact_fetcher,
                    worker_broker_sender,
                    local_worker_cache,
                    slots,
                );

                let handle_worker_message =
                    |msg, worker: &mut local_worker::Dispatcher<_, _, _, _>| -> Result<()> {
                        if let local_worker::Message::Shutdown(error) = msg {
                            Err(error)
                        } else {
                            worker.receive_message(msg);
                            Ok(())
                        }
                    };

                // Spawn a task for the local_worker.
                let log_clone = log.clone();
                task::spawn(async move {
                    let shutdown_error = loop {
                        let msg = local_worker_receiver
                            .recv()
                            .await
                            .expect("missing shutdown");
                        if let Err(err) = handle_worker_message(msg, &mut worker_dispatcher) {
                            break err;
                        }
                    };

                    info!(
                        log_clone,
                        "shutting down local worker due to {shutdown_error}"
                    );
                    worker_dispatcher
                        .receive_message(local_worker::Message::Shutdown(shutdown_error));
                    info!(
                        log_clone,
                        "canceling {} running jobs",
                        worker_dispatcher.num_executing()
                    );

                    while worker_dispatcher.num_executing() > 0 {
                        let msg = local_worker_receiver
                            .recv()
                            .await
                            .expect("missing shutdown");
                        let _ = handle_worker_message(msg, &mut worker_dispatcher);
                    }

                    info!(log_clone, "local worker exiting");
                })
            };

            Ok((
                ClientState {
                    local_broker_sender,
                    layer_builder: Arc::new(LayerBuilder::new(
                        cache_dir,
                        project_dir,
                        MANIFEST_INLINE_LIMIT,
                    )),
                    artifact_upload_tracker,
                    image_download_tracker,
                    container_image_depot,
                    log,
                    locked: Arc::new(Mutex::new(ClientStateLocked {
                        digest_repo,
                        processed_artifact_digests: HashSet::default(),
                        cached_layers: LayerCache::new(),
                        containers: HashMap::new(),
                    })),
                    layer_building_semaphore: Arc::new(tokio::sync::Semaphore::new(
                        MAX_IN_FLIGHT_LAYER_BUILDS,
                    )),
                },
                join_set,
                worker_handle,
            ))
        }

        let (_, activation_handle) = self.state_machine.try_to_begin_activation()?;

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
            accept_invalid_remote_container_tls_certs,
        )
        .await;
        match result {
            Ok((state, mut join_set, worker_handle)) => {
                let shutdown_sender = state.local_broker_sender.clone();
                let state_machine_clone = self.state_machine.clone();
                let fail = move |msg: String| {
                    state_machine_clone.fail(msg.clone());
                    let _ = shutdown_sender.send(router::Message::Shutdown(anyhow!(msg)));
                };

                let log = state.log.clone();
                let fail_clone = fail.clone();
                tokio::task::spawn(async move {
                    let signal = maelstrom_worker::signals::wait_for_signal(log.clone()).await;
                    fail_clone(format!("received signal {signal}"));
                });

                let log = state.log.clone();
                debug!(log, "client started successfully");

                let shutdown_sender = state.local_broker_sender.clone();
                self.clean_up.add_work(async move {
                    let _ = shutdown_sender
                        .send(router::Message::Shutdown(anyhow!("connection closed")));
                    let _ = worker_handle.await;
                });
                activation_handle.activate(state);

                task::spawn(async move {
                    while let Some(res) = join_set.join_next().await {
                        match res {
                            Err(err) => {
                                // This means that the task was either cancelled or it panicked.
                                warn!(log, "task join failed"; "err" => ?err);
                                fail(format!("{err:?}"));
                                return;
                            }
                            Ok(Err(err)) => {
                                // One of the tasks ran into an error. Log it and return.
                                debug!(log, "task completed with error"; "err" => ?err);
                                fail(format!("{err:?}"));
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
                    fail("client unexpectedly exited prematurely".to_string());
                });
                Ok(())
            }
            Err(err) => {
                activation_handle.fail(format!("{err:?}"));
                Err(err)
            }
        }
    }

    pub async fn run_job(
        &self,
        spec: JobSpec,
    ) -> Result<futures::channel::mpsc::UnboundedReceiver<JobStatus>> {
        let state = self.state_machine.active()?;
        let (sender, receiver) = futures::channel::mpsc::unbounded();
        debug!(state.log, "run_job"; "spec" => ?spec);

        let container = match spec.container {
            ContainerRef::Name(n) => {
                let locked = state.locked.lock().await;
                locked
                    .containers
                    .get(&n)
                    .ok_or_else(|| anyhow!("container {n:?} unknown"))?
                    .clone()
            }
            ContainerRef::Inline(c) => {
                c.check_for_local_network_and_sys_mount()?;
                c
            }
        };

        let mut layers = container.layers;
        let mut initial_env = Default::default();
        let mut image_working_directory = None;
        if let Some(image_spec) = container.image {
            let image = state.get_container_image(&image_spec.name).await?;
            let image_config = ImageConfig {
                layers: image.layers.clone(),
                environment: image.env().cloned(),
                working_directory: image.working_dir().map(From::from),
            };
            let image = ConvertedImage::new(&image_spec.name, image_config);
            if image_spec.use_layers {
                let end = mem::replace(&mut layers, image.layers()?);
                layers.extend(end);
            }
            if image_spec.use_environment {
                initial_env = image.environment()?;
            }
            if image_spec.use_working_directory {
                image_working_directory = Some(image.working_directory()?);
            }
        }
        if image_working_directory.is_some() && container.working_directory.is_some() {
            bail!("can't provide both `working_directory` and `image.use_working_directory`");
        }

        let working_directory = image_working_directory.or(container.working_directory);
        let converted_layers = state.get_layers(layers).await?;

        let spec = maelstrom_base::JobSpec {
            program: spec.program,
            arguments: spec.arguments,
            environment: environment_eval(initial_env, container.environment, std_env_lookup)?,
            layers: converted_layers
                .try_into()
                .map_err(|_| anyhow!("missing layers"))?,
            mounts: container.mounts,
            network: container.network,
            root_overlay: container.root_overlay,
            working_directory,
            user: container.user,
            group: container.group,
            timeout: spec.timeout,
            estimated_duration: spec.estimated_duration,
            allocate_tty: spec.allocate_tty,
            priority: spec.priority,
        };
        state
            .local_broker_sender
            .send(router::Message::RunJob(spec, sender))?;
        Ok(receiver)
    }

    pub async fn add_container(&self, name: String, container: ContainerSpec) -> Result<()> {
        let state = self.state_machine.active()?;
        let mut locked = state.locked.lock().await;

        container.check_for_local_network_and_sys_mount()?;

        debug!(state.log, "add_container"; "name" => ?name, "container" => ?container);
        if let Some(existing) = locked.containers.insert(name, container) {
            debug!(state.log, "add_container replacing existing"; "existing" => ?existing);
        }
        Ok(())
    }

    pub async fn introspect(&self) -> Result<IntrospectResponse> {
        let state = self.state_machine.active()?;
        let artifact_uploads = state.artifact_upload_tracker.get_remote_progresses();
        let image_downloads = state.image_download_tracker.get_remote_progresses();
        Ok(IntrospectResponse {
            artifact_uploads,
            image_downloads,
        })
    }

    pub async fn clear_cached_layers(&self) -> Result<()> {
        let state = self.state_machine.active()?;
        state.clear_cached_layers().await
    }

    pub async fn shutdown(&self) {
        self.clean_up.execute().await;
    }
}
