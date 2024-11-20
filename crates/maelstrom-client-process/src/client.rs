pub mod layer_builder;
mod state_machine;

use crate::{
    artifact_pusher, digest_repo::DigestRepository, preparer, progress::ProgressTracker, router,
};
use anyhow::{anyhow, Context as _, Error, Result};
use async_trait::async_trait;
use layer_builder::LayerBuilder;
use maelstrom_base::{
    proto::{Hello, WorkerToBroker},
    Sha256Digest,
};
use maelstrom_client_base::{
    spec::{self, ContainerSpec},
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, IntrospectResponse, JobStatus, ProjectDir,
    StateDir, MANIFEST_DIR, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR,
};
use maelstrom_container::{self as container, ContainerImageDepot, ContainerImageDepotDir};
use maelstrom_util::{
    async_fs,
    config::common::{BrokerAddr, CacheSize, InlineLimit, Slots},
    net::{self, AsRawFdExt as _},
    root::RootBuf,
    signal,
};
use maelstrom_worker::local_worker;
use slog::{debug, warn, Logger};
use state_machine::StateMachine;
use std::{collections::HashSet, future::Future, path::Path, pin::Pin, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex, Semaphore},
    task::{self, JoinHandle, JoinSet},
};

#[derive(Clone)]
pub struct Client {
    state_machine: Arc<StateMachine<ClientState>>,
    clean_up: Arc<CleanUpWork>,
}

struct ClientState {
    router_sender: router::Sender,
    artifact_upload_tracker: ProgressTracker,
    image_download_tracker: ProgressTracker,
    log: Logger,
    preparer_sender: preparer::task::Sender,
}

pub struct ClientStateLocked {
    pub digest_repo: DigestRepository,
    pub processed_artifact_digests: HashSet<Sha256Digest>,
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
pub struct Uploader {
    pub log: slog::Logger,
    pub router_sender: router::Sender,
    pub locked: Arc<Mutex<ClientStateLocked>>,
}

impl Uploader {
    pub async fn upload(&self, path: &Path) -> Result<Sha256Digest> {
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
            self.router_sender
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

/// For files under this size, the data is stashed in the manifest rather than uploaded separately
const MANIFEST_INLINE_LIMIT: u64 = 200 * 1024;

/// Maximum number of layers to build simultaneously
const MAX_IN_FLIGHT_LAYER_BUILDS: usize = 10;

impl Client {
    pub fn new() -> Self {
        Self {
            state_machine: Arc::new(StateMachine::default()),
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
        ) -> Result<(ClientState, JoinSet<Result<()>>, JoinHandle<Error>)> {
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
            let (router_sender, router_receiver) = router::channel();
            let (local_worker_sender, local_worker_receiver) = local_worker::channel();

            let standalone;
            if let Some(broker_addr) = broker_addr {
                // We have a broker_addr, which means we're not in standalone mode.
                standalone = false;

                // Connect to the broker.
                let (broker_socket_read_half, mut broker_socket_write_half) =
                    TcpStream::connect(broker_addr.inner())
                        .await
                        .with_context(|| format!("failed to connect to {broker_addr}"))?
                        .set_socket_options()?
                        .into_split();
                debug!(log, "client connected to broker"; "broker_addr" => ?broker_addr);

                // Send it a Hello message.
                net::write_message_to_async_socket(
                    &mut broker_socket_write_half,
                    Hello::Client,
                    &log,
                )
                .await?;

                // Spawn a task to read from the socket and write to the router's channel.
                let log_clone = log.clone();
                let router_sender_clone = router_sender.clone();
                join_set.spawn(async move {
                    net::async_socket_reader(
                        broker_socket_read_half,
                        router_sender_clone,
                        router::Message::Broker,
                        &log_clone,
                    )
                    .await
                    .context("reading from broker")
                });

                // Spawn a task to read from the broker's channel and write to the socket.
                let log_clone = log.clone();
                join_set.spawn(async move {
                    net::async_socket_writer(broker_receiver, broker_socket_write_half, &log_clone)
                        .await
                        .context("writing to broker")
                });

                // Spawn a task for the artifact_pusher.
                artifact_pusher::start_task(
                    &mut join_set,
                    artifact_pusher_receiver,
                    broker_addr,
                    artifact_upload_tracker.clone(),
                    log.clone(),
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
                router_receiver,
                broker_sender,
                artifact_pusher_sender,
                local_worker_sender.clone(),
            );

            // Start the local worker.
            struct ArtifactFetcher(router::Sender);
            impl local_worker::ArtifactFetcher for ArtifactFetcher {
                fn start_artifact_fetch(&mut self, digest: Sha256Digest) {
                    let _ = self
                        .0
                        .send(router::Message::LocalWorkerStartArtifactFetch(digest));
                }
            }

            struct BrokerSender(Option<router::Sender>);
            impl local_worker::BrokerSender for BrokerSender {
                fn send_message_to_broker(&mut self, msg: WorkerToBroker) {
                    if let Some(sender) = self.0.as_ref() {
                        let _ = sender.send(router::Message::LocalWorker(msg));
                    }
                }
                fn close(&mut self) {
                    self.0 = None;
                }
            }

            let local_worker_handle = local_worker::start_task(
                ArtifactFetcher(router_sender.clone()),
                BrokerSender(Some(router_sender.clone())),
                local_worker::Config {
                    cache_root: cache_dir.join(LOCAL_WORKER_DIR),
                    cache_size,
                    inline_limit,
                    slots,
                },
                local_worker_receiver,
                local_worker_sender,
                &log,
            )?;

            let layer_builder = Arc::new(LayerBuilder::new(
                cache_dir,
                project_dir,
                MANIFEST_INLINE_LIMIT,
            ));
            let layer_building_semaphore = Arc::new(Semaphore::new(MAX_IN_FLIGHT_LAYER_BUILDS));
            let locked = Arc::new(Mutex::new(ClientStateLocked {
                digest_repo,
                processed_artifact_digests: HashSet::default(),
            }));

            let (preparer_sender, preparer_receiver) = mpsc::unbounded_channel();

            preparer::task::start(
                &mut join_set,
                preparer_sender.clone(),
                preparer_receiver,
                Arc::new(container_image_depot),
                image_download_tracker.clone(),
                layer_builder,
                layer_building_semaphore,
                log.clone(),
                locked,
                router_sender.clone(),
            );

            Ok((
                ClientState {
                    router_sender,
                    artifact_upload_tracker,
                    image_download_tracker,
                    log,
                    preparer_sender,
                },
                join_set,
                local_worker_handle,
            ))
        }

        let activation_handle = self.state_machine.try_to_begin_activation()?;

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
            Ok((state, mut join_set, local_worker_handle)) => {
                let shutdown_sender = state.router_sender.clone();
                let state_machine_clone = self.state_machine.clone();
                let fail = move |msg: String| {
                    state_machine_clone.fail(msg.clone());
                    let _ = shutdown_sender.send(router::Message::Shutdown(anyhow!(msg)));
                };

                let log = state.log.clone();
                let fail_clone = fail.clone();
                task::spawn(async move {
                    let signal = signal::wait_for_signal(log.clone()).await;
                    fail_clone(format!("received signal {signal}"));
                });

                let log = state.log.clone();
                debug!(log, "client started successfully");

                let shutdown_sender = state.router_sender.clone();
                let log_clone = log.clone();
                self.clean_up.add_work(async move {
                    let _ = shutdown_sender
                        .send(router::Message::Shutdown(anyhow!("connection closed")));
                    let err = local_worker_handle.await.unwrap();
                    debug!(log_clone, "local worker shut down"; "error" => %err);
                });
                activation_handle.activate(state);

                task::spawn(async move {
                    while let Some(res) = join_set.join_next().await {
                        match res {
                            Err(err) => {
                                // This means that the task was either cancelled or it panicked.
                                warn!(log, "error joining task"; "error" => %err);
                                fail(format!("{err:?}"));
                                return;
                            }
                            Ok(Err(err)) => {
                                // One of the tasks ran into an error. Log it and return.
                                debug!(log, "task completed with error"; "error" => %err);
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
        spec: spec::JobSpec,
    ) -> Result<futures::channel::mpsc::UnboundedReceiver<JobStatus>> {
        let state = self.state_machine.active()?;
        debug!(state.log, "run_job"; "spec" => ?spec);

        let (sender, receiver) = tokio::sync::oneshot::channel();
        state
            .preparer_sender
            .send(preparer::Message::PrepareJob(sender, spec))?;
        let spec = receiver.await?.map_err(Error::msg)?;

        let (sender, receiver) = futures::channel::mpsc::unbounded();
        state
            .router_sender
            .send(router::Message::RunJob(spec, sender))?;

        Ok(receiver)
    }

    pub async fn add_container(&self, name: String, container: ContainerSpec) -> Result<()> {
        let state = self.state_machine.active()?;
        debug!(state.log, "add_container"; "name" => ?name, "container" => ?container);

        container
            .check_for_local_network_and_sys_mount()
            .map_err(Error::msg)?;

        let (sender, receiver) = tokio::sync::oneshot::channel();
        state
            .preparer_sender
            .send(preparer::Message::AddContainer(sender, name, container))?;

        if let Some(existing) = receiver.await? {
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

    pub async fn shutdown(&self) {
        self.clean_up.execute().await;
    }
}
