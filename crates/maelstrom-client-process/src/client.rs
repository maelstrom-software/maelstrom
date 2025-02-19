pub mod layer_builder;

use crate::{artifact_pusher, preparer, progress::ProgressTracker, router};
use anyhow::{anyhow, bail, Context as _, Error, Result};
use maelstrom_base::proto::Hello;
use maelstrom_client_base::{
    spec::{self, ContainerSpec},
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, IntrospectResponse, JobStatus, ProjectDir,
    StateDir, MANIFEST_DIR, STUB_MANIFEST_DIR, SYMLINK_MANIFEST_DIR,
};
use maelstrom_container::ContainerImageDepotDir;
use maelstrom_util::{
    async_fs,
    config::common::{ArtifactTransferStrategy, BrokerAddr, CacheSize, InlineLimit, Slots},
    net::{self, AsRawFdExt as _},
    root::RootBuf,
    signal,
};
use maelstrom_worker::local_worker;
use slog::{debug, o, warn, Logger};
use std::{future::Future, pin::Pin};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, RwLock},
    task::{self, JoinHandle, JoinSet},
};

#[derive(Default)]
pub struct Client(RwLock<Option<ClientState>>);

struct ClientState {
    router_sender: router::Sender,
    artifact_upload_tracker: ProgressTracker,
    image_download_tracker: ProgressTracker,
    log: Logger,
    preparer_sender: preparer::task::Sender,
    clean_up: Vec<Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>>,
}

/// For files under this size, the data is stashed in the manifest rather than uploaded separately
const MANIFEST_INLINE_LIMIT: u64 = 200 * 1024;

/// Maximum number of layers to build simultaneously
const MAX_PENDING_LAYER_BUILDS: usize = 10;

impl Client {
    #[allow(clippy::too_many_arguments)]
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
        artifact_transfer_strategy: ArtifactTransferStrategy,
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
                MAX_PENDING_LAYER_BUILDS * 3 +
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

        // Create shared standalone sub-components.
        let artifact_upload_tracker = ProgressTracker::default();
        let image_download_tracker = ProgressTracker::default();

        // Create the JoinSet we're going to put tasks in. If we bail early from this function,
        // we'll abort all tasks we have started thus far.
        let mut join_set = JoinSet::new();

        // Create all of the channels we're going to need to connect everything up.
        let (artifact_pusher_sender, artifact_pusher_receiver) = artifact_pusher::channel();
        let (broker_sender, broker_receiver) = mpsc::unbounded_channel();
        let (local_worker_sender, local_worker_receiver) = local_worker::channel();
        let (preparer_sender, preparer_receiver) = preparer::task::channel();
        let (router_sender, router_receiver) = router::channel();

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
            net::write_message_to_async_socket(&mut broker_socket_write_half, Hello::Client, &log)
                .await?;

            // Spawn a task to read from the socket and write to the router's channel.
            join_set.spawn(net::async_socket_reader(
                broker_socket_read_half,
                router_sender.clone(),
                router::Message::Broker,
                log.new(o!("task" => "broker socket reader")),
                "reading from broker socket",
            ));

            // Spawn a task to read from the broker's channel and write to the socket.
            join_set.spawn(net::async_socket_writer(
                broker_receiver,
                broker_socket_write_half,
                log.new(o!("task" => "broker socket writer")),
                "writing to broker socket",
            ));

            // Spawn a task for the artifact_pusher.
            artifact_pusher::start_task(
                artifact_transfer_strategy,
                broker_addr,
                &mut join_set,
                log.new(o!("task" => "artifact pusher")),
                artifact_pusher_receiver,
                artifact_upload_tracker.clone(),
            )?;
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
            artifact_pusher_sender,
            broker_sender,
            &mut join_set,
            local_worker_sender.clone(),
            router_receiver,
            standalone,
        );

        // Spawn a task for the preparer.
        preparer::task::start(
            accept_invalid_remote_container_tls_certs,
            cache_dir.clone(),
            container_image_depot_cache_dir,
            image_download_tracker.clone(),
            &mut join_set,
            log.clone(),
            MANIFEST_INLINE_LIMIT,
            MAX_PENDING_LAYER_BUILDS.try_into().unwrap(),
            project_dir,
            preparer_receiver,
            router_sender.clone(),
            preparer_sender.clone(),
        )?;

        // Start the local worker.
        let router_sender_clone_1 = router_sender.clone();
        let router_sender_clone_2 = router_sender.clone();
        let local_worker_handle = local_worker::start_task(
            move |digest| {
                router_sender_clone_1.send(router::Message::LocalWorkerStartArtifactFetch(digest))
            },
            move |msg| router_sender_clone_2.send(router::Message::LocalWorker(msg)),
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

        Ok((
            ClientState {
                router_sender,
                artifact_upload_tracker,
                image_download_tracker,
                log,
                preparer_sender,
                clean_up: Default::default(),
            },
            join_set,
            local_worker_handle,
        ))
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
        artifact_transfer_strategy: ArtifactTransferStrategy,
    ) -> Result<()> {
        let mut guard = self.0.write().await;
        if guard.is_some() {
            bail!("client already started");
        }

        let (mut state, mut join_set, local_worker_handle) = Self::try_to_start(
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
            artifact_transfer_strategy,
        )
        .await?;

        let shutdown_sender = state.router_sender.clone();
        let fail = move |msg: String| {
            let _ = shutdown_sender.send(router::Message::Shutdown(anyhow!(msg)));
        };

        let log = state.log.clone();
        join_set.spawn(async move {
            let signal = signal::wait_for_signal(log.clone()).await;
            Err(anyhow!("received signal {signal}"))
        });

        let log = state.log.clone();
        debug!(log, "client started successfully");

        let shutdown_sender = state.router_sender.clone();
        let log_clone = log.clone();
        state.clean_up.push(Box::pin(async move {
            let _ = shutdown_sender.send(router::Message::Shutdown(anyhow!("connection closed")));
            let err = local_worker_handle.await.unwrap();
            debug!(log_clone, "local worker shut down"; "error" => %err);
        }));

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

        *guard = Some(state);
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        let mut guard = self.0.write().await;
        let state = guard.take().ok_or_else(|| anyhow!("client not started"))?;

        // Even though we just took the state, wait to drop the lock until we're done shutting
        // down. This will block a subsequent start, if there were to be one.
        for work in state.clean_up {
            work.await;
        }

        Ok(())
    }

    pub async fn run_job(
        &self,
        spec: spec::JobSpec,
    ) -> Result<futures::channel::mpsc::UnboundedReceiver<JobStatus>> {
        let guard = self.0.read().await;
        let state = guard
            .as_ref()
            .ok_or_else(|| anyhow!("client not started"))?;

        debug!(state.log, "run_job"; "spec" => ?spec);

        let (sender, receiver) = oneshot::channel();
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
        let guard = self.0.read().await;
        let state = guard
            .as_ref()
            .ok_or_else(|| anyhow!("client not started"))?;

        debug!(state.log, "add_container"; "name" => ?name, "container" => ?container);

        let (sender, receiver) = oneshot::channel();
        state
            .preparer_sender
            .send(preparer::Message::AddContainer(sender, name, container))?;

        if let Some(existing) = receiver.await? {
            debug!(state.log, "add_container replacing existing"; "existing" => ?existing);
        }

        Ok(())
    }

    pub async fn introspect(&self) -> Result<IntrospectResponse> {
        let guard = self.0.read().await;
        let state = guard
            .as_ref()
            .ok_or_else(|| anyhow!("client not started"))?;

        let artifact_uploads = state.artifact_upload_tracker.get_remote_progresses();
        let image_downloads = state.image_download_tracker.get_remote_progresses();
        Ok(IntrospectResponse {
            artifact_uploads,
            image_downloads,
        })
    }
}
