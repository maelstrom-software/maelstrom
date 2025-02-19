use super::{Deps, Message, Preparer};
use crate::{
    client::layer_builder::LayerBuilder,
    digest_repo::DigestRepository,
    progress::{LazyProgress, ProgressTracker},
    router, util,
};
use anyhow::Result;
use async_trait::async_trait;
use maelstrom_base::{JobSpec, Sha256Digest};
use maelstrom_client_base::{
    spec::{self, ContainerSpec, ConvertedImage, EnvironmentSpec, ImageConfig, LayerSpec},
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, ProjectDir,
};
use maelstrom_container::{ContainerImageDepot, ContainerImageDepotDir};
use maelstrom_util::{async_fs, root::RootBuf, sync};
use slog::{debug, Logger};
use std::{
    collections::{BTreeMap, HashSet},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
};
use tokio::{
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot, Mutex,
    },
    task::{self, JoinSet},
};

pub type Sender = UnboundedSender<Message<Adapter>>;
pub type Receiver = UnboundedReceiver<Message<Adapter>>;

pub fn channel() -> (Sender, Receiver) {
    mpsc::unbounded_channel()
}

struct ClientStateLocked {
    digest_repo: DigestRepository,
    processed_artifact_digests: HashSet<Sha256Digest>,
}

#[derive(Clone)]
pub struct Uploader {
    log: slog::Logger,
    router_sender: router::Sender,
    locked: Arc<Mutex<ClientStateLocked>>,
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
            let (mtime, digest) = util::calculate_digest(&path).await?;
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
impl maelstrom_util::manifest::DataUpload for &Uploader {
    async fn upload(&mut self, path: &Path) -> Result<Sha256Digest> {
        Uploader::upload(self, path).await
    }
}

#[allow(clippy::too_many_arguments)]
pub fn start(
    accept_invalid_remote_container_tls_certs: AcceptInvalidRemoteContainerTlsCerts,
    cache_dir: RootBuf<CacheDir>,
    container_image_depot_cache_dir: RootBuf<ContainerImageDepotDir>,
    image_download_tracker: ProgressTracker,
    join_set: &mut JoinSet<Result<()>>,
    log: Logger,
    manifest_inline_limit: u64,
    max_pending_layer_builds: NonZeroUsize,
    project_dir: RootBuf<ProjectDir>,
    receiver: Receiver,
    router_sender: router::Sender,
    sender: Sender,
) -> Result<()> {
    let container_image_depot = ContainerImageDepot::new(
        project_dir.transmute::<maelstrom_container::ProjectDir>(),
        container_image_depot_cache_dir,
        accept_invalid_remote_container_tls_certs.into_inner(),
    )?;

    let digest_repo = DigestRepository::new(&cache_dir);
    let layer_builder = LayerBuilder::new(cache_dir, project_dir, manifest_inline_limit);
    let locked = Arc::new(Mutex::new(ClientStateLocked {
        digest_repo,
        processed_artifact_digests: HashSet::default(),
    }));
    let uploader = Uploader {
        log,
        router_sender,
        locked,
    };

    let adapter = Adapter {
        container_image_depot: Arc::new(container_image_depot),
        image_download_tracker,
        sender,
        layer_builder: Arc::new(layer_builder),
        uploader,
    };
    let mut preparer = Preparer::new(adapter, max_pending_layer_builds);
    join_set.spawn(sync::channel_reader(receiver, move |msg| {
        preparer.receive_message(msg)
    }));

    Ok(())
}

pub struct Adapter {
    pub container_image_depot: Arc<ContainerImageDepot>,
    pub image_download_tracker: ProgressTracker,
    pub sender: Sender,
    pub uploader: Uploader,
    pub layer_builder: Arc<LayerBuilder>,
}

impl Deps for Adapter {
    type PrepareJobHandle = oneshot::Sender<Result<JobSpec, Self::Error>>;
    type AddContainerHandle = oneshot::Sender<Option<ContainerSpec>>;
    type Error = String;

    fn error_from_string(err: String) -> Self::Error {
        err
    }

    fn evaluate_environment(
        &self,
        initial: BTreeMap<String, String>,
        specs: Vec<EnvironmentSpec>,
    ) -> Result<Vec<String>, Self::Error> {
        spec::environment_eval(initial, specs, spec::std_env_lookup).map_err(|err| err.to_string())
    }

    fn job_prepared(&self, handle: Self::PrepareJobHandle, result: Result<JobSpec, Self::Error>) {
        let _ = handle.send(result);
    }

    fn container_added(&self, handle: Self::AddContainerHandle, old: Option<ContainerSpec>) {
        let _ = handle.send(old);
    }

    fn get_image(&self, name: String) {
        let tracker = self.image_download_tracker.clone();
        let depot = self.container_image_depot.clone();
        let sender = self.sender.clone();
        task::spawn(async move {
            let name_clone = name.clone();
            let lazy_progress = LazyProgress::new(move |size| tracker.new_task(&name_clone, size));
            let result = depot
                .get_container_image(&name, lazy_progress)
                .await
                .map_err(|err| err.to_string())
                .map(|image| {
                    let (layers, environment, working_directory) =
                        image.into_layers_environment_and_working_directory();
                    ConvertedImage::new(
                        &name,
                        ImageConfig {
                            layers,
                            environment,
                            working_directory,
                        },
                    )
                });
            let _ = sender.send(Message::GotImage(name, result));
        });
    }

    fn build_layer(&self, spec: LayerSpec) {
        let uploader = self.uploader.clone();
        let layer_builder = self.layer_builder.clone();
        let sender_clone = self.sender.clone();
        task::spawn(async move {
            let spec_clone = spec.clone();
            let build_fn = async {
                let (artifact_path, artifact_type) =
                    layer_builder.build_layer(spec_clone, &uploader).await?;
                let artifact_digest = uploader.upload(&artifact_path).await?;
                Result::<_>::Ok((artifact_digest, artifact_type))
            };
            let _ = sender_clone.send(Message::GotLayer(
                spec,
                build_fn.await.map_err(|err| err.to_string()),
            ));
        });
    }
}
