use super::{Deps, Message, Preparer};
use crate::{
    client::{layer_builder::LayerBuilder, ClientStateLocked, Uploader},
    progress::{LazyProgress, ProgressTracker},
    router,
};
use anyhow::Result;
use maelstrom_base::{self as base};
use maelstrom_client_base::spec::{
    self, ContainerSpec, ConvertedImage, EnvironmentSpec, ImageConfig, LayerSpec,
};
use maelstrom_container::ContainerImageDepot;
use maelstrom_util::sync;
use slog::Logger;
use std::{collections::BTreeMap, sync::Arc};
use tokio::{
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot, Mutex, Semaphore,
    },
    task::{self, JoinSet},
};

pub type Sender = UnboundedSender<Message<Adapter>>;
pub type Receiver = UnboundedReceiver<Message<Adapter>>;

pub fn channel() -> (Sender, Receiver) {
    mpsc::unbounded_channel()
}

#[allow(clippy::too_many_arguments)]
pub fn start(
    join_set: &mut JoinSet<Result<()>>,
    sender: Sender,
    receiver: Receiver,
    container_image_depot: Arc<ContainerImageDepot>,
    image_download_tracker: ProgressTracker,
    layer_builder: Arc<LayerBuilder>,
    layer_building_semaphore: Arc<Semaphore>,
    log: Logger,
    locked: Arc<Mutex<ClientStateLocked>>,
    router_sender: router::Sender,
) {
    let adapter = Adapter {
        container_image_depot,
        image_download_tracker,
        sender,
        layer_builder,
        layer_building_semaphore,
        log,
        locked,
        router_sender,
    };
    let mut preparer = Preparer::new(adapter);
    join_set.spawn(sync::channel_reader(receiver, move |msg| {
        preparer.receive_message(msg)
    }));
}

pub struct Adapter {
    pub container_image_depot: Arc<ContainerImageDepot>,
    pub image_download_tracker: ProgressTracker,
    pub sender: Sender,
    pub layer_builder: Arc<LayerBuilder>,
    pub layer_building_semaphore: Arc<Semaphore>,
    pub log: Logger,
    pub locked: Arc<Mutex<ClientStateLocked>>,
    pub router_sender: router::Sender,
}

impl Deps for Adapter {
    type PrepareJobHandle = oneshot::Sender<Result<base::JobSpec, Self::Error>>;
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

    fn job_prepared(
        &self,
        handle: Self::PrepareJobHandle,
        result: Result<base::JobSpec, Self::Error>,
    ) {
        let _ = handle.send(result);
    }

    fn container_added(&self, handle: Self::AddContainerHandle, old: Option<ContainerSpec>) {
        let _ = handle.send(old);
    }

    fn get_image(&self, name: String) {
        let name_clone = name.clone();
        let tracker_clone = self.image_download_tracker.clone();
        let lazy_progress =
            LazyProgress::new(move |size| tracker_clone.new_task(&name_clone, size));
        let depot_clone = self.container_image_depot.clone();
        let sender_clone = self.sender.clone();
        task::spawn(async move {
            let result = depot_clone.get_container_image(&name, lazy_progress).await;
            let _ = sender_clone.send(Message::GotImage(
                name.clone(),
                result.map_err(|err| err.to_string()).map(|image| {
                    let image_config = ImageConfig {
                        layers: image.layers.clone(),
                        environment: image.env().cloned(),
                        working_directory: image.working_dir().map(From::from),
                    };
                    ConvertedImage::new(&name, image_config)
                }),
            ));
        });
    }

    fn build_layer(&self, spec: LayerSpec) {
        let uploader = Uploader {
            log: self.log.clone(),
            router_sender: self.router_sender.clone(),
            locked: self.locked.clone(),
        };
        let layer_builder = self.layer_builder.clone();
        let sem = self.layer_building_semaphore.clone();
        let sender_clone = self.sender.clone();
        task::spawn(async move {
            let spec_clone = spec.clone();
            let build_fn = async {
                let _permit = sem.acquire().await.unwrap();
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
