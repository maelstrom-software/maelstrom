use super::{Deps, Message, Preparer};
use crate::{
    client::{layer_builder::LayerBuilder, Uploader},
    progress::{LazyProgress, ProgressTracker},
};
use anyhow::Result;
use maelstrom_base::JobSpec;
use maelstrom_client_base::spec::{
    self, ContainerSpec, ConvertedImage, EnvironmentSpec, ImageConfig, LayerSpec,
};
use maelstrom_container::ContainerImageDepot;
use maelstrom_util::sync;
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};
use tokio::{
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
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
    max_pending_layer_builds: NonZeroUsize,
    sender: Sender,
    receiver: Receiver,
    container_image_depot: ContainerImageDepot,
    image_download_tracker: ProgressTracker,
    layer_builder: LayerBuilder,
    uploader: Uploader,
) {
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
    type AddContainerHandle = oneshot::Sender<Result<Option<ContainerSpec>, Self::Error>>;
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

    fn container_added(
        &self,
        handle: Self::AddContainerHandle,
        result: Result<Option<ContainerSpec>, Self::Error>,
    ) {
        let _ = handle.send(result);
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
