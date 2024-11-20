pub mod task;

use maelstrom_base::{
    ArtifactType, GroupId, JobMount, JobNetwork, JobRootOverlay, JobSpec, JobTty, NonEmpty,
    Sha256Digest, Timeout, UserId, Utf8PathBuf,
};
use maelstrom_client_base::spec::{
    ContainerRef, ContainerSpec, ConvertedImage, EnvironmentSpec, ImageSpec,
    JobSpec as ClientJobSpec, LayerSpec,
};
use maelstrom_util::ext::OptionExt as _;
use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap},
    mem,
    time::Duration,
};

pub trait Deps {
    type PrepareJobHandle;
    type AddContainerHandle;
    type Error: Clone;

    fn error_from_string(err: String) -> Self::Error;

    fn evaluate_environment(
        &self,
        initial: BTreeMap<String, String>,
        specs: Vec<EnvironmentSpec>,
    ) -> Result<Vec<String>, Self::Error>;
    fn job_prepared(&self, handle: Self::PrepareJobHandle, result: Result<JobSpec, Self::Error>);
    fn container_added(
        &self,
        handle: Self::AddContainerHandle,
        old: Result<Option<ContainerSpec>, Self::Error>,
    );
    fn get_image(&self, name: String);
    fn build_layer(&self, spec: LayerSpec);
}

pub enum Message<DepsT: Deps> {
    AddContainer(DepsT::AddContainerHandle, String, ContainerSpec),
    PrepareJob(DepsT::PrepareJobHandle, ClientJobSpec),
    GotImage(String, Result<ConvertedImage, DepsT::Error>),
    GotLayer(
        LayerSpec,
        Result<(Sha256Digest, ArtifactType), DepsT::Error>,
    ),
}

pub struct Preparer<DepsT: Deps> {
    deps: DepsT,
    containers: HashMap<String, ContainerSpec>,
    images: HashMap<String, ImageEntry<DepsT>>,
    layers: HashMap<LayerSpec, LayerEntry<DepsT>>,
    jobs: HashMap<u64, Job<DepsT>>,
    next_ijid: u64,
}

enum ImageEntry<DepsT: Deps> {
    Got(Result<ConvertedImage, DepsT::Error>),
    Getting(Vec<u64>),
}

enum LayerEntry<DepsT: Deps> {
    Got(Result<(Sha256Digest, ArtifactType), DepsT::Error>),
    Getting(Vec<(bool, u64, usize)>),
}

impl<DepsT: Deps> Preparer<DepsT> {
    pub fn new(deps: DepsT) -> Self {
        Self {
            deps,
            containers: Default::default(),
            images: Default::default(),
            layers: Default::default(),
            jobs: Default::default(),
            next_ijid: Default::default(),
        }
    }

    pub fn receive_message(&mut self, msg: Message<DepsT>) {
        match msg {
            Message::PrepareJob(handle, spec) => {
                self.receive_prepare_job(handle, spec);
            }
            Message::AddContainer(handle, name, spec) => {
                self.receive_add_container(handle, name, spec);
            }
            Message::GotImage(name, result) => {
                self.receive_got_image(&name, result);
            }
            Message::GotLayer(spec, result) => {
                self.receive_got_layer(spec, result);
            }
        }
    }

    fn receive_prepare_job(&mut self, handle: DepsT::PrepareJobHandle, spec: ClientJobSpec) {
        let container = match spec.container {
            ContainerRef::Name(name) => {
                let Some(container) = self.containers.get(&name) else {
                    self.deps.job_prepared(
                        handle,
                        Err(DepsT::error_from_string(format!(
                            r#"container "{name}" unknown"#
                        ))),
                    );
                    return;
                };
                container.clone()
            }
            ContainerRef::Inline(container) => container,
        };

        if let Err(err) = Self::check_for_local_network_and_sys_mount(&container) {
            self.deps.job_prepared(handle, Err(err));
            return;
        }

        let ijid = self.next_ijid;
        self.next_ijid = self.next_ijid.checked_add(1).unwrap();

        let layers = match Self::evaluate_layers(
            &self.deps,
            &mut self.layers,
            container.layers,
            ijid,
            false,
        ) {
            Ok(layers) => layers,
            Err(err) => {
                self.deps.job_prepared(handle, Err(err));
                return;
            }
        };

        let (pending_image, get_image) = {
            match container.image {
                Some(ImageSpec {
                    name,
                    use_layers: layers,
                    use_environment: environment,
                    use_working_directory: working_directory,
                }) => (
                    Some(ImageUse {
                        layers,
                        environment,
                        working_directory,
                    }),
                    Some(name),
                ),
                None => (None, None),
            }
        };

        let job = Job {
            handle,
            pending_image,
            image_layers: vec![],
            layers,
            root_overlay: container.root_overlay,
            initial_environment: Default::default(),
            environment: container.environment,
            working_directory: container.working_directory,
            mounts: container.mounts,
            network: container.network,
            user: container.user,
            group: container.group,
            program: spec.program,
            arguments: spec.arguments,
            timeout: spec.timeout,
            estimated_duration: spec.estimated_duration,
            allocate_tty: spec.allocate_tty,
            priority: spec.priority,
        };

        if let Some(image_name) = get_image {
            self.jobs.insert(ijid, job).assert_is_none();
            match self.images.get_mut(&image_name) {
                None => {
                    self.images
                        .insert(image_name.clone(), ImageEntry::Getting(vec![ijid]));
                    self.deps.get_image(image_name);
                }
                Some(ImageEntry::Getting(waiting)) => {
                    waiting.push(ijid);
                }
                Some(ImageEntry::Got(Ok(image))) => {
                    let image_clone = image.clone();
                    self.got_image_success(ijid, &image_clone);
                }
                Some(ImageEntry::Got(Err(err))) => {
                    Self::job_error(&self.deps, &mut self.jobs, ijid, err.clone());
                }
            }
        } else if job.is_ready() {
            Self::start_job(&self.deps, job);
        } else {
            self.jobs.insert(ijid, job);
        }
    }

    fn receive_add_container(
        &mut self,
        handle: DepsT::AddContainerHandle,
        name: String,
        spec: ContainerSpec,
    ) {
        let result = if let Err(err) = Self::check_for_local_network_and_sys_mount(&spec) {
            Err(err)
        } else {
            Ok(self.containers.insert(name, spec))
        };
        self.deps.container_added(handle, result);
    }

    fn receive_got_image(&mut self, name: &str, result: Result<ConvertedImage, DepsT::Error>) {
        let Some(entry) = self.images.get_mut(name) else {
            panic!(r#"received `got_image` for unexpected image "{name}""#);
        };
        match mem::replace(entry, ImageEntry::Got(result.clone())) {
            ImageEntry::Got(_) => {
                panic!(r#"received `got_image` for image "{name}" which we already have"#);
            }
            ImageEntry::Getting(waiting) => match &result {
                Ok(image) => {
                    for ijid in waiting {
                        self.got_image_success(ijid, image);
                    }
                }
                Err(err) => {
                    for ijid in waiting {
                        Self::job_error(&self.deps, &mut self.jobs, ijid, err.clone());
                    }
                }
            },
        }
    }

    fn got_image_success(&mut self, ijid: u64, image: &ConvertedImage) {
        if let Err(err) = self.got_image_success_inner(ijid, image) {
            Self::job_error(&self.deps, &mut self.jobs, ijid, err);
        }
    }

    fn got_image_success_inner(
        &mut self,
        ijid: u64,
        image: &ConvertedImage,
    ) -> Result<(), DepsT::Error> {
        let Entry::Occupied(mut job_entry) = self.jobs.entry(ijid) else {
            return Ok(());
        };
        let job = job_entry.get_mut();
        let image_use = job.pending_image.take().unwrap();

        if image_use.layers {
            job.image_layers = Self::evaluate_layers(
                &self.deps,
                &mut self.layers,
                image.layers().map_err(DepsT::error_from_string)?,
                ijid,
                true,
            )?;
        }

        if image_use.environment {
            job.initial_environment = image.environment().map_err(DepsT::error_from_string)?;
        }

        if image_use.working_directory {
            if job.working_directory.is_some() {
                return Err(DepsT::error_from_string(
                    "can't provide both `working_directory` and `image.use_working_directory`"
                        .into(),
                ));
            }
            job.working_directory = Some(
                image
                    .working_directory()
                    .map_err(DepsT::error_from_string)?,
            );
        }

        if job.is_ready() {
            Self::start_job(&self.deps, job_entry.remove());
        }

        Ok(())
    }

    fn evaluate_layers(
        deps: &DepsT,
        layer_map: &mut HashMap<LayerSpec, LayerEntry<DepsT>>,
        layers: Vec<LayerSpec>,
        ijid: u64,
        image: bool,
    ) -> Result<Vec<Option<(Sha256Digest, ArtifactType)>>, DepsT::Error> {
        layers
            .into_iter()
            .enumerate()
            .map(|(idx, layer_spec)| match layer_map.entry(layer_spec) {
                Entry::Occupied(entry) => match entry.into_mut() {
                    LayerEntry::Got(Ok((digest, artifact_type))) => {
                        Ok(Some((digest.clone(), *artifact_type)))
                    }
                    LayerEntry::Got(Err(err)) => Err(err.clone()),
                    LayerEntry::Getting(ijids) => {
                        ijids.push((image, ijid, idx));
                        Ok(None)
                    }
                },
                Entry::Vacant(entry) => {
                    deps.build_layer(entry.key().clone());
                    entry.insert(LayerEntry::Getting(vec![(image, ijid, idx)]));
                    Ok(None)
                }
            })
            .collect()
    }

    fn receive_got_layer(
        &mut self,
        spec: LayerSpec,
        result: Result<(Sha256Digest, ArtifactType), DepsT::Error>,
    ) {
        let Some(layer) = self.layers.get_mut(&spec) else {
            panic!(r#"received `got_layer` for unexpected layer {spec:?}"#);
        };
        match mem::replace(layer, LayerEntry::Got(result.clone())) {
            LayerEntry::Got(_) => {
                panic!(r#"received `got_layer` for layer {spec:?} which we already have"#);
            }
            LayerEntry::Getting(waiting) => match result {
                Ok((digest, artifact_type)) => {
                    for (image, ijid, idx) in waiting {
                        let Entry::Occupied(mut job_entry) = self.jobs.entry(ijid) else {
                            continue;
                        };
                        let job = job_entry.get_mut();

                        *(if image {
                            &mut job.image_layers
                        } else {
                            &mut job.layers
                        })
                        .get_mut(idx)
                        .unwrap() = Some((digest.clone(), artifact_type));

                        if job.is_ready() {
                            Self::start_job(&self.deps, job_entry.remove());
                        }
                    }
                }
                Err(err) => {
                    for (_, ijid, _) in waiting {
                        Self::job_error(&self.deps, &mut self.jobs, ijid, err.clone());
                    }
                }
            },
        }
    }

    fn start_job(deps: &DepsT, job: Job<DepsT>) {
        let (handle, result) = job.into_handle_and_spec(deps);
        deps.job_prepared(handle, result);
    }

    fn job_error(
        deps: &DepsT,
        spec_map: &mut HashMap<u64, Job<DepsT>>,
        ijid: u64,
        err: DepsT::Error,
    ) {
        if let Some(spec) = spec_map.remove(&ijid) {
            deps.job_prepared(spec.handle, Err(err));
        }
    }

    fn check_for_local_network_and_sys_mount(spec: &ContainerSpec) -> Result<(), DepsT::Error> {
        if spec.network == JobNetwork::Local
            && spec
                .mounts
                .iter()
                .any(|m| matches!(m, JobMount::Sys { .. }))
        {
            Err(DepsT::error_from_string(
                "A \"sys\" mount is not compatible with local networking. \
                Check the documentation for the \"network\" field of \"JobSpec\"."
                    .into(),
            ))
        } else {
            Ok(())
        }
    }
}

struct Job<DepsT: Deps> {
    handle: DepsT::PrepareJobHandle,
    pending_image: Option<ImageUse>,
    image_layers: Vec<Option<(Sha256Digest, ArtifactType)>>,
    layers: Vec<Option<(Sha256Digest, ArtifactType)>>,
    root_overlay: JobRootOverlay,
    initial_environment: BTreeMap<String, String>,
    environment: Vec<EnvironmentSpec>,
    working_directory: Option<Utf8PathBuf>,
    mounts: Vec<JobMount>,
    network: JobNetwork,
    user: Option<UserId>,
    group: Option<GroupId>,
    program: Utf8PathBuf,
    arguments: Vec<String>,
    timeout: Option<Timeout>,
    estimated_duration: Option<Duration>,
    allocate_tty: Option<JobTty>,
    priority: i8,
}

struct ImageUse {
    layers: bool,
    environment: bool,
    working_directory: bool,
}

impl<DepsT: Deps> Job<DepsT> {
    fn into_handle_and_spec(
        self,
        deps: &DepsT,
    ) -> (DepsT::PrepareJobHandle, Result<JobSpec, DepsT::Error>) {
        assert!(self.is_ready());
        (
            self.handle,
            if self.image_layers.is_empty() && self.layers.is_empty() {
                Err(DepsT::error_from_string(
                    "job specification has no layers".into(),
                ))
            } else {
                deps.evaluate_environment(self.initial_environment, self.environment)
                    .map(|environment| JobSpec {
                        program: self.program,
                        arguments: self.arguments,
                        environment,
                        layers: NonEmpty::collect(
                            self.image_layers
                                .into_iter()
                                .chain(self.layers)
                                .map(|layer| {
                                    let Some((digest, artifact_type)) = layer else {
                                        panic!("shouldn't be called while awaiting any layers");
                                    };
                                    (digest, artifact_type)
                                }),
                        )
                        .unwrap(),
                        mounts: self.mounts,
                        network: self.network,
                        root_overlay: self.root_overlay,
                        working_directory: self.working_directory,
                        user: self.user,
                        group: self.group,
                        timeout: self.timeout,
                        estimated_duration: self.estimated_duration,
                        allocate_tty: self.allocate_tty,
                        priority: self.priority,
                    })
            },
        )
    }

    fn is_ready(&self) -> bool {
        self.pending_image.is_none()
            && self.image_layers.iter().all(Option::is_some)
            && self.layers.iter().all(Option::is_some)
    }
}
