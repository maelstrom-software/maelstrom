pub mod task;

use maelstrom_base::{
    ArtifactType, EnumSet, GroupId, JobMount, JobNetwork, JobRootOverlay, JobSpec, JobTty,
    NonEmpty, Sha256Digest, Timeout, UserId, Utf8PathBuf,
};
use maelstrom_client_base::spec::{
    ContainerParent, ContainerSpec, ConvertedImage, EnvironmentSpec, ImageSpec, ImageUse,
    JobSpec as ClientJobSpec, LayerSpec,
};
use maelstrom_util::ext::OptionExt as _;
use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap, VecDeque},
    mem,
    num::NonZeroUsize,
    time::Duration,
};

pub trait Deps {
    type Error: Clone;

    type PrepareJobHandle;
    type AddContainerHandle;

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
    layer_builds: LayerBuilds,
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

struct LayerBuilds {
    max_pending: NonZeroUsize,
    pending: usize,
    waiting: VecDeque<LayerSpec>,
}

impl LayerBuilds {
    fn new(max_pending: NonZeroUsize) -> Self {
        Self {
            max_pending,
            pending: 0,
            waiting: Default::default(),
        }
    }

    fn push<DepsT: Deps>(&mut self, deps: &DepsT, layer: LayerSpec) {
        if self.pending < self.max_pending.into() {
            self.pending += 1;
            deps.build_layer(layer);
        } else {
            self.waiting.push_back(layer);
        }
    }

    fn pop<DepsT: Deps>(&mut self, deps: &DepsT) {
        if let Some(layer) = self.waiting.pop_front() {
            deps.build_layer(layer);
        } else {
            self.pending = self.pending.checked_sub(1).unwrap();
            assert!(self.pending < self.max_pending.into());
        }
    }
}

impl<DepsT: Deps> Preparer<DepsT> {
    pub fn new(deps: DepsT, max_pending_layer_builds: NonZeroUsize) -> Self {
        Self {
            deps,
            layer_builds: LayerBuilds::new(max_pending_layer_builds),
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
        let container = spec.container;

        if let Err(err) = Self::check_for_local_network_and_sys_mount(&container) {
            self.deps.job_prepared(handle, Err(err));
            return;
        }

        let ijid = self.next_ijid;
        self.next_ijid = self.next_ijid.checked_add(1).unwrap();

        let layers = match Self::evaluate_layers(
            &self.deps,
            &mut self.layer_builds,
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
            match container.parent {
                None => (None, None),
                Some(ContainerParent::Image(ImageSpec { name, r#use })) => {
                    if r#use.is_empty() {
                        (None, None)
                    } else {
                        (Some(r#use), Some(name))
                    }
                }
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

        if image_use.contains(ImageUse::Layers) {
            job.image_layers = Self::evaluate_layers(
                &self.deps,
                &mut self.layer_builds,
                &mut self.layers,
                image.layers().map_err(DepsT::error_from_string)?,
                ijid,
                true,
            )?;
        }

        if image_use.contains(ImageUse::Environment) {
            job.initial_environment = image.environment().map_err(DepsT::error_from_string)?;
        }

        if image_use.contains(ImageUse::WorkingDirectory) {
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
        layer_builds: &mut LayerBuilds,
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
                    layer_builds.push(deps, entry.key().clone());
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
            LayerEntry::Getting(waiting) => {
                self.layer_builds.pop(&self.deps);
                match result {
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
                }
            }
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
    pending_image: Option<EnumSet<ImageUse>>,
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

#[cfg(test)]
mod tests {
    use super::{Message::*, *};
    use maelstrom_base::{job_spec, tar_digest, WindowSize};
    use maelstrom_client::spec;
    use maelstrom_client_base::{
        container_spec, image_container_parent, job_spec as client_job_spec, spec::ImageConfig,
    };
    use maelstrom_test::{millis, string, tar_layer};
    use std::{cell::RefCell, ffi::OsStr, rc::Rc};
    use TestMessage::*;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum TestMessage {
        JobPrepared(u32, Result<JobSpec, String>),
        ContainerAdded(u32, Result<Option<ContainerSpec>, String>),
        GetImage(String),
        BuildLayer(LayerSpec),
    }

    struct TestState {
        messages: Vec<TestMessage>,
    }

    impl Deps for Rc<RefCell<TestState>> {
        type Error = String;
        type PrepareJobHandle = u32;
        type AddContainerHandle = u32;

        fn error_from_string(err: String) -> Self::Error {
            err
        }

        fn evaluate_environment(
            &self,
            initial: BTreeMap<String, String>,
            specs: Vec<EnvironmentSpec>,
        ) -> Result<Vec<String>, Self::Error> {
            spec::environment_eval(initial, specs, |_| Ok(None)).map_err(|err| err.to_string())
        }

        fn job_prepared(
            &self,
            handle: Self::PrepareJobHandle,
            result: Result<JobSpec, Self::Error>,
        ) {
            self.borrow_mut()
                .messages
                .push(TestMessage::JobPrepared(handle, result));
        }

        fn container_added(
            &self,
            handle: Self::AddContainerHandle,
            result: Result<Option<ContainerSpec>, Self::Error>,
        ) {
            self.borrow_mut()
                .messages
                .push(TestMessage::ContainerAdded(handle, result));
        }

        fn get_image(&self, name: String) {
            self.borrow_mut().messages.push(TestMessage::GetImage(name));
        }

        fn build_layer(&self, spec: LayerSpec) {
            self.borrow_mut()
                .messages
                .push(TestMessage::BuildLayer(spec));
        }
    }

    struct Fixture {
        test_state: Rc<RefCell<TestState>>,
        preparer: Preparer<Rc<RefCell<TestState>>>,
    }

    impl Default for Fixture {
        fn default() -> Self {
            Self::new(NonZeroUsize::new(usize::MAX).unwrap())
        }
    }

    impl Fixture {
        fn new(max_pending_layer_builds: NonZeroUsize) -> Self {
            let test_state = Rc::new(RefCell::new(TestState {
                messages: Vec::default(),
            }));
            let preparer = Preparer::new(test_state.clone(), max_pending_layer_builds);
            Fixture {
                test_state,
                preparer,
            }
        }

        #[track_caller]
        fn expect_messages_in_any_order(&mut self, mut expected: Vec<TestMessage>) {
            expected.sort();
            let messages = &mut self.test_state.borrow_mut().messages;
            messages.sort();
            if expected == *messages {
                messages.clear();
                return;
            }
            panic!(
                "Expected messages didn't match actual messages in any order.\n\
                Expected: {expected:#?}\n\
                Actual: {messages:#?}\n\
                Diff: {}",
                colored_diff::PrettyDifference {
                    expected: &format!("{expected:#?}"),
                    actual: &format!("{messages:#?}")
                }
            );
        }
    }

    macro_rules! sys_mount {
        ($mount_point:expr) => {
            JobMount::Sys {
                mount_point: $mount_point.into(),
            }
        };
    }

    macro_rules! proc_mount {
        ($mount_point:expr) => {
            JobMount::Proc {
                mount_point: $mount_point.into(),
            }
        };
    }

    macro_rules! tmp_mount {
        ($mount_point:expr) => {
            JobMount::Tmp {
                mount_point: $mount_point.into(),
            }
        };
    }

    macro_rules! converted_image {
        (@expand [$name:expr] [] -> []) => {
            ConvertedImage::new($name.into(), ImageConfig::default())
        };
        (@expand [$name:expr] [] -> [$($field_out:tt)+]) => {
            ConvertedImage::new($name.into(), ImageConfig {
                $($field_out)+,
                .. ImageConfig::default()
            })
        };
        (@expand [$name:expr] [layers: [$($($layer:expr),+ $(,)?)?] $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
            converted_image!(@expand [$name] [$($($field_in)*)?] -> [
                $($($field_out)+,)? layers: vec![$($($layer.into()),+)?]
            ])
        };
        (@expand [$name:expr] [environment: [$($($var:expr),+ $(,)?)?] $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
            converted_image!(@expand [$name] [$($($field_in)*)?] -> [
                $($($field_out)+,)? environment: Some(vec![$($($var.into()),+)?])
            ])
        };
        (@expand [$name:expr] [working_directory: $working_directory:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
            converted_image!(@expand [$name] [$($($field_in)*)?] -> [
                $($($field_out)+,)? working_directory: Some($working_directory.into())
            ])
        };
        ($name:expr $(,$($field_in:tt)*)?) => {
            converted_image!(@expand [$name] [$($($field_in)*)?] -> [])
        };
    }

    macro_rules! environment_spec {
        ($extend:expr $(, $($($key:expr => $value:expr),+ $(,)?)?)?) => {
            EnvironmentSpec {
                extend: $extend.into(),
                vars: BTreeMap::from([$($($(($key.into(), $value.into())),+)?)?]),
            }
        }
    }

    macro_rules! script_test {
        ($test_name:ident, $fixture:expr, $($in_msg:expr => { $($out_msg:expr),* $(,)? });* $(;)?) => {
            #[test]
            fn $test_name() {
                let mut fixture = $fixture;
                $(
                    fixture.preparer.receive_message($in_msg);
                    fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
                )*
            }
        };
        ($test_name:ident, $($in_msg:expr => { $($out_msg:expr),* $(,)? });* $(;)?) => {
            script_test! { $test_name, Fixture::default(), $($in_msg => { $($out_msg),* });* }
        };
    }

    script_test! {
        prepare_job_no_layers,
        PrepareJob(1, client_job_spec!{"foo"}) => {
            JobPrepared(1, Err(string!("job specification has no layers"))),
        };
    }

    script_test! {
        prepare_job_inline_container_with_local_network_and_sys_mount,
        PrepareJob(
            2,
            client_job_spec! {
                "foo",
                network: JobNetwork::Local,
                mounts: [ sys_mount!("/mnt") ],
            },
        ) => {
            JobPrepared(
                2,
                Err(string!(
                    "A \"sys\" mount is not compatible with local networking. \
                    Check the documentation for the \"network\" field of \"JobSpec\"."
                ))
            ),
        };
    }

    /*
    script_test! {
        prepare_job_unknown_named_container,
        PrepareJob(3, client_job_spec!{"foo", container: named_container!{"foo"}}) => {
            JobPrepared(3, Err(string!("container \"foo\" unknown"))),
        };
    }
    */

    script_test! {
        prepare_job_one_layer,
        PrepareJob(
            1,
            client_job_spec! {
                "foo",
                arguments: ["arg1", "arg2"],
                layers: [tar_layer!("foo.tar")],
            },
        ) => {
            BuildLayer(tar_layer!("foo.tar")),
        };
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(42))) => {
            JobPrepared(1, Ok(job_spec!("foo", [tar_digest!(42)], arguments: ["arg1", "arg2"]))),
        };
    }

    script_test! {
        prepare_job_two_layers,
        PrepareJob(
            1,
            client_job_spec! {
                "foo",
                layers: [ tar_layer!("foo.tar"), tar_layer!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer!("foo.tar")),
            BuildLayer(tar_layer!("bar.tar")),
        };
        GotLayer(tar_layer!("bar.tar"), Ok(tar_digest!(1))) => {};
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(2))) => {
            JobPrepared(1, Ok(job_spec!("foo", [ tar_digest!(2), tar_digest!(1) ]))),
        };
    }

    script_test! {
        prepare_job_two_jobs_1,
        PrepareJob(
            1,
            client_job_spec! {
                "foo",
                layers: [ tar_layer!("foo.tar"), tar_layer!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer!("foo.tar")),
            BuildLayer(tar_layer!("bar.tar")),
        };
        PrepareJob(
            2,
            client_job_spec! {
                "bar",
                layers: [ tar_layer!("foo.tar"), tar_layer!("baz.tar") ],
            },
        ) => {
            BuildLayer(tar_layer!("baz.tar")),
        };
        GotLayer(tar_layer!("bar.tar"), Ok(tar_digest!(1))) => {};
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(2))) => {
            JobPrepared(1, Ok(job_spec!("foo", [ tar_digest!(2), tar_digest!(1) ]))),
        };
        GotLayer(tar_layer!("baz.tar"), Ok(tar_digest!(3))) => {
            JobPrepared(2, Ok(job_spec!("bar", [ tar_digest!(2), tar_digest!(3) ]))),
        };
    }

    script_test! {
        prepare_job_two_jobs_2,
        PrepareJob(
            1,
            client_job_spec! {
                "foo",
                layers: [ tar_layer!("foo.tar"), tar_layer!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer!("foo.tar")),
            BuildLayer(tar_layer!("bar.tar")),
        };
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(1))) => {};
        PrepareJob(
            2,
            client_job_spec! {
                "bar",
                layers: [ tar_layer!("foo.tar"), tar_layer!("baz.tar") ],
            },
        ) => {
            BuildLayer(tar_layer!("baz.tar")),
        };
        GotLayer(tar_layer!("baz.tar"), Ok(tar_digest!(3))) => {
            JobPrepared(2, Ok(job_spec!("bar", [ tar_digest!(1), tar_digest!(3) ]))),
        };
        GotLayer(tar_layer!("bar.tar"), Ok(tar_digest!(2))) => {
            JobPrepared(1, Ok(job_spec!("foo", [ tar_digest!(1), tar_digest!(2) ]))),
        };
    }

    script_test! {
        prepare_job_two_jobs_3,
        PrepareJob(
            1,
            client_job_spec! {
                "foo",
                layers: [ tar_layer!("foo.tar"), tar_layer!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer!("foo.tar")),
            BuildLayer(tar_layer!("bar.tar")),
        };
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(1))) => {};
        GotLayer(tar_layer!("bar.tar"), Ok(tar_digest!(2))) => {
            JobPrepared(1, Ok(job_spec!("foo", [ tar_digest!(1), tar_digest!(2) ]))),
        };
        PrepareJob(
            2,
            client_job_spec! {
                "bar",
                layers: [ tar_layer!("bar.tar"), tar_layer!("foo.tar") ],
            },
        ) => {
            JobPrepared(2, Ok(job_spec!("bar", [ tar_digest!(2), tar_digest!(1) ]))),
        };
    }

    script_test! {
        prepare_job_jobs_with_error,

        PrepareJob(
            1,
            client_job_spec! {
                "one",
                layers: [ tar_layer!("foo.tar"), tar_layer!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer!("foo.tar")),
            BuildLayer(tar_layer!("bar.tar")),
        };
        PrepareJob(
            2,
            client_job_spec! {
                "two",
                layers: [ tar_layer!("foo.tar"), tar_layer!("baz.tar") ],
            },
        ) => {
            BuildLayer(tar_layer!("baz.tar")),
        };
        PrepareJob(
            3,
            client_job_spec! {
                "three",
                layers: [ tar_layer!("bar.tar"), tar_layer!("baz.tar") ],
            },
        ) => {};

        GotLayer(tar_layer!("foo.tar"), Err(string!("foo.tar error"))) => {
            JobPrepared(1, Err(string!("foo.tar error"))),
            JobPrepared(2, Err(string!("foo.tar error"))),
        };
        GotLayer(tar_layer!("bar.tar"), Ok(tar_digest!(2))) => {};
        GotLayer(tar_layer!("baz.tar"), Ok(tar_digest!(3))) => {
            JobPrepared(3, Ok(job_spec!("three", [ tar_digest!(2), tar_digest!(3) ]))),
        };

        PrepareJob(
            4,
            client_job_spec! {
                "four",
                layers: [ tar_layer!("foo.tar"), tar_layer!("bar.tar") ],
            },
        ) => {
            JobPrepared(4, Err(string!("foo.tar error"))),
        };
        PrepareJob(
            5,
            client_job_spec! {
                "five",
                layers: [ tar_layer!("bar.tar"), tar_layer!("baz.tar") ],
            },
        ) => {
            JobPrepared(5, Ok(job_spec!("five", [ tar_digest!(2), tar_digest!(3) ]))),
        };
    }

    script_test! {
        prepare_job_jobs_with_images,

        PrepareJob(
            1,
            client_job_spec! {
                "one",
                parent: image_container_parent!("image1", layers),
                layers: [ tar_layer!("foo.tar"), tar_layer!("bar.tar") ],
            },
        ) => {
            GetImage(string!("image1")),
            BuildLayer(tar_layer!("foo.tar")),
            BuildLayer(tar_layer!("bar.tar")),
        };
        GotImage(string!("image1"), Ok(converted_image!("image1", layers: [ "image1/1.tar", "image1/2.tar" ]))) => {
            BuildLayer(tar_layer!("image1/1.tar")),
            BuildLayer(tar_layer!("image1/2.tar")),
        };
        GotLayer(tar_layer!("bar.tar"), Ok(tar_digest!(4))) => {};
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(3))) => {};
        GotLayer(tar_layer!("image1/2.tar"), Ok(tar_digest!(12))) => {};
        GotLayer(tar_layer!("image1/1.tar"), Ok(tar_digest!(11))) => {
            JobPrepared(1, Ok(job_spec!("one", [
                tar_digest!(11), tar_digest!(12), tar_digest!(3), tar_digest!(4),
            ]))),
        };

        PrepareJob(
            2,
            client_job_spec! {
                "two",
                parent: image_container_parent!("image2", layers),
                layers: [ tar_layer!("foo.tar"), tar_layer!("bar.tar") ],
            },
        ) => {
            GetImage(string!("image2")),
        };
        PrepareJob(
            3,
            client_job_spec! {
                "three",
                parent: image_container_parent!("image1", layers),
                layers: [ tar_layer!("foo.tar"), tar_layer!("baz.tar") ],
            },
        ) => {
            BuildLayer(tar_layer!("baz.tar")),
        };
        GotImage(string!("image2"), Ok(converted_image!("image2", layers: [ "image1/1.tar", "image2/2.tar" ]))) => {
            BuildLayer(tar_layer!("image2/2.tar")),
        };
        GotLayer(tar_layer!("image2/2.tar"), Ok(tar_digest!(22))) => {
            JobPrepared(2, Ok(job_spec!("two", [
                tar_digest!(11), tar_digest!(22), tar_digest!(3), tar_digest!(4),
            ]))),
        };
        GotLayer(tar_layer!("baz.tar"), Ok(tar_digest!(5))) => {
            JobPrepared(3, Ok(job_spec!("three", [
                tar_digest!(11), tar_digest!(12), tar_digest!(3), tar_digest!(5),
            ]))),
        };

        PrepareJob(
            4,
            client_job_spec! {
                "four",
                parent: image_container_parent!("image1", layers),
                layers: [ tar_layer!("foo.tar"), tar_layer!("bar.tar") ],
            },
        ) => {
            JobPrepared(4, Ok(job_spec!("four", [
                tar_digest!(11), tar_digest!(12), tar_digest!(3), tar_digest!(4),
            ]))),
        };
    }

    script_test! {
        prepare_job_jobs_with_empty_image_use,

        PrepareJob(
            1,
            client_job_spec! {
                "one",
                parent: image_container_parent!("image1"),
                layers: [ tar_layer!("foo.tar"), tar_layer!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer!("foo.tar")),
            BuildLayer(tar_layer!("bar.tar")),
        };
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(3))) => {};
        GotLayer(tar_layer!("bar.tar"), Ok(tar_digest!(4))) => {
            JobPrepared(1, Ok(job_spec!("one", [ tar_digest!(3), tar_digest!(4) ]))),
        };
    }

    script_test! {
        prepare_job_jobs_with_image_errors,

        PrepareJob(
            1,
            client_job_spec! {
                "one",
                parent: image_container_parent!("image", layers),
            },
        ) => {
            GetImage(string!("image")),
        };
        PrepareJob(
            2,
            client_job_spec! {
                "two",
                parent: image_container_parent!("image", working_directory),
                layers: [tar_layer!("foo.tar"), tar_layer!("bar.tar")],
            },
        ) => {
            BuildLayer(tar_layer!("foo.tar")),
            BuildLayer(tar_layer!("bar.tar")),
        };
        PrepareJob(
            3,
            client_job_spec! {
                "three",
                layers: [tar_layer!("foo.tar")],
            },
        ) => {};

        GotImage(string!("image"), Err(string!("image error"))) => {
            JobPrepared(1, Err(string!("image error"))),
            JobPrepared(2, Err(string!("image error"))),
        };
        GotLayer(tar_layer!("bar.tar"), Ok(tar_digest!(2))) => {};
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(1))) => {
            JobPrepared(3, Ok(job_spec!("three", [tar_digest!(1)]))),
        };

        PrepareJob(
            4,
            client_job_spec! {
                "four",
                parent: image_container_parent!("image", layers),
                layers: [tar_layer!("baz.tar")],
            },
        ) => {
            BuildLayer(tar_layer!("baz.tar")),
            JobPrepared(4, Err(string!("image error"))),
        };
        GotLayer(tar_layer!("baz.tar"), Ok(tar_digest!(3))) => {};
    }

    script_test! {
        prepare_job_all_fields,

        PrepareJob(1, client_job_spec! {
            "one",
            layers: [tar_layer!("foo.tar"), tar_layer!("bar.tar")],
            root_overlay: JobRootOverlay::Tmp,
            environment: [
                environment_spec!{false, "FOO" => "foo", "BAR" => "bar"},
                environment_spec!{true, "FOO" => "frob", "BAZ" => "baz"},
            ],
            working_directory: "/root",
            mounts: [
                proc_mount!("/proc"),
                tmp_mount!("/tmp"),
            ],
            network: JobNetwork::Local,
            user: 100,
            group: 101,
            arguments: ["arg1", "arg2"],
            timeout: 10,
            estimated_duration: millis!(100),
            allocate_tty: JobTty::new(b"123456", WindowSize::new(50, 100)),
            priority: 42,
        }) => {
            BuildLayer(tar_layer!("foo.tar")),
            BuildLayer(tar_layer!("bar.tar")),
        };
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(1))) => {};
        GotLayer(tar_layer!("bar.tar"), Ok(tar_digest!(2))) => {
            JobPrepared(1, Ok(job_spec! {
                "one",
                [tar_digest!(1), tar_digest!(2)],
                arguments: ["arg1", "arg2"],
                environment: [
                    "BAR=bar",
                    "BAZ=baz",
                    "FOO=frob",
                ],
                mounts: [
                    proc_mount!("/proc"),
                    tmp_mount!("/tmp"),
                ],
                network: JobNetwork::Local,
                root_overlay: JobRootOverlay::Tmp,
                working_directory: "/root",
                user: 100,
                group: 101,
                timeout: 10,
                estimated_duration: millis!(100),
                allocate_tty: JobTty::new(b"123456", WindowSize::new(50, 100)),
                priority: 42,
            })),
        };
    }

    script_test! {
        prepare_job_environment_from_image_missing,

        PrepareJob(1, client_job_spec! {
            "one",
            parent: image_container_parent!("image", environment),
            layers: [tar_layer!("foo.tar")],
        }) => {
            GetImage(string!("image")),
            BuildLayer(tar_layer!("foo.tar")),
        };
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(1))) => {};
        GotImage(string!("image"), Ok(converted_image!("image", layers: ["image1/1.tar"]))) => {
            JobPrepared(1, Err(string!("image image has no environment to use"))),
        };
    }

    script_test! {
        prepare_job_environment_from_image,

        PrepareJob(1, client_job_spec! {
            "one",
            parent: image_container_parent!("image", environment),
            layers: [tar_layer!("foo.tar")],
        }) => {
            GetImage(string!("image")),
            BuildLayer(tar_layer!("foo.tar")),
        };
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(1))) => {};
        GotImage(string!("image"), Ok(converted_image! {
            "image",
            layers: ["image1/1.tar"],
            environment: ["FOO=foo", "BAR=bar" ]
        })) => {
            JobPrepared(1, Ok(job_spec! {
                "one",
                [tar_digest!(1)],
                environment: ["BAR=bar", "FOO=foo"],
            })),
        };

        PrepareJob(2, client_job_spec! {
            "two",
            parent: image_container_parent!("image", environment),
            layers: [tar_layer!("foo.tar")],
            environment: [
                environment_spec!{false, "OLD_FOO" => "old_$prev{FOO}", "BAZ" => "$prev{BAR}"},
                environment_spec!{true, "FOO" => "food" },
            ],
        }) => {
            JobPrepared(2, Ok(job_spec! {
                "two",
                [tar_digest!(1)],
                environment: ["BAZ=bar", "FOO=food", "OLD_FOO=old_foo"],
            })),
        };
    }

    script_test! {
        prepare_job_environment_from_image_vs_not,

        PrepareJob(1, client_job_spec! {
            "one",
            parent: image_container_parent!("image", environment),
            layers: [tar_layer!("foo.tar")],
            environment: [environment_spec!{true, "BAZ" => "baz" }],
        }) => {
            GetImage(string!("image")),
            BuildLayer(tar_layer!("foo.tar")),
        };

        PrepareJob(2, client_job_spec! {
            "two",
            layers: [tar_layer!("foo.tar")],
            environment: [environment_spec!{true, "BAZ" => "baz" }],
        }) => {};

        GotImage(string!("image"), Ok(converted_image! {
            "image",
            layers: ["image1/1.tar"],
            environment: ["FOO=foo", "BAR=bar"],
        })) => {};

        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(1))) => {
            JobPrepared(1, Ok(job_spec! {
                "one",
                [tar_digest!(1)],
                environment: ["BAR=bar", "BAZ=baz", "FOO=foo"],
            })),
            JobPrepared(2, Ok(job_spec! {
                "two",
                [tar_digest!(1)],
                environment: ["BAZ=baz"],
            })),
        };
    }

    script_test! {
        prepare_job_environment_error,

        PrepareJob(1, client_job_spec! {
            "one",
            layers: [tar_layer!("foo.tar")],
            environment: [environment_spec!{true, "OLD_FOO" => "old_$prev{FOO}"}],
        }) => {
            BuildLayer(tar_layer!("foo.tar")),
        };
        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(1))) => {
            JobPrepared(1, Err(string!(r#"unknown variable "FOO""#))),
        };

        PrepareJob(2, client_job_spec! {
            "two",
            layers: [tar_layer!("foo.tar")],
            environment: [environment_spec!{true, "FOO" => "$env{FOO}"}],
        }) => {
            JobPrepared(2, Err(string!(r#"unknown variable "FOO""#))),
        };
    }

    script_test! {
        prepare_job_working_directory_from_image,

        PrepareJob(1, client_job_spec! {
            "one",
            parent: image_container_parent!("image", working_directory),
            layers: [tar_layer!("foo.tar")],
        }) => {
            GetImage(string!("image")),
            BuildLayer(tar_layer!("foo.tar")),
        };

        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(1))) => {};

        GotImage(string!("image"), Ok(converted_image! {
            "image",
            working_directory: "/root",
        })) => {
            JobPrepared(1, Ok(job_spec! {
                "one",
                [tar_digest!(1)],
                working_directory: "/root",
            })),
        };
    }

    script_test! {
        prepare_job_working_directory_from_image_and_specified_directly,

        PrepareJob(1, client_job_spec! {
            "one",
            parent: image_container_parent!("image", working_directory),
            layers: [tar_layer!("foo.tar")],
            working_directory: "/root2",
        }) => {
            GetImage(string!("image")),
            BuildLayer(tar_layer!("foo.tar")),
        };

        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(1))) => {};

        GotImage(string!("image"), Ok(converted_image! {
            "image",
            working_directory: "/root",
        })) => {
            JobPrepared(1, Err(string!("can't provide both `working_directory` and `image.use_working_directory`"))),
        };
    }

    script_test! {
        prepare_job_working_directory_from_image_missing_working_directory,

        PrepareJob(1, client_job_spec! {
            "one",
            parent: image_container_parent!("image", working_directory),
            layers: [tar_layer!("foo.tar")],
        }) => {
            GetImage(string!("image")),
            BuildLayer(tar_layer!("foo.tar")),
        };

        GotLayer(tar_layer!("foo.tar"), Ok(tar_digest!(1))) => {};

        GotImage(string!("image"), Ok(converted_image!{"image"})) => {
            JobPrepared(1, Err(string!("image image has no working directory to use"))),
        };
    }

    script_test! {
        prepare_job_layers_from_image_bad_path,

        PrepareJob(1, client_job_spec! {
            "one",
            parent: image_container_parent!("image", layers),
        }) => {
            GetImage(string!("image")),
        };

        GotImage(string!("image"), Ok(converted_image! {
            "image",
            layers: [
                unsafe { OsStr::from_encoded_bytes_unchecked(b"\xff\xff\xff\xff") },
            ],
        })) => {
            JobPrepared(1, Err(string!(r#"image image has a non-UTF-8 layer path "\xFF\xFF\xFF\xFF""#))),
        };
    }

    script_test! {
        prepare_job_pending_builds_limited,
        Fixture::new(NonZeroUsize::new(1).unwrap()),

        PrepareJob(1, client_job_spec! {
            "one",
            layers: [tar_layer!("foo.tar"), tar_layer!("bar.tar")],
        }) => {
            BuildLayer(tar_layer!("foo.tar")),
        };

        PrepareJob(2, client_job_spec! {
            "two",
            layers: [tar_layer!("foo.tar"), tar_layer!("baz.tar")],
        }) => {};

        GotLayer(tar_layer!("foo.tar"), Err(string!("foo.tar error"))) => {
            BuildLayer(tar_layer!("bar.tar")),
            JobPrepared(1, Err(string!("foo.tar error"))),
            JobPrepared(2, Err(string!("foo.tar error"))),
        };

        GotLayer(tar_layer!("bar.tar"), Ok(tar_digest!(2))) => {
            BuildLayer(tar_layer!("baz.tar")),
        };

        GotLayer(tar_layer!("baz.tar"), Ok(tar_digest!(3))) => {};

        PrepareJob(3, client_job_spec! {
            "three",
            layers: [
                tar_layer!("bar.tar"),
                tar_layer!("baz.tar"),
                tar_layer!("qux.tar"),
                tar_layer!("frob.tar"),
            ],
        }) => {
            BuildLayer(tar_layer!("qux.tar")),
        };

        GotLayer(tar_layer!("qux.tar"), Ok(tar_digest!(4))) => {
            BuildLayer(tar_layer!("frob.tar")),
        };

        GotLayer(tar_layer!("frob.tar"), Ok(tar_digest!(5))) => {
            JobPrepared(3, Ok(job_spec! {
                "three",
                [tar_digest!(2), tar_digest!(3), tar_digest!(4), tar_digest!(5)],
            })),
        };
    }

    script_test! {
        add_container_duplicate,
        AddContainer(0, string!("foo"), container_spec!{ network: JobNetwork::Loopback }) => {
            ContainerAdded(0, Ok(None)),
        };
        AddContainer(1, string!("foo"), container_spec!{ network: JobNetwork::Local }) => {
            ContainerAdded(1, Ok(Some(container_spec!{ network: JobNetwork::Loopback }))),
        };
    }

    script_test! {
        add_container_with_local_network_and_sys_mount,
        AddContainer(
            0,
            string!("foo"),
            container_spec! {
                network: JobNetwork::Local,
                mounts: [ sys_mount!("/mnt") ],
            }
        ) => {
            ContainerAdded(
                0,
                Err(string!(
                    "A \"sys\" mount is not compatible with local networking. \
                    Check the documentation for the \"network\" field of \"JobSpec\"."
                ))
            ),
        };
    }
}
