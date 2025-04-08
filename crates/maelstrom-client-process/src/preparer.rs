//! This module contains the [`Preparer`] struct and its related types.
//!
//! The Job of the `Preparer` is to take [`maelstrom_client_base::spec::JobSpec`]s (which are
//! called [`ClientJobSpec`]s in this module) and turn them into [`maelstrom_base::JobSpec`]s.
//!
//! This requires the `Preparer` do a few things.
//!
//! First, inheritance chain defined by the `parent` field of the [`ClientJobSpec`], it's `parent`,
//! and so on needs to be "collapsed" down into a single [`CollapsedJobSpec`].
//!
//! Second, if the `JobSpec` is based on an image, the image must be fetched before building the
//! `JobSpec` can be completed.
//!
//! Third, [`LayerSpec`]s need to be turned into [`Sha256Digest`]s by "building" the layers. Note,
//! images' layers are treated as [`LayerSpec::Tar`]s. They also need to be "built" once the image
//! is fetched.

pub mod task;

use crate::collapsed_job_spec::CollapsedJobSpec;
use assert_matches::assert_matches;
use maelstrom_base::{ArtifactType, JobSpec, Sha256Digest};
use maelstrom_client_base::spec::{
    ContainerSpec, ConvertedImage, EnvironmentSpec, ImageRef, JobSpec as ClientJobSpec, LayerSpec,
};
use maelstrom_util::executor::{self, Executor, StartResult};
use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap, VecDeque},
    fmt::Debug,
    mem,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
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
    fn container_added(&self, handle: Self::AddContainerHandle, old: Option<ContainerSpec>);
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
    executor_adapter: ExecutorAdapter<DepsT>,
    containers: HashMap<String, ContainerSpec>,
    images: HashMap<String, ImageEntry<DepsT>>,
    jobs: Arc<Mutex<HashMap<u64, Job<DepsT>>>>,
    next_ijid: u64,
    executor: Executor<ExecutorAdapter<DepsT>>,
}

struct ExecutorAdapter<DepsT: Deps> {
    deps: DepsT,
    layer_builds: LayerBuilds,
    jobs: Arc<Mutex<HashMap<u64, Job<DepsT>>>>,
}

enum CompletedHandle {
    BuildLayer(bool, u64, usize),
}

#[derive(Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Tag {
    BuildLayer(LayerSpec),
}

#[derive(Debug)]
enum Output<DepsT: Deps> {
    BuildLayer(Result<(Sha256Digest, ArtifactType), DepsT::Error>),
}

impl<DepsT: Deps> executor::Deps for ExecutorAdapter<DepsT> {
    type CompletedHandle = CompletedHandle;
    type Tag = Tag;
    type Partial = ();
    type Output = Output<DepsT>;

    fn start(
        &mut self,
        tag: &Self::Tag,
        state: &Option<Self::Partial>,
        inputs: Vec<&Self::Output>,
    ) -> StartResult<Self::Tag, Self::Partial, Self::Output> {
        match tag {
            Tag::BuildLayer(layer_spec) => {
                assert_matches!(state, None);
                assert!(inputs.is_empty());
                self.layer_builds.push(&self.deps, layer_spec.clone());
                StartResult::InProgress
            }
        }
    }

    fn completed(
        &mut self,
        handle: Self::CompletedHandle,
        _tag: &Self::Tag,
        output: &Self::Output,
    ) {
        let CompletedHandle::BuildLayer(image, ijid, index) = handle;
        let Output::BuildLayer(result) = output;
        let mut jobs = self.jobs.lock().unwrap();
        match result {
            Err(err) => {
                job_error(&self.deps, &mut *jobs, ijid, err.clone());
            }
            Ok((digest, artifact_type)) => {
                let Entry::Occupied(mut job_entry) = jobs.entry(ijid) else {
                    return;
                };
                let job = job_entry.get_mut();

                (if image {
                    &mut job.image_layers
                } else {
                    &mut job.layers
                })[index] = Some((digest.clone(), *artifact_type));

                if job.is_ready() {
                    start_job(&self.deps, job_entry.remove());
                }
            }
        }
    }
}

enum ImageEntry<DepsT: Deps> {
    Got(Result<ConvertedImage, DepsT::Error>),
    Getting(Vec<u64>),
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
        if self.pending < self.max_pending.get() {
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
            assert!(self.pending < self.max_pending.get());
        }
    }
}

fn job_error<DepsT: Deps>(
    deps: &DepsT,
    spec_map: &mut HashMap<u64, Job<DepsT>>,
    ijid: u64,
    err: DepsT::Error,
) {
    if let Some(spec) = spec_map.remove(&ijid) {
        deps.job_prepared(spec.handle, Err(err));
    }
}

fn start_job<DepsT: Deps>(deps: &DepsT, job: Job<DepsT>) {
    let (handle, result) = job.into_handle_and_spec(deps);
    deps.job_prepared(handle, result);
}

impl<DepsT: Deps> Preparer<DepsT> {
    pub fn new(deps: DepsT, max_pending_layer_builds: NonZeroUsize) -> Self {
        let jobs = Arc::new(Mutex::new(Default::default()));
        let executor_adapter = ExecutorAdapter {
            deps,
            layer_builds: LayerBuilds::new(max_pending_layer_builds),
            jobs: jobs.clone(),
        };
        Self {
            executor_adapter,
            containers: Default::default(),
            images: Default::default(),
            jobs,
            next_ijid: Default::default(),
            executor: Default::default(),
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

    fn receive_prepare_job(&mut self, handle: DepsT::PrepareJobHandle, job_spec: ClientJobSpec) {
        let ijid = self.next_ijid;
        self.next_ijid = self.next_ijid.checked_add(1).unwrap();

        let job_spec = match CollapsedJobSpec::new(job_spec, &|c| self.containers.get(c)) {
            Ok(job_spec) => job_spec,
            Err(err) => {
                self.executor_adapter
                    .deps
                    .job_prepared(handle, Err(DepsT::error_from_string(err)));
                return;
            }
        };

        let has_image = job_spec.image().is_some();
        let layer_specs = job_spec.layers().to_owned();
        let mut jobs = self.jobs.lock().unwrap();
        let Entry::Vacant(job_entry) = jobs.entry(ijid) else {
            panic!("duplicate ijid {ijid}");
        };
        job_entry.insert(Job {
            handle,
            job_spec,
            image_layers: vec![],
            layers: vec![None; layer_specs.len()],
        });
        drop(jobs);

        for (index, layer_spec) in layer_specs.into_iter().enumerate() {
            self.executor.add(Tag::BuildLayer(layer_spec.clone()));
            self.executor.evaluate(
                &mut self.executor_adapter,
                CompletedHandle::BuildLayer(false, ijid, index),
                &Tag::BuildLayer(layer_spec.clone()),
            );
        }

        if has_image {
            let mut jobs = self.jobs.lock().unwrap();
            if let Some(job) = jobs.get_mut(&ijid) {
                let ImageRef { name, .. } = job.job_spec.image().unwrap();
                let image_name = name.clone();
                match self.images.get_mut(&image_name) {
                    None => {
                        self.images
                            .insert(image_name.clone(), ImageEntry::Getting(vec![ijid]));
                        self.executor_adapter.deps.get_image(image_name);
                    }
                    Some(ImageEntry::Getting(waiting)) => {
                        waiting.push(ijid);
                    }
                    Some(ImageEntry::Got(Ok(image))) => {
                        let image_clone = image.clone();
                        drop(jobs);
                        self.got_image_success(ijid, &image_clone);
                    }
                    Some(ImageEntry::Got(Err(err))) => {
                        job_error(&self.executor_adapter.deps, &mut *jobs, ijid, err.clone());
                    }
                }
            }
        }

        let mut jobs = self.jobs.lock().unwrap();
        if let Entry::Occupied(job_entry) = jobs.entry(ijid) {
            if job_entry.get().is_ready() {
                let job = job_entry.remove();
                drop(jobs);
                start_job(&self.executor_adapter.deps, job);
            }
        }
    }

    fn receive_add_container(
        &mut self,
        handle: DepsT::AddContainerHandle,
        name: String,
        spec: ContainerSpec,
    ) {
        self.executor_adapter
            .deps
            .container_added(handle, self.containers.insert(name, spec));
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
                        job_error(
                            &self.executor_adapter.deps,
                            &mut self.jobs.lock().unwrap(),
                            ijid,
                            err.clone(),
                        );
                    }
                }
            },
        }
    }

    fn got_image_success(&mut self, ijid: u64, image: &ConvertedImage) {
        if let Err(err) = self.got_image_success_inner(ijid, image) {
            job_error(
                &self.executor_adapter.deps,
                &mut self.jobs.lock().unwrap(),
                ijid,
                err,
            );
        }
    }

    fn got_image_success_inner(
        &mut self,
        ijid: u64,
        image: &ConvertedImage,
    ) -> Result<(), DepsT::Error> {
        let mut jobs = self.jobs.lock().unwrap();
        let Entry::Occupied(mut job_entry) = jobs.entry(ijid) else {
            return Ok(());
        };
        let job = job_entry.get_mut();

        job.job_spec
            .integrate_image(image)
            .map_err(DepsT::error_from_string)?;

        let layer_specs = job.job_spec.image_layers().to_owned();
        job.image_layers = vec![None; layer_specs.len()];
        drop(jobs);
        for (index, layer_spec) in layer_specs.into_iter().enumerate() {
            self.executor.add(Tag::BuildLayer(layer_spec.clone()));
            self.executor.evaluate(
                &mut self.executor_adapter,
                CompletedHandle::BuildLayer(true, ijid, index),
                &Tag::BuildLayer(layer_spec.clone()),
            );
        }

        let mut jobs = self.jobs.lock().unwrap();
        if let Entry::Occupied(job_entry) = jobs.entry(ijid) {
            if job_entry.get().is_ready() {
                let job = job_entry.remove();
                drop(jobs);
                start_job(&self.executor_adapter.deps, job);
            }
        }

        Ok(())
    }

    fn receive_got_layer(
        &mut self,
        spec: LayerSpec,
        result: Result<(Sha256Digest, ArtifactType), DepsT::Error>,
    ) {
        self.executor.receive_completed(
            &mut self.executor_adapter,
            &Tag::BuildLayer(spec),
            Output::BuildLayer(result),
        );
        self.executor_adapter
            .layer_builds
            .pop(&self.executor_adapter.deps);
    }
}

struct Job<DepsT: Deps> {
    handle: DepsT::PrepareJobHandle,
    job_spec: CollapsedJobSpec,
    image_layers: Vec<Option<(Sha256Digest, ArtifactType)>>,
    layers: Vec<Option<(Sha256Digest, ArtifactType)>>,
}

impl<DepsT: Deps> Job<DepsT> {
    fn into_handle_and_spec(
        self,
        deps: &DepsT,
    ) -> (DepsT::PrepareJobHandle, Result<JobSpec, DepsT::Error>) {
        assert!(self.is_ready());
        let Job {
            handle,
            mut job_spec,
            image_layers,
            layers,
        } = self;
        (handle, {
            let (initial_environment, environment) = job_spec.take_environment();
            deps.evaluate_environment(initial_environment, environment)
                .and_then(|environment| {
                    job_spec
                        .into_job_spec(
                            image_layers.into_iter().chain(layers).map(|layer| {
                                let Some((digest, artifact_type)) = layer else {
                                    panic!("shouldn't be called while awaiting any layers");
                                };
                                (digest, artifact_type)
                            }),
                            environment,
                        )
                        .map_err(DepsT::error_from_string)
                })
        })
    }

    fn is_ready(&self) -> bool {
        self.job_spec.image().is_none()
            && self.image_layers.iter().all(Option::is_some)
            && self.layers.iter().all(Option::is_some)
    }
}

#[cfg(test)]
mod tests {
    use super::{Message::*, *};
    use maelstrom_base::{
        job_spec, proc_mount, sys_mount, tar_digest, tmp_mount, CaptureFileSystemChanges,
        JobNetwork, JobRootOverlay, JobTty, WindowSize,
    };
    use maelstrom_client::spec;
    use maelstrom_client_base::{
        container_spec, converted_image, environment_spec, image_container_parent,
        job_spec as client_job_spec, tar_layer_spec,
    };
    use maelstrom_test::{millis, string};
    use std::{cell::RefCell, ffi::OsStr, rc::Rc, time::Duration};
    use TestMessage::*;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum TestMessage {
        JobPrepared(u32, Result<JobSpec, String>),
        ContainerAdded(u32, Option<ContainerSpec>),
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

        fn container_added(&self, handle: Self::AddContainerHandle, old: Option<ContainerSpec>) {
            self.borrow_mut()
                .messages
                .push(TestMessage::ContainerAdded(handle, old));
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
            JobPrepared(
                1,
                Err(string!(
                    "At least one layer must be specified, or an image must be used \
                    that has at least one layer."
                ))
            ),
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

    script_test! {
        prepare_job_one_layer,
        PrepareJob(
            1,
            client_job_spec! {
                "foo",
                arguments: ["arg1", "arg2"],
                layers: [tar_layer_spec!("foo.tar")],
            },
        ) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
        };
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(42))) => {
            JobPrepared(1, Ok(job_spec!("foo", [tar_digest!(42)], arguments: ["arg1", "arg2"]))),
        };
    }

    script_test! {
        prepare_job_two_layers,
        PrepareJob(
            1,
            client_job_spec! {
                "foo",
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
            BuildLayer(tar_layer_spec!("bar.tar")),
        };
        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(1))) => {};
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(2))) => {
            JobPrepared(1, Ok(job_spec!("foo", [ tar_digest!(2), tar_digest!(1) ]))),
        };
    }

    script_test! {
        prepare_job_two_jobs_1,
        PrepareJob(
            1,
            client_job_spec! {
                "foo",
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
            BuildLayer(tar_layer_spec!("bar.tar")),
        };
        PrepareJob(
            2,
            client_job_spec! {
                "bar",
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("baz.tar") ],
            },
        ) => {
            BuildLayer(tar_layer_spec!("baz.tar")),
        };
        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(1))) => {};
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(2))) => {
            JobPrepared(1, Ok(job_spec!("foo", [ tar_digest!(2), tar_digest!(1) ]))),
        };
        GotLayer(tar_layer_spec!("baz.tar"), Ok(tar_digest!(3))) => {
            JobPrepared(2, Ok(job_spec!("bar", [ tar_digest!(2), tar_digest!(3) ]))),
        };
    }

    script_test! {
        prepare_job_two_jobs_2,
        PrepareJob(
            1,
            client_job_spec! {
                "foo",
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
            BuildLayer(tar_layer_spec!("bar.tar")),
        };
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {};
        PrepareJob(
            2,
            client_job_spec! {
                "bar",
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("baz.tar") ],
            },
        ) => {
            BuildLayer(tar_layer_spec!("baz.tar")),
        };
        GotLayer(tar_layer_spec!("baz.tar"), Ok(tar_digest!(3))) => {
            JobPrepared(2, Ok(job_spec!("bar", [ tar_digest!(1), tar_digest!(3) ]))),
        };
        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(2))) => {
            JobPrepared(1, Ok(job_spec!("foo", [ tar_digest!(1), tar_digest!(2) ]))),
        };
    }

    script_test! {
        prepare_job_two_jobs_3,
        PrepareJob(
            1,
            client_job_spec! {
                "foo",
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
            BuildLayer(tar_layer_spec!("bar.tar")),
        };
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {};
        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(2))) => {
            JobPrepared(1, Ok(job_spec!("foo", [ tar_digest!(1), tar_digest!(2) ]))),
        };
        PrepareJob(
            2,
            client_job_spec! {
                "bar",
                layers: [ tar_layer_spec!("bar.tar"), tar_layer_spec!("foo.tar") ],
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
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
            BuildLayer(tar_layer_spec!("bar.tar")),
        };
        PrepareJob(
            2,
            client_job_spec! {
                "two",
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("baz.tar") ],
            },
        ) => {
            BuildLayer(tar_layer_spec!("baz.tar")),
        };
        PrepareJob(
            3,
            client_job_spec! {
                "three",
                layers: [ tar_layer_spec!("bar.tar"), tar_layer_spec!("baz.tar") ],
            },
        ) => {};

        GotLayer(tar_layer_spec!("foo.tar"), Err(string!("foo.tar error"))) => {
            JobPrepared(1, Err(string!("foo.tar error"))),
            JobPrepared(2, Err(string!("foo.tar error"))),
        };
        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(2))) => {};
        GotLayer(tar_layer_spec!("baz.tar"), Ok(tar_digest!(3))) => {
            JobPrepared(3, Ok(job_spec!("three", [ tar_digest!(2), tar_digest!(3) ]))),
        };

        PrepareJob(
            4,
            client_job_spec! {
                "four",
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar") ],
            },
        ) => {
            JobPrepared(4, Err(string!("foo.tar error"))),
        };
        PrepareJob(
            5,
            client_job_spec! {
                "five",
                layers: [ tar_layer_spec!("bar.tar"), tar_layer_spec!("baz.tar") ],
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
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar") ],
            },
        ) => {
            GetImage(string!("image1")),
            BuildLayer(tar_layer_spec!("foo.tar")),
            BuildLayer(tar_layer_spec!("bar.tar")),
        };
        GotImage(string!("image1"), Ok(converted_image!("image1", layers: [ "image1/1.tar", "image1/2.tar" ]))) => {
            BuildLayer(tar_layer_spec!("image1/1.tar")),
            BuildLayer(tar_layer_spec!("image1/2.tar")),
        };
        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(4))) => {};
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(3))) => {};
        GotLayer(tar_layer_spec!("image1/2.tar"), Ok(tar_digest!(12))) => {};
        GotLayer(tar_layer_spec!("image1/1.tar"), Ok(tar_digest!(11))) => {
            JobPrepared(1, Ok(job_spec!("one", [
                tar_digest!(11), tar_digest!(12), tar_digest!(3), tar_digest!(4),
            ]))),
        };

        PrepareJob(
            2,
            client_job_spec! {
                "two",
                parent: image_container_parent!("image2", layers),
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar") ],
            },
        ) => {
            GetImage(string!("image2")),
        };
        PrepareJob(
            3,
            client_job_spec! {
                "three",
                parent: image_container_parent!("image1", layers),
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("baz.tar") ],
            },
        ) => {
            BuildLayer(tar_layer_spec!("baz.tar")),
        };
        GotImage(string!("image2"), Ok(converted_image!("image2", layers: [ "image1/1.tar", "image2/2.tar" ]))) => {
            BuildLayer(tar_layer_spec!("image2/2.tar")),
        };
        GotLayer(tar_layer_spec!("image2/2.tar"), Ok(tar_digest!(22))) => {
            JobPrepared(2, Ok(job_spec!("two", [
                tar_digest!(11), tar_digest!(22), tar_digest!(3), tar_digest!(4),
            ]))),
        };
        GotLayer(tar_layer_spec!("baz.tar"), Ok(tar_digest!(5))) => {
            JobPrepared(3, Ok(job_spec!("three", [
                tar_digest!(11), tar_digest!(12), tar_digest!(3), tar_digest!(5),
            ]))),
        };

        PrepareJob(
            4,
            client_job_spec! {
                "four",
                parent: image_container_parent!("image1", layers),
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar") ],
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
                layers: [ tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar") ],
            },
        ) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
            BuildLayer(tar_layer_spec!("bar.tar")),
        };
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(3))) => {};
        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(4))) => {
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
                layers: [tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar")],
            },
        ) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
            BuildLayer(tar_layer_spec!("bar.tar")),
        };
        PrepareJob(
            3,
            client_job_spec! {
                "three",
                layers: [tar_layer_spec!("foo.tar")],
            },
        ) => {};

        GotImage(string!("image"), Err(string!("image error"))) => {
            JobPrepared(1, Err(string!("image error"))),
            JobPrepared(2, Err(string!("image error"))),
        };
        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(2))) => {};
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {
            JobPrepared(3, Ok(job_spec!("three", [tar_digest!(1)]))),
        };

        PrepareJob(
            4,
            client_job_spec! {
                "four",
                parent: image_container_parent!("image", layers),
                layers: [tar_layer_spec!("baz.tar")],
            },
        ) => {
            BuildLayer(tar_layer_spec!("baz.tar")),
            JobPrepared(4, Err(string!("image error"))),
        };
        GotLayer(tar_layer_spec!("baz.tar"), Ok(tar_digest!(3))) => {};
    }

    script_test! {
        prepare_job_all_fields,

        PrepareJob(1, client_job_spec! {
            "one",
            layers: [tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar")],
            enable_writable_file_system: true,
            environment: [
                environment_spec!(extend: false, "FOO" => "foo", "BAR" => "bar"),
                environment_spec!("FOO" => "frob", "BAZ" => "baz"),
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
            capture_file_system_changes: CaptureFileSystemChanges {
                upper: "upper".into(),
                work: "work".into(),
            },
        }) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
            BuildLayer(tar_layer_spec!("bar.tar")),
        };
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {};
        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(2))) => {
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
                root_overlay: JobRootOverlay::Local(CaptureFileSystemChanges {
                    upper: "upper".into(),
                    work: "work".into(),
                }),
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
            layers: [tar_layer_spec!("foo.tar")],
        }) => {
            GetImage(string!("image")),
            BuildLayer(tar_layer_spec!("foo.tar")),
        };
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {};
        GotImage(string!("image"), Ok(converted_image!("image", layers: ["image1/1.tar"]))) => {
            JobPrepared(1, Ok(job_spec! {
                "one",
                [tar_digest!(1)],
            })),
        };
    }

    script_test! {
        prepare_job_environment_from_image,

        PrepareJob(1, client_job_spec! {
            "one",
            parent: image_container_parent!("image", environment),
            layers: [tar_layer_spec!("foo.tar")],
        }) => {
            GetImage(string!("image")),
            BuildLayer(tar_layer_spec!("foo.tar")),
        };
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {};
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
            layers: [tar_layer_spec!("foo.tar")],
            environment: [
                environment_spec!(extend: false, "OLD_FOO" => "old_$prev{FOO}", "BAZ" => "$prev{BAR}"),
                environment_spec!(extend: true, "FOO" => "food"),
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
            layers: [tar_layer_spec!("foo.tar")],
            environment: [environment_spec!("BAZ" => "baz")],
        }) => {
            GetImage(string!("image")),
            BuildLayer(tar_layer_spec!("foo.tar")),
        };

        PrepareJob(2, client_job_spec! {
            "two",
            layers: [tar_layer_spec!("foo.tar")],
            environment: [environment_spec!("BAZ" => "baz")],
        }) => {};

        GotImage(string!("image"), Ok(converted_image! {
            "image",
            layers: ["image1/1.tar"],
            environment: ["FOO=foo", "BAR=bar"],
        })) => {};

        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {
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
            layers: [tar_layer_spec!("foo.tar")],
            environment: [environment_spec!("OLD_FOO" => "old_$prev{FOO}")],
        }) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
        };
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {
            JobPrepared(1, Err(string!(r#"unknown variable "FOO""#))),
        };

        PrepareJob(2, client_job_spec! {
            "two",
            layers: [tar_layer_spec!("foo.tar")],
            environment: [environment_spec!("FOO" => "$env{FOO}")],
        }) => {
            JobPrepared(2, Err(string!(r#"unknown variable "FOO""#))),
        };
    }

    script_test! {
        prepare_job_working_directory_from_image,

        PrepareJob(1, client_job_spec! {
            "one",
            parent: image_container_parent!("image", working_directory),
            layers: [tar_layer_spec!("foo.tar")],
        }) => {
            GetImage(string!("image")),
            BuildLayer(tar_layer_spec!("foo.tar")),
        };

        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {};

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
            layers: [tar_layer_spec!("foo.tar")],
            working_directory: "/root2",
        }) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
        };

        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {
            JobPrepared(1, Ok(job_spec!{
                "one",
                [tar_digest!(1)],
                working_directory: "/root2",
            })),
        };
    }

    script_test! {
        prepare_job_working_directory_from_image_missing_working_directory,

        PrepareJob(1, client_job_spec! {
            "one",
            parent: image_container_parent!("image", working_directory),
            layers: [tar_layer_spec!("foo.tar")],
        }) => {
            GetImage(string!("image")),
            BuildLayer(tar_layer_spec!("foo.tar")),
        };

        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {};

        GotImage(string!("image"), Ok(converted_image!{"image"})) => {
            JobPrepared(1, Ok(job_spec!{"one", [tar_digest!(1)]})),
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
            layers: [tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar")],
        }) => {
            BuildLayer(tar_layer_spec!("foo.tar")),
        };

        PrepareJob(2, client_job_spec! {
            "two",
            layers: [tar_layer_spec!("foo.tar"), tar_layer_spec!("baz.tar")],
        }) => {};

        GotLayer(tar_layer_spec!("foo.tar"), Err(string!("foo.tar error"))) => {
            BuildLayer(tar_layer_spec!("bar.tar")),
            JobPrepared(1, Err(string!("foo.tar error"))),
            JobPrepared(2, Err(string!("foo.tar error"))),
        };

        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(2))) => {
            BuildLayer(tar_layer_spec!("baz.tar")),
        };

        GotLayer(tar_layer_spec!("baz.tar"), Ok(tar_digest!(3))) => {};

        PrepareJob(3, client_job_spec! {
            "three",
            layers: [
                tar_layer_spec!("bar.tar"),
                tar_layer_spec!("baz.tar"),
                tar_layer_spec!("qux.tar"),
                tar_layer_spec!("frob.tar"),
            ],
        }) => {
            BuildLayer(tar_layer_spec!("qux.tar")),
        };

        GotLayer(tar_layer_spec!("qux.tar"), Ok(tar_digest!(4))) => {
            BuildLayer(tar_layer_spec!("frob.tar")),
        };

        GotLayer(tar_layer_spec!("frob.tar"), Ok(tar_digest!(5))) => {
            JobPrepared(3, Ok(job_spec! {
                "three",
                [tar_digest!(2), tar_digest!(3), tar_digest!(4), tar_digest!(5)],
            })),
        };
    }

    script_test! {
        add_container_duplicate,
        AddContainer(0, string!("foo"), container_spec!{ network: JobNetwork::Loopback }) => {
            ContainerAdded(0, None),
        };
        AddContainer(1, string!("foo"), container_spec!{ network: JobNetwork::Local }) => {
            ContainerAdded(1, Some(container_spec!{ network: JobNetwork::Loopback })),
        };
    }
}
