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
    ContainerSpec, ConvertedImage, EnvironmentSpec, ImageUse, JobSpec as ClientJobSpec, LayerSpec,
};
use maelstrom_util::executor::{self, Executor, Graph, StartResult};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::Debug,
    num::NonZeroUsize,
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
    ) -> Result<Vec<String>, String>;
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
    executor: Executor<ExecutorAdapter<DepsT>>,
}

struct ExecutorAdapter<DepsT: Deps> {
    deps: DepsT,
    layer_builds: LayerBuilds,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Tag {
    JobSpec(CollapsedJobSpec),
    Image(String),
    ImageWithLayers(String),
    Layer(LayerSpec),
}

#[derive(Debug)]
enum Output<DepsT: Deps> {
    JobSpec(Result<JobSpec, DepsT::Error>),
    Image(Result<ConvertedImage, DepsT::Error>),
    #[allow(clippy::type_complexity)]
    ImageWithLayers(Result<(ConvertedImage, Vec<(Sha256Digest, ArtifactType)>), DepsT::Error>),
    Layer(Result<(Sha256Digest, ArtifactType), DepsT::Error>),
}

#[derive(Debug)]
enum Partial {
    ImageWithLayersStage2,
}

impl<DepsT: Deps> executor::Deps for ExecutorAdapter<DepsT> {
    type CompletedHandle = DepsT::PrepareJobHandle;
    type Tag = Tag;
    type Partial = Partial;
    type Output = Output<DepsT>;

    fn start(
        &mut self,
        tag: Self::Tag,
        partial: Option<Self::Partial>,
        inputs: Vec<&Self::Output>,
        _graph: &mut Graph<Self>,
    ) -> StartResult<Self::Tag, Self::Partial, Self::Output> {
        eprintln!("got {tag:?} {partial:?}");
        match tag {
            Tag::Layer(layer_spec) => {
                assert_matches!(partial, None);
                assert!(inputs.is_empty());
                self.layer_builds.push(&self.deps, layer_spec.clone());
                StartResult::InProgress
            }
            Tag::Image(image_name) => {
                assert_matches!(partial, None);
                assert!(inputs.is_empty());
                self.deps.get_image(image_name.to_owned());
                StartResult::InProgress
            }
            Tag::ImageWithLayers(_) => match partial {
                None => match &inputs[..] {
                    [Output::Image(Ok(converted_image))] => match converted_image.layers() {
                        Err(err) => StartResult::Completed(Output::ImageWithLayers(Err(
                            DepsT::error_from_string(err.clone()),
                        ))),
                        Ok(layers) => StartResult::Expand {
                            partial: Partial::ImageWithLayersStage2,
                            added_inputs: layers
                                .iter()
                                .map(|layer_spec| (Tag::Layer(layer_spec.clone()), vec![]))
                                .collect(),
                        },
                    },
                    [Output::Image(Err(err))] => {
                        StartResult::Completed(Output::ImageWithLayers(Err(err.clone())))
                    }
                    _ => {
                        panic!("expected exactly one Output::Image as input");
                    }
                },
                Some(Partial::ImageWithLayersStage2) => match &inputs[..] {
                    [Output::Image(Ok(converted_image)), layers @ ..] => {
                        let layers: Result<Vec<_>, _> = layers
                            .iter()
                            .map(|layer| {
                                let Output::Layer(layer) = layer else {
                                    panic!("wrong inputs");
                                };
                                layer.clone()
                            })
                            .collect();
                        match layers {
                            Err(err) => StartResult::Completed(Output::ImageWithLayers(Err(err))),
                            Ok(layers) => StartResult::Completed(Output::ImageWithLayers(Ok((
                                converted_image.clone(),
                                layers,
                            )))),
                        }
                    }
                    _ => {
                        panic!("wrong inputs");
                    }
                },
            },
            Tag::JobSpec(collapsed_job_spec) => {
                assert_matches!(partial, None);
                match collapsed_job_spec.image() {
                    None => {
                        let layers: Result<Vec<_>, _> = inputs
                            .iter()
                            .map(|layer| {
                                let Output::Layer(layer) = layer else {
                                    panic!("wrong inputs");
                                };
                                layer.clone()
                            })
                            .collect();
                        match layers {
                            Err(err) => StartResult::Completed(Output::JobSpec(Err(err))),
                            Ok(layers) => StartResult::Completed(Output::JobSpec(
                                collapsed_job_spec
                                    .clone()
                                    .into_job_spec_without_image(
                                        |initial_environment, environment| {
                                            self.deps.evaluate_environment(
                                                initial_environment,
                                                environment,
                                            )
                                        },
                                        layers,
                                    )
                                    .map_err(DepsT::error_from_string),
                            )),
                        }
                    }
                    Some(image) if !image.r#use.contains(ImageUse::Layers) => match &inputs[..] {
                        [Output::Image(Err(err)), ..] => {
                            StartResult::Completed(Output::JobSpec(Err(err.clone())))
                        }
                        [Output::Image(Ok(image)), layers @ ..] => {
                            let layers: Result<Vec<_>, _> = layers
                                .iter()
                                .map(|layer| {
                                    let Output::Layer(layer) = layer else {
                                        panic!("wrong inputs");
                                    };
                                    layer.clone()
                                })
                                .collect();
                            match layers {
                                Err(err) => StartResult::Completed(Output::JobSpec(Err(err))),
                                Ok(layers) => StartResult::Completed(Output::JobSpec(
                                    collapsed_job_spec
                                        .clone()
                                        .into_job_spec_with_image(
                                            |initial_environment, environment| {
                                                self.deps.evaluate_environment(
                                                    initial_environment,
                                                    environment,
                                                )
                                            },
                                            layers,
                                            image,
                                            [],
                                        )
                                        .map_err(DepsT::error_from_string),
                                )),
                            }
                        }
                        _ => {
                            panic!("wrong inputs");
                        }
                    },
                    Some(_) => match &inputs[..] {
                        [Output::ImageWithLayers(Err(err)), ..] => {
                            StartResult::Completed(Output::JobSpec(Err(err.clone())))
                        }
                        [Output::ImageWithLayers(Ok((image, image_layers))), layers @ ..] => {
                            let layers: Result<Vec<_>, _> = layers
                                .iter()
                                .map(|layer| {
                                    let Output::Layer(layer) = layer else {
                                        panic!("wrong inputs");
                                    };
                                    layer.clone()
                                })
                                .collect();
                            match layers {
                                Err(err) => StartResult::Completed(Output::JobSpec(Err(err))),
                                Ok(layers) => StartResult::Completed(Output::JobSpec(
                                    collapsed_job_spec
                                        .clone()
                                        .into_job_spec_with_image(
                                            |initial_environment, environment| {
                                                self.deps.evaluate_environment(
                                                    initial_environment,
                                                    environment,
                                                )
                                            },
                                            layers,
                                            image,
                                            image_layers.iter().cloned(),
                                        )
                                        .map_err(DepsT::error_from_string),
                                )),
                            }
                        }
                        _ => {
                            panic!("wrong inputs");
                        }
                    },
                }
            }
        }
    }

    fn completed(&mut self, handle: Self::CompletedHandle, tag: &Self::Tag, output: &Self::Output) {
        assert_matches!(tag, Tag::JobSpec(_));
        let Output::JobSpec(result) = output else {
            panic!("wrong output type");
        };
        self.deps.job_prepared(handle, result.clone());
    }
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

impl<DepsT: Deps> Preparer<DepsT> {
    pub fn new(deps: DepsT, max_pending_layer_builds: NonZeroUsize) -> Self {
        Self {
            executor_adapter: ExecutorAdapter {
                deps,
                layer_builds: LayerBuilds::new(max_pending_layer_builds),
            },
            containers: Default::default(),
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
        let collapsed_job_spec = match CollapsedJobSpec::new(job_spec, &|c| self.containers.get(c))
        {
            Ok(job_spec) => job_spec,
            Err(err) => {
                self.executor_adapter
                    .deps
                    .job_prepared(handle, Err(DepsT::error_from_string(err)));
                return;
            }
        };

        let inputs = (match collapsed_job_spec.image() {
            None => None,
            Some(image) if !image.r#use.contains(ImageUse::Layers) => {
                let tag = Tag::Image(image.name.clone());
                self.executor.add(tag.clone());
                Some(tag)
            }
            Some(image) => {
                let image_tag = Tag::Image(image.name.clone());
                self.executor.add(image_tag.clone());
                let tag = Tag::ImageWithLayers(image.name.clone());
                self.executor.add_with_inputs(tag.clone(), [image_tag]);
                Some(tag)
            }
        })
        .into_iter()
        .chain(collapsed_job_spec.layers().iter().map(|layer_spec| {
            let tag = Tag::Layer(layer_spec.clone());
            self.executor.add(tag.clone());
            tag
        }))
        .collect::<Vec<_>>();

        let tag = Tag::JobSpec(collapsed_job_spec);
        let evaluation_handle = self.executor.add_with_inputs(tag, inputs);
        self.executor
            .evaluate(&mut self.executor_adapter, evaluation_handle, handle);
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
        self.executor.receive_completed(
            &mut self.executor_adapter,
            &Tag::Image(name.to_owned()),
            Output::Image(result),
        );
    }

    fn receive_got_layer(
        &mut self,
        spec: LayerSpec,
        result: Result<(Sha256Digest, ArtifactType), DepsT::Error>,
    ) {
        self.executor.receive_completed(
            &mut self.executor_adapter,
            &Tag::Layer(spec),
            Output::Layer(result),
        );
        self.executor_adapter
            .layer_builds
            .pop(&self.executor_adapter.deps);
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

        GotLayer(tar_layer_spec!("foo.tar"), Err(string!("foo.tar error"))) => {};
        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(2))) => {
            JobPrepared(1, Err(string!("foo.tar error"))),
        };
        GotLayer(tar_layer_spec!("baz.tar"), Ok(tar_digest!(3))) => {
            JobPrepared(2, Err(string!("foo.tar error"))),
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
        };
        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(2))) => {};
        GotLayer(tar_layer_spec!("foo.tar"), Ok(tar_digest!(1))) => {
            JobPrepared(2, Err(string!("image error"))),
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
        };
        GotLayer(tar_layer_spec!("baz.tar"), Ok(tar_digest!(3))) => {
            JobPrepared(4, Err(string!("image error"))),
        };
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
        };

        GotLayer(tar_layer_spec!("bar.tar"), Ok(tar_digest!(2))) => {
            BuildLayer(tar_layer_spec!("baz.tar")),
            JobPrepared(1, Err(string!("foo.tar error"))),
        };

        GotLayer(tar_layer_spec!("baz.tar"), Ok(tar_digest!(3))) => {
            JobPrepared(2, Err(string!("foo.tar error"))),
        };

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
