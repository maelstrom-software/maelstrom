use maelstrom_base::{
    ArtifactType, EnumSet, GroupId, JobMount, JobNetwork, JobRootOverlay, JobSpec as BaseJobSpec,
    JobTty, NonEmpty, Sha256Digest, Timeout, UserId, Utf8PathBuf,
};
use maelstrom_client_base::spec::{
    ContainerParent, ContainerSpec, ContainerUse, ConvertedImage, EnvironmentSpec, ImageRef,
    ImageUse, JobSpec, LayerSpec,
};
use std::{collections::BTreeMap, time::Duration};

/// A [`CollapsedJobSpec`] is a bridge between a [`maelstrom_client_base::spec::JobSpec`] and a
/// [`maelstrom_base::JobSpec`]. The former has an embedded
/// [`maelstrom_client_base::spec::ContainerSpec`], which has a `parent` field, which can reference
/// another named [`maelstrom_client_base::spec::ContainerSpec`], an image, or nothing. Moreover,
/// the `parent` field allows for selecting only certain parts of the referenced named container or
/// image.
///
/// Taken together, [`maelstrom_client_base::spec::JobSpec`] can be viewed as a linked list of
/// containers, with an optional image at the beginning of the list, and where the last element of
/// the list has some extra fields. This struct represents what happens when that linked list is
/// collapsed into a single struct. In that regard, it much more closely resembles
/// [`maelstrom_base::JobSpec`] than it does [`maelstrom_client_base::spec::JobSpec`].
///
/// However, it is different in the sense that tracks if an image needs to be fetched, and if so,
/// what aspects of that image need to be integrated into the job spec. It also keeps tracks some
/// fields separately to make the job of the [`crate::preparer::Preparer`] easier.
#[derive(Debug, Eq, PartialEq)]
pub struct CollapsedJobSpec {
    pub layers: Vec<LayerSpec>,
    pub root_overlay: Option<JobRootOverlay>,
    pub environment: Vec<EnvironmentSpec>,
    pub working_directory: Option<Utf8PathBuf>,
    pub mounts: Vec<JobMount>,
    pub network: Option<JobNetwork>,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
    pub image: Option<ImageRef>,
    pub initial_environment: BTreeMap<String, String>,
    pub image_layers: Vec<LayerSpec>,
    pub program: Utf8PathBuf,
    pub arguments: Vec<String>,
    pub timeout: Option<Timeout>,
    pub estimated_duration: Option<Duration>,
    pub allocate_tty: Option<JobTty>,
    pub priority: i8,
}

#[macro_export]
/// Easily create a [`CollapsedJobSpec`].
///
/// The macro must be passed at least one argument. The first argument will be set to the
/// `program` field. For example:
///
/// ```
/// assert_eq!(collapsed_job_spec! { "cat" }.program, "dog");
/// assert_eq!(collapsed_job_spec! { "cat", user: 1 }.program, "cat");
/// ```
///
/// Extra fields can be specified using the field name followed by the value. If the field type is
/// `Option`, then the value will automatically be wrapped in `Some`. Also, `Into::into` will be
/// called on scalar values, and collection values will be run through an `IntoIterator` and a map
/// of `Into::into`. Some examples:
///
/// ```
/// assert_eq!(
///     collapsed_job_spec! { "cat", layers: [tar_layer!("foo.tar")] }.layers,
///     vec![tar_layer!("foo.tar")]);
/// assert_eq!(collapsed_job_spec! { "cat", user: 3 }.user, User::new(3));
/// ```
///
#[cfg(test)]
macro_rules! collapsed_job_spec {
    (@expand [$program:expr] [] -> []) => {
        $crate::collapsed_job_spec::CollapsedJobSpec {
            layers: Default::default(),
            root_overlay: Default::default(),
            environment: Default::default(),
            working_directory: Default::default(),
            mounts: Default::default(),
            network: Default::default(),
            user: Default::default(),
            group: Default::default(),
            image: Default::default(),
            initial_environment: Default::default(),
            image_layers: Default::default(),
            program: $program.into(),
            arguments: Default::default(),
            timeout: Default::default(),
            estimated_duration: Default::default(),
            allocate_tty: Default::default(),
            priority: Default::default(),
        }
    };
    (@expand [$program:expr] [] -> [$($field:tt)+]) => {
        $crate::collapsed_job_spec::CollapsedJobSpec {
            $($field)+,
            .. collapsed_job_spec!(@expand [$program] [] -> [])
        }
    };
    (@expand [$program:expr] [image: $image:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? image: Some($image.into())])
    };
    (@expand [$program:expr] [initial_environment: $initial_environment:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? initial_environment: $initial_environment.into_iter().map(|(k, v)| (k.into(), v.into())).collect()])
    };
    (@expand [$program:expr] [image_layers: $image_layers:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? image_layers: $image_layers.into_iter().map(Into::into).collect()])
    };
    (@expand [$program:expr] [layers: $layers:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? layers: $layers.into_iter().map(Into::into).collect()])
    };
    (@expand [$program:expr] [root_overlay: $root_overlay:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? root_overlay: $root_overlay.into()])
    };
    (@expand [$program:expr] [environment: $environment:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? environment: $environment.into_iter().map(Into::into).collect()])
    };
    (@expand [$program:expr] [working_directory: $working_directory:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? working_directory: Some($working_directory.into())])
    };
    (@expand [$program:expr] [mounts: $mounts:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? mounts: $mounts.into_iter().map(Into::into).collect()])
    };
    (@expand [$program:expr] [network: $network:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? network: $network.into()])
    };
    (@expand [$program:expr] [user: $user:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? user: Some($user.into())])
    };
    (@expand [$program:expr] [group: $group:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? group: Some($group.into())])
    };
    (@expand [$program:expr] [arguments: $arguments:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? arguments: $arguments.into_iter().map(Into::into).collect()])
    };
    (@expand [$program:expr] [timeout: $timeout:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? timeout: ::maelstrom_base::Timeout::new($timeout)])
    };
    (@expand [$program:expr] [estimated_duration: $estimated_duration:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? estimated_duration: Some($estimated_duration.into())])
    };
    (@expand [$program:expr] [allocate_tty: $allocate_tty:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? allocate_tty: Some($allocate_tty.into())])
    };
    (@expand [$program:expr] [priority: $priority:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? priority: $priority.into()])
    };
    ($program:expr $(,$($field_in:tt)*)?) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] -> [])
    };
}

impl CollapsedJobSpec {
    /// Build a [`CollapsedJobSpec`] from a [`maelstrom_client_base::spec::JobSpec`].
    ///
    /// If the provided spec has a parent field that refers to a named container, that container
    /// will be looked up using the provided `container_resolver`. This will continue recursively
    /// until either: a) an ancestor container has no parent, b) an ancestor container's parent is
    /// an image, c) a named ancestor can't be found, or d) an ancestor cycle is found. In the
    /// latter two cases, an error will be returned.
    ///
    /// If the most distant ancestor is an image, then the `image` field will be `Some` and will
    /// contain the name of the image and what aspects of it should be imported. When the image is
    /// fetched, [`Self::integrate_image`] should be called to complete construction.
    pub fn new<'a, F>(job_spec: JobSpec, container_resolver: &'a F) -> Result<Self, String>
    where
        F: for<'b> Fn(&'b str) -> Option<&'a ContainerSpec>,
    {
        let JobSpec {
            container:
                ContainerSpec {
                    parent: mut next_parent,
                    mut layers,
                    mut root_overlay,
                    mut environment,
                    mut working_directory,
                    mut mounts,
                    mut network,
                    mut user,
                    mut group,
                },
            program,
            arguments,
            timeout,
            estimated_duration,
            allocate_tty,
            priority,
        } = job_spec;
        let mut use_mask = EnumSet::all();
        let mut image = None;
        while let Some(parent) = next_parent {
            match parent {
                ContainerParent::Image(mut image_ref) => {
                    // Remove fields that have been previously masked out by more direct ancestors.
                    for image_use in EnumSet::<ImageUse>::all() {
                        if !use_mask.contains(image_use.to_container_use()) {
                            image_ref.r#use.remove(image_use);
                        }
                    }

                    // If the working directory has been set by a more direct ancestor, don't
                    // include it from the image.
                    if working_directory.is_some() {
                        image_ref.r#use.remove(ImageUse::WorkingDirectory);
                    }

                    // If we're not going to use any fields from the image, don't bother getting
                    // it.
                    if !image_ref.r#use.is_empty() {
                        image = Some(image_ref);
                    }

                    break;
                }

                ContainerParent::Container(container_ref) => {
                    let parent = container_resolver(&container_ref.name).ok_or_else(|| {
                        format!("couldn't find parent container {:?}", &container_ref.name)
                    })?;
                    use_mask = use_mask.intersection(container_ref.r#use);
                    for container_use in use_mask {
                        match container_use {
                            ContainerUse::Layers => {
                                layers = parent.layers.iter().cloned().chain(layers).collect();
                            }
                            ContainerUse::RootOverlay => {
                                root_overlay = root_overlay.or_else(|| parent.root_overlay.clone());
                            }
                            ContainerUse::Environment => {
                                environment = parent
                                    .environment
                                    .iter()
                                    .cloned()
                                    .chain(environment)
                                    .collect();
                            }
                            ContainerUse::WorkingDirectory => {
                                working_directory =
                                    working_directory.or_else(|| parent.working_directory.clone());
                            }
                            ContainerUse::Mounts => {
                                mounts = parent.mounts.iter().cloned().chain(mounts).collect();
                            }
                            ContainerUse::Network => {
                                network = network.or(parent.network);
                            }
                            ContainerUse::User => {
                                user = user.or(parent.user);
                            }
                            ContainerUse::Group => {
                                group = group.or(parent.group);
                            }
                        }
                    }
                    next_parent = parent.parent.clone();
                }
            }
        }
        Ok(CollapsedJobSpec {
            layers,
            root_overlay,
            environment,
            working_directory,
            mounts,
            network,
            user,
            group,
            image,
            initial_environment: Default::default(),
            image_layers: Default::default(),
            program,
            arguments,
            timeout,
            estimated_duration,
            allocate_tty,
            priority,
        })
    }

    /// Integrate the fields of an image into [`Self`].
    ///
    /// This function only makes sense when the `self.image` field is `Some`. The required fields
    /// from the image will be integrated into `image_layers`, `initial_environment`, and
    /// `working_directory` as specified. It is an error for any of those fields to be set to
    /// non-default values beforehand.
    ///
    /// After this function is called, `self.image` will be `None`.
    pub fn integrate_image(&mut self, image: &ConvertedImage) -> Result<(), String> {
        let image_use = self.image.take().unwrap().r#use;
        if image_use.contains(ImageUse::Layers) {
            assert!(self.image_layers.is_empty());
            self.image_layers = image.layers()?;
        }
        if image_use.contains(ImageUse::Environment) {
            assert!(self.initial_environment.is_empty());
            self.initial_environment = image.environment()?;
        }
        if image_use.contains(ImageUse::WorkingDirectory) {
            assert!(self.working_directory.is_none());
            self.working_directory = image.working_directory();
        }
        Ok(())
    }

    fn check(&self) -> Result<(), String> {
        assert!(self.image.is_none());
        if self.network == Some(JobNetwork::Local)
            && self
                .mounts
                .iter()
                .any(|m| matches!(m, JobMount::Sys { .. }))
        {
            Err("A \"sys\" mount is not compatible with local networking. \
                Check the documentation for the \"network\" field of \"JobSpec\"."
                .into())
        } else if self.layers.is_empty() && self.image_layers.is_empty() {
            Err("At least one layer must be specified, or an image must be \
                used that has at least one layer."
                .into())
        } else {
            Ok(())
        }
    }

    pub fn into_job_spec(
        self,
        layers: impl Iterator<Item = (Sha256Digest, ArtifactType)>,
        environment: Vec<String>,
    ) -> Result<BaseJobSpec, String> {
        self.check()?;
        let layers = NonEmpty::collect(layers).unwrap();
        assert_eq!(layers.len(), self.image_layers.len() + self.layers.len());
        let Self {
            layers: _,
            root_overlay,
            environment: _,
            working_directory,
            mounts,
            network,
            user,
            group,
            image: _,
            initial_environment: _,
            image_layers: _,
            program,
            arguments,
            timeout,
            estimated_duration,
            allocate_tty,
            priority,
        } = self;
        Ok(BaseJobSpec {
            program,
            arguments,
            environment,
            layers,
            mounts,
            network: network.unwrap_or_default(),
            root_overlay: root_overlay.unwrap_or_default(),
            working_directory: working_directory.unwrap_or_else(|| "/".into()),
            user: user.unwrap_or(0.into()),
            group: group.unwrap_or(0.into()),
            timeout,
            estimated_duration,
            allocate_tty,
            priority,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maelstrom_base::{proc_mount, tmp_mount, WindowSize};
    use maelstrom_client_base::{
        container_container_parent, container_spec, converted_image, environment_spec,
        image_container_parent, image_ref, job_spec,
    };
    use maelstrom_test::{millis, tar_layer};
    use std::collections::HashMap;

    #[test]
    fn program() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! {parent: image_container_parent!("image", all)},
            ),
            (
                "p2",
                container_spec! {parent: container_container_parent!("p1", all)},
            ),
            (
                "p3",
                container_spec! {parent: container_container_parent!("p2", all)},
            ),
            ("p4", container_spec! {}),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(job_spec! {"prog"}, &|c| containers.get(c)),
            Ok(collapsed_job_spec! {"prog"}),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {"prog", parent: container_container_parent!("p1", all)},
                &|c| containers.get(c),
            ),
            Ok(collapsed_job_spec! {"prog", image: image_ref!("image", all)}),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {"prog", parent: container_container_parent!("p3", all)},
                &|c| containers.get(c),
            ),
            Ok(collapsed_job_spec! {"prog", image: image_ref!("image", all)}),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {"prog", parent: container_container_parent!("p4", all)},
                &|c| containers.get(c),
            ),
            Ok(collapsed_job_spec! {"prog"}),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {"prog", parent: image_container_parent!("image", all)},
                &|c| containers.get(c),
            ),
            Ok(collapsed_job_spec! {"prog", image: image_ref!("image", all)}),
        );
    }

    #[test]
    fn layers() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! {
                    parent: image_container_parent!("image", all),
                    layers: [tar_layer!("p1.tar")],
                },
            ),
            (
                "p2",
                container_spec! {
                    parent: container_container_parent!("p1", all),
                    layers: [tar_layer!("p2.tar")],
                },
            ),
            (
                "p3",
                container_spec! {
                    parent: container_container_parent!("p2", environment),
                    layers: [tar_layer!("p3.tar")],
                },
            ),
            (
                "p4",
                container_spec! {
                    layers: [tar_layer!("p4.tar")],
                },
            ),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer!("foo.tar")],
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer!("foo.tar")],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer!("foo.tar")],
                    parent: image_container_parent!("image", layers),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer!("foo.tar")],
                image: image_ref!("image", layers),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer!("foo.tar")],
                    parent: image_container_parent!("image"),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer!("foo.tar")],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer!("foo.tar")],
                    parent: container_container_parent!("p1", layers),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer!("p1.tar"), tar_layer!("foo.tar")],
                image: image_ref!("image", layers),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer!("foo.tar")],
                    parent: container_container_parent!("p1"),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer!("foo.tar")],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer!("foo.tar")],
                    parent: container_container_parent!("p2", layers),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer!("p1.tar"), tar_layer!("p2.tar"), tar_layer!("foo.tar")],
                image: image_ref!("image", layers),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer!("foo.tar")],
                    parent: container_container_parent!("p3", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer!("p3.tar"), tar_layer!("foo.tar")],
                image: image_ref!("image", environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer!("foo.tar")],
                    parent: container_container_parent!("p4", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer!("p4.tar"), tar_layer!("foo.tar")],
            }),
        );
    }

    #[test]
    fn root_overlay() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! {
                    parent: image_container_parent!("image", all),
                    root_overlay: JobRootOverlay::Tmp,
                },
            ),
            (
                "p2",
                container_spec! {
                    parent: container_container_parent!("p1", all),
                },
            ),
            (
                "p3",
                container_spec! {
                    parent: container_container_parent!("p2", all),
                    root_overlay: JobRootOverlay::None,
                },
            ),
            (
                "p4",
                container_spec! {
                    parent: container_container_parent!("p2", environment),
                },
            ),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(job_spec! {"prog"}, &|c| containers.get(c)),
            Ok(collapsed_job_spec! {"prog"}),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    root_overlay: JobRootOverlay::Local {
                        upper: "upper".into(),
                        work: "work".into(),
                    },
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                root_overlay: JobRootOverlay::Local {
                    upper: "upper".into(),
                    work: "work".into(),
                },
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    root_overlay: JobRootOverlay::Local {
                        upper: "upper".into(),
                        work: "work".into(),
                    },
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                root_overlay: JobRootOverlay::Local {
                    upper: "upper".into(),
                    work: "work".into(),
                },
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    root_overlay: JobRootOverlay::Local {
                        upper: "upper".into(),
                        work: "work".into(),
                    },
                    parent: image_container_parent!("image", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                root_overlay: JobRootOverlay::Local {
                    upper: "upper".into(),
                    work: "work".into(),
                },
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                root_overlay: JobRootOverlay::Tmp,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p2", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                root_overlay: JobRootOverlay::Tmp,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p3", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                root_overlay: JobRootOverlay::None,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p4", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                image: image_ref!("image", environment),
            }),
        );
    }

    #[test]
    fn environment() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! {
                    parent: image_container_parent!("image", all),
                    environment: [environment_spec!{true, "FOO" => "foo1"}],
                },
            ),
            (
                "p2",
                container_spec! {
                    parent: container_container_parent!("p1", all),
                    environment: [environment_spec!{true, "FOO" => "foo2"}],
                },
            ),
            (
                "p3",
                container_spec! {
                    parent: container_container_parent!("p2", layers),
                    environment: [environment_spec!{true, "FOO" => "foo3"}],
                },
            ),
            (
                "p4",
                container_spec! {
                    environment: [environment_spec!{true, "FOO" => "foo4"}],
                },
            ),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!{true, "FOO" => "foo"}],
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [environment_spec!{true, "FOO" => "foo"}],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!{true, "FOO" => "foo"}],
                    parent: image_container_parent!("image", environment),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [environment_spec!{true, "FOO" => "foo"}],
                image: image_ref!("image", environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!{true, "FOO" => "foo"}],
                    parent: image_container_parent!("image", layers),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [environment_spec!{true, "FOO" => "foo"}],
                image: image_ref!("image", layers),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!{true, "FOO" => "foo"}],
                    parent: container_container_parent!("p1", environment),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [
                    environment_spec!{true, "FOO" => "foo1"},
                    environment_spec!{true, "FOO" => "foo"},
                ],
                image: image_ref!("image", environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!{true, "FOO" => "foo"}],
                    parent: container_container_parent!("p2", environment),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [
                    environment_spec!{true, "FOO" => "foo1"},
                    environment_spec!{true, "FOO" => "foo2"},
                    environment_spec!{true, "FOO" => "foo"},
                ],
                image: image_ref!("image", environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!{true, "FOO" => "foo"}],
                    parent: container_container_parent!("p3", environment),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [
                    environment_spec!{true, "FOO" => "foo3"},
                    environment_spec!{true, "FOO" => "foo"},
                ],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!{true, "FOO" => "foo"}],
                    parent: container_container_parent!("p4", environment),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [
                    environment_spec!{true, "FOO" => "foo4"},
                    environment_spec!{true, "FOO" => "foo"},
                ],
            }),
        );
    }

    #[test]
    fn working_directory() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! {
                    parent: image_container_parent!("image", all),
                    working_directory: "/root1",
                },
            ),
            (
                "p2",
                container_spec! {
                    parent: container_container_parent!("p1", all),
                },
            ),
            (
                "p3",
                container_spec! {
                    parent: container_container_parent!("p2", all),
                    working_directory: "/root3",
                },
            ),
            (
                "p4",
                container_spec! {
                    parent: container_container_parent!("p2", layers),
                },
            ),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(job_spec! {"prog"}, &|c| containers.get(c)),
            Ok(collapsed_job_spec! {"prog"}),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    working_directory: "/root",
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                working_directory: "/root",
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    working_directory: "/root",
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                working_directory: "/root",
                image: image_ref!("image", layers, environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    working_directory: "/root",
                    parent: image_container_parent!("image", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                working_directory: "/root",
                image: image_ref!("image", layers, environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                working_directory: "/root1",
                image: image_ref!("image", layers, environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p2", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                working_directory: "/root1",
                image: image_ref!("image", layers, environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p3", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                working_directory: "/root3",
                image: image_ref!("image", layers, environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p4", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                image: image_ref!("image", layers),
            }),
        );
    }

    #[test]
    fn mounts() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! {
                    parent: image_container_parent!("image", all),
                    mounts: [proc_mount!("/proc1")],
                },
            ),
            (
                "p2",
                container_spec! {
                    parent: container_container_parent!("p1", all),
                    mounts: [tmp_mount!("/tmp2")],
                },
            ),
            (
                "p3",
                container_spec! {
                    parent: container_container_parent!("p2", layers),
                    mounts: [proc_mount!("/proc3")],
                },
            ),
            (
                "p4",
                container_spec! {
                    mounts: [tmp_mount!("/tmp4")],
                },
            ),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    mounts: [proc_mount!("/proc")],
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                mounts: [proc_mount!("/proc")],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    mounts: [proc_mount!("/proc")],
                    parent: image_container_parent!("image", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                mounts: [proc_mount!("/proc")],
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    mounts: [proc_mount!("/proc")],
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                mounts: [proc_mount!("/proc1"), proc_mount!("/proc")],
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    mounts: [proc_mount!("/proc")],
                    parent: container_container_parent!("p1", mounts),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                mounts: [proc_mount!("/proc1"), proc_mount!("/proc")],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    mounts: [proc_mount!("/proc")],
                    parent: container_container_parent!("p2", mounts),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                mounts: [proc_mount!("/proc1"), tmp_mount!("/tmp2"), proc_mount!("/proc")],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    mounts: [proc_mount!("/proc")],
                    parent: container_container_parent!("p3", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                mounts: [proc_mount!("/proc3"), proc_mount!("/proc")],
                image: image_ref!("image", layers),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    mounts: [proc_mount!("/proc")],
                    parent: container_container_parent!("p4", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                mounts: [tmp_mount!("/tmp4"), proc_mount!("/proc")],
            }),
        );
    }

    #[test]
    fn network() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! {
                    parent: image_container_parent!("image", all),
                    network: JobNetwork::Loopback,
                },
            ),
            (
                "p2",
                container_spec! {
                    parent: container_container_parent!("p1", all),
                },
            ),
            (
                "p3",
                container_spec! {
                    parent: container_container_parent!("p2", all),
                    network: JobNetwork::Disabled,
                },
            ),
            (
                "p4",
                container_spec! {
                    parent: container_container_parent!("p2", environment),
                },
            ),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(job_spec! {"prog"}, &|c| containers.get(c)),
            Ok(collapsed_job_spec! {"prog"}),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    network: JobNetwork::Local,
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                network: JobNetwork::Local,
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    network: JobNetwork::Local,
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                network: JobNetwork::Local,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    network: JobNetwork::Local,
                    parent: image_container_parent!("image", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                network: JobNetwork::Local,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                network: JobNetwork::Loopback,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p2", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                network: JobNetwork::Loopback,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p3", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                network: JobNetwork::Disabled,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p4", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                image: image_ref!("image", environment),
            }),
        );
    }

    #[test]
    fn user() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! {
                    parent: image_container_parent!("image", all),
                    user: 101,
                },
            ),
            (
                "p2",
                container_spec! {
                    parent: container_container_parent!("p1", all),
                },
            ),
            (
                "p3",
                container_spec! {
                    parent: container_container_parent!("p2", all),
                    user: 103,
                },
            ),
            (
                "p4",
                container_spec! {
                    parent: container_container_parent!("p2", environment),
                },
            ),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(job_spec! {"prog"}, &|c| containers.get(c)),
            Ok(collapsed_job_spec! {"prog"}),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    user: 100,
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                user: 100,
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    user: 100,
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                user: 100,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    user: 100,
                    parent: image_container_parent!("image", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                user: 100,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                user: 101,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p2", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                user: 101,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p3", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                user: 103,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p4", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                image: image_ref!("image", environment),
            }),
        );
    }

    #[test]
    fn group() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! {
                    parent: image_container_parent!("image", all),
                    group: 101,
                },
            ),
            (
                "p2",
                container_spec! {
                    parent: container_container_parent!("p1", all),
                },
            ),
            (
                "p3",
                container_spec! {
                    parent: container_container_parent!("p2", all),
                    group: 103,
                },
            ),
            (
                "p4",
                container_spec! {
                    parent: container_container_parent!("p2", environment),
                },
            ),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(job_spec! {"prog"}, &|c| containers.get(c)),
            Ok(collapsed_job_spec! {"prog"}),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    group: 100,
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                group: 100,
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    group: 100,
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                group: 100,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    group: 100,
                    parent: image_container_parent!("image", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                group: 100,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                group: 101,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p2", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                group: 101,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p3", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                group: 103,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p4", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                image: image_ref!("image", environment),
            }),
        );
    }

    #[test]
    fn arguments() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    arguments: ["arg1", "arg2"],
                },
                &|_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                arguments: ["arg1", "arg2"],
            }),
        );
    }

    #[test]
    fn timeout() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    timeout: 10,
                },
                &|_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                timeout: 10,
            }),
        );
    }

    #[test]
    fn estimated_duration() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    estimated_duration: millis!(100),
                },
                &|_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                estimated_duration: millis!(100),
            }),
        );
    }

    #[test]
    fn allocate_tty() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    allocate_tty: JobTty::new(b"123456", WindowSize::new(50, 100)),
                },
                &|_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                allocate_tty: JobTty::new(b"123456", WindowSize::new(50, 100)),
            }),
        );
    }

    #[test]
    fn priority() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    priority: 42,
                },
                &|_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                priority: 42,
            }),
        );
    }

    #[test]
    fn image_parent() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: image_container_parent!("image1", layers, environment, working_directory),
                },
                &|_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                image: image_ref!("image1", layers, environment, working_directory),
            }),
        )
    }

    #[test]
    fn image_parent_with_working_directory() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: image_container_parent!("image1", layers, environment, working_directory),
                    working_directory: "/root",
                },
                &|_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                image: image_ref!("image1", layers, environment),
                working_directory: "/root",
            }),
        )
    }

    #[test]
    fn image_parent_with_working_directory_only() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: image_container_parent!("image1", working_directory),
                    working_directory: "/root",
                },
                &|_| None,
            ),
            Ok(collapsed_job_spec! {
                "prog",
                working_directory: "/root",
            }),
        )
    }

    #[test]
    fn image_parent_with_no_fields() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: image_container_parent!("image1"),
                },
                &|_| None,
            ),
            Ok(collapsed_job_spec!("prog")),
        )
    }

    #[test]
    fn integrate_image_layers() {
        let mut job_spec = collapsed_job_spec! {
            "prog",
            image: image_ref!("image", layers),
        };
        job_spec
            .integrate_image(&converted_image! {"image", layers: ["foo.tar", "bar.tar"]})
            .unwrap();
        assert_eq!(
            job_spec,
            collapsed_job_spec! {
                "prog",
                image_layers: [tar_layer!("foo.tar"), tar_layer!("bar.tar")],
            }
        );
    }

    #[test]
    fn integrate_image_environment() {
        let mut job_spec = collapsed_job_spec! {
            "prog",
            image: image_ref!("image", environment),
        };
        job_spec
            .integrate_image(&converted_image! {"image", environment: ["FOO=foo", "BAR=bar"]})
            .unwrap();
        assert_eq!(
            job_spec,
            collapsed_job_spec! {
                "prog",
                initial_environment: [("BAR", "bar"), ("FOO", "foo")],
            }
        );
    }

    #[test]
    fn integrate_image_environment_no_environment() {
        let mut job_spec = collapsed_job_spec! {
            "prog",
            image: image_ref!("image", environment),
        };
        job_spec
            .integrate_image(&converted_image! {"image", working_directory: "/root" })
            .unwrap();
        assert_eq!(job_spec, collapsed_job_spec! {"prog"});
    }

    #[test]
    fn integrate_image_environment_bad_environment() {
        let mut job_spec = collapsed_job_spec! {
            "prog",
            image: image_ref!("image", environment),
        };
        assert_eq!(
            job_spec
                .integrate_image(&converted_image! {"image", environment: ["FOO"]})
                .unwrap_err(),
            "image image has an invalid environment variable FOO"
        );
    }

    #[test]
    fn integrate_image_working_directory_some() {
        let mut job_spec = collapsed_job_spec! {
            "prog",
            image: image_ref!("image", working_directory),
        };
        job_spec
            .integrate_image(&converted_image! {"image", working_directory: "/root"})
            .unwrap();
        assert_eq!(
            job_spec,
            collapsed_job_spec! {
                "prog",
                working_directory: "/root",
            }
        );
    }

    #[test]
    fn integrate_image_working_directory_none() {
        let mut job_spec = collapsed_job_spec! {
            "prog",
            image: image_ref!("image", layers),
        };
        job_spec
            .integrate_image(&converted_image! {"image"})
            .unwrap();
        assert_eq!(job_spec, collapsed_job_spec! {"prog"});
    }
}
