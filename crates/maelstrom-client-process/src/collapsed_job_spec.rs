use indexmap::IndexSet;
use maelstrom_base::{
    ArtifactType, CaptureFileSystemChanges, EnumSet, GroupId, JobMount, JobNetwork, JobRootOverlay,
    JobSpec as BaseJobSpec, JobTty, NonEmpty, Sha256Digest, Timeout, UserId, Utf8PathBuf,
};
use maelstrom_client_base::spec::{
    self, ContainerParent, ContainerSpec, ContainerUse, ConvertedImage, EnvironmentSpec, ImageRef,
    ImageUse, JobSpec, LayerSpec,
};
use std::{collections::BTreeMap, mem, time::Duration};

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
    layers: Vec<LayerSpec>,
    enable_writable_file_system: Option<bool>,
    environment: Vec<EnvironmentSpec>,
    working_directory: Option<Utf8PathBuf>,
    mounts: Vec<JobMount>,
    network: Option<JobNetwork>,
    user: Option<UserId>,
    group: Option<GroupId>,
    image: Option<ImageRef>,
    initial_environment: BTreeMap<String, String>,
    image_layers: Vec<LayerSpec>,
    program: Utf8PathBuf,
    arguments: Vec<String>,
    timeout: Option<Timeout>,
    estimated_duration: Option<Duration>,
    allocate_tty: Option<JobTty>,
    priority: i8,
    capture_file_system_changes: Option<CaptureFileSystemChanges>,
}

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
///     collapsed_job_spec! { "cat", layers: [tar_layer_spec!("foo.tar")] }.layers,
///     vec![tar_layer_spec!("foo.tar")]);
/// assert_eq!(collapsed_job_spec! { "cat", user: 3 }.user, User::new(3));
/// ```
///
#[cfg(test)]
macro_rules! collapsed_job_spec {
    (@expand [$program:expr] [] -> [$($($field:tt)+)?]) => {
        $crate::collapsed_job_spec::CollapsedJobSpec {
            $($($field)+,)?
            .. $crate::collapsed_job_spec::CollapsedJobSpec {
                layers: Default::default(),
                enable_writable_file_system: Default::default(),
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
                capture_file_system_changes: Default::default(),
            }
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
    (@expand [$program:expr] [enable_writable_file_system: $enable_writable_file_system:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? enable_writable_file_system: $enable_writable_file_system.into()])
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
    (@expand [$program:expr] [capture_file_system_changes: $capture_file_system_changes:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?]) => {
        collapsed_job_spec!(@expand [$program] [$($($field_in)*)?] ->
            [$($($field_out)+,)? capture_file_system_changes: Some($capture_file_system_changes.into())])
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
                    mut enable_writable_file_system,
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
            capture_file_system_changes,
        } = job_spec;
        let mut image = None;
        let mut ancestors = IndexSet::<String>::default();
        let mut use_mask = EnumSet::all()
            .into_iter()
            .filter(|container_use| match container_use {
                ContainerUse::Layers | ContainerUse::Environment | ContainerUse::Mounts => true,
                ContainerUse::EnableWritableFileSystem => enable_writable_file_system.is_none(),
                ContainerUse::WorkingDirectory => working_directory.is_none(),
                ContainerUse::Network => network.is_none(),
                ContainerUse::User => user.is_none(),
                ContainerUse::Group => group.is_none(),
            })
            .collect();

        while let Some(parent) = next_parent {
            match parent {
                ContainerParent::Image(mut image_ref) => {
                    // Remove fields that have been previously masked out by more direct ancestors.
                    // If we're not going to use any fields from the image, don't bother getting
                    // it.
                    image_ref.r#use &= spec::project_container_use_set_to_image_use_set(use_mask);
                    if !image_ref.r#use.is_empty() {
                        image = Some(image_ref);
                    }
                    break;
                }

                ContainerParent::Container(container_ref) => {
                    use_mask &= container_ref.r#use;
                    if use_mask.is_empty() {
                        break;
                    }
                    let parent = container_resolver(&container_ref.name).ok_or_else(|| {
                        format!("couldn't find parent container {:?}", &container_ref.name)
                    })?;
                    if !ancestors.insert(container_ref.name.clone()) {
                        return Err(ancestors.iter().chain(Some(&container_ref.name)).fold(
                            String::default(),
                            |lhs, ancestor| {
                                if lhs.is_empty() {
                                    format!("parent loop found: {ancestor:?}")
                                } else {
                                    format!("{lhs} -> {ancestor:?}")
                                }
                            },
                        ));
                    }
                    use_mask = use_mask
                        .into_iter()
                        .filter(|container_use| match container_use {
                            ContainerUse::Layers => {
                                layers = parent
                                    .layers
                                    .iter()
                                    .cloned()
                                    .chain(layers.drain(..))
                                    .collect();
                                true
                            }
                            ContainerUse::EnableWritableFileSystem => {
                                enable_writable_file_system = parent.enable_writable_file_system;
                                enable_writable_file_system.is_none()
                            }
                            ContainerUse::Environment => {
                                environment = parent
                                    .environment
                                    .iter()
                                    .cloned()
                                    .chain(environment.drain(..))
                                    .collect();
                                true
                            }
                            ContainerUse::WorkingDirectory => {
                                working_directory = parent.working_directory.clone();
                                working_directory.is_none()
                            }
                            ContainerUse::Mounts => {
                                mounts = parent
                                    .mounts
                                    .iter()
                                    .cloned()
                                    .chain(mounts.drain(..))
                                    .collect();
                                true
                            }
                            ContainerUse::Network => {
                                network = parent.network;
                                network.is_none()
                            }
                            ContainerUse::User => {
                                user = parent.user;
                                user.is_none()
                            }
                            ContainerUse::Group => {
                                group = parent.group;
                                group.is_none()
                            }
                        })
                        .collect();
                    next_parent = parent.parent.clone();
                }
            }
        }

        Ok(CollapsedJobSpec {
            layers,
            enable_writable_file_system,
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
            capture_file_system_changes,
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
        for image_use in self.image.take().unwrap().r#use {
            match image_use {
                ImageUse::Layers => {
                    assert!(self.image_layers.is_empty());
                    self.image_layers = image.layers()?;
                }
                ImageUse::Environment => {
                    assert!(self.initial_environment.is_empty());
                    self.initial_environment = image.environment()?;
                }
                ImageUse::WorkingDirectory => {
                    assert!(self.working_directory.is_none());
                    self.working_directory = image.working_directory();
                }
            }
        }
        Ok(())
    }

    pub fn layers(&self) -> &[LayerSpec] {
        &self.layers
    }

    pub fn image_layers(&self) -> &[LayerSpec] {
        &self.image_layers
    }

    pub fn image(&self) -> Option<&ImageRef> {
        self.image.as_ref()
    }

    pub fn take_environment(&mut self) -> (BTreeMap<String, String>, Vec<EnvironmentSpec>) {
        (
            mem::take(&mut self.initial_environment),
            mem::take(&mut self.environment),
        )
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
            enable_writable_file_system,
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
            capture_file_system_changes,
        } = self;
        let root_overlay = capture_file_system_changes
            .map_or_else(
                || {
                    enable_writable_file_system.map(|enable| {
                        if enable {
                            JobRootOverlay::Tmp
                        } else {
                            JobRootOverlay::None
                        }
                    })
                },
                |capture| Some(JobRootOverlay::Local(capture)),
            )
            .unwrap_or_default();
        Ok(BaseJobSpec {
            program,
            arguments,
            environment,
            layers,
            mounts,
            network: network.unwrap_or_default(),
            root_overlay,
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
        image_container_parent, image_ref, job_spec, tar_layer_spec,
    };
    use maelstrom_test::millis;
    use std::collections::HashMap;

    #[test]
    fn missing_parent() {
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {"prog", parent: container_container_parent!("unknown", all)},
                &|_| None
            ),
            Err(r#"couldn't find parent container "unknown""#.into()),
        );
    }

    #[test]
    fn parent_loop() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! { parent: container_container_parent!("p3", all) },
            ),
            (
                "p2",
                container_spec! { parent: container_container_parent!("p1", all) },
            ),
            (
                "p3",
                container_spec! { parent: container_container_parent!("p2", all) },
            ),
            (
                "p4",
                container_spec! { parent: container_container_parent!("p4", all) },
            ),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {"prog", parent: container_container_parent!("p1", all)},
                &|c| containers.get(c)
            ),
            Err(r#"parent loop found: "p1" -> "p3" -> "p2" -> "p1""#.into()),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {"prog", parent: container_container_parent!("p4", all)},
                &|c| containers.get(c)
            ),
            Err(r#"parent loop found: "p4" -> "p4""#.into()),
        );
    }

    #[test]
    fn parent_traversal_short_circuits() {
        let containers = HashMap::from([(
            "p1",
            container_spec! { group: 202, parent: container_container_parent!("unknown", all) },
        )]);
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    parent: container_container_parent!("p1", user, group),
                    user: 101,
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                user: 101,
                group: 202,
            }),
        );
    }

    #[test]
    fn layers() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! {
                    parent: image_container_parent!("image", all),
                    layers: [tar_layer_spec!("p1.tar")],
                },
            ),
            (
                "p2",
                container_spec! {
                    parent: container_container_parent!("p1", all),
                    layers: [tar_layer_spec!("p2.tar")],
                },
            ),
            (
                "p3",
                container_spec! {
                    parent: container_container_parent!("p2", environment),
                    layers: [tar_layer_spec!("p3.tar")],
                },
            ),
            (
                "p4",
                container_spec! {
                    layers: [tar_layer_spec!("p4.tar")],
                },
            ),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer_spec!("foo.tar")],
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer_spec!("foo.tar")],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer_spec!("foo.tar")],
                    parent: image_container_parent!("image", layers),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer_spec!("foo.tar")],
                image: image_ref!("image", layers),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer_spec!("foo.tar")],
                    parent: image_container_parent!("image"),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer_spec!("foo.tar")],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer_spec!("foo.tar")],
                    parent: container_container_parent!("p1", layers),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer_spec!("p1.tar"), tar_layer_spec!("foo.tar")],
                image: image_ref!("image", layers),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer_spec!("foo.tar")],
                    parent: container_container_parent!("p1"),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer_spec!("foo.tar")],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer_spec!("foo.tar")],
                    parent: container_container_parent!("p2", layers),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer_spec!("p1.tar"), tar_layer_spec!("p2.tar"), tar_layer_spec!("foo.tar")],
                image: image_ref!("image", layers),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer_spec!("foo.tar")],
                    parent: container_container_parent!("p3", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer_spec!("p3.tar"), tar_layer_spec!("foo.tar")],
                image: image_ref!("image", environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    layers: [tar_layer_spec!("foo.tar")],
                    parent: container_container_parent!("p4", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                layers: [tar_layer_spec!("p4.tar"), tar_layer_spec!("foo.tar")],
            }),
        );
    }

    #[test]
    fn enable_writable_file_system() {
        let containers = HashMap::from([
            (
                "p1",
                container_spec! {
                    parent: image_container_parent!("image", all),
                    enable_writable_file_system: true,
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
                    enable_writable_file_system: false,
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
                    enable_writable_file_system: true,
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                enable_writable_file_system: true,
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    enable_writable_file_system: true,
                    parent: container_container_parent!("p1", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                enable_writable_file_system: true,
                image: image_ref!("image", all),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    enable_writable_file_system: true,
                    parent: image_container_parent!("image", all),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                enable_writable_file_system: true,
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
                enable_writable_file_system: true,
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
                enable_writable_file_system: true,
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
                enable_writable_file_system: false,
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
                    environment: [environment_spec!("FOO" => "foo1")],
                },
            ),
            (
                "p2",
                container_spec! {
                    parent: container_container_parent!("p1", all),
                    environment: [environment_spec!("FOO" => "foo2")],
                },
            ),
            (
                "p3",
                container_spec! {
                    parent: container_container_parent!("p2", layers),
                    environment: [environment_spec!("FOO" => "foo3")],
                },
            ),
            (
                "p4",
                container_spec! {
                    environment: [environment_spec!("FOO" => "foo4")],
                },
            ),
        ]);
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!("FOO" => "foo")],
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [environment_spec!("FOO" => "foo")],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!("FOO" => "foo")],
                    parent: image_container_parent!("image", environment),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [environment_spec!("FOO" => "foo")],
                image: image_ref!("image", environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!("FOO" => "foo")],
                    parent: image_container_parent!("image", layers),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [environment_spec!("FOO" => "foo")],
                image: image_ref!("image", layers),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!("FOO" => "foo")],
                    parent: container_container_parent!("p1", environment),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [
                    environment_spec!("FOO" => "foo1"),
                    environment_spec!("FOO" => "foo"),
                ],
                image: image_ref!("image", environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!("FOO" => "foo")],
                    parent: container_container_parent!("p2", environment),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [
                    environment_spec!("FOO" => "foo1"),
                    environment_spec!("FOO" => "foo2"),
                    environment_spec!("FOO" => "foo"),
                ],
                image: image_ref!("image", environment),
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!("FOO" => "foo")],
                    parent: container_container_parent!("p3", environment),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [
                    environment_spec!("FOO" => "foo3"),
                    environment_spec!("FOO" => "foo"),
                ],
            }),
        );
        assert_eq!(
            CollapsedJobSpec::new(
                job_spec! {
                    "prog",
                    environment: [environment_spec!("FOO" => "foo")],
                    parent: container_container_parent!("p4", environment),
                },
                &|c| containers.get(c)
            ),
            Ok(collapsed_job_spec! {
                "prog",
                environment: [
                    environment_spec!("FOO" => "foo4"),
                    environment_spec!("FOO" => "foo"),
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
                image_layers: [tar_layer_spec!("foo.tar"), tar_layer_spec!("bar.tar")],
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
