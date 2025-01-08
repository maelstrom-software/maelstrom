mod directive;
pub mod store;

pub use store::Store;

use anyhow::Result;
use directive::{Directive, DirectiveContainer, DirectiveContainerAugment};
use maelstrom_base::Timeout;
use maelstrom_client::spec::{ContainerSpec, EnvironmentSpec};

#[derive(Clone)]
pub struct Metadata {
    pub container: ContainerSpec,
    pub include_shared_libraries: bool,
    pub timeout: Option<Timeout>,
    pub ignore: bool,
}

impl Metadata {
    /// The logic here is that if they explicitly set the value to something, we should return
    /// that. Otherwise, we should see if they are using layers from an image. If they are, we can
    /// assume that the image has shared libraries, and we shouldn't push shared libraries on top
    /// of it. Otherwise, they probably don't want to have to explicitly provide a layer with
    /// shared libraries in it, so we should include shared libraries for them.
    fn new(metadata: MetadataInternal, uses_image_layers: bool) -> Self {
        let MetadataInternal {
            container,
            include_shared_libraries,
            timeout,
            ignore,
        } = metadata;
        Self {
            container,
            include_shared_libraries: include_shared_libraries.unwrap_or(!uses_image_layers),
            timeout,
            ignore,
        }
    }
}

#[derive(Default)]
struct MetadataInternal {
    container: ContainerSpec,
    include_shared_libraries: Option<bool>,
    timeout: Option<Timeout>,
    ignore: bool,
}

impl MetadataInternal {
    fn try_fold<TestFilterT>(self, directive: &Directive<TestFilterT>) -> Result<Self> {
        let Self {
            mut container,
            mut include_shared_libraries,
            mut timeout,
            mut ignore,
        } = self;

        let Directive {
            filter: _,
            container: new_container,
            include_shared_libraries: new_include_shared_libraries,
            timeout: new_timeout,
            ignore: new_ignore,
        } = directive;

        container = match new_container {
            DirectiveContainer::Override(new_container) => new_container.clone(),

            DirectiveContainer::Augment(DirectiveContainerAugment {
                layers: new_layers,
                added_layers,
                environment: new_environment,
                added_environment,
                working_directory: new_working_directory,
                enable_writable_file_system: new_enable_writable_file_system,
                mounts: new_mounts,
                added_mounts,
                network: new_network,
                user: new_user,
                group: new_group,
            }) => {
                let ContainerSpec {
                    parent,
                    mut layers,
                    mut environment,
                    mut working_directory,
                    mut enable_writable_file_system,
                    mut mounts,
                    mut network,
                    mut user,
                    mut group,
                } = container;

                if let Some(new_layers) = new_layers {
                    layers = new_layers.clone();
                }
                layers.extend(added_layers.iter().flatten().cloned());

                if let Some(new_environment) = new_environment {
                    environment.push(EnvironmentSpec {
                        vars: new_environment.clone(),
                        extend: false,
                    });
                }
                environment.extend(
                    added_environment
                        .iter()
                        .cloned()
                        .map(|vars| EnvironmentSpec { vars, extend: true }),
                );

                if new_working_directory.is_some() {
                    working_directory = new_working_directory.clone();
                }

                if new_enable_writable_file_system.is_some() {
                    enable_writable_file_system = *new_enable_writable_file_system;
                }

                if let Some(new_mounts) = new_mounts {
                    mounts = new_mounts.clone();
                }
                mounts.extend(added_mounts.iter().flatten().cloned());

                if new_network.is_some() {
                    network = *new_network;
                }

                if new_user.is_some() {
                    user = *new_user;
                }

                if new_group.is_some() {
                    group = *new_group;
                }

                ContainerSpec {
                    parent,
                    layers,
                    environment,
                    working_directory,
                    enable_writable_file_system,
                    mounts,
                    network,
                    user,
                    group,
                }
            }
        };

        if new_include_shared_libraries.is_some() {
            include_shared_libraries = *new_include_shared_libraries;
        }

        if let Some(new_timeout) = new_timeout {
            timeout = *new_timeout;
        }

        if let Some(new_ignore) = new_ignore {
            ignore = *new_ignore;
        }

        Ok(Self {
            container,
            include_shared_libraries,
            timeout,
            ignore,
        })
    }
}
