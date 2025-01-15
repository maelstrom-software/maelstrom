mod directive;
pub mod store;

pub use store::Store;

use directive::{Directive, DirectiveContainer, DirectiveContainerAugment};
use maelstrom_base::Timeout;
use maelstrom_client::spec::{ContainerSpec, IntoEnvironment};

#[derive(Clone, Debug, PartialEq)]
pub struct Metadata {
    pub container: ContainerSpec,
    pub include_shared_libraries: bool,
    pub timeout: Option<Timeout>,
    pub ignore: bool,
}

#[cfg(test)]
macro_rules! metadata {
    (@expand [] -> [$($($fields:tt)+)?] [$($container_fields:tt)*]) => {
        Metadata {
            $($($fields)+,)?
            .. Metadata {
                container: maelstrom_client::container_spec!($($container_fields)*),
                include_shared_libraries: Default::default(),
                timeout: Default::default(),
                ignore: Default::default(),
            }
        }
    };
    (@expand [include_shared_libraries: $include_shared_libraries:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        metadata!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? include_shared_libraries: $include_shared_libraries.into()] [$($container_field)*])
    };
    (@expand [timeout: $timeout:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        metadata!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? timeout: Timeout::new($timeout)] [$($container_field)*])
    };
    (@expand [ignore: $ignore:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        metadata!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? ignore: $ignore.into()] [$($container_field)*])
    };
    (@expand [$container_field_name:ident: $container_field_value:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        metadata!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? $container_field_name: $container_field_value])
    };
    ($($field_in:tt)*) => {
        metadata!(@expand [$($field_in)*] -> [] [])
    };
}

#[cfg(test)]
pub(crate) use metadata;

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

#[derive(Debug, Default, PartialEq)]
struct MetadataInternal {
    container: ContainerSpec,
    include_shared_libraries: Option<bool>,
    timeout: Option<Timeout>,
    ignore: bool,
}

#[cfg(test)]
macro_rules! metadata_internal {
    (@expand [] -> [$($($fields:tt)+)?] [$($container_fields:tt)*]) => {
        MetadataInternal {
            $($($fields)+,)?
            .. MetadataInternal {
                container: maelstrom_client::container_spec!($($container_fields)*),
                include_shared_libraries: Default::default(),
                timeout: Default::default(),
                ignore: Default::default(),
            }
        }
    };
    (@expand [include_shared_libraries: $include_shared_libraries:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        metadata_internal!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? include_shared_libraries: Some($include_shared_libraries.into())] [$($container_field)*])
    };
    (@expand [timeout: $timeout:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        metadata_internal!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? timeout: Timeout::new($timeout)] [$($container_field)*])
    };
    (@expand [ignore: $ignore:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        metadata_internal!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? ignore: $ignore.into()] [$($container_field)*])
    };
    (@expand [$container_field_name:ident: $container_field_value:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        metadata_internal!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? $container_field_name: $container_field_value])
    };
    ($($field_in:tt)*) => {
        metadata_internal!(@expand [$($field_in)*] -> [] [])
    };
}

impl MetadataInternal {
    fn fold<TestFilterT>(self, directive: &Directive<TestFilterT>) -> Self {
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
                layers.extend(added_layers.clone());

                if let Some(new_environment) = new_environment {
                    environment = new_environment.clone().into_environment();
                }
                environment.extend(added_environment.clone().into_environment());

                if new_working_directory.is_some() {
                    working_directory = new_working_directory.clone();
                }

                if new_enable_writable_file_system.is_some() {
                    enable_writable_file_system = *new_enable_writable_file_system;
                }

                if let Some(new_mounts) = new_mounts {
                    mounts = new_mounts.clone();
                }
                mounts.extend(added_mounts.clone());

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

        Self {
            container,
            include_shared_libraries,
            timeout,
            ignore,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use directive::{augment_directive, override_directive};
    use maelstrom_base::{proc_mount, sys_mount, tmp_mount, JobNetwork};
    use maelstrom_client::{container_container_parent, environment_spec, tar_layer_spec};

    #[track_caller]
    fn fold_test(before: MetadataInternal, directive: Directive<()>, expected: MetadataInternal) {
        assert_eq!(before.fold(&directive), expected);
    }

    #[test]
    fn r#override() {
        fold_test(
            metadata_internal! {
                layers: [tar_layer_spec!("foo.tar")],
                user: 101,
                enable_writable_file_system: true,
                include_shared_libraries: true,
                timeout: 1,
                ignore: true,
            },
            override_directive! {
                parent: container_container_parent!("parent", environment),
                group: 202,
            },
            metadata_internal! {
                parent: container_container_parent!("parent", environment),
                group: 202,
                include_shared_libraries: true,
                timeout: 1,
                ignore: true,
            },
        );
        fold_test(
            metadata_internal!(include_shared_libraries: false),
            augment_directive!(),
            metadata_internal!(include_shared_libraries: false),
        );
        fold_test(
            metadata_internal!(include_shared_libraries: false),
            augment_directive!(include_shared_libraries: true),
            metadata_internal!(include_shared_libraries: true),
        );
    }

    #[test]
    fn layers() {
        fold_test(
            metadata_internal!(),
            augment_directive!(layers: [tar_layer_spec!("foo.tar")]),
            metadata_internal!(layers: [tar_layer_spec!("foo.tar")]),
        );
        fold_test(
            metadata_internal!(layers: [tar_layer_spec!("bar.tar")]),
            augment_directive!(),
            metadata_internal!(layers: [tar_layer_spec!("bar.tar")]),
        );
        fold_test(
            metadata_internal!(layers: [tar_layer_spec!("bar.tar")]),
            augment_directive!(layers: [tar_layer_spec!("foo.tar")]),
            metadata_internal!(layers: [tar_layer_spec!("foo.tar")]),
        );
    }

    #[test]
    fn added_layers() {
        fold_test(
            metadata_internal!(),
            augment_directive!(added_layers: [tar_layer_spec!("foo.tar")]),
            metadata_internal!(layers: [tar_layer_spec!("foo.tar")]),
        );
        fold_test(
            metadata_internal!(layers: [tar_layer_spec!("bar.tar")]),
            augment_directive!(added_layers: [tar_layer_spec!("foo.tar")]),
            metadata_internal!(layers: [tar_layer_spec!("bar.tar"), tar_layer_spec!("foo.tar")]),
        );
        fold_test(
            metadata_internal!(),
            augment_directive! {
                layers: [tar_layer_spec!("baz.tar")],
                added_layers: [tar_layer_spec!("foo.tar")]
            },
            metadata_internal!(layers: [tar_layer_spec!("baz.tar"), tar_layer_spec!("foo.tar")]),
        );
        fold_test(
            metadata_internal!(layers: [tar_layer_spec!("bar.tar")]),
            augment_directive! {
                layers: [tar_layer_spec!("baz.tar")],
                added_layers: [tar_layer_spec!("foo.tar")]
            },
            metadata_internal!(layers: [tar_layer_spec!("baz.tar"), tar_layer_spec!("foo.tar")]),
        );
    }

    #[test]
    fn environment() {
        fold_test(
            metadata_internal!(),
            augment_directive!(environment: [("foo", "bar")]),
            metadata_internal!(environment: [("foo", "bar")]),
        );
        fold_test(
            metadata_internal!(environment: [("frob", "baz")]),
            augment_directive!(),
            metadata_internal!(environment: [("frob", "baz")]),
        );
        fold_test(
            metadata_internal!(environment: [("frob", "baz")]),
            augment_directive!(environment: [("foo", "bar")]),
            metadata_internal!(environment: [("foo", "bar")]),
        );
    }

    #[test]
    fn added_environment() {
        fold_test(
            metadata_internal!(),
            augment_directive!(added_environment: [("foo", "bar")]),
            metadata_internal!(environment: [("foo", "bar")]),
        );
        fold_test(
            metadata_internal!(environment: [("frob", "baz")]),
            augment_directive!(added_environment: [("foo", "bar")]),
            metadata_internal! {
                environment: [
                    environment_spec!(true, "frob" => "baz"),
                    environment_spec!(true, "foo" => "bar"),
                ],
            },
        );
        fold_test(
            metadata_internal!(),
            augment_directive! {
                environment: [("qux", "quux")],
                added_environment: [("foo", "bar")],
            },
            metadata_internal! {
                environment: [
                    environment_spec!(true, "qux" => "quux"),
                    environment_spec!(true, "foo" => "bar"),
                ],
            },
        );
        fold_test(
            metadata_internal!(environment: [("frob", "baz")]),
            augment_directive! {
                environment: [("qux", "quux")],
                added_environment: [("foo", "bar")],
            },
            metadata_internal! {
                environment: [
                    environment_spec!(true, "qux" => "quux"),
                    environment_spec!(true, "foo" => "bar"),
                ],
            },
        );
    }

    #[test]
    fn working_directory() {
        fold_test(
            metadata_internal!(),
            augment_directive!(working_directory: "/root"),
            metadata_internal!(working_directory: "/root"),
        );
        fold_test(
            metadata_internal!(working_directory: "/home"),
            augment_directive!(),
            metadata_internal!(working_directory: "/home"),
        );
        fold_test(
            metadata_internal!(working_directory: "/home"),
            augment_directive!(working_directory: "/root"),
            metadata_internal!(working_directory: "/root"),
        );
    }

    #[test]
    fn enable_writable_file_system() {
        fold_test(
            metadata_internal!(),
            augment_directive!(enable_writable_file_system: true),
            metadata_internal!(enable_writable_file_system: true),
        );
        fold_test(
            metadata_internal!(enable_writable_file_system: false),
            augment_directive!(),
            metadata_internal!(enable_writable_file_system: false),
        );
        fold_test(
            metadata_internal!(enable_writable_file_system: false),
            augment_directive!(enable_writable_file_system: true),
            metadata_internal!(enable_writable_file_system: true),
        );
    }

    #[test]
    fn mounts() {
        fold_test(
            metadata_internal!(),
            augment_directive!(mounts: [tmp_mount!("/tmp")]),
            metadata_internal!(mounts: [tmp_mount!("/tmp")]),
        );
        fold_test(
            metadata_internal!(mounts: [proc_mount!("/proc")]),
            augment_directive!(),
            metadata_internal!(mounts: [proc_mount!("/proc")]),
        );
        fold_test(
            metadata_internal!(mounts: [proc_mount!("/proc")]),
            augment_directive!(mounts: [tmp_mount!("/tmp")]),
            metadata_internal!(mounts: [tmp_mount!("/tmp")]),
        );
    }

    #[test]
    fn added_mounts() {
        fold_test(
            metadata_internal!(),
            augment_directive!(added_mounts: [tmp_mount!("/tmp")]),
            metadata_internal!(mounts: [tmp_mount!("/tmp")]),
        );
        fold_test(
            metadata_internal!(mounts: [proc_mount!("/proc")]),
            augment_directive!(added_mounts: [tmp_mount!("/tmp")]),
            metadata_internal! {
                mounts: [
                    proc_mount!("/proc"),
                    tmp_mount!("/tmp"),
                ],
            },
        );
        fold_test(
            metadata_internal!(),
            augment_directive! {
                mounts: [sys_mount!("/sys")],
                added_mounts: [tmp_mount!("/tmp")],
            },
            metadata_internal! {
                mounts: [
                    sys_mount!("/sys"),
                    tmp_mount!("/tmp"),
                ],
            },
        );
        fold_test(
            metadata_internal!(mounts: [proc_mount!("/proc")]),
            augment_directive! {
                mounts: [sys_mount!("/sys")],
                added_mounts: [tmp_mount!("/tmp")],
            },
            metadata_internal! {
                mounts: [
                    sys_mount!("/sys"),
                    tmp_mount!("/tmp"),
                ],
            },
        );
    }

    #[test]
    fn network() {
        fold_test(
            metadata_internal!(),
            augment_directive!(network: JobNetwork::Loopback),
            metadata_internal!(network: JobNetwork::Loopback),
        );
        fold_test(
            metadata_internal!(network: JobNetwork::Local),
            augment_directive!(),
            metadata_internal!(network: JobNetwork::Local),
        );
        fold_test(
            metadata_internal!(network: JobNetwork::Local),
            augment_directive!(network: JobNetwork::Loopback),
            metadata_internal!(network: JobNetwork::Loopback),
        );
    }

    #[test]
    fn user() {
        fold_test(
            metadata_internal!(),
            augment_directive!(user: 101),
            metadata_internal!(user: 101),
        );
        fold_test(
            metadata_internal!(user: 102),
            augment_directive!(),
            metadata_internal!(user: 102),
        );
        fold_test(
            metadata_internal!(user: 102),
            augment_directive!(user: 101),
            metadata_internal!(user: 101),
        );
    }

    #[test]
    fn group() {
        fold_test(
            metadata_internal!(),
            augment_directive!(group: 101),
            metadata_internal!(group: 101),
        );
        fold_test(
            metadata_internal!(group: 102),
            augment_directive!(),
            metadata_internal!(group: 102),
        );
        fold_test(
            metadata_internal!(group: 102),
            augment_directive!(group: 101),
            metadata_internal!(group: 101),
        );
    }

    #[test]
    fn include_shared_libraries() {
        fold_test(
            metadata_internal!(),
            augment_directive!(include_shared_libraries: true),
            metadata_internal!(include_shared_libraries: true),
        );
        fold_test(
            metadata_internal!(include_shared_libraries: false),
            augment_directive!(),
            metadata_internal!(include_shared_libraries: false),
        );
        fold_test(
            metadata_internal!(include_shared_libraries: false),
            augment_directive!(include_shared_libraries: true),
            metadata_internal!(include_shared_libraries: true),
        );
    }

    #[test]
    fn timeout() {
        fold_test(
            metadata_internal!(),
            augment_directive!(timeout: 1),
            metadata_internal!(timeout: 1),
        );
        fold_test(
            metadata_internal!(timeout: 0),
            augment_directive!(),
            metadata_internal!(timeout: 0),
        );
        fold_test(
            metadata_internal!(timeout: 0),
            augment_directive!(timeout: 1),
            metadata_internal!(timeout: 1),
        );
    }

    #[test]
    fn ignore() {
        fold_test(
            metadata_internal!(),
            augment_directive!(ignore: true),
            metadata_internal!(ignore: true),
        );
        fold_test(
            metadata_internal!(ignore: true),
            augment_directive!(),
            metadata_internal!(ignore: true),
        );
        fold_test(
            metadata_internal!(ignore: true),
            augment_directive!(ignore: false),
            metadata_internal!(ignore: false),
        );
    }
}
