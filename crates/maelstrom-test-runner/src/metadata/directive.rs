#![allow(unused_imports)]
use anyhow::Result;
use maelstrom_base::{
    GroupId, JobMount, JobMountForTomlAndJson, JobNetwork, Timeout, UserId, Utf8PathBuf,
};
use maelstrom_client::spec::{
    ContainerRefWithImplicitOrExplicitUse, ContainerSpec, ContainerSpecForTomlAndJson, EnvSelector,
    EnvironmentSpec, ImageRef, ImageRefWithImplicitOrExplicitUse, ImageUse, IntoEnvironment as _,
    LayerSpec,
};
use serde::{de, Deserialize, Deserializer};
use std::{
    collections::BTreeMap,
    fmt::Display,
    str::{self, FromStr},
};

#[derive(Debug, Deserialize, PartialEq)]
#[serde(try_from = "DirectiveForTomlAndJson")]
#[serde(bound(deserialize = "FilterT: FromStr, FilterT::Err: Display"))]
pub struct Directive<FilterT> {
    pub filter: Option<FilterT>,
    pub container: DirectiveContainer,
    pub include_shared_libraries: Option<bool>,
    pub timeout: Option<Option<Timeout>>,
    pub ignore: Option<bool>,
}

#[cfg(test)]
macro_rules! augment_directive {
    (@expand [] -> [$($($fields:tt)+)?] [$($($container_fields:tt)+)?]) => {
        Directive {
            $($($fields)+,)?
            .. Directive {
                filter: Default::default(),
                container: DirectiveContainer::Augment(
                    DirectiveContainerAugment {
                        $($($container_fields)+,)?
                        .. Default::default()
                    }
                ),
                include_shared_libraries: Default::default(),
                timeout: Default::default(),
                ignore: Default::default(),
            }
        }
    };
    (@expand [filter: $filter:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? filter: Some(FromStr::from_str($filter).unwrap())] [$($container_field)*])
    };
    (@expand [include_shared_libraries: $include_shared_libraries:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? include_shared_libraries: Some($include_shared_libraries.into())] [$($container_field)*])
    };
    (@expand [timeout: $timeout:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? timeout: Some(Timeout::new($timeout))] [$($container_field)*])
    };
    (@expand [ignore: $ignore:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? ignore: Some($ignore.into())] [$($container_field)*])
    };
    (@expand [layers: $layers:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? layers: Some($layers.into_iter().map(Into::into).collect())])
    };
    (@expand [added_layers: $added_layers:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? added_layers: $added_layers.into_iter().map(Into::into).collect()])
    };
    (@expand [environment: $environment:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? environment: Some(::maelstrom_client::spec::IntoEnvironment::into_environment($environment))])
    };
    (@expand [added_environment: $added_environment:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? added_environment: ::maelstrom_client::spec::IntoEnvironment::into_environment($added_environment)])
    };
    (@expand [working_directory: $working_directory:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? working_directory: Some($working_directory.into())])
    };
    (@expand [enable_writable_file_system: $enable_writable_file_system:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? enable_writable_file_system: Some($enable_writable_file_system.into())])
    };
    (@expand [mounts: $mounts:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? mounts: Some($mounts.into_iter().map(Into::into).collect())])
    };
    (@expand [added_mounts: $added_mounts:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? added_mounts: $added_mounts.into_iter().map(Into::into).collect()])
    };
    (@expand [network: $network:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? network: Some($network.into())])
    };
    (@expand [user: $user:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? user: Some($user.into())])
    };
    (@expand [group: $group:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        augment_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? group: Some($group.into())])
    };
    ($($field_in:tt)*) => {
        augment_directive!(@expand [$($field_in)*] -> [] [])
    };
}

#[cfg(test)]
pub(crate) use augment_directive;

#[cfg(test)]
macro_rules! override_directive {
    (@expand [] -> [$($($fields:tt)+)?] [$($container_fields:tt)*]) => {
        Directive {
            $($($fields)+,)?
            .. Directive {
                filter: Default::default(),
                container: DirectiveContainer::Override(
                    maelstrom_client::container_spec!($($container_fields)*)
                ),
                include_shared_libraries: Default::default(),
                timeout: Default::default(),
                ignore: Default::default(),
            }
        }
    };
    (@expand [filter: $filter:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        override_directive!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? filter: Some(FromStr::from_str($filter).unwrap())] [$($container_field)*])
    };
    (@expand [include_shared_libraries: $include_shared_libraries:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        override_directive!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? include_shared_libraries: Some($include_shared_libraries.into())] [$($container_field)*])
    };
    (@expand [timeout: $timeout:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        override_directive!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? timeout: Some(Timeout::new($timeout))] [$($container_field)*])
    };
    (@expand [ignore: $ignore:expr $(,$($field_in:tt)*)?] -> [$($($field_out:tt)+)?] [$($container_field:tt)*]) => {
        override_directive!(@expand [$($($field_in)*)?] -> [$($($field_out)+,)? ignore: Some($ignore.into())] [$($container_field)*])
    };
    (@expand [$container_field_name:ident: $container_field_value:expr $(,$($field_in:tt)*)?] -> [$($field_out:tt)*] [$($($container_field:tt)+)?]) => {
        override_directive!(@expand [$($($field_in)*)?] -> [$($field_out)*] [$($($container_field)+,)? $container_field_name: $container_field_value])
    };
    ($($field_in:tt)*) => {
        override_directive!(@expand [$($field_in)*] -> [] [])
    };
}

#[cfg(test)]
pub(crate) use override_directive;

// The derived Default will put a FilterT: Default bound on the implementation
impl<FilterT> Default for Directive<FilterT> {
    fn default() -> Self {
        Self {
            filter: Default::default(),
            container: DirectiveContainer::Augment(Default::default()),
            include_shared_libraries: Default::default(),
            timeout: Default::default(),
            ignore: Default::default(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum DirectiveContainer {
    Override(ContainerSpec),
    Augment(DirectiveContainerAugment),
}

#[derive(Debug, Default, PartialEq)]
pub struct DirectiveContainerAugment {
    pub layers: Option<Vec<LayerSpec>>,
    pub added_layers: Vec<LayerSpec>,
    pub environment: Option<Vec<EnvironmentSpec>>,
    pub added_environment: Vec<EnvironmentSpec>,
    pub working_directory: Option<Utf8PathBuf>,
    pub enable_writable_file_system: Option<bool>,
    pub mounts: Option<Vec<JobMount>>,
    pub added_mounts: Vec<JobMount>,
    pub network: Option<JobNetwork>,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectiveForTomlAndJson {
    filter: Option<String>,
    image: Option<ImageRefWithImplicitOrExplicitUse>,
    parent: Option<ContainerRefWithImplicitOrExplicitUse>,
    layers: Option<Vec<LayerSpec>>,
    added_layers: Option<Vec<LayerSpec>>,
    environment: Option<EnvSelector>,
    added_environment: Option<EnvSelector>,
    working_directory: Option<Utf8PathBuf>,
    enable_writable_file_system: Option<bool>,
    mounts: Option<Vec<JobMountForTomlAndJson>>,
    added_mounts: Option<Vec<JobMountForTomlAndJson>>,
    network: Option<JobNetwork>,
    user: Option<UserId>,
    group: Option<GroupId>,
    include_shared_libraries: Option<bool>,
    timeout: Option<u32>,
    ignore: Option<bool>,
}

impl<FilterT> TryFrom<DirectiveForTomlAndJson> for Directive<FilterT>
where
    FilterT: FromStr,
    FilterT::Err: Display,
{
    type Error = String;

    /// There are basically two types of directives: those that augment the current container, and
    /// those that override the whole container. We detect the latter by looking for an `image` or
    /// `parent` field. If we find one, we shove the rest of the fields into the struct used for
    /// parsing ContainerSpecs and then try to convert from that. Otherwise, we treat it as an
    /// augmented container.
    ///
    /// The way the function is written guarantees that we get a compilation error if we're missing
    /// a field somewhere.
    fn try_from(directive: DirectiveForTomlAndJson) -> Result<Self, Self::Error> {
        let filter = directive
            .filter
            .map(|filter| filter.parse::<FilterT>())
            .transpose()
            .map_err(|err| err.to_string())?;
        match directive {
            DirectiveForTomlAndJson {
                filter: _,
                image: None,
                parent: None,
                layers,
                added_layers,
                environment,
                added_environment,
                working_directory,
                enable_writable_file_system,
                mounts,
                added_mounts,
                network,
                user,
                group,
                include_shared_libraries,
                timeout,
                ignore,
            } => Ok(Directive {
                filter,
                container: DirectiveContainer::Augment(DirectiveContainerAugment {
                    layers,
                    added_layers: added_layers.unwrap_or_default(),
                    environment: environment.map(EnvSelector::into_environment),
                    added_environment: added_environment
                        .map(EnvSelector::into_environment)
                        .unwrap_or_default(),
                    working_directory,
                    enable_writable_file_system,
                    mounts: mounts.map(|mounts| mounts.into_iter().map(Into::into).collect()),
                    added_mounts: added_mounts.into_iter().flatten().map(Into::into).collect(),
                    network,
                    user,
                    group,
                }),
                include_shared_libraries,
                timeout: timeout.map(Timeout::new),
                ignore,
            }),
            DirectiveForTomlAndJson {
                filter: _,
                image,
                parent,
                layers,
                added_layers,
                environment,
                added_environment,
                working_directory,
                enable_writable_file_system,
                mounts,
                added_mounts,
                network,
                user,
                group,
                include_shared_libraries,
                timeout,
                ignore,
            } => Ok(Directive {
                filter,
                container: DirectiveContainer::Override(
                    ContainerSpecForTomlAndJson {
                        image,
                        parent,
                        layers,
                        added_layers,
                        environment,
                        added_environment,
                        working_directory,
                        enable_writable_file_system,
                        mounts,
                        added_mounts,
                        network,
                        user,
                        group,
                    }
                    .try_into()?,
                ),
                include_shared_libraries,
                timeout: timeout.map(Timeout::new),
                ignore,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimpleFilter;
    use anyhow::Error;
    use indoc::indoc;
    use maelstrom_base::{enum_set, proc_mount, tmp_mount, JobDeviceForTomlAndJson};
    use maelstrom_client::{
        container_container_parent, container_spec, environment_spec, image_container_parent,
        spec::SymlinkSpec, tar_layer_spec,
    };
    use maelstrom_test::{string, utf8_path_buf};
    use maplit::btreemap;

    fn parse_test_directive(toml: &str) -> Result<Directive<String>, toml::de::Error> {
        toml::from_str(toml)
    }

    #[track_caller]
    fn directive_parse_error_test(toml: &str, expected: &str) {
        let err = parse_test_directive(toml).unwrap_err();
        let actual = err.message();
        assert!(
            actual.starts_with(expected),
            "expected: {expected}; actual: {actual}"
        );
    }

    #[track_caller]
    fn directive_parse_test(toml: &str, expected: Directive<String>) {
        assert_eq!(parse_test_directive(toml).unwrap(), expected);
    }

    #[test]
    fn no_fields() {
        directive_parse_test("", augment_directive!());
    }

    #[test]
    fn unknown_field() {
        directive_parse_error_test(
            r#"unknown = "foo""#,
            "unknown field `unknown`, expected one of",
        );
    }

    #[test]
    fn duplicate_field() {
        directive_parse_error_test(
            indoc! {r#"
                filter = "all"
                filter = "any"
            "#},
            "duplicate key `filter`",
        );
    }

    #[test]
    fn augment_container_filter() {
        directive_parse_test(
            r#"filter = "foo && bar""#,
            augment_directive!(filter: "foo && bar"),
        );
    }

    #[test]
    fn augment_container_container_fields() {
        directive_parse_test(
            indoc! {r#"
                layers = [{ tar = "foo.tar" }]
                added_layers = [{ tar = "bar.tar" }]
                environment = { foo = "bar" }
                added_environment = [{ vars = { frob = "baz" }, extend = false }]
                working_directory = "/root"
                enable_writable_file_system = false
                mounts = [{ type = "tmp", mount_point = "/tmp" }]
                added_mounts = [{ type = "proc", mount_point = "/proc" }]
                network = "loopback"
                user = 101
                group = 202
            "#},
            augment_directive! {
                layers: [tar_layer_spec!("foo.tar")],
                added_layers: [tar_layer_spec!("bar.tar")],
                environment: [("foo", "bar")],
                added_environment: environment_spec!(false, "frob" => "baz"),
                working_directory: "/root",
                enable_writable_file_system: false,
                mounts: [tmp_mount!("/tmp")],
                added_mounts: [proc_mount!("/proc")],
                network: JobNetwork::Loopback,
                user: 101,
                group: 202,
            },
        );
    }

    #[test]
    fn augment_container_include_shared_libraries() {
        directive_parse_test(
            r#"include_shared_libraries = true"#,
            augment_directive!(include_shared_libraries: true),
        );
        directive_parse_test(
            r#"include_shared_libraries = false"#,
            augment_directive!(include_shared_libraries: false),
        );
    }

    #[test]
    fn augment_container_timeout() {
        directive_parse_test(r#"timeout = 0"#, augment_directive!(timeout: 0));
        directive_parse_test(r#"timeout = 1"#, augment_directive!(timeout: 1));
    }

    #[test]
    fn augment_container_ignore() {
        directive_parse_test(r#"ignore = true"#, augment_directive!(ignore: true));
        directive_parse_test(r#"ignore = false"#, augment_directive!(ignore: false));
    }

    #[test]
    fn override_container_image() {
        directive_parse_test(
            indoc! {r#"
                image.name = "foo"
                image.use = ["layers"]
            "#},
            override_directive!(parent: image_container_parent!("foo", layers)),
        );
    }

    #[test]
    fn override_container_parent() {
        directive_parse_test(
            indoc! {r#"
                parent.name = "foo"
                parent.use = ["layers"]
            "#},
            override_directive!(parent: container_container_parent!("foo", layers)),
        );
    }

    #[test]
    fn override_container_image_and_parent() {
        directive_parse_error_test(
            indoc! {r#"
                image = "image1"
                parent = "parent"
            "#},
            "both `image` and `parent` cannot be specified",
        );
    }

    #[test]
    fn override_container_fields() {
        directive_parse_test(
            indoc! {r#"
                parent = "parent"
                layers = [ { tar = "foo.tar" } ]
                user = 123
            "#},
            override_directive! {
                parent: container_container_parent!("parent", all, -layers, -user),
                layers: [tar_layer_spec!("foo.tar")],
                user: 123,
            },
        );
    }

    #[test]
    fn override_container_filter() {
        directive_parse_test(
            indoc! {r#"
                filter = "foo && bar"
                parent = "parent"
            "#},
            override_directive! {
                filter: "foo && bar",
                parent: container_container_parent!("parent", all),
            },
        );
    }

    #[test]
    fn override_container_include_shared_libraries() {
        directive_parse_test(
            indoc! {r#"
                parent = "parent"
                include_shared_libraries = true
            "#},
            override_directive! {
                parent: container_container_parent!("parent", all),
                include_shared_libraries: true,
            },
        );
        directive_parse_test(
            indoc! {r#"
                parent = "parent"
                include_shared_libraries = false
            "#},
            override_directive! {
                parent: container_container_parent!("parent", all),
                include_shared_libraries: false,
            },
        );
    }

    #[test]
    fn override_container_timeout() {
        directive_parse_test(
            indoc! {r#"
                parent = "parent"
                timeout = 0
            "#},
            override_directive! {
                parent: container_container_parent!("parent", all),
                timeout: 0,
            },
        );
        directive_parse_test(
            indoc! {r#"
                parent = "parent"
                timeout = 1
            "#},
            override_directive! {
                parent: container_container_parent!("parent", all),
                timeout: 1,
            },
        );
    }

    #[test]
    fn override_container_ignore() {
        directive_parse_test(
            indoc! {r#"
                parent = "parent"
                ignore = true
            "#},
            override_directive! {
                parent: container_container_parent!("parent", all),
                ignore: true,
            },
        );
        directive_parse_test(
            indoc! {r#"
                parent = "parent"
                ignore = false
            "#},
            override_directive! {
                parent: container_container_parent!("parent", all),
                ignore: false,
            },
        );
    }

    mod augment_directive_macro {
        use super::*;

        #[test]
        fn empty() {
            assert_eq!(augment_directive!(), Directive::<String>::default());
        }

        #[test]
        fn filter() {
            assert_eq!(
                augment_directive!(filter: "package = \"package1\""),
                Directive {
                    filter: Some(SimpleFilter::Package("package1".into())),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn include_shared_libraries() {
            assert_eq!(
                augment_directive!(include_shared_libraries: true),
                Directive::<String> {
                    include_shared_libraries: Some(true),
                    ..Default::default()
                },
            );
            assert_eq!(
                augment_directive!(include_shared_libraries: false),
                Directive::<String> {
                    include_shared_libraries: Some(false),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn timeout() {
            assert_eq!(
                augment_directive!(timeout: 0),
                Directive::<String> {
                    timeout: Some(None),
                    ..Default::default()
                },
            );
            assert_eq!(
                augment_directive!(timeout: 1),
                Directive::<String> {
                    timeout: Some(Timeout::new(1)),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn ignore() {
            assert_eq!(
                augment_directive!(ignore: true),
                Directive::<String> {
                    ignore: Some(true),
                    ..Default::default()
                },
            );
            assert_eq!(
                augment_directive!(ignore: false),
                Directive::<String> {
                    ignore: Some(false),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn layers() {
            assert_eq!(
                augment_directive!(layers: [tar_layer_spec!("foo.tar")]),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        layers: Some(vec![tar_layer_spec!("foo.tar")]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn added_layers() {
            assert_eq!(
                augment_directive!(added_layers: [tar_layer_spec!("foo.tar")]),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        added_layers: vec![tar_layer_spec!("foo.tar")],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn environment() {
            assert_eq!(
                augment_directive!(environment: [("foo", "bar"), ("frob", "baz")]),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        environment: Some(
                            btreemap! {
                                "foo" => "bar",
                                "frob" => "baz",
                            }
                            .into_environment()
                        ),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn added_environment() {
            assert_eq!(
                augment_directive!(added_environment: [("foo", "bar"), ("frob", "baz")]),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        added_environment: btreemap! {
                            "foo" => "bar",
                            "frob" => "baz",
                        }
                        .into_environment(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn working_directory() {
            assert_eq!(
                augment_directive!(working_directory: "/foo"),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        working_directory: Some("/foo".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn enable_writable_file_system() {
            assert_eq!(
                augment_directive!(enable_writable_file_system: true),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        enable_writable_file_system: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
            assert_eq!(
                augment_directive!(enable_writable_file_system: false),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        enable_writable_file_system: Some(false),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn mounts() {
            assert_eq!(
                augment_directive!(mounts: [proc_mount!("/proc"), tmp_mount!("/tmp")]),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        mounts: Some(vec![proc_mount!("/proc"), tmp_mount!("/tmp")]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn added_mounts() {
            assert_eq!(
                augment_directive!(added_mounts: [proc_mount!("/proc"), tmp_mount!("/tmp")]),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        added_mounts: vec![proc_mount!("/proc"), tmp_mount!("/tmp")],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn network() {
            assert_eq!(
                augment_directive!(network: JobNetwork::Loopback),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        network: Some(JobNetwork::Loopback),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
            assert_eq!(
                augment_directive!(network: JobNetwork::Disabled),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        network: Some(JobNetwork::Disabled),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn user() {
            assert_eq!(
                augment_directive!(user: 101),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        user: Some(UserId::new(101)),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn group() {
            assert_eq!(
                augment_directive!(group: 101),
                Directive::<String> {
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        group: Some(GroupId::new(101)),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn multiple() {
            assert_eq!(
                augment_directive! {
                    filter: "package = \"package1\"",
                    include_shared_libraries: true,
                    timeout: 1,
                    ignore: false,
                    layers: [tar_layer_spec!("foo.tar")],
                    added_layers: [tar_layer_spec!("foo.tar")],
                    environment: [("foo", "bar"), ("frob", "baz")],
                    added_environment: [("foo", "bar"), ("frob", "baz")],
                    working_directory: "/foo",
                    network: JobNetwork::Loopback,
                },
                Directive {
                    filter: Some(SimpleFilter::Package("package1".into())),
                    container: DirectiveContainer::Augment(DirectiveContainerAugment {
                        layers: Some(vec![tar_layer_spec!("foo.tar")]),
                        added_layers: vec![tar_layer_spec!("foo.tar")],
                        environment: Some(
                            btreemap! {
                                "foo" => "bar",
                                "frob" => "baz",
                            }
                            .into_environment()
                        ),
                        added_environment: btreemap! {
                            "foo" => "bar",
                            "frob" => "baz",
                        }
                        .into_environment(),
                        working_directory: Some("/foo".into()),
                        network: Some(JobNetwork::Loopback),
                        ..Default::default()
                    }),
                    include_shared_libraries: Some(true),
                    timeout: Some(Timeout::new(1)),
                    ignore: Some(false),
                },
            );
        }
    }

    mod override_directive_macro {
        use super::*;

        #[test]
        fn empty() {
            assert_eq!(
                override_directive!(),
                Directive::<String> {
                    container: DirectiveContainer::Override(Default::default()),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn filter() {
            assert_eq!(
                override_directive!(filter: "package = \"package1\""),
                Directive {
                    filter: Some(SimpleFilter::Package("package1".into())),
                    container: DirectiveContainer::Override(Default::default()),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn container_field() {
            assert_eq!(
                override_directive!(user: 101),
                Directive::<String> {
                    container: DirectiveContainer::Override(container_spec!(user: 101)),
                    ..Default::default()
                },
            );
            assert_eq!(
                override_directive!(group: 202),
                Directive::<String> {
                    container: DirectiveContainer::Override(container_spec!(group: 202)),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn include_shared_libraries() {
            assert_eq!(
                override_directive!(include_shared_libraries: true),
                Directive::<String> {
                    include_shared_libraries: Some(true),
                    container: DirectiveContainer::Override(Default::default()),
                    ..Default::default()
                },
            );
            assert_eq!(
                override_directive!(include_shared_libraries: false),
                Directive::<String> {
                    include_shared_libraries: Some(false),
                    container: DirectiveContainer::Override(Default::default()),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn timeout() {
            assert_eq!(
                override_directive!(timeout: 0),
                Directive::<String> {
                    timeout: Some(None),
                    container: DirectiveContainer::Override(Default::default()),
                    ..Default::default()
                },
            );
            assert_eq!(
                override_directive!(timeout: 1),
                Directive::<String> {
                    timeout: Some(Timeout::new(1)),
                    container: DirectiveContainer::Override(Default::default()),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn ignore() {
            assert_eq!(
                override_directive!(ignore: true),
                Directive::<String> {
                    ignore: Some(true),
                    container: DirectiveContainer::Override(Default::default()),
                    ..Default::default()
                },
            );
            assert_eq!(
                override_directive!(ignore: false),
                Directive::<String> {
                    ignore: Some(false),
                    container: DirectiveContainer::Override(Default::default()),
                    ..Default::default()
                },
            );
        }

        #[test]
        fn multiple() {
            assert_eq!(
                override_directive! {
                    filter: "package = \"package1\"",
                    user: 101,
                    group: 202,
                    include_shared_libraries: true,
                    timeout: 1,
                    ignore: false,
                },
                Directive {
                    filter: Some(SimpleFilter::Package("package1".into())),
                    container: DirectiveContainer::Override(container_spec! {
                        user: 101,
                        group: 202,
                    }),
                    include_shared_libraries: Some(true),
                    timeout: Some(Timeout::new(1)),
                    ignore: Some(false),
                },
            );
        }
    }
}
