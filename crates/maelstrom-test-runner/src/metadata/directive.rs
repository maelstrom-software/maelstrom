#![allow(unused_imports)]
use anyhow::Result;
use maelstrom_base::{GroupId, JobMountForTomlAndJson, JobNetwork, Timeout, UserId, Utf8PathBuf};
use maelstrom_client::spec::{
    ContainerRefWithImplicitOrExplicitUse, ContainerSpec, ContainerSpecForTomlAndJson, EnvSelector,
    ImageRef, ImageRefWithImplicitOrExplicitUse, ImageUse, LayerSpec,
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

// The derived Default will put a FilterT: Default bound on the implementation
impl<FilterT> Default for Directive<FilterT> {
    fn default() -> Self {
        Self {
            filter: Default::default(),
            container: Default::default(),
            include_shared_libraries: Default::default(),
            timeout: Default::default(),
            ignore: Default::default(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum DirectiveContainer {
    Override(ContainerSpec),
    Accumulate(DirectiveContainerAccumulate),
}

impl Default for DirectiveContainer {
    fn default() -> Self {
        Self::Accumulate(Default::default())
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct DirectiveContainerAccumulate {
    pub layers: Option<Vec<LayerSpec>>,
    pub added_layers: Option<Vec<LayerSpec>>,
    pub environment: Option<BTreeMap<String, String>>,
    pub added_environment: Option<BTreeMap<String, String>>,
    pub working_directory: Option<Utf8PathBuf>,
    pub enable_writable_file_system: Option<bool>,
    pub mounts: Option<Vec<JobMountForTomlAndJson>>,
    pub added_mounts: Option<Vec<JobMountForTomlAndJson>>,
    pub network: Option<JobNetwork>,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectiveForTomlAndJson {
    filter: Option<String>,

    // This will be Some if any of the other fields are Some(AllMetadata::Image).
    image: Option<ImageRefWithImplicitOrExplicitUse>,
    layers: Option<Vec<LayerSpec>>,
    added_layers: Option<Vec<LayerSpec>>,
    environment: Option<BTreeMap<String, String>>,
    added_environment: Option<BTreeMap<String, String>>,
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
    fn try_from(directive: DirectiveForTomlAndJson) -> Result<Self, Self::Error> {
        let DirectiveForTomlAndJson {
            filter,
            image,
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
        } = directive;

        let filter = filter
            .map(|filter| filter.parse::<FilterT>())
            .transpose()
            .map_err(|err| err.to_string())?;

        let container = {
            if image.is_some() {
                DirectiveContainer::Override(
                    ContainerSpecForTomlAndJson {
                        image,
                        parent: None,
                        layers,
                        added_layers,
                        environment: environment.map(EnvSelector::Implicit),
                        added_environment: added_environment.map(EnvSelector::Implicit),
                        working_directory,
                        enable_writable_file_system,
                        mounts,
                        added_mounts,
                        network,
                        user,
                        group,
                    }
                    .try_into()?,
                )
            } else {
                DirectiveContainer::Accumulate(DirectiveContainerAccumulate {
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
                })
            }
        };

        Ok(Directive {
            filter,
            container,
            include_shared_libraries,
            timeout: timeout.map(Timeout::new),
            ignore,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Error;
    use indoc::indoc;
    use maelstrom_base::{enum_set, JobDeviceForTomlAndJson};
    use maelstrom_client::spec::SymlinkSpec;
    use maelstrom_test::{
        glob_layer, non_root_utf8_path_buf, paths_layer, shared_library_dependencies_layer, string,
        tar_layer, utf8_path_buf,
    };

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
    fn empty() {
        directive_parse_test("", Directive::default());
    }

    #[test]
    fn unknown_field() {
        directive_parse_error_test(
            r#"
            unknown = "foo"
            "#,
            "unknown field `unknown`, expected one of",
        );
    }

    #[test]
    fn duplicate_field() {
        directive_parse_error_test(
            r#"
            filter = "all"
            filter = "any"
            "#,
            "duplicate key `filter`",
        );
    }

    #[test]
    fn simple_fields() {
        directive_parse_test(
            r#"
            filter = "package.equals(package1) && test.equals(test1)"
            include_shared_libraries = true
            network = "loopback"
            enable_writable_file_system = true
            user = 101
            group = 202
            "#,
            Directive {
                filter: Some(
                    "package.equals(package1) && test.equals(test1)"
                        .parse()
                        .unwrap(),
                ),
                include_shared_libraries: Some(true),
                container: DirectiveContainer::Accumulate(DirectiveContainerAccumulate {
                    network: Some(JobNetwork::Loopback),
                    enable_writable_file_system: Some(true),
                    user: Some(UserId::from(101)),
                    group: Some(GroupId::from(202)),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
    }

    #[test]
    fn nonzero_timeout() {
        directive_parse_test(
            r#"
            filter = "package.equals(package1) && test.equals(test1)"
            timeout = 1
            "#,
            Directive {
                filter: Some(
                    "package.equals(package1) && test.equals(test1)"
                        .parse()
                        .unwrap(),
                ),
                timeout: Some(Timeout::new(1)),
                ..Default::default()
            },
        );
    }

    #[test]
    fn zero_timeout() {
        directive_parse_test(
            r#"
            filter = "package.equals(package1) && test.equals(test1)"
            timeout = 0
            "#,
            Directive {
                filter: Some(
                    "package.equals(package1) && test.equals(test1)"
                        .parse()
                        .unwrap(),
                ),
                timeout: Some(None),
                ..Default::default()
            },
        );
    }
}
