#![allow(unused_imports)]
use anyhow::Result;
use maelstrom_base::{GroupId, JobMountForTomlAndJson, JobNetwork, Timeout, UserId, Utf8PathBuf};
use maelstrom_client::spec::{
    ContainerRefWithImplicitOrExplicitUse, ImageRef, ImageRefWithImplicitOrExplicitUse, ImageUse,
    LayerSpec, PossiblyImage,
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
    // This will be Some if any of the other fields are Some(AllMetadata::Image).
    pub image: Option<String>,
    pub network: Option<JobNetwork>,
    pub enable_writable_file_system: Option<bool>,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
    pub layers: Option<PossiblyImage<Vec<LayerSpec>>>,
    pub added_layers: Vec<LayerSpec>,
    pub mounts: Option<Vec<JobMountForTomlAndJson>>,
    pub added_mounts: Vec<JobMountForTomlAndJson>,
    pub environment: Option<PossiblyImage<BTreeMap<String, String>>>,
    pub added_environment: BTreeMap<String, String>,
    pub working_directory: Option<PossiblyImage<Utf8PathBuf>>,
    pub include_shared_libraries: Option<bool>,
    pub timeout: Option<Option<Timeout>>,
    pub ignore: Option<bool>,
}

// The derived Default will put a FilterT: Default bound on the implementation
impl<FilterT> Default for Directive<FilterT> {
    fn default() -> Self {
        Self {
            filter: Default::default(),
            image: Default::default(),
            network: Default::default(),
            enable_writable_file_system: Default::default(),
            user: Default::default(),
            group: Default::default(),
            layers: Default::default(),
            added_layers: Default::default(),
            mounts: Default::default(),
            added_mounts: Default::default(),
            environment: Default::default(),
            added_environment: Default::default(),
            working_directory: Default::default(),
            include_shared_libraries: Default::default(),
            timeout: Default::default(),
            ignore: Default::default(),
        }
    }
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

        let image_use = image
            .as_ref()
            .map(|image_ref| image_ref.r#use.as_set())
            .unwrap_or_default();

        let layers = if image_use.contains(ImageUse::Layers) {
            if layers.is_some() {
                return Err(
                    "field `image` cannot use `layers` if field `layers` is also set".into(),
                );
            }
            Some(PossiblyImage::Image)
        } else {
            layers.map(PossiblyImage::Explicit)
        };

        let environment = if image_use.contains(ImageUse::Environment) {
            if environment.is_some() {
                return Err(
                    "field `image` cannot use `environment` if field `environment` is also set"
                        .into(),
                );
            }
            Some(PossiblyImage::Image)
        } else {
            environment.map(PossiblyImage::Explicit)
        };

        let working_directory = if image_use.contains(ImageUse::WorkingDirectory) {
            if working_directory.is_some() {
                return Err(
                    "field `image` cannot use `working_directory` if field `working_directory` is also set".into(),
                );
            }
            Some(PossiblyImage::Image)
        } else {
            working_directory.map(PossiblyImage::Explicit)
        };

        Ok(Directive {
            filter,
            image: image.map(|image_ref| image_ref.name),
            network,
            enable_writable_file_system,
            user,
            group,
            layers,
            environment,
            working_directory,
            mounts,
            added_mounts: added_mounts.unwrap_or_default(),
            added_layers: added_layers.unwrap_or_default(),
            added_environment: added_environment.unwrap_or_default(),
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
                network: Some(JobNetwork::Loopback),
                enable_writable_file_system: Some(true),
                user: Some(UserId::from(101)),
                group: Some(GroupId::from(202)),
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
