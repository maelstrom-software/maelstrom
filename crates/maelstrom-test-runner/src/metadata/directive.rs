#![allow(unused_imports)]
use anyhow::Result;
use maelstrom_base::{GroupId, JobMountForTomlAndJson, JobNetwork, Timeout, UserId, Utf8PathBuf};
use maelstrom_client::spec::{
    incompatible, ImageRef, ImageRefWithImplicitOrExplicitUse, ImageUse, LayerSpec, PossiblyImage,
};
use serde::{de, Deserialize, Deserializer};
use std::{
    collections::BTreeMap,
    fmt::Display,
    str::{self, FromStr},
};

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum DirectiveField {
    Filter,
    IncludeSharedLibraries,
    Timeout,
    Ignore,
    Network,
    EnableWritableFileSystem,
    User,
    Group,
    Mounts,
    AddedMounts,
    Image,
    WorkingDirectory,
    Layers,
    AddedLayers,
    Environment,
    AddedEnvironment,
}

#[derive(Debug, PartialEq)]
pub struct Directive<TestFilterT> {
    pub filter: Option<TestFilterT>,
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

// The derived Default will put a TestFilterT: Default bound on the implementation
impl<TestFilterT> Default for Directive<TestFilterT> {
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

impl<TestFilterT: FromStr> Directive<TestFilterT>
where
    TestFilterT::Err: Display,
{
    fn set_field<'de, A>(&mut self, ident: DirectiveField, map: &mut A) -> Result<(), A::Error>
    where
        A: de::MapAccess<'de>,
    {
        match ident {
            DirectiveField::Filter => {
                self.filter = Some(
                    map.next_value::<String>()?
                        .parse()
                        .map_err(de::Error::custom)?,
                );
            }
            DirectiveField::IncludeSharedLibraries => {
                self.include_shared_libraries = Some(map.next_value()?);
            }
            DirectiveField::Timeout => {
                self.timeout = Some(Timeout::new(map.next_value()?));
            }
            DirectiveField::Ignore => {
                self.ignore = Some(map.next_value()?);
            }
            DirectiveField::Network => {
                self.network = Some(map.next_value()?);
            }
            DirectiveField::EnableWritableFileSystem => {
                self.enable_writable_file_system = Some(map.next_value()?);
            }
            DirectiveField::User => {
                self.user = Some(map.next_value()?);
            }
            DirectiveField::Group => {
                self.group = Some(map.next_value()?);
            }
            DirectiveField::Mounts => {
                self.mounts = Some(map.next_value()?);
            }
            DirectiveField::AddedMounts => {
                self.added_mounts = map.next_value()?;
            }
            DirectiveField::Image => {
                let i = ImageRef::from(map.next_value::<ImageRefWithImplicitOrExplicitUse>()?);
                self.image = Some(i.name);
                for image_use in i.r#use {
                    match image_use {
                        ImageUse::WorkingDirectory => {
                            incompatible(
                                &self.working_directory,
                                "field `image` cannot use `working_directory` if field `working_directory` is also set",
                            )?;
                            self.working_directory = Some(PossiblyImage::Image);
                        }
                        ImageUse::Layers => {
                            incompatible(
                                &self.layers,
                                "field `image` cannot use `layers` if field `layers` is also set",
                            )?;
                            self.layers = Some(PossiblyImage::Image);
                        }
                        ImageUse::Environment => {
                            incompatible(
                                &self.environment,
                                "field `image` cannot use `environment` if field `environment` is also set",
                            )?;
                            self.environment = Some(PossiblyImage::Image);
                        }
                    }
                }
            }
            DirectiveField::WorkingDirectory => {
                incompatible(
                    &self.working_directory,
                    "field `image` cannot use `working_directory` if field `working_directory` is also set",
                )?;
                self.working_directory = Some(PossiblyImage::Explicit(map.next_value()?));
            }
            DirectiveField::Layers => {
                incompatible(
                    &self.layers,
                    "field `image` cannot use `layers` if field `layers` is also set",
                )?;
                self.layers = Some(PossiblyImage::Explicit(map.next_value()?));
            }
            DirectiveField::AddedLayers => {
                self.added_layers = map.next_value()?;
            }
            DirectiveField::Environment => {
                incompatible(
                    &self.environment,
                    "field `image` cannot use `environment` if field `environment` is also set",
                )?;
                self.environment = Some(PossiblyImage::Explicit(map.next_value()?));
            }
            DirectiveField::AddedEnvironment => {
                self.added_environment = map.next_value()?;
            }
        }
        Ok(())
    }
}

impl<'de, TestFilterT: FromStr> de::Visitor<'de> for Directive<TestFilterT>
where
    TestFilterT::Err: Display,
{
    type Value = Self;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "TestDirective")
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        while let Some(key) = map.next_key()? {
            self.set_field(key, &mut map)?;
        }

        Ok(self)
    }
}

impl<'de, TestFilterT: FromStr> de::Deserialize<'de> for Directive<TestFilterT>
where
    TestFilterT::Err: Display,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(Self::default())
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
