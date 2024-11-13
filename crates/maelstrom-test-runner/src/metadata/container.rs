#![allow(unused_imports)]
use anyhow::Result;
use maelstrom_base::{GroupId, JobMountForTomlAndJson, JobNetwork, Timeout, UserId, Utf8PathBuf};
use maelstrom_client::spec::{incompatible, Image, ImageUse, LayerSpec, PossiblyImage};
use serde::{de, Deserialize, Deserializer};
use std::{
    collections::BTreeMap,
    fmt::Display,
    str::{self, FromStr},
};

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
pub enum ContainerField {
    Name,
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

#[derive(Debug, Default, PartialEq)]
pub struct TestContainer {
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
}

#[derive(Default)]
pub struct TestContainerVisitor {
    name: Option<String>,
    image: Option<String>,
    network: Option<JobNetwork>,
    enable_writable_file_system: Option<bool>,
    user: Option<UserId>,
    group: Option<GroupId>,
    layers: Option<PossiblyImage<Vec<LayerSpec>>>,
    added_layers: Vec<LayerSpec>,
    mounts: Option<Vec<JobMountForTomlAndJson>>,
    added_mounts: Vec<JobMountForTomlAndJson>,
    environment: Option<PossiblyImage<BTreeMap<String, String>>>,
    added_environment: BTreeMap<String, String>,
    working_directory: Option<PossiblyImage<Utf8PathBuf>>,
}

impl TestContainerVisitor {
    pub fn add_field<'de, A>(&mut self, ident: ContainerField, map: &mut A) -> Result<(), A::Error>
    where
        A: de::MapAccess<'de>,
    {
        match ident {
            ContainerField::Name => {
                self.name = Some(map.next_value()?);
            }
            ContainerField::Network => {
                self.network = Some(map.next_value()?);
            }
            ContainerField::EnableWritableFileSystem => {
                self.enable_writable_file_system = Some(map.next_value()?);
            }
            ContainerField::User => {
                self.user = Some(map.next_value()?);
            }
            ContainerField::Group => {
                self.group = Some(map.next_value()?);
            }
            ContainerField::Mounts => {
                self.mounts = Some(map.next_value()?);
            }
            ContainerField::AddedMounts => {
                self.added_mounts = map.next_value()?;
            }
            ContainerField::Image => {
                let i = map.next_value::<Image>()?;
                self.image = Some(i.name);
                for use_ in i.use_ {
                    match use_ {
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
            ContainerField::WorkingDirectory => {
                incompatible(
                    &self.working_directory,
                    "field `working_directory` cannot be set after `image` field that uses `working_directory`",
                )?;
                self.working_directory = Some(PossiblyImage::Explicit(map.next_value()?));
            }
            ContainerField::Layers => {
                incompatible(
                    &self.layers,
                    "field `layers` cannot be set after `image` field that uses `layers`",
                )?;
                self.layers = Some(PossiblyImage::Explicit(map.next_value()?));
            }
            ContainerField::AddedLayers => {
                self.added_layers = map.next_value()?;
            }
            ContainerField::Environment => {
                incompatible(
                    &self.environment,
                    "field `environment` cannot be set after `image` field that uses `environment`",
                )?;
                self.environment = Some(PossiblyImage::Explicit(map.next_value()?));
            }
            ContainerField::AddedEnvironment => {
                self.added_environment = map.next_value()?;
            }
        }
        Ok(())
    }

    pub fn build(self) -> TestContainer {
        TestContainer {
            image: self.image,
            network: self.network,
            enable_writable_file_system: self.enable_writable_file_system,
            user: self.user,
            group: self.group,
            layers: self.layers,
            added_layers: self.added_layers,
            mounts: self.mounts,
            added_mounts: self.added_mounts,
            environment: self.environment,
            added_environment: self.added_environment,
            working_directory: self.working_directory,
        }
    }
}

impl<'de> de::Visitor<'de> for TestContainerVisitor {
    type Value = TestContainer;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "TestContainer")
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        while let Some(key) = map.next_key()? {
            self.add_field(key, &mut map)?;
        }

        Ok(self.build())
    }
}

impl<'de> de::Deserialize<'de> for TestContainer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(TestContainerVisitor::default())
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
        glob_layer, non_root_utf8_path_buf, paths_layer, so_deps_layer, string, tar_layer,
        utf8_path_buf,
    };
    use toml::de::Error as TomlError;

    fn parse_test_container(file: &str) -> Result<TestContainer> {
        toml::from_str(file).map_err(Error::new)
    }

    fn assert_toml_error(err: Error, expected: &str) {
        let err = err.downcast_ref::<TomlError>().unwrap();
        let message = err.message();
        assert!(message.starts_with(expected), "message: {message}");
    }

    fn container_error_test(toml: &str, error: &str) {
        assert_toml_error(parse_test_container(toml).unwrap_err(), error);
    }

    fn container_parse_test(toml: &str, expected: TestContainer) {
        assert_eq!(parse_test_container(toml).unwrap(), expected);
    }

    #[test]
    fn unknown_field() {
        container_error_test(
            r#"
            unknown = "foo"
            "#,
            "unknown field `unknown`, expected one of",
        );
    }

    #[test]
    fn duplicate_field() {
        container_error_test(
            r#"
            user = 100
            user = 100
            "#,
            "duplicate key `user`",
        );
    }

    #[test]
    fn simple_fields() {
        container_parse_test(
            r#"
            network = "loopback"
            enable_writable_file_system = true
            user = 101
            group = 202
            "#,
            TestContainer {
                network: Some(JobNetwork::Loopback),
                enable_writable_file_system: Some(true),
                user: Some(UserId::from(101)),
                group: Some(GroupId::from(202)),
                ..Default::default()
            },
        );
    }
}
