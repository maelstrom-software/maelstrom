#![allow(unused_imports)]
use anyhow::Result;
use maelstrom_base::{GroupId, JobMountForTomlAndJson, JobNetwork, UserId, Utf8PathBuf};
use maelstrom_client::spec::{incompatible, ImageRef, ImageUse, LayerSpec, PossiblyImage};
use serde::{de, Deserialize, Deserializer};
use std::{
    collections::BTreeMap,
    fmt::Display,
    str::{self, FromStr},
};

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
pub enum ContainerField {
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

impl TestContainer {
    pub fn set_field<'de, A>(&mut self, ident: ContainerField, map: &mut A) -> Result<(), A::Error>
    where
        A: de::MapAccess<'de>,
    {
        match ident {
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
                let i = map.next_value::<ImageRef>()?;
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
            ContainerField::WorkingDirectory => {
                incompatible(
                    &self.working_directory,
                    "field `image` cannot use `working_directory` if field `working_directory` is also set",
                )?;
                self.working_directory = Some(PossiblyImage::Explicit(map.next_value()?));
            }
            ContainerField::Layers => {
                incompatible(
                    &self.layers,
                    "field `image` cannot use `layers` if field `layers` is also set",
                )?;
                self.layers = Some(PossiblyImage::Explicit(map.next_value()?));
            }
            ContainerField::AddedLayers => {
                self.added_layers = map.next_value()?;
            }
            ContainerField::Environment => {
                incompatible(
                    &self.environment,
                    "field `image` cannot use `environment` if field `environment` is also set",
                )?;
                self.environment = Some(PossiblyImage::Explicit(map.next_value()?));
            }
            ContainerField::AddedEnvironment => {
                self.added_environment = map.next_value()?;
            }
        }
        Ok(())
    }
}

impl<'de> de::Visitor<'de> for TestContainer {
    type Value = Self;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "TestContainer")
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

impl<'de> de::Deserialize<'de> for TestContainer {
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
    use maelstrom_base::{enum_set, JobDeviceForTomlAndJson};
    use maelstrom_client::spec::SymlinkSpec;
    use maelstrom_test::{
        glob_layer, non_root_utf8_path_buf, paths_layer, shared_library_dependencies_layer, string,
        tar_layer, utf8_path_buf,
    };
    use maplit::btreemap;

    #[track_caller]
    fn container_parse_error_test(toml: &str, expected: &str) {
        let err = toml::from_str::<TestContainer>(toml).unwrap_err();
        let actual = err.message();
        assert!(
            actual.starts_with(expected),
            "expected: {expected}; actual: {actual}"
        );
    }

    #[track_caller]
    fn container_parse_test(toml: &str, expected: TestContainer) {
        assert_eq!(toml::from_str::<TestContainer>(toml).unwrap(), expected);
    }

    #[test]
    fn unknown_field() {
        container_parse_error_test(
            r#"
            unknown = "foo"
            "#,
            "unknown field `unknown`, expected one of",
        );
    }

    #[test]
    fn duplicate_field() {
        container_parse_error_test(
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

    #[test]
    fn mounts() {
        container_parse_test(
            r#"
            mounts = [ { type = "proc", mount_point = "/proc" } ]
            "#,
            TestContainer {
                mounts: Some(vec![JobMountForTomlAndJson::Proc {
                    mount_point: non_root_utf8_path_buf!("/proc"),
                }]),
                ..Default::default()
            },
        );
    }

    #[test]
    fn added_mounts() {
        container_parse_test(
            r#"
            added_mounts = [ { type = "tmp", mount_point = "/tmp" } ]
            "#,
            TestContainer {
                added_mounts: vec![JobMountForTomlAndJson::Tmp {
                    mount_point: non_root_utf8_path_buf!("/tmp"),
                }],
                ..Default::default()
            },
        );
    }

    #[test]
    fn working_directory() {
        container_parse_test(
            r#"
            working_directory = "/foo"
            "#,
            TestContainer {
                working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_working_directory() {
        container_parse_test(
            r#"
            image = { name = "rust", use = ["layers", "working_directory"] }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Image),
                layers: Some(PossiblyImage::Image),
                ..Default::default()
            },
        );
    }

    #[test]
    fn working_directory_before_image_without_working_directory() {
        container_parse_test(
            r#"
            working_directory = "/foo"
            image = "rust"
            "#,
            TestContainer {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                layers: Some(PossiblyImage::Image),
                environment: Some(PossiblyImage::Image),
                ..Default::default()
            },
        );
    }

    #[test]
    fn working_directory_after_image_without_working_directory() {
        container_parse_test(
            r#"
            image = "rust"
            working_directory = "/foo"
            "#,
            TestContainer {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                layers: Some(PossiblyImage::Image),
                environment: Some(PossiblyImage::Image),
                ..Default::default()
            },
        );
    }

    #[test]
    fn working_directory_before_image_with_working_directory() {
        container_parse_error_test(
            r#"
            working_directory = "/foo"
            image = { name = "rust", use = ["layers", "working_directory"] }
            "#,
            "field `image` cannot use `working_directory` if field `working_directory` is also set",
        );
    }

    #[test]
    fn working_directory_after_image_with_working_directory() {
        container_parse_error_test(
            r#"
            image = { name = "rust", use = ["layers", "working_directory"] }
            working_directory = "/foo"
            "#,
            "field `image` cannot use `working_directory` if field `working_directory` is also set",
        );
    }

    #[test]
    fn layers() {
        container_parse_test(
            r#"
            layers = [ { tar = "foo.tar" } ]
            "#,
            TestContainer {
                layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                ..Default::default()
            },
        );
    }

    #[test]
    fn added_layers() {
        container_parse_test(
            r#"
            added_layers = [ { tar = "bar.tar" } ]
            "#,
            TestContainer {
                added_layers: vec![tar_layer!("bar.tar")],
                ..Default::default()
            },
        );
    }

    #[test]
    fn layers_before_added_layers() {
        container_parse_test(
            r#"
            layers = [ { tar = "foo.tar" } ]
            added_layers = [ { tar = "bar.tar" } ]
            "#,
            TestContainer {
                layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                added_layers: vec![tar_layer!("bar.tar")],
                ..Default::default()
            },
        );
    }

    #[test]
    fn layers_after_added_layers() {
        container_parse_test(
            r#"
            added_layers = [ { tar = "bar.tar" } ]
            layers = [ { tar = "foo.tar" } ]
            "#,
            TestContainer {
                layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                added_layers: vec![tar_layer!("bar.tar")],
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_layers() {
        container_parse_test(
            r#"
            image = { name = "rust", use = ["layers"] }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                layers: Some(PossiblyImage::Image),
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_layers_before_added_layers() {
        container_parse_test(
            r#"
            image = { name = "rust", use = ["layers"] }
            added_layers = [ { tar = "bar.tar" } ]
            "#,
            TestContainer {
                image: Some(string!("rust")),
                layers: Some(PossiblyImage::Image),
                added_layers: vec![tar_layer!("bar.tar")],
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_layers_after_added_layers() {
        container_parse_test(
            r#"
            added_layers = [ { tar = "bar.tar" } ]
            image = { name = "rust", use = ["layers"] }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                layers: Some(PossiblyImage::Image),
                added_layers: vec![tar_layer!("bar.tar")],
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_without_layers_before_added_layers() {
        container_parse_test(
            r#"
            image = { name = "rust", use = ["environment"] }
            added_layers = [ { tar = "bar.tar" } ]
            "#,
            TestContainer {
                image: Some(string!("rust")),
                environment: Some(PossiblyImage::Image),
                added_layers: vec![tar_layer!("bar.tar")],
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_without_layers_after_added_layers() {
        container_parse_test(
            r#"
            added_layers = [ { tar = "bar.tar" } ]
            image = { name = "rust", use = ["environment"] }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                environment: Some(PossiblyImage::Image),
                added_layers: vec![tar_layer!("bar.tar")],
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_layers_before_layers() {
        container_parse_error_test(
            r#"
            image = { name = "rust", use = ["layers"] }
            layers = [ { tar = "foo.tar" } ]
            "#,
            "field `image` cannot use `layers` if field `layers` is also set",
        );
    }

    #[test]
    fn image_with_layers_after_layers() {
        container_parse_error_test(
            r#"
            layers = [ { tar = "foo.tar" } ]
            image = { name = "rust", use = ["layers"] }
            "#,
            "field `image` cannot use `layers` if field `layers` is also set",
        );
    }

    #[test]
    fn image_without_layers_before_layers() {
        container_parse_test(
            r#"
            image = { name = "rust", use = ["environment"] }
            layers = [ { tar = "foo.tar" } ]
            "#,
            TestContainer {
                image: Some(string!("rust")),
                environment: Some(PossiblyImage::Image),
                layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_without_layers_after_layers() {
        container_parse_test(
            r#"
            layers = [ { tar = "foo.tar" } ]
            image = { name = "rust", use = ["environment"] }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                environment: Some(PossiblyImage::Image),
                layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                ..Default::default()
            },
        );
    }

    /*** environment ***/

    #[test]
    fn environment() {
        container_parse_test(
            r#"
            environment = { FOO = "foo" }
            "#,
            TestContainer {
                environment: Some(PossiblyImage::Explicit(
                    btreemap! { string!("FOO") => string!("foo") },
                )),
                ..Default::default()
            },
        );
    }

    #[test]
    fn added_environment() {
        container_parse_test(
            r#"
            added_environment = { BAR = "bar" }
            "#,
            TestContainer {
                added_environment: btreemap! { string!("BAR") => string!("bar") },
                ..Default::default()
            },
        );
    }

    #[test]
    fn environment_before_added_environment() {
        container_parse_test(
            r#"
            environment = { FOO = "foo" }
            added_environment = { BAR = "bar" }
            "#,
            TestContainer {
                environment: Some(PossiblyImage::Explicit(
                    btreemap! { string!("FOO") => string!("foo") },
                )),
                added_environment: btreemap! { string!("BAR") => string!("bar") },
                ..Default::default()
            },
        );
    }

    #[test]
    fn environment_after_added_environment() {
        container_parse_test(
            r#"
            added_environment = { BAR = "bar" }
            environment = { FOO = "foo" }
            "#,
            TestContainer {
                environment: Some(PossiblyImage::Explicit(
                    btreemap! { string!("FOO") => string!("foo") },
                )),
                added_environment: btreemap! { string!("BAR") => string!("bar") },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_environment() {
        container_parse_test(
            r#"
            image = { name = "rust", use = ["environment"] }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                environment: Some(PossiblyImage::Image),
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_environment_before_added_environment() {
        container_parse_test(
            r#"
            image = { name = "rust", use = ["environment"] }
            added_environment = { BAR = "bar" }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                environment: Some(PossiblyImage::Image),
                added_environment: btreemap! { string!("BAR") => string!("bar") },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_environment_after_added_environment() {
        container_parse_test(
            r#"
            added_environment = { BAR = "bar" }
            image = { name = "rust", use = ["environment"] }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                environment: Some(PossiblyImage::Image),
                added_environment: btreemap! { string!("BAR") => string!("bar") },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_without_environment_before_added_environment() {
        container_parse_test(
            r#"
            image = { name = "rust", use = ["layers"] }
            added_environment = { BAR = "bar" }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                layers: Some(PossiblyImage::Image),
                added_environment: btreemap! { string!("BAR") => string!("bar") },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_without_environment_after_added_environment() {
        container_parse_test(
            r#"
            added_environment = { BAR = "bar" }
            image = { name = "rust", use = ["layers"] }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                layers: Some(PossiblyImage::Image),
                added_environment: btreemap! { string!("BAR") => string!("bar") },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_environment_before_environment() {
        container_parse_error_test(
            r#"
            image = { name = "rust", use = ["environment"] }
            environment = { FOO = "foo" }
            "#,
            "field `image` cannot use `environment` if field `environment` is also set",
        );
    }

    #[test]
    fn image_with_environment_after_environment() {
        container_parse_error_test(
            r#"
            environment = { FOO = "foo" }
            image = { name = "rust", use = ["environment"] }
            "#,
            "field `image` cannot use `environment` if field `environment` is also set",
        );
    }

    #[test]
    fn image_without_environment_before_environment() {
        container_parse_test(
            r#"
            image = { name = "rust", use = ["layers"] }
            environment = { FOO = "foo" }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                layers: Some(PossiblyImage::Image),
                environment: Some(PossiblyImage::Explicit(
                    btreemap! { string!("FOO") => string!("foo") },
                )),
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_without_environment_after_environment() {
        container_parse_test(
            r#"
            environment = { FOO = "foo" }
            image = { name = "rust", use = ["layers"] }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                layers: Some(PossiblyImage::Image),
                environment: Some(PossiblyImage::Explicit(
                    btreemap! { string!("FOO") => string!("foo") },
                )),
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_no_use() {
        container_parse_test(
            r#"
            image = { name = "rust" }
            "#,
            TestContainer {
                image: Some(string!("rust")),
                layers: Some(PossiblyImage::Image),
                environment: Some(PossiblyImage::Image),
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_as_string() {
        container_parse_test(
            r#"
            image = "rust"
            "#,
            TestContainer {
                image: Some(string!("rust")),
                layers: Some(PossiblyImage::Image),
                environment: Some(PossiblyImage::Image),
                ..Default::default()
            },
        );
    }
}
