#![allow(unused_imports)]
use super::container::{ContainerField, TestContainer};
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
enum DirectiveField {
    Filter,
    IncludeSharedLibraries,
    Timeout,
    Ignore,
    #[serde(untagged)]
    ContainerField(ContainerField),
}

#[derive(Debug, PartialEq)]
pub struct TestDirective<TestFilterT> {
    pub filter: Option<TestFilterT>,
    pub container: TestContainer,
    pub include_shared_libraries: Option<bool>,
    pub timeout: Option<Option<Timeout>>,
    pub ignore: Option<bool>,
}

// The derived Default will put a TestFilterT: Default bound on the implementation
impl<TestFilterT> Default for TestDirective<TestFilterT> {
    fn default() -> Self {
        Self {
            filter: None,
            container: Default::default(),
            include_shared_libraries: None,
            timeout: None,
            ignore: None,
        }
    }
}

impl<TestFilterT: FromStr> TestDirective<TestFilterT>
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
            DirectiveField::ContainerField(container_field) => {
                self.container.set_field(container_field, map)?;
            }
        }
        Ok(())
    }
}

impl<'de, TestFilterT: FromStr> de::Visitor<'de> for TestDirective<TestFilterT>
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

impl<'de, TestFilterT: FromStr> de::Deserialize<'de> for TestDirective<TestFilterT>
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
    use crate::metadata::container::TestContainer;
    use anyhow::Error;
    use indoc::indoc;
    use maelstrom_base::{enum_set, JobDeviceForTomlAndJson};
    use maelstrom_client::spec::SymlinkSpec;
    use maelstrom_test::{
        glob_layer, non_root_utf8_path_buf, paths_layer, shared_library_dependencies_layer, string,
        tar_layer, utf8_path_buf,
    };
    use toml::de::Error as TomlError;

    fn parse_test_directive(file: &str) -> Result<TestDirective<String>> {
        toml::from_str(file).map_err(Error::new)
    }

    fn parse_test_container(file: &str) -> Result<TestContainer> {
        toml::from_str(file).map_err(Error::new)
    }

    #[track_caller]
    fn assert_toml_error(err: Error, expected: &str) {
        let err = err.downcast_ref::<TomlError>().unwrap();
        let message = err.message();
        assert!(message.starts_with(expected), "message: {message}");
    }

    #[track_caller]
    fn directive_error_test(toml: &str, error: &str) {
        assert_toml_error(parse_test_directive(toml).unwrap_err(), error);
    }

    #[track_caller]
    fn directive_or_container_error_test(toml: &str, error: &str) {
        assert_toml_error(parse_test_directive(toml).unwrap_err(), error);

        let mut toml = toml.to_owned();
        toml += "\nname = \"test\"";
        assert_toml_error(parse_test_container(&toml).unwrap_err(), error);
    }

    #[track_caller]
    fn directive_parse_test(toml: &str, expected: TestDirective<String>) {
        assert_eq!(parse_test_directive(toml).unwrap(), expected);
    }

    #[track_caller]
    fn directive_or_container_parse_test(toml: &str, expected: TestDirective<String>) {
        assert_eq!(parse_test_directive(toml).unwrap(), expected);
        assert_eq!(parse_test_container(&toml).unwrap(), expected.container);
    }

    #[test]
    fn empty() {
        assert_eq!(parse_test_directive("").unwrap(), TestDirective::default(),);
    }

    #[test]
    fn unknown_field() {
        directive_error_test(
            r#"
            unknown = "foo"
            "#,
            "unknown field `unknown`, expected one of",
        );
    }

    #[test]
    fn duplicate_field() {
        directive_error_test(
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
            timeout = 1
            "#,
            TestDirective {
                filter: Some(
                    "package.equals(package1) && test.equals(test1)"
                        .parse()
                        .unwrap(),
                ),
                include_shared_libraries: Some(true),
                timeout: Some(Timeout::new(1)),
                container: TestContainer {
                    network: Some(JobNetwork::Loopback),
                    enable_writable_file_system: Some(true),
                    user: Some(UserId::from(101)),
                    group: Some(GroupId::from(202)),
                    ..Default::default()
                },
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
            TestDirective {
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
            TestDirective {
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

    #[test]
    fn unknown_devices_type() {
        directive_or_container_error_test(
            r#"
            mounts = [{ type = "devices",  devices = ["unknown"] }]
            "#,
            "unknown variant `unknown`, expected one of",
        );
    }

    #[test]
    fn working_directory() {
        directive_or_container_parse_test(
            r#"
            working_directory = "/foo"
            "#,
            TestDirective {
                container: TestContainer {
                    working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_no_use() {
        directive_or_container_parse_test(
            r#"
            image = { name = "rust" }
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    layers: Some(PossiblyImage::Image),
                    environment: Some(PossiblyImage::Image),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_as_string() {
        directive_or_container_parse_test(
            r#"
            image = "rust"
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    layers: Some(PossiblyImage::Image),
                    environment: Some(PossiblyImage::Image),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_working_directory() {
        directive_or_container_parse_test(
            r#"
            image = { name = "rust", use = ["layers", "working_directory"] }
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    layers: Some(PossiblyImage::Image),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn working_directory_after_image_without_working_directory() {
        directive_or_container_parse_test(
            r#"
            image = { name = "rust", use = ["layers"] }
            working_directory = "/foo"
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                    layers: Some(PossiblyImage::Image),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_without_working_directory_after_working_directory() {
        directive_or_container_parse_test(
            r#"
            working_directory = "/foo"
            image = { name = "rust", use = ["layers"] }
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                    layers: Some(PossiblyImage::Image),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn working_directory_after_image_with_working_directory() {
        directive_or_container_error_test(
            r#"
            image = { name = "rust", use = ["layers", "working_directory"] }
            working_directory = "/foo"
            "#,
            "field `image` cannot use `working_directory` if field `working_directory` is also set",
        );
    }

    #[test]
    fn image_with_working_directory_after_working_directory() {
        directive_or_container_error_test(
            r#"
            working_directory = "/foo"
            image = { name = "rust", use = ["layers", "working_directory"] }
            "#,
            "field `image` cannot use `working_directory` if field `working_directory` is also set",
        );
    }

    #[test]
    fn layers_glob() {
        directive_or_container_parse_test(
            r#"
            layers = [{ glob = "foo*.bin" }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![glob_layer!("foo*.bin")])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        directive_or_container_parse_test(
            r#"
            layers = [{ glob = "foo*.bin", strip_prefix = "a" }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![glob_layer!(
                        "foo*.bin",
                        strip_prefix = "a",
                    )])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        directive_or_container_parse_test(
            r#"
            layers = [{ glob = "foo*.bin", prepend_prefix = "b" }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![glob_layer!(
                        "foo*.bin",
                        prepend_prefix = "b",
                    )])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        directive_or_container_parse_test(
            r#"
            layers = [{ glob = "foo*.bin", canonicalize = true }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![glob_layer!(
                        "foo*.bin",
                        canonicalize = true,
                    )])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn layers_paths() {
        directive_or_container_parse_test(
            r#"
            layers = [{ paths = ["foo.bin", "bar.bin"] }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![paths_layer!([
                        "foo.bin", "bar.bin",
                    ])])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        directive_or_container_parse_test(
            r#"
            layers = [{ paths = ["foo.bin", "bar.bin"], strip_prefix = "a" }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![paths_layer!(
                        ["foo.bin", "bar.bin"],
                        strip_prefix = "a",
                    )])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        directive_or_container_parse_test(
            r#"
            layers = [{ paths = ["foo.bin", "bar.bin"], prepend_prefix = "a" }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![paths_layer!(
                        ["foo.bin", "bar.bin"],
                        prepend_prefix = "a",
                    )])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        directive_or_container_parse_test(
            r#"
            layers = [{ paths = ["foo.bin", "bar.bin"], canonicalize = true }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![paths_layer!(
                        ["foo.bin", "bar.bin"],
                        canonicalize = true,
                    )])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn layers_stubs() {
        directive_or_container_parse_test(
            r#"
            layers = [{ stubs = ["/foo/bar", "/bin/{baz,qux}/"] }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![LayerSpec::Stubs {
                        stubs: vec!["/foo/bar".into(), "/bin/{baz,qux}/".into()],
                    }])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn layers_symlinks() {
        directive_or_container_parse_test(
            r#"
            layers = [{ symlinks = [{ link = "/hi", target = "/there" }] }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![LayerSpec::Symlinks {
                        symlinks: vec![SymlinkSpec {
                            link: "/hi".into(),
                            target: "/there".into(),
                        }],
                    }])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn layers_shared_library_dependencies() {
        directive_or_container_parse_test(
            r#"
            layers = [
                { shared_library_dependencies = ["/bin/bash", "/bin/sh"] }
            ]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![
                        shared_library_dependencies_layer!(["/bin/bash", "/bin/sh"]),
                    ])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        directive_or_container_parse_test(
            r#"
            layers = [
                { shared_library_dependencies = ["/bin/bash", "/bin/sh"], prepend_prefix = "/usr" }
            ]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![
                        shared_library_dependencies_layer!(
                            ["/bin/bash", "/bin/sh"],
                            prepend_prefix = "/usr",
                        ),
                    ])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        directive_or_container_parse_test(
            r#"
            layers = [
                { shared_library_dependencies = ["/bin/bash", "/bin/sh"], canonicalize = true }
            ]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![
                        shared_library_dependencies_layer!(
                            ["/bin/bash", "/bin/sh"],
                            canonicalize = true,
                        ),
                    ])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_layers() {
        directive_or_container_parse_test(
            r#"
            image = { name = "rust", use = ["layers", "working_directory"] }
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    layers: Some(PossiblyImage::Image),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn layers_after_image_without_layers() {
        directive_or_container_parse_test(
            r#"
            image = { name = "rust", use = ["working_directory"] }
            layers = [{ tar = "foo.tar" }]
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_without_layers_after_layers() {
        directive_or_container_parse_test(
            r#"
            layers = [{ tar = "foo.tar" }]
            image = { name = "rust", use = ["working_directory"] }
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn layers_after_image_with_layers() {
        directive_or_container_error_test(
            r#"
            image = { name = "rust", use = ["layers", "working_directory"] }
            layers = [{ tar = "foo.tar" }]
            "#,
            "field `image` cannot use `layers` if field `layers` is also set",
        )
    }

    #[test]
    fn image_with_layers_after_layers() {
        directive_or_container_error_test(
            r#"
            layers = [{ tar = "foo.tar" }]
            image = { name = "rust", use = ["layers", "working_directory"] }
            "#,
            "field `image` cannot use `layers` if field `layers` is also set",
        )
    }

    #[test]
    fn added_layers() {
        directive_or_container_parse_test(
            r#"
            added_layers = [{ tar = "foo.tar" }]
            "#,
            TestDirective {
                container: TestContainer {
                    added_layers: vec![tar_layer!("foo.tar")],
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn added_layers_after_layers() {
        directive_or_container_parse_test(
            r#"
            layers = [{ tar = "foo.tar" }]
            added_layers = [{ tar = "bar.tar" }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                    added_layers: vec![tar_layer!("bar.tar")],
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn layers_after_added_layers() {
        directive_or_container_parse_test(
            r#"
            added_layers = [{ tar = "bar.tar" }]
            layers = [{ tar = "foo.tar" }]
            "#,
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                    added_layers: vec![tar_layer!("bar.tar")],
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn added_layers_after_image_with_layers() {
        directive_or_container_parse_test(
            r#"
            image = { name = "rust", use = ["layers"] }
            added_layers = [{ tar = "foo.tar" }]
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    layers: Some(PossiblyImage::Image),
                    added_layers: vec![tar_layer!("foo.tar")],
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_layers_after_added_layers() {
        directive_or_container_parse_test(
            r#"
            added_layers = [{ tar = "foo.tar" }]
            image = { name = "rust", use = ["layers"] }
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    layers: Some(PossiblyImage::Image),
                    added_layers: vec![tar_layer!("foo.tar")],
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn environment() {
        directive_or_container_parse_test(
            r#"
            environment = { FOO = "foo" }
            "#,
            TestDirective {
                container: TestContainer {
                    environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                        string!("FOO"),
                        string!("foo"),
                    )]))),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_environment() {
        directive_or_container_parse_test(
            r#"
            image = { name = "rust", use = ["environment", "working_directory"] }
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    environment: Some(PossiblyImage::Image),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn environment_after_image_without_environment() {
        directive_or_container_parse_test(
            r#"
            image = { name = "rust", use = ["working_directory"] }
            environment = { FOO = "foo" }
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                        string!("FOO"),
                        string!("foo"),
                    )]))),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_without_environment_after_environment() {
        directive_or_container_parse_test(
            r#"
            environment = { FOO = "foo" }
            image = { name = "rust", use = ["working_directory"] }
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                        string!("FOO"),
                        string!("foo"),
                    )]))),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn environment_after_image_with_environment() {
        directive_or_container_error_test(
            r#"
            image = { name = "rust", use = ["environment", "working_directory"] }
            environment = { FOO = "foo" }
            "#,
            "field `image` cannot use `environment` if field `environment` is also set",
        )
    }

    #[test]
    fn image_with_environment_after_environment() {
        directive_or_container_error_test(
            r#"
            environment = { FOO = "foo" }
            image = { name = "rust", use = ["environment", "working_directory"] }
            "#,
            "field `image` cannot use `environment` if field `environment` is also set",
        )
    }

    #[test]
    fn added_environment() {
        directive_or_container_parse_test(
            r#"
            added_environment = { BAR = "bar" }
            "#,
            TestDirective {
                container: TestContainer {
                    added_environment: BTreeMap::from([(string!("BAR"), string!("bar"))]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn added_environment_after_environment() {
        directive_or_container_parse_test(
            r#"
            environment = { FOO = "foo" }
            added_environment = { BAR = "bar" }
            "#,
            TestDirective {
                container: TestContainer {
                    environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                        string!("FOO"),
                        string!("foo"),
                    )]))),
                    added_environment: BTreeMap::from([(string!("BAR"), string!("bar"))]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn environment_after_added_environment() {
        directive_or_container_parse_test(
            r#"
            added_environment = { BAR = "bar" }
            environment = { FOO = "foo" }
            "#,
            TestDirective {
                container: TestContainer {
                    environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                        string!("FOO"),
                        string!("foo"),
                    )]))),
                    added_environment: BTreeMap::from([(string!("BAR"), string!("bar"))]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn added_environment_after_image_with_environment() {
        directive_or_container_parse_test(
            r#"
            image = { name = "rust", use = ["environment"] }
            added_environment = { BAR = "bar" }
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    environment: Some(PossiblyImage::Image),
                    added_environment: BTreeMap::from([(string!("BAR"), string!("bar"))]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn image_with_environment_after_added_environment() {
        directive_or_container_parse_test(
            r#"
            added_environment = { BAR = "bar" }
            image = { name = "rust", use = ["environment"] }
            "#,
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    environment: Some(PossiblyImage::Image),
                    added_environment: BTreeMap::from([(string!("BAR"), string!("bar"))]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }
}
