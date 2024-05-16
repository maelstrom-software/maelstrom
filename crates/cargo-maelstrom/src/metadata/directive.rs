use crate::pattern;
use anyhow::Result;
use maelstrom_base::{
    EnumSet, GroupId, JobDevice, JobDeviceForTomlAndJson, JobMountForTomlAndJson, JobNetwork,
    Timeout, UserId, Utf8PathBuf,
};
use maelstrom_client::spec::{incompatible, Image, ImageUse, Layer, PossiblyImage};
use serde::{de, Deserialize, Deserializer};
use std::{collections::BTreeMap, str};

#[derive(Debug, Default, PartialEq)]
pub struct TestDirective {
    pub filter: Option<pattern::Pattern>,
    // This will be Some if any of the other fields are Some(AllMetadata::Image).
    pub image: Option<String>,
    pub include_shared_libraries: Option<bool>,
    pub network: Option<JobNetwork>,
    pub enable_writable_file_system: Option<bool>,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
    pub timeout: Option<Option<Timeout>>,
    pub layers: Option<PossiblyImage<Vec<Layer>>>,
    pub added_layers: Vec<Layer>,
    pub mounts: Option<Vec<JobMountForTomlAndJson>>,
    pub added_mounts: Vec<JobMountForTomlAndJson>,
    pub devices: Option<EnumSet<JobDevice>>,
    pub added_devices: EnumSet<JobDevice>,
    pub environment: Option<PossiblyImage<BTreeMap<String, String>>>,
    pub added_environment: BTreeMap<String, String>,
    pub working_directory: Option<PossiblyImage<Utf8PathBuf>>,
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum DirectiveField {
    Filter,
    IncludeSharedLibraries,
    Network,
    EnableWritableFileSystem,
    User,
    Group,
    Timeout,
    Mounts,
    AddedMounts,
    Devices,
    AddedDevices,
    Image,
    WorkingDirectory,
    Layers,
    AddedLayers,
    Environment,
    AddedEnvironment,
}

struct DirectiveVisitor;

impl<'de> de::Visitor<'de> for DirectiveVisitor {
    type Value = TestDirective;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "TestDirective")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut filter = None;
        let mut include_shared_libraries = None;
        let mut network = None;
        let mut enable_writable_file_system = None;
        let mut user = None;
        let mut group = None;
        let mut timeout = None;
        let mut mounts = None;
        let mut added_mounts = None;
        let mut devices = None;
        let mut added_devices = None;
        let mut image = None;
        let mut working_directory = None;
        let mut layers = None;
        let mut added_layers = None;
        let mut environment = None;
        let mut added_environment = None;
        while let Some(key) = map.next_key()? {
            match key {
                DirectiveField::Filter => {
                    filter = Some(
                        map.next_value::<String>()?
                            .parse()
                            .map_err(serde::de::Error::custom)?,
                    );
                }
                DirectiveField::IncludeSharedLibraries => {
                    include_shared_libraries = Some(map.next_value()?);
                }
                DirectiveField::Network => {
                    network = Some(map.next_value()?);
                }
                DirectiveField::EnableWritableFileSystem => {
                    enable_writable_file_system = Some(map.next_value()?);
                }
                DirectiveField::User => {
                    user = Some(map.next_value()?);
                }
                DirectiveField::Group => {
                    group = Some(map.next_value()?);
                }
                DirectiveField::Timeout => {
                    timeout = Some(Timeout::new(map.next_value()?));
                }
                DirectiveField::Mounts => {
                    incompatible(
                        &added_mounts,
                        "field `mounts` cannot be set after `added_mounts`",
                    )?;
                    mounts = Some(map.next_value()?);
                }
                DirectiveField::AddedMounts => {
                    added_mounts = Some(map.next_value()?);
                }
                DirectiveField::Devices => {
                    incompatible(
                        &added_devices,
                        "field `devices` cannot be set after `added_devices`",
                    )?;
                    let d = map.next_value::<EnumSet<JobDeviceForTomlAndJson>>()?;
                    devices = Some(d.into_iter().map(JobDevice::from).collect());
                }
                DirectiveField::AddedDevices => {
                    let d = map.next_value::<EnumSet<JobDeviceForTomlAndJson>>()?;
                    added_devices = Some(d.into_iter().map(JobDevice::from).collect());
                }
                DirectiveField::Image => {
                    let i = map.next_value::<Image>()?;
                    image = Some(i.name);
                    for use_ in i.use_ {
                        match use_ {
                            ImageUse::WorkingDirectory => {
                                incompatible(
                                    &working_directory,
                                    "field `image` cannot use `working_directory` if field `working_directory` is also set",
                                )?;
                                working_directory = Some(PossiblyImage::Image);
                            }
                            ImageUse::Layers => {
                                incompatible(
                                    &layers,
                                    "field `image` cannot use `layers` if field `layers` is also set",
                                )?;
                                incompatible(
                                    &added_layers,
                                    "field `image` that uses `layers` cannot be set after `added_layers`",
                                )?;
                                layers = Some(PossiblyImage::Image);
                            }
                            ImageUse::Environment => {
                                incompatible(
                                    &environment,
                                    "field `image` cannot use `environment` if field `environment` is also set",
                                )?;
                                incompatible(
                                    &added_environment,
                                    "field `image` that uses `environment` cannot be set after `added_environment`",
                                )?;
                                environment = Some(PossiblyImage::Image);
                            }
                        }
                    }
                }
                DirectiveField::WorkingDirectory => {
                    incompatible(
                        &working_directory,
                        "field `working_directory` cannot be set after `image` field that uses `working_directory`",
                    )?;
                    working_directory = Some(PossiblyImage::Explicit(map.next_value()?));
                }
                DirectiveField::Layers => {
                    incompatible(
                        &layers,
                        "field `layers` cannot be set after `image` field that uses `layers`",
                    )?;
                    incompatible(
                        &added_layers,
                        "field `layers` cannot be set after `added_layers`",
                    )?;
                    layers = Some(PossiblyImage::Explicit(map.next_value()?));
                }
                DirectiveField::AddedLayers => {
                    added_layers = Some(map.next_value()?);
                }
                DirectiveField::Environment => {
                    incompatible(
                        &environment,
                        "field `environment` cannot be set after `image` field that uses `environment`",
                    )?;
                    incompatible(
                        &added_environment,
                        "field `environment` cannot be set after `added_environment`",
                    )?;
                    environment = Some(PossiblyImage::Explicit(map.next_value()?));
                }
                DirectiveField::AddedEnvironment => {
                    added_environment = Some(map.next_value()?);
                }
            }
        }
        Ok(TestDirective {
            filter,
            include_shared_libraries,
            network,
            enable_writable_file_system,
            user,
            group,
            timeout,
            layers,
            added_layers: added_layers.unwrap_or_default(),
            mounts,
            added_mounts: added_mounts.unwrap_or_default(),
            image,
            working_directory,
            devices,
            added_devices: added_devices.unwrap_or_default(),
            environment,
            added_environment: added_environment.unwrap_or_default(),
        })
    }
}

impl<'de> de::Deserialize<'de> for TestDirective {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(DirectiveVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Error;
    use indoc::indoc;
    use maelstrom_base::{enum_set, BindMountFlagForTomlAndJson, JobMount};
    use maelstrom_client::spec::SymlinkSpec;
    use maelstrom_test::{glob_layer, paths_layer, string, tar_layer, utf8_path_buf};
    use toml::de::Error as TomlError;

    fn parse_test_directive(file: &str) -> Result<TestDirective> {
        toml::from_str(file).map_err(Error::new)
    }

    fn assert_toml_error(err: Error, expected: &str) {
        let err = err.downcast_ref::<TomlError>().unwrap();
        let message = err.message();
        assert!(message.starts_with(expected), "message: {message}");
    }

    #[test]
    fn empty() {
        assert_eq!(parse_test_directive("").unwrap(), TestDirective::default(),);
    }

    #[test]
    fn unknown_field() {
        assert_toml_error(
            parse_test_directive(
                r#"
                unknown = "foo"
                "#,
            )
            .unwrap_err(),
            "unknown field `unknown`, expected one of",
        );
    }

    #[test]
    fn duplicate_field() {
        assert_toml_error(
            parse_test_directive(
                r#"
                filter = "all"
                filter = "any"
                "#,
            )
            .unwrap_err(),
            "duplicate key `filter`",
        );
    }

    #[test]
    fn simple_fields() {
        assert_eq!(
            parse_test_directive(
                r#"
                filter = "package.equals(package1) && test.equals(test1)"
                include_shared_libraries = true
                network = "loopback"
                enable_writable_file_system = true
                user = 101
                group = 202
                timeout = 1
                "#
            )
            .unwrap(),
            TestDirective {
                filter: Some(
                    "package.equals(package1) && test.equals(test1)"
                        .parse()
                        .unwrap()
                ),
                include_shared_libraries: Some(true),
                network: Some(JobNetwork::Loopback),
                enable_writable_file_system: Some(true),
                user: Some(UserId::from(101)),
                group: Some(GroupId::from(202)),
                timeout: Some(Timeout::new(1)),
                ..Default::default()
            }
        );
    }

    #[test]
    fn zero_timeout() {
        assert_eq!(
            parse_test_directive(
                r#"
                filter = "package.equals(package1) && test.equals(test1)"
                timeout = 0
                "#
            )
            .unwrap(),
            TestDirective {
                filter: Some(
                    "package.equals(package1) && test.equals(test1)"
                        .parse()
                        .unwrap()
                ),
                timeout: Some(None),
                ..Default::default()
            }
        );
    }

    #[test]
    fn mounts() {
        assert_eq!(
            parse_test_directive(indoc! {r#"
                mounts = [
                    { fs_type = "proc", mount_point = "/proc" },
                    { fs_type = "bind", mount_point = "/bind", local_path = "/local" },
                    { fs_type = "bind", mount_point = "/bind", local_path = "/local", flags = [ "recursive" ] },
                ]
            "#})
            .unwrap(),
            TestDirective {
                mounts: Some(vec![
                    JobMountForTomlAndJson::Proc { mount_point: utf8_path_buf!("/proc") },
                    JobMountForTomlAndJson::Bind {
                        mount_point: utf8_path_buf!("/bind"),
                        local_path: utf8_path_buf!("/local"),
                        flags: Default::default(),
                    },
                    JobMountForTomlAndJson::Bind {
                        mount_point: utf8_path_buf!("/bind"),
                        local_path: utf8_path_buf!("/local"),
                        flags: enum_set!(BindMountFlagForTomlAndJson::Recursive),
                    },
                ]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn added_mounts() {
        assert_eq!(
            parse_test_directive(indoc! {r#"
                added_mounts = [
                    { fs_type = "proc", mount_point = "/proc" },
                    { fs_type = "bind", mount_point = "/bind", local_path = "/local", flags = ["read-only", "recursive"] },
                ]
            "#})
            .unwrap(),
            TestDirective {
                added_mounts: vec![
                    JobMountForTomlAndJson::Proc { mount_point: utf8_path_buf!("/proc") },
                    JobMountForTomlAndJson::Bind {
                        mount_point: utf8_path_buf!("/bind"),
                        local_path: utf8_path_buf!("/local"),
                        flags: enum_set!(BindMountFlagForTomlAndJson::ReadOnly | BindMountFlagForTomlAndJson::Recursive),
                    }
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn mounts_before_added_mounts() {
        assert_eq!(
            parse_test_directive(indoc! {r#"
                mounts = [ { fs_type = "proc", mount_point = "/proc" } ]
                added_mounts = [ { fs_type = "tmp", mount_point = "/tmp" } ]
            "#})
            .unwrap(),
            TestDirective {
                mounts: Some(vec![JobMountForTomlAndJson::Proc {
                    mount_point: utf8_path_buf!("/proc"),
                }]),
                added_mounts: vec![JobMountForTomlAndJson::Tmp {
                    mount_point: utf8_path_buf!("/tmp"),
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn mounts_after_added_mounts() {
        assert_toml_error(
            parse_test_directive(indoc! {r#"
                added_mounts = [ { fs_type = "tmp", mount_point = "/tmp" } ]
                mounts = [ { fs_type = "proc", mount_point = "/proc" } ]
            "#})
            .unwrap_err(),
            "field `mounts` cannot be set after `added_mounts`",
        );
    }

    #[test]
    fn unknown_field_in_simple_mount() {
        assert_toml_error(
            parse_test_directive(indoc! {r#"
                mounts = [ { fs_type = "proc", mount_point = "/proc", unknown = "true" } ]
            "#})
            .unwrap_err(),
            "unknown field `unknown`, expected",
        );
    }

    #[test]
    fn unknown_field_in_bind_mount() {
        assert_toml_error(
            parse_test_directive(indoc! {r#"
                mounts = [ { fs_type = "bind", mount_point = "/bind", local_path = "/a", unknown = "true" } ]
            "#})
            .unwrap_err(),
            "unknown field `unknown`, expected",
        );
    }

    #[test]
    fn missing_field_in_simple_mount() {
        assert_toml_error(
            parse_test_directive(indoc! {r#"
                mounts = [ { fs_type = "proc" } ]
            "#})
            .unwrap_err(),
            "missing field `mount_point`",
        );
    }

    #[test]
    fn missing_field_in_bind_mount() {
        assert_toml_error(
            parse_test_directive(indoc! {r#"
                mounts = [ { fs_type = "bind", mount_point = "/bind" } ]
            "#})
            .unwrap_err(),
            "missing field `local_path`",
        );
    }

    #[test]
    fn missing_flags_field_in_bind_mount_is_okay() {
        assert_eq!(
            parse_test_directive(indoc! {r#"
                mounts = [ { fs_type = "bind", mount_point = "/bind", local_path = "/a" } ]
            "#})
            .unwrap(),
            TestDirective {
                mounts: Some(vec![JobMountForTomlAndJson::Bind {
                    mount_point: utf8_path_buf!("/bind"),
                    local_path: utf8_path_buf!("/a"),
                    flags: Default::default(),
                }]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn devices() {
        assert_eq!(
            parse_test_directive(indoc! {r#"
                devices = [ "null", "zero" ]
            "#})
            .unwrap(),
            TestDirective {
                devices: Some(enum_set!(JobDevice::Null | JobDevice::Zero)),
                ..Default::default()
            }
        );
    }

    #[test]
    fn added_devices() {
        assert_eq!(
            parse_test_directive(
                r#"
                added_devices = [ "null", "zero" ]
                "#
            )
            .unwrap(),
            TestDirective {
                added_devices: enum_set!(JobDevice::Null | JobDevice::Zero),
                ..Default::default()
            }
        );
    }

    #[test]
    fn devices_before_added_devices() {
        assert_eq!(
            parse_test_directive(
                r#"
                devices = [ "null", "zero" ]
                added_devices = [ "full", "tty" ]
                "#
            )
            .unwrap(),
            TestDirective {
                devices: Some(enum_set!(JobDevice::Null | JobDevice::Zero)),
                added_devices: enum_set!(JobDevice::Full | JobDevice::Tty),
                ..Default::default()
            }
        );
    }

    #[test]
    fn devices_after_added_devices() {
        assert_toml_error(
            parse_test_directive(
                r#"
                added_devices = [ "full", "tty" ]
                devices = [ "null", "zero" ]
                "#,
            )
            .unwrap_err(),
            "field `devices` cannot be set after `added_devices`",
        );
    }

    #[test]
    fn unknown_devices_type() {
        assert_toml_error(
            parse_test_directive(
                r#"
                devices = ["unknown"]
                "#,
            )
            .unwrap_err(),
            "unknown variant `unknown`, expected one of",
        );
    }

    #[test]
    fn working_directory() {
        assert_eq!(
            parse_test_directive(
                r#"
                working_directory = "/foo"
                "#
            )
            .unwrap(),
            TestDirective {
                working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                ..Default::default()
            }
        );
    }

    #[test]
    fn image_with_working_directory() {
        assert_eq!(
            parse_test_directive(
                r#"
                image = { name = "rust", use = ["layers", "working_directory"] }
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Image),
                layers: Some(PossiblyImage::Image),
                ..Default::default()
            }
        );
    }

    #[test]
    fn working_directory_after_image_without_working_directory() {
        assert_eq!(
            parse_test_directive(
                r#"
                image = { name = "rust", use = ["layers"] }
                working_directory = "/foo"
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                layers: Some(PossiblyImage::Image),
                ..Default::default()
            }
        );
    }

    #[test]
    fn image_without_working_directory_after_working_directory() {
        assert_eq!(
            parse_test_directive(
                r#"
                working_directory = "/foo"
                image = { name = "rust", use = ["layers"] }
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                layers: Some(PossiblyImage::Image),
                ..Default::default()
            }
        );
    }

    #[test]
    fn working_directory_after_image_with_working_directory() {
        assert_toml_error(
            parse_test_directive(
                r#"
                image = { name = "rust", use = ["layers", "working_directory"] }
                working_directory = "/foo"
                "#
            )
            .unwrap_err(),
            "field `working_directory` cannot be set after `image` field that uses `working_directory`"
        );
    }

    #[test]
    fn image_with_working_directory_after_working_directory() {
        assert_toml_error(
            parse_test_directive(
                r#"
                working_directory = "/foo"
                image = { name = "rust", use = ["layers", "working_directory"] }
                "#,
            )
            .unwrap_err(),
            "field `image` cannot use `working_directory` if field `working_directory` is also set",
        );
    }

    #[test]
    fn layers_tar() {
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ tar = "foo.tar" }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                ..Default::default()
            }
        );
    }

    #[test]
    fn layers_glob() {
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ glob = "foo*.bin" }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![glob_layer!("foo*.bin")])),
                ..Default::default()
            }
        );
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ glob = "foo*.bin", strip_prefix = "a" }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![glob_layer!(
                    "foo*.bin",
                    strip_prefix = "a"
                )])),
                ..Default::default()
            }
        );
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ glob = "foo*.bin", prepend_prefix = "b" }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![glob_layer!(
                    "foo*.bin",
                    prepend_prefix = "b"
                )])),
                ..Default::default()
            }
        );
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ glob = "foo*.bin", canonicalize = true }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![glob_layer!(
                    "foo*.bin",
                    canonicalize = true
                )])),
                ..Default::default()
            }
        );
    }

    #[test]
    fn layers_paths() {
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ paths = ["foo.bin", "bar.bin"] }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![paths_layer!([
                    "foo.bin", "bar.bin"
                ])])),
                ..Default::default()
            }
        );
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ paths = ["foo.bin", "bar.bin"], strip_prefix = "a" }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![paths_layer!(
                    ["foo.bin", "bar.bin"],
                    strip_prefix = "a"
                )])),
                ..Default::default()
            }
        );
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ paths = ["foo.bin", "bar.bin"], prepend_prefix = "a" }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![paths_layer!(
                    ["foo.bin", "bar.bin"],
                    prepend_prefix = "a"
                )])),
                ..Default::default()
            }
        );
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ paths = ["foo.bin", "bar.bin"], canonicalize = true }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![paths_layer!(
                    ["foo.bin", "bar.bin"],
                    canonicalize = true
                )])),
                ..Default::default()
            }
        );
    }

    #[test]
    fn layers_stubs() {
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ stubs = ["/foo/bar", "/bin/{baz,qux}/"] }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![Layer::Stubs {
                    stubs: vec!["/foo/bar".into(), "/bin/{baz,qux}/".into()]
                }])),
                ..Default::default()
            }
        );
    }

    #[test]
    fn layers_symlinks() {
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ symlinks = [{ link = "/hi", target = "/there" }] }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![Layer::Symlinks {
                    symlinks: vec![SymlinkSpec {
                        link: "/hi".into(),
                        target: "/there".into()
                    }],
                }])),
                ..Default::default()
            }
        );
    }

    #[test]
    fn image_with_layers() {
        assert_eq!(
            parse_test_directive(
                r#"
                image = { name = "rust", use = ["layers", "working_directory"] }
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Image),
                layers: Some(PossiblyImage::Image),
                ..Default::default()
            }
        );
    }

    #[test]
    fn layers_after_image_without_layers() {
        assert_eq!(
            parse_test_directive(
                r#"
                image = { name = "rust", use = ["working_directory"] }
                layers = [{ tar = "foo.tar" }]
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Image),
                layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                ..Default::default()
            }
        );
    }

    #[test]
    fn image_without_layers_after_layers() {
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ tar = "foo.tar" }]
                image = { name = "rust", use = ["working_directory"] }
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Image),
                layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                ..Default::default()
            }
        );
    }

    #[test]
    fn layers_after_image_with_layers() {
        assert_toml_error(
            parse_test_directive(
                r#"
                image = { name = "rust", use = ["layers", "working_directory"] }
                layers = [{ tar = "foo.tar" }]
                "#,
            )
            .unwrap_err(),
            "field `layers` cannot be set after `image` field that uses `layers`",
        )
    }

    #[test]
    fn image_with_layers_after_layers() {
        assert_toml_error(
            parse_test_directive(
                r#"
                layers = [{ tar = "foo.tar" }]
                image = { name = "rust", use = ["layers", "working_directory"] }
                "#,
            )
            .unwrap_err(),
            "field `image` cannot use `layers` if field `layers` is also set",
        )
    }

    #[test]
    fn added_layers() {
        assert_eq!(
            parse_test_directive(
                r#"
                added_layers = [{ tar = "foo.tar" }]
                "#
            )
            .unwrap(),
            TestDirective {
                added_layers: vec![tar_layer!("foo.tar")],
                ..Default::default()
            }
        );
    }

    #[test]
    fn added_layers_after_layers() {
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [{ tar = "foo.tar" }]
                added_layers = [{ tar = "bar.tar" }]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                added_layers: vec![tar_layer!("bar.tar")],
                ..Default::default()
            }
        );
    }

    #[test]
    fn added_layers_after_image_with_layers() {
        assert_eq!(
            parse_test_directive(
                r#"
                image = { name = "rust", use = ["layers"] }
                added_layers = [{ tar = "foo.tar" }]
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some(string!("rust")),
                layers: Some(PossiblyImage::Image),
                added_layers: vec![tar_layer!("foo.tar")],
                ..Default::default()
            }
        );
    }

    #[test]
    fn layers_after_added_layers() {
        assert_toml_error(
            parse_test_directive(
                r#"
                added_layers = [{ tar = "bar.tar" }]
                layers = [{ tar = "foo.tar" }]
                "#,
            )
            .unwrap_err(),
            "field `layers` cannot be set after `added_layers`",
        );
    }

    #[test]
    fn image_with_layers_after_added_layers() {
        assert_toml_error(
            parse_test_directive(
                r#"
                added_layers = [{ tar = "bar.tar" }]
                image = { name = "rust", use = ["layers"] }
                "#,
            )
            .unwrap_err(),
            "field `image` that uses `layers` cannot be set after `added_layers`",
        );
    }

    #[test]
    fn environment() {
        assert_eq!(
            parse_test_directive(
                r#"
                environment = { FOO = "foo" }
                "#
            )
            .unwrap(),
            TestDirective {
                environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                    string!("FOO"),
                    string!("foo")
                )]))),
                ..Default::default()
            }
        );
    }

    #[test]
    fn image_with_environment() {
        assert_eq!(
            parse_test_directive(
                r#"
                image = { name = "rust", use = ["environment", "working_directory"] }
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Image),
                environment: Some(PossiblyImage::Image),
                ..Default::default()
            }
        );
    }

    #[test]
    fn environment_after_image_without_environment() {
        assert_eq!(
            parse_test_directive(
                r#"
                image = { name = "rust", use = ["working_directory"] }
                environment = { FOO = "foo" }
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Image),
                environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                    string!("FOO"),
                    string!("foo")
                )]))),
                ..Default::default()
            }
        );
    }

    #[test]
    fn image_without_environment_after_environment() {
        assert_eq!(
            parse_test_directive(
                r#"
                environment = { FOO = "foo" }
                image = { name = "rust", use = ["working_directory"] }
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some(string!("rust")),
                working_directory: Some(PossiblyImage::Image),
                environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                    string!("FOO"),
                    string!("foo")
                )]))),
                ..Default::default()
            }
        );
    }

    #[test]
    fn environment_after_image_with_environment() {
        assert_toml_error(
            parse_test_directive(
                r#"
                image = { name = "rust", use = ["environment", "working_directory"] }
                environment = { FOO = "foo" }
                "#,
            )
            .unwrap_err(),
            "field `environment` cannot be set after `image` field that uses `environment`",
        )
    }

    #[test]
    fn image_with_environment_after_environment() {
        assert_toml_error(
            parse_test_directive(
                r#"
                environment = { FOO = "foo" }
                image = { name = "rust", use = ["environment", "working_directory"] }
                "#,
            )
            .unwrap_err(),
            "field `image` cannot use `environment` if field `environment` is also set",
        )
    }

    #[test]
    fn added_environment() {
        assert_eq!(
            parse_test_directive(
                r#"
                added_environment = { BAR = "bar" }
                "#
            )
            .unwrap(),
            TestDirective {
                added_environment: BTreeMap::from([(string!("BAR"), string!("bar"))]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn added_environment_after_environment() {
        assert_eq!(
            parse_test_directive(
                r#"
                environment = { FOO = "foo" }
                added_environment = { BAR = "bar" }
                "#
            )
            .unwrap(),
            TestDirective {
                environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                    string!("FOO"),
                    string!("foo")
                )]))),
                added_environment: BTreeMap::from([(string!("BAR"), string!("bar"))]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn added_environment_after_image_with_environment() {
        assert_eq!(
            parse_test_directive(
                r#"
                image = { name = "rust", use = ["environment"] }
                added_environment = { BAR = "bar" }
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some(string!("rust")),
                environment: Some(PossiblyImage::Image),
                added_environment: BTreeMap::from([(string!("BAR"), string!("bar"))]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn environment_after_added_environment() {
        assert_toml_error(
            parse_test_directive(
                r#"
                added_environment = { BAR = "bar" }
                environment = { FOO = "foo" }
                "#,
            )
            .unwrap_err(),
            "field `environment` cannot be set after `added_environment`",
        );
    }

    #[test]
    fn image_with_environment_after_added_environment() {
        assert_toml_error(
            parse_test_directive(
                r#"
                added_environment = { BAR = "bar" }
                image = { name = "rust", use = ["environment"] }
                "#,
            )
            .unwrap_err(),
            "field `image` that uses `environment` cannot be set after `added_environment`",
        );
    }
}
