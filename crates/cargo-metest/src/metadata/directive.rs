use anyhow::Result;
use enumset::EnumSetType;
use meticulous_base::{EnumSet, GroupId, JobDevice, JobDeviceListDeserialize, JobMount, UserId};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{collections::BTreeMap, path::PathBuf, str};

#[derive(PartialEq, Eq, Debug, Deserialize)]
pub enum PossiblyImage<T> {
    Image,
    Explicit(T),
}

#[derive(PartialEq, Eq, Debug, Default)]
pub struct TestDirective {
    pub tests: Option<String>,
    pub package: Option<String>,
    // This will be Some if any of the other fields are Some(AllMetadata::Image).
    pub image: Option<String>,
    pub include_shared_libraries: Option<bool>,
    pub enable_loopback: Option<bool>,
    pub enable_writable_file_system: Option<bool>,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
    pub layers: Option<PossiblyImage<Vec<String>>>,
    pub added_layers: Vec<String>,
    pub mounts: Option<Vec<JobMount>>,
    pub added_mounts: Vec<JobMount>,
    pub devices: Option<EnumSet<JobDevice>>,
    pub added_devices: EnumSet<JobDevice>,
    pub environment: Option<PossiblyImage<BTreeMap<String, String>>>,
    pub added_environment: BTreeMap<String, String>,
    pub working_directory: Option<PossiblyImage<PathBuf>>,
}

#[derive(Debug, Deserialize, EnumSetType, Serialize)]
#[serde(rename_all = "snake_case")]
#[enumset(serialize_repr = "list")]
enum ImageUse {
    Layers,
    Environment,
    WorkingDirectory,
}

#[derive(Deserialize)]
struct DirectiveImage {
    name: String,
    #[serde(rename = "use")]
    use_: EnumSet<ImageUse>,
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum DirectiveField {
    Package,
    Tests,
    IncludeSharedLibraries,
    EnableLoopback,
    EnableWritableFileSystem,
    User,
    Group,
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
        let mut package = None;
        let mut tests = None;
        let mut include_shared_libraries = None;
        let mut enable_loopback = None;
        let mut enable_writable_file_system = None;
        let mut user = None;
        let mut group = None;
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
                DirectiveField::Package => {
                    package = Some(map.next_value()?);
                }
                DirectiveField::Tests => {
                    tests = Some(map.next_value()?);
                }
                DirectiveField::IncludeSharedLibraries => {
                    include_shared_libraries = Some(map.next_value()?);
                }
                DirectiveField::EnableLoopback => {
                    enable_loopback = Some(map.next_value()?);
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
                DirectiveField::Mounts => {
                    if added_mounts.is_some() {
                        return Err(de::Error::custom(format_args!(
                            "field `mounts` cannot be set after `added_mounts`"
                        )));
                    }
                    mounts = Some(map.next_value()?);
                }
                DirectiveField::AddedMounts => {
                    added_mounts = Some(map.next_value()?);
                }
                DirectiveField::Devices => {
                    if added_devices.is_some() {
                        return Err(de::Error::custom(format_args!(
                            "field `devices` cannot be set after `added_devices`"
                        )));
                    }
                    let d = map.next_value::<EnumSet<JobDeviceListDeserialize>>()?;
                    devices = Some(d.into_iter().map(JobDevice::from).collect());
                }
                DirectiveField::AddedDevices => {
                    let d = map.next_value::<EnumSet<JobDeviceListDeserialize>>()?;
                    added_devices = Some(d.into_iter().map(JobDevice::from).collect());
                }
                DirectiveField::Image => {
                    let i = map.next_value::<DirectiveImage>()?;
                    image = Some(i.name);
                    for use_ in i.use_ {
                        match use_ {
                            ImageUse::WorkingDirectory => {
                                if working_directory.is_some() {
                                    assert!(matches!(
                                        working_directory,
                                        Some(PossiblyImage::Explicit(_))
                                    ));
                                    return Err(de::Error::custom(format_args!(
                                        "field `image` cannot use `working_directory` if field `working_directory` is also set"
                                    )));
                                }
                                working_directory = Some(PossiblyImage::Image);
                            }
                            ImageUse::Layers => {
                                if layers.is_some() {
                                    assert!(matches!(layers, Some(PossiblyImage::Explicit(_))));
                                    return Err(de::Error::custom(format_args!(
                                        "field `image` cannot use `layers` if field `layers` is also set"
                                    )));
                                }
                                if added_layers.is_some() {
                                    return Err(de::Error::custom(format_args!(
                                        "field `image` that uses `layers` cannot be set after `added_layers`"
                                    )));
                                }
                                layers = Some(PossiblyImage::Image);
                            }
                            ImageUse::Environment => {
                                if environment.is_some() {
                                    assert!(matches!(
                                        environment,
                                        Some(PossiblyImage::Explicit(_))
                                    ));
                                    return Err(de::Error::custom(format_args!(
                                        "field `image` cannot use `environment` if field `environment` is also set"
                                    )));
                                }
                                if added_environment.is_some() {
                                    return Err(de::Error::custom(format_args!(
                                        "field `image` that uses `environment` cannot be set after `added_environment`"
                                    )));
                                }
                                environment = Some(PossiblyImage::Image);
                            }
                        }
                    }
                }
                DirectiveField::WorkingDirectory => {
                    if working_directory.is_some() {
                        assert!(matches!(working_directory, Some(PossiblyImage::Image)));
                        return Err(de::Error::custom(format_args!(
                            "field `working_directory` cannot be set after `image` field that uses `working_directory`"
                        )));
                    }
                    working_directory = Some(PossiblyImage::Explicit(map.next_value()?));
                }
                DirectiveField::Layers => {
                    if layers.is_some() {
                        assert!(matches!(layers, Some(PossiblyImage::Image)));
                        return Err(de::Error::custom(format_args!(
                            "field `layers` cannot be set after `image` field that uses `layers`"
                        )));
                    }
                    if added_layers.is_some() {
                        return Err(de::Error::custom(format_args!(
                            "field `layers` cannot be set after `added_layers`"
                        )));
                    }
                    layers = Some(PossiblyImage::Explicit(map.next_value()?));
                }
                DirectiveField::AddedLayers => {
                    added_layers = Some(map.next_value()?);
                }
                DirectiveField::Environment => {
                    if environment.is_some() {
                        assert!(matches!(environment, Some(PossiblyImage::Image)));
                        return Err(de::Error::custom(format_args!(
                            "field `environment` cannot be set after `image` field that uses `environment`"
                        )));
                    }
                    if added_environment.is_some() {
                        return Err(de::Error::custom(format_args!(
                            "field `environment` cannot be set after `added_environment`"
                        )));
                    }
                    environment = Some(PossiblyImage::Explicit(map.next_value()?));
                }
                DirectiveField::AddedEnvironment => {
                    added_environment = Some(map.next_value()?);
                }
            }
        }
        Ok(TestDirective {
            package,
            tests,
            include_shared_libraries,
            enable_loopback,
            enable_writable_file_system,
            user,
            group,
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
mod test {
    use super::*;
    use anyhow::Error;
    use meticulous_base::{enum_set, JobMountFsType};
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
                package = "package1"
                package = "package2"
                "#,
            )
            .unwrap_err(),
            "duplicate key `package`",
        );
    }

    #[test]
    fn simple_fields() {
        assert_eq!(
            parse_test_directive(
                r#"
                package = "package1"
                tests = "test1"
                include_shared_libraries = true
                enable_loopback = false
                enable_writable_file_system = true
                user = 101
                group = 202
                "#
            )
            .unwrap(),
            TestDirective {
                package: Some("package1".to_string()),
                tests: Some("test1".to_string()),
                include_shared_libraries: Some(true),
                enable_loopback: Some(false),
                enable_writable_file_system: Some(true),
                user: Some(UserId::from(101)),
                group: Some(GroupId::from(202)),
                ..Default::default()
            }
        );
    }

    #[test]
    fn mounts() {
        assert_eq!(
            parse_test_directive(
                r#"
                mounts = [ { fs_type = "proc", mount_point = "/proc" } ]
                "#
            )
            .unwrap(),
            TestDirective {
                mounts: Some(vec![JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: "/proc".to_string()
                }]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn added_mounts() {
        assert_eq!(
            parse_test_directive(
                r#"
                added_mounts = [ { fs_type = "proc", mount_point = "/proc" } ]
                "#
            )
            .unwrap(),
            TestDirective {
                added_mounts: vec![JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: "/proc".to_string()
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn mounts_before_added_mounts() {
        assert_eq!(
            parse_test_directive(
                r#"
                mounts = [ { fs_type = "proc", mount_point = "/proc" } ]
                added_mounts = [ { fs_type = "tmp", mount_point = "/tmp" } ]
                "#
            )
            .unwrap(),
            TestDirective {
                mounts: Some(vec![JobMount {
                    fs_type: JobMountFsType::Proc,
                    mount_point: "/proc".to_string()
                }]),
                added_mounts: vec![JobMount {
                    fs_type: JobMountFsType::Tmp,
                    mount_point: "/tmp".to_string()
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn mounts_after_added_mounts() {
        assert_toml_error(
            parse_test_directive(
                r#"
                added_mounts = [ { fs_type = "tmp", mount_point = "/tmp" } ]
                mounts = [ { fs_type = "proc", mount_point = "/proc" } ]
                "#,
            )
            .unwrap_err(),
            "field `mounts` cannot be set after `added_mounts`",
        );
    }

    #[test]
    fn unknown_field_in_mount() {
        assert_toml_error(
            parse_test_directive(
                r#"
                mounts = [ { fs_type = "proc", mount_point = "/proc", unknown = "true" } ]
                "#,
            )
            .unwrap_err(),
            "unknown field `unknown`, expected",
        );
    }

    #[test]
    fn missing_field_in_mount() {
        assert_toml_error(
            parse_test_directive(
                r#"
                mounts = [ { fs_type = "proc" } ]
                "#,
            )
            .unwrap_err(),
            "missing field `mount_point`",
        );
    }

    #[test]
    fn devices() {
        assert_eq!(
            parse_test_directive(
                r#"
                devices = [ "null", "zero" ]
                "#
            )
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
                image: Some("rust".to_string()),
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
                image: Some("rust".to_string()),
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
                image: Some("rust".to_string()),
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
    fn layers() {
        assert_eq!(
            parse_test_directive(
                r#"
                layers = ["foo.tar"]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec!["foo.tar".to_string()])),
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
                image: Some("rust".to_string()),
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
                layers = ["foo.tar"]
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some("rust".to_string()),
                working_directory: Some(PossiblyImage::Image),
                layers: Some(PossiblyImage::Explicit(vec!["foo.tar".to_string()])),
                ..Default::default()
            }
        );
    }

    #[test]
    fn image_without_layers_after_layers() {
        assert_eq!(
            parse_test_directive(
                r#"
                layers = ["foo.tar"]
                image = { name = "rust", use = ["working_directory"] }
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some("rust".to_string()),
                working_directory: Some(PossiblyImage::Image),
                layers: Some(PossiblyImage::Explicit(vec!["foo.tar".to_string()])),
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
                layers = ["foo.tar"]
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
                layers = ["foo.tar"]
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
                added_layers = ["foo.tar"]
                "#
            )
            .unwrap(),
            TestDirective {
                added_layers: vec!["foo.tar".to_string()],
                ..Default::default()
            }
        );
    }

    #[test]
    fn added_layers_after_layers() {
        assert_eq!(
            parse_test_directive(
                r#"
                layers = ["foo.tar"]
                added_layers = ["bar.tar"]
                "#
            )
            .unwrap(),
            TestDirective {
                layers: Some(PossiblyImage::Explicit(vec!["foo.tar".to_string()])),
                added_layers: vec!["bar.tar".to_string()],
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
                added_layers = ["foo.tar"]
                "#
            )
            .unwrap(),
            TestDirective {
                image: Some("rust".to_string()),
                layers: Some(PossiblyImage::Image),
                added_layers: vec!["foo.tar".to_string()],
                ..Default::default()
            }
        );
    }

    #[test]
    fn layers_after_added_layers() {
        assert_toml_error(
            parse_test_directive(
                r#"
                added_layers = ["bar.tar"]
                layers = ["foo.tar"]
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
                added_layers = ["bar.tar"]
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
                    "FOO".to_string(),
                    "foo".to_string()
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
                image: Some("rust".to_string()),
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
                image: Some("rust".to_string()),
                working_directory: Some(PossiblyImage::Image),
                environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                    "FOO".to_string(),
                    "foo".to_string()
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
                image: Some("rust".to_string()),
                working_directory: Some(PossiblyImage::Image),
                environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                    "FOO".to_string(),
                    "foo".to_string()
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
                added_environment: BTreeMap::from([("BAR".to_string(), "bar".to_string())]),
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
                    "FOO".to_string(),
                    "foo".to_string()
                )]))),
                added_environment: BTreeMap::from([("BAR".to_string(), "bar".to_string())]),
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
                image: Some("rust".to_string()),
                environment: Some(PossiblyImage::Image),
                added_environment: BTreeMap::from([("BAR".to_string(), "bar".to_string())]),
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
