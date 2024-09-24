use anyhow::Result;
use maelstrom_base::{GroupId, JobMountForTomlAndJson, JobNetwork, Timeout, UserId, Utf8PathBuf};
use maelstrom_client::spec::{incompatible, Image, ImageUse, LayerSpec, PossiblyImage};
use serde::{de, Deserialize, Deserializer};
use std::{
    collections::BTreeMap,
    fmt::Display,
    str::{self, FromStr},
};

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

impl DirectiveField {
    fn into_container_field(self) -> Option<ContainerField> {
        match self {
            Self::Filter => None,
            Self::IncludeSharedLibraries => None,
            Self::Timeout => None,
            Self::Ignore => None,
            Self::Network => Some(ContainerField::Network),
            Self::EnableWritableFileSystem => Some(ContainerField::EnableWritableFileSystem),
            Self::User => Some(ContainerField::User),
            Self::Group => Some(ContainerField::Group),
            Self::Mounts => Some(ContainerField::Mounts),
            Self::AddedMounts => Some(ContainerField::AddedMounts),
            Self::Image => Some(ContainerField::Image),
            Self::WorkingDirectory => Some(ContainerField::WorkingDirectory),
            Self::Layers => Some(ContainerField::Layers),
            Self::AddedLayers => Some(ContainerField::AddedLayers),
            Self::Environment => Some(ContainerField::Environment),
            Self::AddedEnvironment => Some(ContainerField::AddedEnvironment),
        }
    }
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum ContainerField {
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

struct DirectiveVisitor<TestFilterT> {
    value: TestDirective<TestFilterT>,
    container_visitor: TestContainerVisitor,
}

impl<TestFilterT> Default for DirectiveVisitor<TestFilterT> {
    fn default() -> Self {
        Self {
            value: Default::default(),
            container_visitor: Default::default(),
        }
    }
}

impl<TestFilterT: FromStr> DirectiveVisitor<TestFilterT>
where
    TestFilterT::Err: Display,
{
    fn fill_entry<'de, A>(&mut self, ident: DirectiveField, map: &mut A) -> Result<(), A::Error>
    where
        A: de::MapAccess<'de>,
    {
        match ident {
            DirectiveField::Filter => {
                self.value.filter = Some(
                    map.next_value::<String>()?
                        .parse()
                        .map_err(de::Error::custom)?,
                );
            }
            DirectiveField::IncludeSharedLibraries => {
                self.value.include_shared_libraries = Some(map.next_value()?);
            }
            DirectiveField::Timeout => {
                self.value.timeout = Some(Timeout::new(map.next_value()?));
            }
            DirectiveField::Ignore => {
                self.value.ignore = Some(map.next_value()?);
            }
            c => {
                self.container_visitor
                    .fill_entry(c.into_container_field().unwrap(), map)?;
            }
        }
        Ok(())
    }
}

impl<'de, TestFilterT: FromStr> de::Visitor<'de> for DirectiveVisitor<TestFilterT>
where
    TestFilterT::Err: Display,
{
    type Value = TestDirective<TestFilterT>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "TestDirective")
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        while let Some(key) = map.next_key()? {
            self.fill_entry(key, &mut map)?;
        }

        self.value.container = self.container_visitor.into_value();
        Ok(self.value)
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
        deserializer.deserialize_any(DirectiveVisitor::<TestFilterT>::default())
    }
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
struct TestContainerVisitor {
    image: Option<String>,
    network: Option<JobNetwork>,
    enable_writable_file_system: Option<bool>,
    user: Option<UserId>,
    group: Option<GroupId>,
    layers: Option<PossiblyImage<Vec<LayerSpec>>>,
    added_layers: Option<Vec<LayerSpec>>,
    mounts: Option<Vec<JobMountForTomlAndJson>>,
    added_mounts: Option<Vec<JobMountForTomlAndJson>>,
    environment: Option<PossiblyImage<BTreeMap<String, String>>>,
    added_environment: Option<BTreeMap<String, String>>,
    working_directory: Option<PossiblyImage<Utf8PathBuf>>,
}

impl TestContainerVisitor {
    fn fill_entry<'de, A>(&mut self, ident: ContainerField, map: &mut A) -> Result<(), A::Error>
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
                incompatible(
                    &self.added_mounts,
                    "field `mounts` cannot be set after `added_mounts`",
                )?;
                self.mounts = Some(map.next_value()?);
            }
            ContainerField::AddedMounts => {
                self.added_mounts = Some(map.next_value()?);
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
                            incompatible(
                                &self.added_layers,
                                "field `image` that uses `layers` cannot be set after `added_layers`",
                            )?;
                            self.layers = Some(PossiblyImage::Image);
                        }
                        ImageUse::Environment => {
                            incompatible(
                                &self.environment,
                                "field `image` cannot use `environment` if field `environment` is also set",
                            )?;
                            incompatible(
                                &self.added_environment,
                                "field `image` that uses `environment` cannot be set after `added_environment`",
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
                incompatible(
                    &self.added_layers,
                    "field `layers` cannot be set after `added_layers`",
                )?;
                self.layers = Some(PossiblyImage::Explicit(map.next_value()?));
            }
            ContainerField::AddedLayers => {
                self.added_layers = Some(map.next_value()?);
            }
            ContainerField::Environment => {
                incompatible(
                    &self.environment,
                    "field `environment` cannot be set after `image` field that uses `environment`",
                )?;
                incompatible(
                    &self.added_environment,
                    "field `environment` cannot be set after `added_environment`",
                )?;
                self.environment = Some(PossiblyImage::Explicit(map.next_value()?));
            }
            ContainerField::AddedEnvironment => {
                self.added_environment = Some(map.next_value()?);
            }
        }
        Ok(())
    }

    fn into_value(self) -> TestContainer {
        TestContainer {
            image: self.image,
            network: self.network,
            enable_writable_file_system: self.enable_writable_file_system,
            user: self.user,
            group: self.group,
            layers: self.layers,
            added_layers: self.added_layers.unwrap_or_default(),
            mounts: self.mounts,
            added_mounts: self.added_mounts.unwrap_or_default(),
            environment: self.environment,
            added_environment: self.added_environment.unwrap_or_default(),
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
            self.fill_entry(key, &mut map)?;
        }

        Ok(self.into_value())
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

    fn parse_test_directive(file: &str) -> Result<TestDirective<String>> {
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
                timeout: Some(Timeout::new(1)),
                container: TestContainer {
                    network: Some(JobNetwork::Loopback),
                    enable_writable_file_system: Some(true),
                    user: Some(UserId::from(101)),
                    group: Some(GroupId::from(202)),
                    ..Default::default()
                },
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
                    { type = "proc", mount_point = "/proc" },
                    { type = "bind", mount_point = "/bind", local_path = "/local" },
                    { type = "bind", mount_point = "/bind2", local_path = "/local2", read_only = true },
                    { type = "devices", devices = ["null", "zero"] },
                ]
            "#})
            .unwrap(),
            TestDirective {
                container: TestContainer {
                    mounts: Some(vec![
                        JobMountForTomlAndJson::Proc {
                            mount_point: non_root_utf8_path_buf!("/proc")
                        },
                        JobMountForTomlAndJson::Bind {
                            mount_point: non_root_utf8_path_buf!("/bind"),
                            local_path: utf8_path_buf!("/local"),
                            read_only: false,
                        },
                        JobMountForTomlAndJson::Bind {
                            mount_point: non_root_utf8_path_buf!("/bind2"),
                            local_path: utf8_path_buf!("/local2"),
                            read_only: true,
                        },
                        JobMountForTomlAndJson::Devices {
                            devices: enum_set!(
                                JobDeviceForTomlAndJson::Null | JobDeviceForTomlAndJson::Zero
                            ),
                        },
                    ]),
                    ..Default::default()
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn added_mounts() {
        assert_eq!(
            parse_test_directive(indoc! {r#"
                added_mounts = [
                    { type = "proc", mount_point = "/proc" },
                    { type = "bind", mount_point = "/bind", local_path = "/local", read_only = true },
                    { type = "devices", devices = ["null", "zero"] },
                ]
            "#})
            .unwrap(),
            TestDirective {
                container: TestContainer {
                    added_mounts: vec![
                        JobMountForTomlAndJson::Proc { mount_point: non_root_utf8_path_buf!("/proc") },
                        JobMountForTomlAndJson::Bind {
                            mount_point: non_root_utf8_path_buf!("/bind"),
                            local_path: utf8_path_buf!("/local"),
                            read_only: true,
                        },
                        JobMountForTomlAndJson::Devices {
                            devices: enum_set!(
                                JobDeviceForTomlAndJson::Null | JobDeviceForTomlAndJson::Zero
                            ),
                        },
                    ],
                    ..Default::default()
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn mounts_before_added_mounts() {
        assert_eq!(
            parse_test_directive(indoc! {r#"
                mounts = [ { type = "proc", mount_point = "/proc" } ]
                added_mounts = [ { type = "tmp", mount_point = "/tmp" } ]
            "#})
            .unwrap(),
            TestDirective {
                container: TestContainer {
                    mounts: Some(vec![JobMountForTomlAndJson::Proc {
                        mount_point: non_root_utf8_path_buf!("/proc"),
                    }]),
                    added_mounts: vec![JobMountForTomlAndJson::Tmp {
                        mount_point: non_root_utf8_path_buf!("/tmp"),
                    }],
                    ..Default::default()
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn mounts_after_added_mounts() {
        assert_toml_error(
            parse_test_directive(indoc! {r#"
                added_mounts = [ { type = "tmp", mount_point = "/tmp" } ]
                mounts = [ { type = "proc", mount_point = "/proc" } ]
            "#})
            .unwrap_err(),
            "field `mounts` cannot be set after `added_mounts`",
        );
    }

    #[test]
    fn unknown_field_in_simple_mount() {
        assert_toml_error(
            parse_test_directive(indoc! {r#"
                mounts = [ { type = "proc", mount_point = "/proc", unknown = "true" } ]
            "#})
            .unwrap_err(),
            "unknown field `unknown`, expected",
        );
    }

    #[test]
    fn unknown_field_in_bind_mount() {
        assert_toml_error(
            parse_test_directive(indoc! {r#"
                mounts = [ { type = "bind", mount_point = "/bind", local_path = "/a", unknown = "true" } ]
            "#})
            .unwrap_err(),
            "unknown field `unknown`, expected",
        );
    }

    #[test]
    fn missing_field_in_simple_mount() {
        assert_toml_error(
            parse_test_directive(indoc! {r#"
                mounts = [ { type = "proc" } ]
            "#})
            .unwrap_err(),
            "missing field `mount_point`",
        );
    }

    #[test]
    fn missing_field_in_bind_mount() {
        assert_toml_error(
            parse_test_directive(indoc! {r#"
                mounts = [ { type = "bind", mount_point = "/bind" } ]
            "#})
            .unwrap_err(),
            "missing field `local_path`",
        );
    }

    #[test]
    fn missing_flags_field_in_bind_mount_is_okay() {
        assert_eq!(
            parse_test_directive(indoc! {r#"
                mounts = [ { type = "bind", mount_point = "/bind", local_path = "/a" } ]
            "#})
            .unwrap(),
            TestDirective {
                container: TestContainer {
                    mounts: Some(vec![JobMountForTomlAndJson::Bind {
                        mount_point: non_root_utf8_path_buf!("/bind"),
                        local_path: utf8_path_buf!("/a"),
                        read_only: false,
                    }]),
                    ..Default::default()
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn mount_point_of_root_is_disallowed() {
        let mounts = [
            r#"{ type = "bind", mount_point = "/", local_path = "/a" }"#,
            r#"{ type = "proc", mount_point = "/" }"#,
            r#"{ type = "tmp", mount_point = "/" }"#,
            r#"{ type = "sys", mount_point = "/" }"#,
        ];
        for mount in mounts {
            assert!(parse_test_directive(&format!("mounts = [ {mount} ]"))
                .unwrap_err()
                .to_string()
                .contains("a path of \"/\" not allowed"));
        }
    }

    #[test]
    fn unknown_devices_type() {
        assert_toml_error(
            parse_test_directive(
                r#"
                mounts = [{ type = "devices",  devices = ["unknown"] }]
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
                container: TestContainer {
                    working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                    ..Default::default()
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn image_with_no_use() {
        assert_eq!(
            parse_test_directive(
                r#"
                image = { name = "rust" }
                "#
            )
            .unwrap(),
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    layers: Some(PossiblyImage::Image),
                    environment: Some(PossiblyImage::Image),
                    ..Default::default()
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn image_as_string() {
        assert_eq!(
            parse_test_directive(
                r#"
                image = "rust"
                "#
            )
            .unwrap(),
            TestDirective {
                container: TestContainer {
                    image: Some(string!("rust")),
                    layers: Some(PossiblyImage::Image),
                    environment: Some(PossiblyImage::Image),
                    ..Default::default()
                },
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
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    layers: Some(PossiblyImage::Image),
                    ..Default::default()
                },
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
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                    layers: Some(PossiblyImage::Image),
                    ..Default::default()
                },
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
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Explicit("/foo".into())),
                    layers: Some(PossiblyImage::Image),
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![glob_layer!("foo*.bin")])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![glob_layer!(
                        "foo*.bin",
                        strip_prefix = "a"
                    )])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![glob_layer!(
                        "foo*.bin",
                        prepend_prefix = "b"
                    )])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![glob_layer!(
                        "foo*.bin",
                        canonicalize = true
                    )])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![paths_layer!([
                        "foo.bin", "bar.bin"
                    ])])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![paths_layer!(
                        ["foo.bin", "bar.bin"],
                        strip_prefix = "a"
                    )])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![paths_layer!(
                        ["foo.bin", "bar.bin"],
                        prepend_prefix = "a"
                    )])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![paths_layer!(
                        ["foo.bin", "bar.bin"],
                        canonicalize = true
                    )])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![LayerSpec::Stubs {
                        stubs: vec!["/foo/bar".into(), "/bin/{baz,qux}/".into()]
                    }])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![LayerSpec::Symlinks {
                        symlinks: vec![SymlinkSpec {
                            link: "/hi".into(),
                            target: "/there".into()
                        }],
                    }])),
                    ..Default::default()
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn layers_shared_library_dependencies() {
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [
                    { shared-library-dependencies = ["/bin/bash", "/bin/sh"] }
                ]
                "#
            )
            .unwrap(),
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![so_deps_layer!([
                        "/bin/bash",
                        "/bin/sh"
                    ])])),
                    ..Default::default()
                },
                ..Default::default()
            }
        );
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [
                    { shared-library-dependencies = ["/bin/bash", "/bin/sh"], prepend_prefix = "/usr" }
                ]
                "#
            )
            .unwrap(),
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![so_deps_layer!(
                        ["/bin/bash", "/bin/sh"],
                        prepend_prefix = "/usr"
                    )])),
                    ..Default::default()
                },
                ..Default::default()
            }
        );
        assert_eq!(
            parse_test_directive(
                r#"
                layers = [
                    { shared-library-dependencies = ["/bin/bash", "/bin/sh"], canonicalize = true }
                ]
                "#
            )
            .unwrap(),
            TestDirective {
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![so_deps_layer!(
                        ["/bin/bash", "/bin/sh"],
                        canonicalize = true
                    )])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    layers: Some(PossiblyImage::Image),
                    ..Default::default()
                },
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
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                    ..Default::default()
                },
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
                container: TestContainer {
                    added_layers: vec![tar_layer!("foo.tar")],
                    ..Default::default()
                },
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
                container: TestContainer {
                    layers: Some(PossiblyImage::Explicit(vec![tar_layer!("foo.tar")])),
                    added_layers: vec![tar_layer!("bar.tar")],
                    ..Default::default()
                },
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
                container: TestContainer {
                    image: Some(string!("rust")),
                    layers: Some(PossiblyImage::Image),
                    added_layers: vec![tar_layer!("foo.tar")],
                    ..Default::default()
                },
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
                container: TestContainer {
                    environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                        string!("FOO"),
                        string!("foo")
                    )]))),
                    ..Default::default()
                },
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
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    environment: Some(PossiblyImage::Image),
                    ..Default::default()
                },
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
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                        string!("FOO"),
                        string!("foo")
                    )]))),
                    ..Default::default()
                },
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
                container: TestContainer {
                    image: Some(string!("rust")),
                    working_directory: Some(PossiblyImage::Image),
                    environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                        string!("FOO"),
                        string!("foo")
                    )]))),
                    ..Default::default()
                },
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
                container: TestContainer {
                    added_environment: BTreeMap::from([(string!("BAR"), string!("bar"))]),
                    ..Default::default()
                },
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
                container: TestContainer {
                    environment: Some(PossiblyImage::Explicit(BTreeMap::from([(
                        string!("FOO"),
                        string!("foo")
                    )]))),
                    added_environment: BTreeMap::from([(string!("BAR"), string!("bar"))]),
                    ..Default::default()
                },
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
                container: TestContainer {
                    image: Some(string!("rust")),
                    environment: Some(PossiblyImage::Image),
                    added_environment: BTreeMap::from([(string!("BAR"), string!("bar"))]),
                    ..Default::default()
                },
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
