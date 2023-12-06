use crate::substitute;
use anyhow::{Context as _, Error, Result};
use meticulous_base::{EnumSet, JobDevice, JobDeviceListDeserialize, JobMount};
use meticulous_util::fs::Fs;
use serde::{Deserialize, Deserializer};
use std::{collections::BTreeMap, path::Path, str};

fn deserialize_devices<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<EnumSet<JobDevice>>, D::Error>
where
    D: Deserializer<'de>,
{
    let devices = Option::<EnumSet<JobDeviceListDeserialize>>::deserialize(deserializer)?;
    Ok(devices.map(|d| d.iter().map(JobDevice::from).collect()))
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestDirective {
    tests: Option<String>,
    package: Option<String>,
    include_shared_libraries: Option<bool>,
    enable_loopback: Option<bool>,
    layers: Option<Vec<String>>,
    mounts: Option<Vec<JobMount>>,
    #[serde(default, deserialize_with = "deserialize_devices")]
    devices: Option<EnumSet<JobDevice>>,
    environment: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AllMetadata {
    #[serde(default)]
    directives: Vec<TestDirective>,
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct TestMetadata {
    include_shared_libraries: Option<bool>,
    pub enable_loopback: bool,
    pub layers: Vec<String>,
    pub mounts: Vec<JobMount>,
    pub devices: EnumSet<JobDevice>,
    environment: BTreeMap<String, String>,
}

impl TestMetadata {
    /// Return whether to include a layer of shared library dependencies.
    ///
    /// The logic here is that if they explicitly set the value to something, we should return
    /// that. Otherwise, we should see if they set any layers. If they explicitly added layers,
    /// they probably don't want us pushing shared libraries on those layers.
    pub fn include_shared_libraries(&self) -> bool {
        match self.include_shared_libraries {
            Some(val) => val,
            None => self.layers.is_empty(),
        }
    }

    pub fn environment(&self) -> Vec<String> {
        self.environment
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect()
    }

    fn fold(mut self, directive: &TestDirective) -> Result<Self> {
        if directive.include_shared_libraries.is_some() {
            self.include_shared_libraries = directive.include_shared_libraries;
        }
        if let Some(enable_loopback) = directive.enable_loopback {
            self.enable_loopback = enable_loopback
        }
        if directive
            .layers
            .as_ref()
            .map(Vec::is_empty)
            .unwrap_or(false)
        {
            self.layers.clear();
        } else {
            self.layers
                .extend(directive.layers.iter().flatten().cloned());
        }
        if directive
            .mounts
            .as_ref()
            .map(Vec::is_empty)
            .unwrap_or(false)
        {
            self.mounts.clear();
        } else {
            self.mounts
                .extend(directive.mounts.iter().flatten().cloned());
        }
        if directive
            .devices
            .as_ref()
            .map(EnumSet::is_empty)
            .unwrap_or(false)
        {
            self.devices.clear();
        } else {
            self.devices = self.devices.union(directive.devices.unwrap_or_default());
        }
        if directive
            .environment
            .as_ref()
            .map(BTreeMap::is_empty)
            .unwrap_or(false)
        {
            self.environment.clear();
        } else {
            let to_insert = directive
                .environment
                .iter()
                .flatten()
                .map(|(k, v)| {
                    substitute::substitute(
                        v,
                        |_| Ok(None),
                        |var| self.environment.get(var).map(String::as_str),
                    )
                    .map(|v| (k.clone(), String::from(v)))
                    .map_err(Error::new)
                })
                .collect::<Result<Vec<(String, String)>>>()?;
            self.environment.extend(to_insert);
        }
        Ok(self)
    }
}

impl AllMetadata {
    pub fn get_metadata_for_test(&self, package: &str, test: &str) -> Result<TestMetadata> {
        self.directives
            .iter()
            .filter(|directive| match &directive.tests {
                Some(directive_tests) => test.contains(directive_tests.as_str()),
                None => true,
            })
            .filter(|directive| match &directive.package {
                Some(directive_package) => package == directive_package,
                None => true,
            })
            .try_fold(TestMetadata::default(), TestMetadata::fold)
    }

    fn from_str(contents: &str) -> Result<Self> {
        Ok(toml::from_str(contents)?)
    }

    pub fn load(workspace_root: &impl AsRef<Path>) -> Result<Self> {
        let path = workspace_root.as_ref().join("meticulous-test.toml");

        Ok(Fs::new()
            .read_to_string_if_exists(&path)?
            .map(|c| Self::from_str(&c).with_context(|| format!("parsing {}", path.display())))
            .transpose()?
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use meticulous_base::{enum_set, JobMountFsType};
    use toml::de::Error as TomlError;

    #[test]
    fn default() {
        assert_eq!(
            AllMetadata { directives: vec![] }
                .get_metadata_for_test("mod", "test")
                .unwrap(),
            TestMetadata::default(),
        );
    }

    #[test]
    fn enable_loopback() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            package = "package1"
            enable_loopback = true

            [[directives]]
            package = "package1"
            tests = "test1"
            enable_loopback = false
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test("package1", "test1")
                .unwrap()
                .enable_loopback,
            false
        );
        assert_eq!(
            all.get_metadata_for_test("package1", "test2")
                .unwrap()
                .enable_loopback,
            true
        );
        assert_eq!(
            all.get_metadata_for_test("package2", "test1")
                .unwrap()
                .enable_loopback,
            false
        );
    }

    #[test]
    fn include_shared_libraries_defaults() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            package = "package1"
            layers = ["layer1"]

            [[directives]]
            package = "package1"
            tests = "test1"
            layers = []
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test("package1", "test1")
                .unwrap()
                .include_shared_libraries(),
            true
        );
        assert_eq!(
            all.get_metadata_for_test("package1", "test2")
                .unwrap()
                .include_shared_libraries(),
            false
        );
        assert_eq!(
            all.get_metadata_for_test("package2", "test1")
                .unwrap()
                .include_shared_libraries(),
            true
        );
    }

    #[test]
    fn include_shared_libraries_can_be_set() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            include_shared_libraries = false

            [[directives]]
            package = "package1"
            include_shared_libraries = true
            layers = ["layer1"]

            [[directives]]
            package = "package1"
            tests = "test1"
            layers = []
            "#,
        )
        .unwrap();
        assert_eq!(
            all.get_metadata_for_test("package1", "test1")
                .unwrap()
                .include_shared_libraries(),
            true
        );
        assert_eq!(
            all.get_metadata_for_test("package1", "test2")
                .unwrap()
                .include_shared_libraries(),
            true
        );
        assert_eq!(
            all.get_metadata_for_test("package2", "test1")
                .unwrap()
                .include_shared_libraries(),
            false
        );
    }

    #[test]
    fn layers_are_appended_in_order_ignoring_unset_directives() {
        assert_eq!(
            AllMetadata::from_str(
                r#"
                [[directives]]
                layers = ["layer1", "layer2"]

                [[directives]]
                enable_loopback = true

                [[directives]]
                layers = ["layer3", "layer4"]
                "#
            )
            .unwrap()
            .get_metadata_for_test("mod", "test")
            .unwrap(),
            TestMetadata {
                enable_loopback: true,
                layers: vec![
                    "layer1".to_string(),
                    "layer2".to_string(),
                    "layer3".to_string(),
                    "layer4".to_string()
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn layers_are_reset_with_empty_vec() {
        assert_eq!(
            AllMetadata::from_str(
                r#"
                [[directives]]
                layers = ["layer1", "layer2"]

                [[directives]]
                layers = []

                [[directives]]
                layers = ["layer3", "layer4"]
                "#
            )
            .unwrap()
            .get_metadata_for_test("mod", "test")
            .unwrap(),
            TestMetadata {
                layers: vec!["layer3".to_string(), "layer4".to_string()],
                ..Default::default()
            }
        );
    }

    #[test]
    fn mounts_are_appended_in_order_ignoring_unset_directives() {
        assert_eq!(
            AllMetadata::from_str(
                r#"
                [[directives]]
                mounts = [ { fs_type = "proc", mount_point = "/proc" } ]

                [[directives]]
                enable_loopback = true

                [[directives]]
                mounts = [ { fs_type = "tmp", mount_point = "/tmp" } ]
                "#
            )
            .unwrap()
            .get_metadata_for_test("mod", "test")
            .unwrap(),
            TestMetadata {
                enable_loopback: true,
                mounts: vec![
                    JobMount {
                        fs_type: JobMountFsType::Proc,
                        mount_point: "/proc".to_string()
                    },
                    JobMount {
                        fs_type: JobMountFsType::Tmp,
                        mount_point: "/tmp".to_string()
                    },
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn mounts_are_reset_with_empty_vec() {
        assert_eq!(
            AllMetadata::from_str(
                r#"
                [[directives]]
                mounts = [ { fs_type = "proc", mount_point = "/proc" } ]

                [[directives]]
                mounts = []

                [[directives]]
                mounts = [ { fs_type = "tmp", mount_point = "/tmp" } ]
                "#
            )
            .unwrap()
            .get_metadata_for_test("mod", "test")
            .unwrap(),
            TestMetadata {
                mounts: vec![JobMount {
                    fs_type: JobMountFsType::Tmp,
                    mount_point: "/tmp".to_string()
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn devices_are_unioned_ignoring_unset_directives() {
        assert_eq!(
            AllMetadata::from_str(
                r#"
                [[directives]]
                devices = [ "full" ]

                [[directives]]
                enable_loopback = true

                [[directives]]
                devices = [ "null" ]

                [[directives]]
                devices = [ "null", "zero" ]
                "#
            )
            .unwrap()
            .get_metadata_for_test("mod", "test")
            .unwrap(),
            TestMetadata {
                enable_loopback: true,
                devices: enum_set! {
                    JobDevice::Full | JobDevice::Null | JobDevice::Zero
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn devices_are_reset_with_empty_set() {
        assert_eq!(
            AllMetadata::from_str(
                r#"
                [[directives]]
                devices = [ "full" ]

                [[directives]]
                devices = [ "null", "zero" ]

                [[directives]]
                devices = []

                [[directives]]
                devices = [ "null" ]
                "#
            )
            .unwrap()
            .get_metadata_for_test("mod", "test")
            .unwrap(),
            TestMetadata {
                devices: enum_set! {
                    JobDevice::Null
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn environment() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            package = "package1"
            environment = { FOO = "foo", BAR = "bar" }

            [[directives]]
            package = "package1"

            [[directives]]
            package = "package1"
            tests = "test1"
            environment = { BAR = "baz", FROB = "frob" }

            [[directives]]
            package = "package1"
            tests = "test3"
            environment = {}

            [[directives]]
            package = "package1"
            tests = "test31"
            environment = { A = "a" }
            "#,
        )
        .unwrap();

        assert_eq!(
            all.get_metadata_for_test("package1", "test1")
                .unwrap()
                .environment(),
            vec![
                "BAR=baz".to_string(),
                "FOO=foo".to_string(),
                "FROB=frob".to_string(),
            ],
        );
        assert_eq!(
            all.get_metadata_for_test("package1", "test2")
                .unwrap()
                .environment(),
            vec!["BAR=bar".to_string(), "FOO=foo".to_string(),],
        );
        assert_eq!(
            all.get_metadata_for_test("package1", "test3")
                .unwrap()
                .environment(),
            Vec::<String>::default(),
        );
        assert_eq!(
            all.get_metadata_for_test("package1", "test31")
                .unwrap()
                .environment(),
            vec!["A=a".to_string()],
        );
        assert_eq!(
            all.get_metadata_for_test("package2", "test1")
                .unwrap()
                .environment(),
            Vec::<String>::default(),
        );
    }

    #[test]
    fn environment_substitution() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            environment = { FOO = "foo", BAR = "bar" }

            [[directives]]
            environment = { FOO = "y$prev{BAR}y", BAR = "x$prev{FOO}x" }
            "#,
        )
        .unwrap();

        assert_eq!(
            all.get_metadata_for_test("package1", "test1")
                .unwrap()
                .environment(),
            vec!["BAR=xfoox".to_string(), "FOO=ybary".to_string(),],
        );
    }

    #[test]
    fn environment_substitution_error() {
        let all = AllMetadata::from_str(
            r#"
            [[directives]]
            environment = { FOO = "y$prev{BAR}y", BAR = "x$prev{FOO}x" }
            "#,
        )
        .unwrap();
        let err = all.get_metadata_for_test("package1", "test1").unwrap_err();
        assert_eq!(format!("{err}"), "unknown variable \"FOO\"");
    }

    #[test]
    fn bad_field_in_all_metadata() {
        let err = AllMetadata::from_str(
            r#"
            [not_a_field]
            foo = "three"
            "#,
        )
        .unwrap_err();
        let err = err.downcast_ref::<TomlError>().unwrap();
        let message = err.message();
        assert!(
            message.starts_with("unknown field `not_a_field`, expected `directives`"),
            "message: {message}"
        );
    }

    #[test]
    fn bad_field_in_test_directive() {
        let err = AllMetadata::from_str(
            r#"
            [[directives]]
            not_a_field = "three"
            "#,
        )
        .unwrap_err();
        let err = err.downcast_ref::<TomlError>().unwrap();
        let message = err.message();
        assert!(
            message.starts_with("unknown field `not_a_field`, expected"),
            "message: {message}"
        );
    }

    #[test]
    fn bad_field_in_job_mount() {
        let err = AllMetadata::from_str(
            r#"
            [[directives]]
            mounts = [ { not_a_field = "foo" } ]
            "#,
        )
        .unwrap_err();
        let err = err.downcast_ref::<TomlError>().unwrap();
        let message = err.message();
        assert!(
            message.starts_with("unknown field `not_a_field`, expected"),
            "message: {message}"
        );
    }

    #[test]
    fn bad_devices_field() {
        let err = AllMetadata::from_str(
            r#"
            [[directives]]
            devices = ["not_a_value"]
            "#,
        )
        .unwrap_err();
        let err = err.downcast_ref::<TomlError>().unwrap();
        let message = err.message();
        assert!(
            message.starts_with("unknown variant `not_a_value`, expected one of"),
            "message: {message}"
        );
    }
}
