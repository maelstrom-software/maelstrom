use anyhow::{Context as _, Result};
use meticulous_base::{EnumSet, JobDevice, JobDeviceListDeserialize, JobMount};
use meticulous_util::fs::Fs;
use serde::{Deserialize, Deserializer};
use std::{path::Path, str};

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
struct TestDirective {
    tests: Option<String>,
    module: Option<String>,
    #[serde(default)]
    include_shared_libraries: bool,
    loopback_enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_devices")]
    devices: Option<EnumSet<JobDevice>>,
    layers: Option<Vec<String>>,
    mounts: Option<Vec<JobMount>>,
}

#[derive(Debug, Deserialize, Default)]
pub struct AllMetadata {
    #[serde(default)]
    directives: Vec<TestDirective>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct TestMetadata {
    pub include_shared_libraries: bool,
    pub loopback_enabled: bool,
    pub layers: Vec<String>,
    pub mounts: Vec<JobMount>,
    pub devices: EnumSet<JobDevice>,
}

impl Default for TestMetadata {
    fn default() -> Self {
        TestMetadata {
            include_shared_libraries: true,
            loopback_enabled: false,
            layers: vec![],
            mounts: vec![],
            devices: EnumSet::EMPTY,
        }
    }
}

impl TestMetadata {
    fn fold(mut self, directive: &TestDirective) -> Self {
        self.include_shared_libraries = directive.include_shared_libraries;
        if let Some(loopback) = directive.loopback_enabled {
            self.loopback_enabled = loopback
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
        self
    }
}

impl AllMetadata {
    pub fn get_metadata_for_test(&self, module: &str, test: &str) -> TestMetadata {
        self.directives
            .iter()
            .filter(|directive| match &directive.tests {
                Some(directive_tests) => test.contains(directive_tests.as_str()),
                None => true,
            })
            .filter(|directive| match &directive.module {
                Some(directive_module) => module == directive_module,
                None => true,
            })
            .fold(TestMetadata::default(), TestMetadata::fold)
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

    #[test]
    fn default() {
        assert_eq!(
            AllMetadata { directives: vec![] }.get_metadata_for_test("mod", "test"),
            TestMetadata::default(),
        );
    }

    #[test]
    fn any_directive_sets_include_shared_libraries_to_false() {
        assert_eq!(
            AllMetadata::from_str(
                r#"
                [[directives]]
                layers = ["layer1", "layer2"]
                "#
            )
            .unwrap()
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                layers: vec!["layer1".to_string(), "layer2".to_string()],
                include_shared_libraries: false,
                ..Default::default()
            }
        );
    }

    #[test]
    fn include_shared_libraries_can_be_set() {
        assert_eq!(
            AllMetadata::from_str(
                r#"
                [[directives]]
                layers = ["layer1", "layer2"]

                [[directives]]
                include_shared_libraries = true
                layers = ["layer3", "layer4"]
                "#
            )
            .unwrap()
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                layers: vec![
                    "layer1".to_string(),
                    "layer2".to_string(),
                    "layer3".to_string(),
                    "layer4".to_string()
                ],
                include_shared_libraries: true,
                ..Default::default()
            }
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
                loopback_enabled = true

                [[directives]]
                layers = ["layer3", "layer4"]
                "#
            )
            .unwrap()
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                loopback_enabled: true,
                layers: vec![
                    "layer1".to_string(),
                    "layer2".to_string(),
                    "layer3".to_string(),
                    "layer4".to_string()
                ],
                include_shared_libraries: false,
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
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                layers: vec!["layer3".to_string(), "layer4".to_string()],
                include_shared_libraries: false,
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
                loopback_enabled = true

                [[directives]]
                mounts = [ { fs_type = "tmp", mount_point = "/tmp" } ]
                "#
            )
            .unwrap()
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                loopback_enabled: true,
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
                include_shared_libraries: false,
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
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                mounts: vec![JobMount {
                    fs_type: JobMountFsType::Tmp,
                    mount_point: "/tmp".to_string()
                }],
                include_shared_libraries: false,
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
                loopback_enabled = true

                [[directives]]
                devices = [ "null" ]

                [[directives]]
                devices = [ "null", "zero" ]
                "#
            )
            .unwrap()
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                loopback_enabled: true,
                devices: enum_set! {
                    JobDevice::Full | JobDevice::Null | JobDevice::Zero
                },
                include_shared_libraries: false,
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
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                devices: enum_set! {
                    JobDevice::Null
                },
                include_shared_libraries: false,
                ..Default::default()
            }
        );
    }
}
