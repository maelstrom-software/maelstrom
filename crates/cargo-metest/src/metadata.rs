use anyhow::{Context as _, Result};
use meticulous_base::{EnumSet, JobDevice, JobDeviceListDeserialize, JobMount};
use meticulous_util::fs::Fs;
use serde::Deserialize;
use std::{path::Path, str};

#[derive(Debug, Default, Deserialize)]
struct TestGroup {
    tests: Option<String>,
    module: Option<String>,
    #[serde(default)]
    include_shared_libraries: bool,
    devices: Option<EnumSet<JobDeviceListDeserialize>>,
    layers: Option<Vec<String>>,
    mounts: Option<Vec<JobMount>>,
    loopback: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
pub struct AllMetadata {
    #[serde(default)]
    groups: Vec<TestGroup>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct TestMetadata {
    pub include_shared_libraries: bool,
    pub loopback: bool,
    pub layers: Vec<String>,
    pub mounts: Vec<JobMount>,
    pub devices: EnumSet<JobDevice>,
}

impl Default for TestMetadata {
    fn default() -> Self {
        TestMetadata {
            include_shared_libraries: true,
            loopback: false,
            layers: vec![],
            mounts: vec![],
            devices: EnumSet::EMPTY,
        }
    }
}

impl TestMetadata {
    fn fold(mut self, group: &TestGroup) -> Self {
        self.include_shared_libraries = group.include_shared_libraries;
        if let Some(loopback) = group.loopback {
            self.loopback = loopback
        }
        if group.layers.as_ref().map(Vec::is_empty).unwrap_or(false) {
            self.layers.clear();
        } else {
            self.layers.extend(group.layers.iter().flatten().cloned());
        }
        if group.mounts.as_ref().map(Vec::is_empty).unwrap_or(false) {
            self.mounts.clear();
        } else {
            self.mounts.extend(group.mounts.iter().flatten().cloned());
        }
        if group
            .devices
            .as_ref()
            .map(EnumSet::is_empty)
            .unwrap_or(false)
        {
            self.devices.clear();
        } else {
            self.devices = self.devices.union(
                group
                    .devices
                    .unwrap_or_default()
                    .iter()
                    .map(JobDevice::from)
                    .collect(),
            );
        }
        self
    }
}

impl AllMetadata {
    pub fn get_metadata_for_test(&self, module: &str, test: &str) -> TestMetadata {
        self.groups
            .iter()
            .filter(|group| match &group.tests {
                Some(group_tests) => test.contains(group_tests.as_str()),
                None => true,
            })
            .filter(|group| match &group.module {
                Some(group_module) => module == group_module,
                None => true,
            })
            .fold(TestMetadata::default(), TestMetadata::fold)
    }

    pub fn load(workspace_root: &impl AsRef<Path>) -> Result<AllMetadata> {
        let path = workspace_root.as_ref().join("metest-metadata.toml");

        Ok(Fs::new()
            .read_to_string_if_exists(&path)?
            .map(|c| -> Result<AllMetadata> {
                toml::from_str(&c).with_context(|| format!("parsing {}", path.display()))
            })
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
            AllMetadata { groups: vec![] }.get_metadata_for_test("mod", "test"),
            TestMetadata::default(),
        );
    }

    #[test]
    fn any_group_sets_include_shared_libraries_to_false() {
        assert_eq!(
            AllMetadata {
                groups: vec![TestGroup {
                    layers: Some(vec!["layer1".to_string(), "layer2".to_string()]),
                    ..Default::default()
                }]
            }
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
            AllMetadata {
                groups: vec![
                    TestGroup {
                        layers: Some(vec!["layer1".to_string(), "layer2".to_string()]),
                        ..Default::default()
                    },
                    TestGroup {
                        layers: Some(vec!["layer3".to_string(), "layer4".to_string()]),
                        include_shared_libraries: true,
                        ..Default::default()
                    },
                ]
            }
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
    fn layers_are_appended_in_order_ignoring_unset_groups() {
        assert_eq!(
            AllMetadata {
                groups: vec![
                    TestGroup {
                        layers: Some(vec!["layer1".to_string(), "layer2".to_string()]),
                        ..Default::default()
                    },
                    TestGroup {
                        loopback: Some(true),
                        ..Default::default()
                    },
                    TestGroup {
                        layers: Some(vec!["layer3".to_string(), "layer4".to_string()]),
                        ..Default::default()
                    },
                ]
            }
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                loopback: true,
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
            AllMetadata {
                groups: vec![
                    TestGroup {
                        layers: Some(vec!["layer1".to_string(), "layer2".to_string()]),
                        ..Default::default()
                    },
                    TestGroup {
                        layers: Some(vec![]),
                        ..Default::default()
                    },
                    TestGroup {
                        layers: Some(vec!["layer3".to_string(), "layer4".to_string()]),
                        ..Default::default()
                    },
                ]
            }
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                layers: vec!["layer3".to_string(), "layer4".to_string()],
                include_shared_libraries: false,
                ..Default::default()
            }
        );
    }

    #[test]
    fn mounts_are_appended_in_order_ignoring_unset_groups() {
        assert_eq!(
            AllMetadata {
                groups: vec![
                    TestGroup {
                        mounts: Some(vec![JobMount {
                            fs_type: JobMountFsType::Proc,
                            mount_point: "/proc".to_string()
                        }]),
                        ..Default::default()
                    },
                    TestGroup {
                        loopback: Some(true),
                        ..Default::default()
                    },
                    TestGroup {
                        mounts: Some(vec![JobMount {
                            fs_type: JobMountFsType::Tmp,
                            mount_point: "/tmp".to_string()
                        }]),
                        ..Default::default()
                    },
                ]
            }
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                loopback: true,
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
            AllMetadata {
                groups: vec![
                    TestGroup {
                        mounts: Some(vec![JobMount {
                            fs_type: JobMountFsType::Proc,
                            mount_point: "/proc".to_string()
                        }]),
                        ..Default::default()
                    },
                    TestGroup {
                        mounts: Some(vec![]),
                        ..Default::default()
                    },
                    TestGroup {
                        mounts: Some(vec![JobMount {
                            fs_type: JobMountFsType::Tmp,
                            mount_point: "/tmp".to_string()
                        }]),
                        ..Default::default()
                    },
                ]
            }
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
    fn devices_are_unioned_ignoring_unset_groups() {
        assert_eq!(
            AllMetadata {
                groups: vec![
                    TestGroup {
                        devices: Some(enum_set! {
                            JobDeviceListDeserialize::Full
                        }),
                        ..Default::default()
                    },
                    TestGroup {
                        loopback: Some(true),
                        ..Default::default()
                    },
                    TestGroup {
                        devices: Some(enum_set! {
                            JobDeviceListDeserialize::Null
                        }),
                        ..Default::default()
                    },
                    TestGroup {
                        devices: Some(enum_set! {
                            JobDeviceListDeserialize::Null | JobDeviceListDeserialize::Zero
                        }),
                        ..Default::default()
                    },
                ],
            }
            .get_metadata_for_test("mod", "test"),
            TestMetadata {
                loopback: true,
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
            AllMetadata {
                groups: vec![
                    TestGroup {
                        devices: Some(enum_set! {
                            JobDeviceListDeserialize::Full
                        }),
                        ..Default::default()
                    },
                    TestGroup {
                        devices: Some(enum_set! {
                            JobDeviceListDeserialize::Null | JobDeviceListDeserialize::Zero
                        }),
                        ..Default::default()
                    },
                    TestGroup {
                        devices: Some(enum_set! {}),
                        ..Default::default()
                    },
                    TestGroup {
                        devices: Some(enum_set! {
                            JobDeviceListDeserialize::Null
                        }),
                        ..Default::default()
                    },
                ],
            }
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
