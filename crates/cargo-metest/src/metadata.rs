use meticulous_base::{EnumSet, JobDevice, JobDeviceListDeserialize, JobMount};
use serde::Deserialize;
use std::{iter, str};

#[derive(Debug, Deserialize)]
pub struct TestGroup {
    pub tests: Option<String>,
    pub module: Option<String>,
    #[serde(default)]
    pub include_shared_libraries: bool,
    pub devices: Option<EnumSet<JobDeviceListDeserialize>>,
    pub layers: Option<Vec<String>>,
    pub mounts: Option<Vec<JobMount>>,
    pub loopback: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
pub struct TestMetadata {
    #[serde(default)]
    pub groups: Vec<TestGroup>,
}

impl TestMetadata {
    pub fn include_shared_libraries_for_test(&self, module: &str, test: &str) -> bool {
        self.groups
            .iter()
            .filter(|group| !matches!(&group.tests, Some(group_tests) if !test.contains(group_tests.as_str())))
            .filter(|group| !matches!(&group.module, Some(group_module) if module != group_module))
            .map(|group| group.include_shared_libraries)
            .chain(iter::once(true))
            .next().unwrap()
    }

    pub fn get_layers_for_test(&self, module: &str, test: &str) -> Vec<&str> {
        self.groups
            .iter()
            .filter(|group| !matches!(&group.tests, Some(group_tests) if !test.contains(group_tests.as_str())))
            .filter(|group| !matches!(&group.module, Some(group_module) if module != group_module))
            .flat_map(|group| group.layers.iter())
            .flatten()
            .map(String::as_str)
            .collect()
    }

    pub fn get_mounts_for_test(&self, module: &str, test: &str) -> Vec<JobMount> {
        self.groups
            .iter()
            .filter(|group| !matches!(&group.tests, Some(group_tests) if !test.contains(group_tests.as_str())))
            .filter(|group| !matches!(&group.module, Some(group_module) if module != group_module))
            .flat_map(|group| group.mounts.iter())
            .flatten()
            .cloned()
            .collect()
    }

    pub fn get_devices_for_test(&self, module: &str, test: &str) -> EnumSet<JobDevice> {
        self.groups
            .iter()
            .filter(|group| !matches!(&group.tests, Some(group_tests) if !test.contains(group_tests.as_str())))
            .filter(|group| !matches!(&group.module, Some(group_module) if module != group_module))
            .flat_map(|group| group.devices)
            .flatten()
            .map(JobDevice::from)
            .collect()
    }

    pub fn get_loopback_for_test(&self, module: &str, test: &str) -> bool {
        self.groups
            .iter()
            .filter(|group| !matches!(&group.tests, Some(group_tests) if !test.contains(group_tests.as_str())))
            .filter(|group| !matches!(&group.module, Some(group_module) if module != group_module))
            .flat_map(|group| group.loopback.iter())
            .cloned()
            .next()
            .unwrap_or(false)
    }
}
