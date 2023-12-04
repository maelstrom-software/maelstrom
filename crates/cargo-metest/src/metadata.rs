use anyhow::{Context as _, Result};
use meticulous_base::{EnumSet, JobDevice, JobDeviceListDeserialize, JobMount};
use meticulous_util::fs::Fs;
use serde::Deserialize;
use std::{iter, path::Path, str};

#[derive(Debug, Deserialize)]
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

impl AllMetadata {
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
