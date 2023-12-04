use anyhow::{Context as _, Result};
use meticulous_base::{EnumSet, JobDevice, JobDeviceListDeserialize, JobMount};
use meticulous_util::fs::Fs;
use serde::Deserialize;
use std::{iter, path::Path, str};

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

    pub fn load(workspace_root: &Path) -> Result<TestMetadata> {
        let path = workspace_root.join("metest-metadata.toml");

        Ok(Fs::new()
            .read_to_string_if_exists(&path)?
            .map(|c| -> Result<TestMetadata> {
                toml::from_str(&c).with_context(|| format!("parsing {}", path.display()))
            })
            .transpose()?
            .unwrap_or_default())
    }
}
