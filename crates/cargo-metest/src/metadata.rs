use anyhow::{Context as _, Result};
use meticulous_base::{EnumSet, JobDevice, JobDeviceListDeserialize, JobMount};
use meticulous_util::fs::Fs;
use serde::Deserialize;
use std::{path::Path, str};

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
        if let Some(devices) = group.devices {
            self.devices = devices.iter().map(JobDevice::from).collect();
        }
        if let Some(layers) = &group.layers {
            self.layers = layers.clone();
        }
        if let Some(mounts) = &group.mounts {
            self.mounts = mounts.clone();
        }
        self
    }
}

impl AllMetadata {
    pub fn get_metadata_for_test(&self, module: &str, test: &str) -> TestMetadata {
        self.groups.iter()
            .filter(|group| !matches!(&group.tests, Some(group_tests) if !test.contains(group_tests.as_str())))
            .filter(|group| !matches!(&group.module, Some(group_module) if module != group_module))
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
