use anyhow::{anyhow, Result};
use cargo_metadata::{Artifact as CargoArtifact, Package as CargoPackage, Target as CargoTarget};
use meticulous_util::fs::Fs;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_with::{serde_as, FromInto};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

pub use crate::pattern::ArtifactKind;

#[derive(Default, Copy, Clone, Debug, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u32)]
pub enum TestListingVersion {
    V0 = 0,
    #[default]
    V1 = 1,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ArtifactKey {
    pub name: String,
    pub kind: ArtifactKind,
}

impl ArtifactKey {
    fn from_target(target: &CargoTarget) -> Self {
        Self {
            name: target.name.clone(),
            kind: ArtifactKind::from_target(target),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ArtifactCases {
    pub cases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Artifact {
    #[serde(flatten)]
    key: ArtifactKey,
    #[serde(flatten)]
    value: ArtifactCases,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
struct ArtifactVec(Vec<Artifact>);

impl From<ArtifactVec> for BTreeMap<ArtifactKey, ArtifactCases> {
    fn from(v: ArtifactVec) -> Self {
        v.0.into_iter().map(|a| (a.key, a.value)).collect()
    }
}

impl From<BTreeMap<ArtifactKey, ArtifactCases>> for ArtifactVec {
    fn from(m: BTreeMap<ArtifactKey, ArtifactCases>) -> Self {
        Self(
            m.into_iter()
                .map(|(key, value)| Artifact { key, value })
                .collect(),
        )
    }
}

#[serde_as]
#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Package {
    #[serde_as(as = "FromInto<ArtifactVec>")]
    pub artifacts: BTreeMap<ArtifactKey, ArtifactCases>,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestListing {
    pub version: TestListingVersion,
    #[serde(flatten)]
    pub packages: BTreeMap<String, Package>,
}

impl TestListing {
    pub fn add_cases(&mut self, artifact: &CargoArtifact, cases: &[String]) {
        let package_name = artifact.package_id.repr.split(' ').next().unwrap().into();
        let artifact_key = ArtifactKey::from_target(&artifact.target);
        let package = self.packages.entry(package_name).or_default();
        package.artifacts.insert(
            artifact_key,
            ArtifactCases {
                cases: cases.to_vec(),
            },
        );
    }

    pub fn remove_package(&mut self, package: &str) {
        self.packages.remove(package);
    }

    pub fn expected_job_count(&self, package_f: &Option<String>, filter: &Option<String>) -> u64 {
        self.packages
            .iter()
            .filter_map(|(p, package)| {
                (package_f.is_none() || package_f.as_ref().is_some_and(|exp_p| exp_p == p))
                    .then_some(package.artifacts.iter().flat_map(|(_, a)| a.cases.iter()))
            })
            .flatten()
            .filter(|c| filter.is_none() || filter.as_ref().is_some_and(|exp_c| c.contains(exp_c)))
            .count() as u64
    }

    pub fn retain_packages(&mut self, existing_packages_slice: &[&CargoPackage]) {
        let existing_packages: HashMap<&String, &CargoPackage> = existing_packages_slice
            .iter()
            .map(|p| (&p.name, *p))
            .collect();
        let mut new_packages = BTreeMap::new();
        for (name, mut pkg) in std::mem::take(&mut self.packages).into_iter() {
            let Some(existing_package) = existing_packages.get(&name) else {
                continue;
            };
            let artifact_keys: HashSet<_> = existing_package
                .targets
                .iter()
                .map(ArtifactKey::from_target)
                .collect();
            pkg.artifacts.retain(|key, _| artifact_keys.contains(key));
            new_packages.insert(name, pkg);
        }

        self.packages = new_packages;
    }
}

pub const LAST_TEST_LISTING_NAME: &str = "meticulous-test-listing.toml";

pub fn load_test_listing(path: &Path) -> Result<Option<TestListing>> {
    let fs = Fs::new();
    if let Some(contents) = fs.read_to_string_if_exists(path)? {
        let mut table: toml::Table = toml::from_str(&contents)?;
        let version: TestListingVersion = table
            .remove("version")
            .ok_or(anyhow!("missing version"))?
            .try_into()?;
        if version != TestListingVersion::default() {
            Ok(None)
        } else {
            Ok(toml::from_str(&contents)?)
        }
    } else {
        Ok(None)
    }
}

pub fn write_test_listing(path: &Path, job_listing: &TestListing) -> Result<()> {
    let fs = Fs::new();
    fs.write(path, toml::to_string_pretty(job_listing)?)?;
    Ok(())
}
