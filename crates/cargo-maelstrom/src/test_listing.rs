use crate::pattern;
use anyhow::{anyhow, Result};
use cargo_metadata::{Artifact as CargoArtifact, Package as CargoPackage, Target as CargoTarget};
use maelstrom_client::StateDir;
use maelstrom_util::{
    fs::Fs,
    root::{Root, RootBuf},
};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_with::{serde_as, FromInto};
use std::collections::{BTreeMap, HashMap, HashSet};

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

fn filter_case(
    package: &str,
    artifact: &ArtifactKey,
    case: &str,
    filter: &pattern::Pattern,
) -> bool {
    let c = pattern::Context {
        package: package.into(),
        artifact: Some(pattern::Artifact {
            name: artifact.name.clone(),
            kind: artifact.kind,
        }),
        case: Some(pattern::Case { name: case.into() }),
    };
    pattern::interpret_pattern(filter, &c).expect("case is provided")
}

impl TestListing {
    pub fn add_cases(&mut self, package_name: &str, artifact: &CargoArtifact, cases: &[String]) {
        let artifact_key = ArtifactKey::from_target(&artifact.target);
        let package = self.packages.entry(package_name.into()).or_default();
        package.artifacts.insert(
            artifact_key,
            ArtifactCases {
                cases: cases.to_vec(),
            },
        );
    }

    pub fn expected_job_count(&self, filter: &pattern::Pattern) -> u64 {
        self.packages
            .iter()
            .flat_map(|(p, a)| {
                a.artifacts
                    .iter()
                    .flat_map(move |(a, c)| c.cases.iter().map(move |c| (p, a, c)))
            })
            .filter(|(p, a, c)| filter_case(p, a, c, filter))
            .count() as u64
    }

    pub fn retain_packages(&mut self, existing_packages_slice: &[&CargoPackage]) {
        let existing_packages: HashMap<&String, &CargoPackage> = existing_packages_slice
            .iter()
            .map(|p| (&p.name, *p))
            .collect();
        self.packages.retain(|name, pkg| {
            let Some(existing_package) = existing_packages.get(&name) else {
                return false;
            };
            let existing_artifacts: HashSet<_> = existing_package
                .targets
                .iter()
                .map(ArtifactKey::from_target)
                .collect();
            pkg.artifacts
                .retain(|key, _| existing_artifacts.contains(key));
            true
        });
    }
}

pub struct TestListingFile;

pub fn test_listing_file(state_dir: impl AsRef<Root<StateDir>>) -> RootBuf<TestListingFile> {
    state_dir.as_ref().join("test-listing.toml")
}

pub fn load_test_listing(fs: &Fs, path: &Root<TestListingFile>) -> Result<TestListing> {
    if let Some(contents) = fs.read_to_string_if_exists(path)? {
        let mut table: toml::Table = toml::from_str(&contents)?;
        let version: TestListingVersion = table
            .remove("version")
            .ok_or(anyhow!("missing version"))?
            .try_into()?;
        if version != TestListingVersion::default() {
            Ok(Default::default())
        } else {
            Ok(toml::from_str(&contents)?)
        }
    } else {
        Ok(Default::default())
    }
}

pub fn write_test_listing(
    fs: &Fs,
    path: &Root<TestListingFile>,
    job_listing: &TestListing,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs.create_dir_all(parent)?;
    }
    fs.write(path, toml::to_string_pretty(job_listing)?)?;
    Ok(())
}
