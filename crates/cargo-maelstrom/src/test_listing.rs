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

/*  _
 * (_)_ __    _ __ ___   ___ _ __ ___   ___  _ __ _   _
 * | | '_ \  | '_ ` _ \ / _ \ '_ ` _ \ / _ \| '__| | | |
 * | | | | | | | | | | |  __/ | | | | | (_) | |  | |_| |
 * |_|_| |_| |_| |_| |_|\___|_| |_| |_|\___/|_|   \__, |
 *                                                |___/
 *  FIGLET: in memory
 */

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Artifact {
    pub cases: HashSet<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Package {
    pub artifacts: HashMap<ArtifactKey, Artifact>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TestListing {
    pub packages: HashMap<String, Package>,
}

impl TestListing {
    pub fn add_cases(&mut self, package_name: &str, artifact: &CargoArtifact, cases: &[String]) {
        let artifact_key = ArtifactKey::from_target(&artifact.target);
        let package = self.packages.entry(package_name.into()).or_default();
        package.artifacts.insert(
            artifact_key,
            Artifact {
                cases: cases.iter().cloned().collect(),
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

/*                    _ _     _
 *   ___  _ __     __| (_)___| | __
 *  / _ \| '_ \   / _` | / __| |/ /
 * | (_) | | | | | (_| | \__ \   <
 *  \___/|_| |_|  \__,_|_|___/_|\_\
 *  FIGLET: on disk
 */

#[derive(Default, Eq, PartialEq, Serialize_repr, Deserialize_repr)]
#[repr(u32)]
enum OnDiskTestListingVersion {
    V0 = 0,
    #[default]
    V1 = 1,
}

#[derive(Clone, Serialize, Deserialize)]
struct OnDiskArtifactCases {
    cases: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct OnDiskArtifact {
    #[serde(flatten)]
    key: ArtifactKey,
    #[serde(flatten)]
    value: OnDiskArtifactCases,
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct OnDiskArtifactVec(Vec<OnDiskArtifact>);

impl From<OnDiskArtifactVec> for BTreeMap<ArtifactKey, OnDiskArtifactCases> {
    fn from(v: OnDiskArtifactVec) -> Self {
        v.0.into_iter().map(|a| (a.key, a.value)).collect()
    }
}

impl From<BTreeMap<ArtifactKey, OnDiskArtifactCases>> for OnDiskArtifactVec {
    fn from(m: BTreeMap<ArtifactKey, OnDiskArtifactCases>) -> Self {
        Self(
            m.into_iter()
                .map(|(key, value)| OnDiskArtifact { key, value })
                .collect(),
        )
    }
}

#[serde_as]
#[derive(Serialize, Deserialize)]
struct OnDiskPackage {
    #[serde_as(as = "FromInto<OnDiskArtifactVec>")]
    artifacts: BTreeMap<ArtifactKey, OnDiskArtifactCases>,
}

#[derive(Serialize, Deserialize)]
struct OnDiskTestListing {
    version: OnDiskTestListingVersion,
    #[serde(flatten)]
    packages: BTreeMap<String, OnDiskPackage>,
}

impl From<TestListing> for OnDiskTestListing {
    fn from(in_memory: TestListing) -> Self {
        Self {
            version: OnDiskTestListingVersion::default(),
            packages: in_memory
                .packages
                .into_iter()
                .map(|(package_name, package)| {
                    (
                        package_name,
                        OnDiskPackage {
                            artifacts: package
                                .artifacts
                                .into_iter()
                                .map(|(artifact_key, artifact)| {
                                    (
                                        artifact_key,
                                        OnDiskArtifactCases {
                                            cases: artifact.cases.into_iter().collect(),
                                        },
                                    )
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<OnDiskTestListing> for TestListing {
    fn from(on_disk: OnDiskTestListing) -> Self {
        Self {
            packages: on_disk
                .packages
                .into_iter()
                .map(|(package_name, package)| {
                    (
                        package_name,
                        Package {
                            artifacts: package
                                .artifacts
                                .into_iter()
                                .map(|(artifact_key, artifact)| {
                                    (
                                        artifact_key,
                                        Artifact {
                                            cases: artifact.cases.into_iter().collect(),
                                        },
                                    )
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
        }
    }
}

struct TestListingFile;

fn test_listing_file(state_dir: impl AsRef<Root<StateDir>>) -> RootBuf<TestListingFile> {
    state_dir.as_ref().join("test-listing.toml")
}

pub fn load_test_listing(fs: &Fs, state_dir: impl AsRef<Root<StateDir>>) -> Result<TestListing> {
    let path = test_listing_file(state_dir);
    if let Some(contents) = fs.read_to_string_if_exists(path)? {
        let mut table: toml::Table = toml::from_str(&contents)?;
        let version: OnDiskTestListingVersion = table
            .remove("version")
            .ok_or(anyhow!("missing version"))?
            .try_into()?;
        if version != OnDiskTestListingVersion::default() {
            Ok(Default::default())
        } else {
            Ok(toml::from_str::<OnDiskTestListing>(&contents)?.into())
        }
    } else {
        Ok(Default::default())
    }
}

pub fn write_test_listing(
    fs: &Fs,
    state_dir: impl AsRef<Root<StateDir>>,
    job_listing: TestListing,
) -> Result<()> {
    let path = test_listing_file(state_dir);
    if let Some(parent) = path.parent() {
        fs.create_dir_all(parent)?;
    }
    fs.write(
        path,
        toml::to_string_pretty::<OnDiskTestListing>(&job_listing.into())?,
    )?;
    Ok(())
}
