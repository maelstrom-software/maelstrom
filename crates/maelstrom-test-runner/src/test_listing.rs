use crate::{TestArtifactKey, TestFilter};
use anyhow::{anyhow, bail, Result};
use maelstrom_client::StateDir;
use maelstrom_util::{
    fs::Fs,
    root::{Root, RootBuf},
};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive as _;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_with::{serde_as, DisplayFromStr, DurationSecondsWithFrac};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    marker::PhantomData,
    path::Path,
    time::Duration,
};

/*  _
 * (_)_ __    _ __ ___   ___ _ __ ___   ___  _ __ _   _
 * | | '_ \  | '_ ` _ \ / _ \ '_ ` _ \ / _ \| '__| | | |
 * | | | | | | | | | | |  __/ | | | | | (_) | |  | |_| |
 * |_|_| |_| |_| |_| |_|\___|_| |_| |_|\___/|_|   \__, |
 *                                                |___/
 *  FIGLET: in memory
 */

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Artifact {
    pub cases: HashMap<String, Vec<Duration>>,
}

impl<K: Into<String>, V: IntoIterator<Item = Duration>> FromIterator<(K, V)> for Artifact {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            cases: HashMap::from_iter(iter.into_iter().map(|(k, v)| (k.into(), Vec::from_iter(v)))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Package<ArtifactKeyT: TestArtifactKey> {
    pub artifacts: HashMap<ArtifactKeyT, Artifact>,
}

impl<ArtifactKeyT: TestArtifactKey> Default for Package<ArtifactKeyT> {
    fn default() -> Self {
        Self {
            artifacts: HashMap::new(),
        }
    }
}

impl<ArtifactKeyT: TestArtifactKey, K: Into<ArtifactKeyT>, V: Into<Artifact>> FromIterator<(K, V)>
    for Package<ArtifactKeyT>
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            artifacts: HashMap::from_iter(iter.into_iter().map(|(k, v)| (k.into(), v.into()))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestListing<ArtifactKeyT: TestArtifactKey> {
    pub packages: HashMap<String, Package<ArtifactKeyT>>,
}

impl<ArtifactKeyT: TestArtifactKey> Default for TestListing<ArtifactKeyT> {
    fn default() -> Self {
        Self {
            packages: HashMap::new(),
        }
    }
}

impl<ArtifactKeyT: TestArtifactKey, K: Into<String>, V: Into<Package<ArtifactKeyT>>>
    FromIterator<(K, V)> for TestListing<ArtifactKeyT>
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            packages: HashMap::from_iter(iter.into_iter().map(|(k, v)| (k.into(), v.into()))),
        }
    }
}

impl<ArtifactKeyT: TestArtifactKey> TestListing<ArtifactKeyT> {
    pub fn update_artifact_cases<K, I, T>(&mut self, package_name: &str, artifact_key: K, cases: I)
    where
        K: Into<ArtifactKeyT>,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let package = self.packages.entry(package_name.into()).or_default();
        let artifact = package.artifacts.entry(artifact_key.into()).or_default();
        let mut cases: HashSet<String> = cases.into_iter().map(Into::into).collect();
        artifact
            .cases
            .retain(|case_name, _| cases.remove(case_name));
        artifact
            .cases
            .extend(cases.into_iter().map(|case_name| (case_name, vec![])));
    }

    pub fn retain_packages_and_artifacts<'a, PI, PN, AI, AK>(&mut self, existing_packages: PI)
    where
        PI: IntoIterator<Item = (PN, AI)>,
        PN: Into<&'a str>,
        AI: IntoIterator<Item = AK>,
        AK: Into<ArtifactKeyT>,
    {
        let existing_packages: HashMap<_, HashSet<_>> = existing_packages
            .into_iter()
            .map(|(pn, ai)| (pn.into(), ai.into_iter().map(Into::into).collect()))
            .collect();
        self.packages.retain(|package_name, package| {
            let Some(existing_artifacts) = existing_packages.get(package_name.as_str()) else {
                return false;
            };
            package
                .artifacts
                .retain(|key, _| existing_artifacts.contains(key));
            true
        });
    }

    pub fn expected_job_count(&self, filter: &impl TestFilter<ArtifactKey = ArtifactKeyT>) -> u64 {
        self.packages
            .iter()
            .flat_map(|(p, a)| {
                a.artifacts
                    .iter()
                    .flat_map(move |(a, c)| c.cases.keys().map(move |c| (p, a, c)))
            })
            .filter(|(p, a, c)| {
                filter
                    .filter(p, Some(a), Some(c))
                    .expect("case is provided")
            })
            .count() as u64
    }

    pub fn add_timing(
        &mut self,
        package_name: &str,
        artifact_key: ArtifactKeyT,
        case_name: &str,
        timing: Duration,
    ) {
        const MAX_TIMINGS_PER_CASE: usize = 3;
        let package = self.packages.entry(package_name.to_owned()).or_default();
        let artifact = package.artifacts.entry(artifact_key).or_default();
        let case = artifact.cases.entry(case_name.to_owned()).or_default();
        case.push(timing);
        while case.len() > MAX_TIMINGS_PER_CASE {
            case.remove(0);
        }
    }

    pub fn get_timing(
        &self,
        package_name: &str,
        artifact_key: &ArtifactKeyT,
        case_name: &str,
    ) -> Option<Duration> {
        let package = self.packages.get(package_name)?;
        let artifact = package.artifacts.get(artifact_key)?;
        let case = artifact.cases.get(case_name)?;
        if case.is_empty() {
            return None;
        }
        let mut avg = Duration::ZERO;
        let len: u32 = case.len().try_into().unwrap();
        for timing in case {
            avg += *timing / len;
        }
        Some(avg)
    }
}

/*                    _ _     _
 *   ___  _ __     __| (_)___| | __
 *  / _ \| '_ \   / _` | / __| |/ /
 * | (_) | | | | | (_| | \__ \   <
 *  \___/|_| |_|  \__,_|_|___/_|\_\
 *  FIGLET: on disk
 */

#[derive(Deserialize_repr, Eq, FromPrimitive, PartialEq, Serialize_repr)]
#[repr(u32)]
enum OnDiskTestListingVersion {
    V2 = 2,
}

#[serde_as]
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
struct OnDiskArtifactTimings {
    #[serde_as(as = "Vec<DurationSecondsWithFrac>")]
    timings: Vec<Duration>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
struct OnDiskArtifact {
    cases: BTreeMap<String, OnDiskArtifactTimings>,
}

#[serde_as]
#[derive(Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
struct OnDiskArtifactKey<ArtifactKeyT: TestArtifactKey> {
    #[serde_as(as = "DisplayFromStr")]
    #[serde(bound(serialize = ""))]
    #[serde(bound(deserialize = ""))]
    key: ArtifactKeyT,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct OnDiskPackage<ArtifactKeyT: TestArtifactKey> {
    #[serde(bound(serialize = ""))]
    #[serde(bound(deserialize = ""))]
    artifacts: BTreeMap<OnDiskArtifactKey<ArtifactKeyT>, OnDiskArtifact>,
}

#[derive(Serialize, Deserialize)]
struct OnDiskTestListing<ArtifactKeyT: TestArtifactKey> {
    version: OnDiskTestListingVersion,
    #[serde(flatten)]
    #[serde(bound(serialize = ""))]
    #[serde(bound(deserialize = ""))]
    packages: BTreeMap<String, OnDiskPackage<ArtifactKeyT>>,
}

impl<ArtifactKeyT: TestArtifactKey> From<TestListing<ArtifactKeyT>>
    for OnDiskTestListing<ArtifactKeyT>
{
    fn from(in_memory: TestListing<ArtifactKeyT>) -> Self {
        Self {
            version: OnDiskTestListingVersion::V2,
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
                                .map(|(key, artifact)| {
                                    (
                                        OnDiskArtifactKey { key },
                                        OnDiskArtifact {
                                            cases: {
                                                let mut cases =
                                                    Vec::from_iter(artifact.cases.into_iter().map(
                                                        |(case, timings)| {
                                                            (
                                                                case,
                                                                OnDiskArtifactTimings { timings },
                                                            )
                                                        },
                                                    ));
                                                cases.sort_by(|(name1, _), (name2, _)| {
                                                    name1.cmp(name2)
                                                });
                                                cases.into_iter().collect()
                                            },
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

impl<ArtifactKeyT: TestArtifactKey> From<OnDiskTestListing<ArtifactKeyT>>
    for TestListing<ArtifactKeyT>
{
    fn from(on_disk: OnDiskTestListing<ArtifactKeyT>) -> Self {
        Self::from_iter(on_disk.packages.into_iter().map(|(package_name, package)| {
            (
                package_name,
                Package::from_iter(package.artifacts.into_iter().map(|(key, artifact)| {
                    (
                        key.key,
                        Artifact::from_iter(
                            artifact
                                .cases
                                .into_iter()
                                .map(|(case, timings)| (case, timings.timings)),
                        ),
                    )
                })),
            )
        }))
    }
}

/*      _
 *  ___| |_ ___  _ __ ___
 * / __| __/ _ \| '__/ _ \
 * \__ \ || (_) | | |  __/
 * |___/\__\___/|_|  \___|
 *  FIGLET: store
 */

pub trait TestListingStoreDeps {
    fn read_to_string_if_exists(&self, path: impl AsRef<Path>) -> Result<Option<String>> {
        unimplemented!("{:?}", path.as_ref());
    }
    fn create_dir_all(&self, path: impl AsRef<Path>) -> Result<()> {
        unimplemented!("{:?}", path.as_ref());
    }
    fn write(&self, path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> Result<()> {
        unimplemented!("{:?} {:?}", path.as_ref(), contents.as_ref());
    }
}

impl TestListingStoreDeps for Fs {
    fn read_to_string_if_exists(&self, path: impl AsRef<Path>) -> Result<Option<String>> {
        Fs::read_to_string_if_exists(self, path)
    }

    fn create_dir_all(&self, path: impl AsRef<Path>) -> Result<()> {
        Fs::create_dir_all(self, path)
    }

    fn write(&self, path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> Result<()> {
        Fs::write(self, path, contents)
    }
}

struct TestListingFile;

pub struct TestListingStore<ArtifactKeyT, DepsT = Fs> {
    artifact_key: PhantomData<ArtifactKeyT>,
    deps: DepsT,
    test_listing_file: RootBuf<TestListingFile>,
}

const MISSING_VERSION: &str = "missing version";
const VERSION_NOT_AN_INTEGER: &str = "version field is not an integer";
const TEST_LISTING_FILE: &str = "test-listing.toml";

impl<ArtifactKeyT, DepsT> TestListingStore<ArtifactKeyT, DepsT> {
    pub fn new(deps: DepsT, state_dir: impl AsRef<Root<StateDir>>) -> Self {
        Self {
            artifact_key: PhantomData,
            deps,
            test_listing_file: state_dir.as_ref().join(TEST_LISTING_FILE),
        }
    }
}

impl<ArtifactKeyT: TestArtifactKey, DepsT: TestListingStoreDeps>
    TestListingStore<ArtifactKeyT, DepsT>
{
    pub fn load(&self) -> Result<TestListing<ArtifactKeyT>> {
        let Some(contents) = self
            .deps
            .read_to_string_if_exists(&self.test_listing_file)?
        else {
            return Ok(Default::default());
        };
        let mut table: toml::Table = toml::from_str(&contents)?;
        let version = table
            .remove("version")
            .ok_or_else(|| anyhow!(MISSING_VERSION))?;
        let Some(version) = version.as_integer() else {
            bail!(VERSION_NOT_AN_INTEGER);
        };
        match OnDiskTestListingVersion::from_i64(version) {
            None => Ok(Default::default()),
            Some(OnDiskTestListingVersion::V2) => {
                Ok(toml::from_str::<OnDiskTestListing<ArtifactKeyT>>(&contents)?.into())
            }
        }
    }
}

impl<ArtifactKeyT: TestArtifactKey, DepsT: TestListingStoreDeps>
    TestListingStore<ArtifactKeyT, DepsT>
{
    pub fn save(&self, job_listing: TestListing<ArtifactKeyT>) -> Result<()> {
        self.deps
            .create_dir_all(self.test_listing_file.parent().unwrap())?;
        self.deps.write(
            &self.test_listing_file,
            toml::to_string::<OnDiskTestListing<ArtifactKeyT>>(&job_listing.into())?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SimpleFilter, StringArtifactKey};
    use indoc::indoc;
    use maelstrom_test::millis;
    use maelstrom_util::ext::OptionExt as _;
    use pretty_assertions::assert_eq;
    use std::{cell::RefCell, rc::Rc, str};

    macro_rules! millis {
        ($millis:expr) => {
            Duration::from_millis($millis)
        };
    }

    #[test]
    fn update_artifact_cases() {
        let mut listing = TestListing::<StringArtifactKey>::from_iter([(
            "package-1",
            Package::from_iter([(
                StringArtifactKey::from("artifact-1.library"),
                Artifact::from_iter([
                    ("case-1-1L-1", vec![millis!(10), millis!(11), millis!(12)]),
                    ("case-1-1L-2", vec![millis!(20), millis!(21)]),
                    ("case-1-1L-3", vec![millis!(30)]),
                ]),
            )]),
        )]);

        // Add some more cases with the same artifact name, but a different kind, in the same
        // package.
        listing.update_artifact_cases(
            "package-1",
            StringArtifactKey::from("artifact-1.binary"),
            ["case-1-1B-1", "case-1-1B-2", "case-1-1B-3"],
        );
        assert_eq!(
            listing,
            TestListing::<StringArtifactKey>::from_iter([(
                "package-1",
                Package::from_iter([
                    (
                        StringArtifactKey::from("artifact-1.library"),
                        Artifact::from_iter([
                            ("case-1-1L-1", vec![millis!(10), millis!(11), millis!(12)]),
                            ("case-1-1L-2", vec![millis!(20), millis!(21)]),
                            ("case-1-1L-3", vec![millis!(30)]),
                        ]),
                    ),
                    (
                        StringArtifactKey::from("artifact-1.binary"),
                        Artifact::from_iter([
                            ("case-1-1B-1", []),
                            ("case-1-1B-2", []),
                            ("case-1-1B-3", []),
                        ]),
                    ),
                ]),
            )]),
        );

        // Add some more cases that partially overlap with previous ones. This should retain the
        // timings, but remove cases that no longer exist.
        listing.update_artifact_cases(
            "package-1",
            StringArtifactKey::from("artifact-1.library"),
            ["case-1-1L-2", "case-1-1L-3", "case-1-1L-4"],
        );
        listing.update_artifact_cases(
            "package-1",
            StringArtifactKey::from("artifact-1.binary"),
            ["case-1-1B-2", "case-1-1B-3", "case-1-1B-4"],
        );
        assert_eq!(
            listing,
            TestListing::<StringArtifactKey>::from_iter([(
                "package-1",
                Package::from_iter([
                    (
                        StringArtifactKey::from("artifact-1.library"),
                        Artifact::from_iter([
                            ("case-1-1L-2", vec![millis!(20), millis!(21)]),
                            ("case-1-1L-3", vec![millis!(30)]),
                            ("case-1-1L-4", vec![]),
                        ]),
                    ),
                    (
                        StringArtifactKey::from("artifact-1.binary"),
                        Artifact::from_iter([
                            ("case-1-1B-2", []),
                            ("case-1-1B-3", []),
                            ("case-1-1B-4", []),
                        ]),
                    ),
                ]),
            )]),
        );

        // Add some more cases for a different package. They should be independent.
        listing.update_artifact_cases(
            "package-2",
            StringArtifactKey::from("artifact-1.library"),
            ["case-2-1L-1", "case-2-1L-2", "case-2-1L-3"],
        );
        assert_eq!(
            listing,
            TestListing::<StringArtifactKey>::from_iter([
                (
                    "package-1",
                    Package::from_iter([
                        (
                            StringArtifactKey::from("artifact-1.library"),
                            Artifact::from_iter([
                                ("case-1-1L-2", vec![millis!(20), millis!(21)]),
                                ("case-1-1L-3", vec![millis!(30)]),
                                ("case-1-1L-4", vec![]),
                            ]),
                        ),
                        (
                            StringArtifactKey::from("artifact-1.binary"),
                            Artifact::from_iter([
                                ("case-1-1B-2", []),
                                ("case-1-1B-3", []),
                                ("case-1-1B-4", []),
                            ]),
                        ),
                    ])
                ),
                (
                    "package-2",
                    Package::from_iter([(
                        StringArtifactKey::from("artifact-1.library"),
                        Artifact::from_iter([
                            ("case-2-1L-1", []),
                            ("case-2-1L-2", []),
                            ("case-2-1L-3", []),
                        ]),
                    )]),
                ),
            ]),
        );
    }

    #[test]
    fn retain_packages_and_artifacts() {
        let mut listing = TestListing::<StringArtifactKey>::from_iter([
            (
                "package-1",
                Package::from_iter([
                    (
                        StringArtifactKey::from("artifact-1.library"),
                        Artifact::from_iter([
                            ("case-1-1L-1", [millis!(10), millis!(11)]),
                            ("case-1-1L-2", [millis!(20), millis!(21)]),
                        ]),
                    ),
                    (
                        StringArtifactKey::from("artifact-1.binary"),
                        Artifact::from_iter([
                            ("case-1-1B-1", [millis!(15), millis!(16)]),
                            ("case-1-1B-2", [millis!(25), millis!(26)]),
                        ]),
                    ),
                ]),
            ),
            (
                "package-2",
                Package::from_iter([(
                    StringArtifactKey::from("artifact-1.library"),
                    Artifact::from_iter([
                        ("case-2-1L-1", [millis!(10), millis!(12)]),
                        ("case-2-1L-2", [millis!(20), millis!(22)]),
                    ]),
                )]),
            ),
        ]);

        listing.retain_packages_and_artifacts([
            (
                "package-1",
                vec![
                    StringArtifactKey::from("artifact-1.library"),
                    StringArtifactKey::from("artifact-2.binary"),
                ],
            ),
            (
                "package-3",
                vec![StringArtifactKey::from("artifact-1.library")],
            ),
        ]);

        assert_eq!(
            listing,
            TestListing::<StringArtifactKey>::from_iter([(
                "package-1",
                Package::from_iter([(
                    StringArtifactKey::from("artifact-1.library"),
                    Artifact::from_iter([
                        ("case-1-1L-1", [millis!(10), millis!(11)]),
                        ("case-1-1L-2", [millis!(20), millis!(21)]),
                    ]),
                )]),
            )]),
        );
    }

    #[test]
    fn expected_job_count() {
        let listing = TestListing::<StringArtifactKey>::from_iter([
            (
                "package-1",
                Package::from_iter([
                    (
                        StringArtifactKey::from("artifact-1.library"),
                        Artifact::from_iter([
                            ("case-1-1L-1", vec![millis!(10), millis!(11)]),
                            ("case-1-1L-2", vec![millis!(20)]),
                            ("case-1-1L-3", vec![]),
                        ]),
                    ),
                    (
                        StringArtifactKey::from("artifact-1.binary"),
                        Artifact::from_iter([
                            ("case-1-1B-1", vec![millis!(15), millis!(16)]),
                            ("case-1-1B-2", vec![]),
                        ]),
                    ),
                ]),
            ),
            (
                "package-2",
                Package::from_iter([(
                    StringArtifactKey::from("artifact-1.library"),
                    Artifact::from_iter([("case-2-1L-1", [millis!(10), millis!(12)])]),
                )]),
            ),
        ]);

        assert_eq!(listing.expected_job_count(&SimpleFilter::All), 6);
        assert_eq!(listing.expected_job_count(&SimpleFilter::None), 0);
        assert_eq!(
            listing.expected_job_count(&SimpleFilter::Package("package-1".into())),
            5
        );
        assert_eq!(
            listing.expected_job_count(&SimpleFilter::ArtifactEndsWith(".library".into())),
            4
        );
    }

    #[test]
    fn add_timing() {
        let mut listing = TestListing::default();

        listing.add_timing(
            "package-1",
            StringArtifactKey::from("artifact-1.library"),
            "case-1-1L-1",
            millis!(10),
        );
        assert_eq!(
            listing,
            TestListing::<StringArtifactKey>::from_iter([(
                "package-1",
                Package::from_iter([(
                    StringArtifactKey::from("artifact-1.library"),
                    Artifact::from_iter([("case-1-1L-1", [millis!(10)])]),
                )]),
            )]),
        );

        listing.add_timing(
            "package-1",
            StringArtifactKey::from("artifact-1.library"),
            "case-1-1L-1",
            millis!(11),
        );
        assert_eq!(
            listing,
            TestListing::<StringArtifactKey>::from_iter([(
                "package-1",
                Package::from_iter([(
                    StringArtifactKey::from("artifact-1.library"),
                    Artifact::from_iter([("case-1-1L-1", [millis!(10), millis!(11)])]),
                )]),
            )]),
        );

        listing.add_timing(
            "package-1",
            StringArtifactKey::from("artifact-1.library"),
            "case-1-1L-1",
            millis!(12),
        );
        assert_eq!(
            listing,
            TestListing::<StringArtifactKey>::from_iter([(
                "package-1",
                Package::from_iter([(
                    StringArtifactKey::from("artifact-1.library"),
                    Artifact::from_iter([("case-1-1L-1", [millis!(10), millis!(11), millis!(12)])]),
                )]),
            )]),
        );

        listing.add_timing(
            "package-1",
            StringArtifactKey::from("artifact-1.library"),
            "case-1-1L-1",
            millis!(13),
        );
        assert_eq!(
            listing,
            TestListing::<StringArtifactKey>::from_iter([(
                "package-1",
                Package::from_iter([(
                    StringArtifactKey::from("artifact-1.library"),
                    Artifact::from_iter([("case-1-1L-1", [millis!(11), millis!(12), millis!(13)])]),
                )]),
            )]),
        );
    }

    #[test]
    fn add_timing_already_too_many() {
        let mut listing = TestListing::<StringArtifactKey>::from_iter([(
            "package-1",
            Package::from_iter([(
                StringArtifactKey::from("artifact-1.library"),
                Artifact::from_iter([(
                    "case-1-1L-1",
                    [
                        millis!(10),
                        millis!(11),
                        millis!(12),
                        millis!(13),
                        millis!(14),
                    ],
                )]),
            )]),
        )]);

        listing.add_timing(
            "package-1",
            StringArtifactKey::from("artifact-1.library"),
            "case-1-1L-1",
            millis!(15),
        );
        assert_eq!(
            listing,
            TestListing::<StringArtifactKey>::from_iter([(
                "package-1",
                Package::from_iter([(
                    StringArtifactKey::from("artifact-1.library"),
                    Artifact::from_iter([("case-1-1L-1", [millis!(13), millis!(14), millis!(15)])]),
                )]),
            )]),
        );
    }

    #[test]
    fn get_timing() {
        let artifact_1 = StringArtifactKey::from("artifact-1.library");
        let listing = TestListing::<StringArtifactKey>::from_iter([(
            "package-1",
            Package::from_iter([(
                artifact_1.clone(),
                Artifact::from_iter([
                    ("case-1", vec![]),
                    ("case-2", vec![millis!(10)]),
                    ("case-3", vec![millis!(10), millis!(12)]),
                    (
                        "case-4",
                        vec![millis!(10), millis!(12), millis!(14), millis!(16)],
                    ),
                ]),
            )]),
        )]);

        assert_eq!(listing.get_timing("package-1", &artifact_1, "case-1"), None);
        assert_eq!(
            listing.get_timing("package-1", &artifact_1, "case-2"),
            Some(millis!(10))
        );
        assert_eq!(
            listing.get_timing("package-1", &artifact_1, "case-3"),
            Some(millis!(11))
        );
        assert_eq!(
            listing.get_timing("package-1", &artifact_1, "case-4"),
            Some(millis!(13))
        );
        assert_eq!(listing.get_timing("package-1", &artifact_1, "case-5"), None);
        let artifact_1_bin = StringArtifactKey::from("artifact-1.binary");
        assert_eq!(
            listing.get_timing("package-1", &artifact_1_bin, "case-1"),
            None
        );
        assert_eq!(listing.get_timing("package-2", &artifact_1, "case-1"), None);
    }

    #[test]
    fn load_passes_proper_path() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, path: impl AsRef<Path>) -> Result<Option<String>> {
                assert_eq!(
                    path.as_ref().to_str().unwrap(),
                    format!("path/to/state/{TEST_LISTING_FILE}")
                );
                Ok(None)
            }
        }
        let _ = TestListingStore::<StringArtifactKey, _>::new(
            Deps,
            RootBuf::new("path/to/state".into()),
        );
    }

    #[test]
    fn error_reading_in_load_propagates_error() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Err(anyhow!("error!"))
            }
        }
        let store = TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("".into()));
        assert_eq!(store.load().unwrap_err().to_string(), "error!");
    }

    #[test]
    fn load_of_nonexistent_file_gives_default_listing() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Ok(None)
            }
        }
        let store = TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("".into()));
        assert_eq!(store.load().unwrap(), TestListing::default());
    }

    #[test]
    fn load_of_file_with_invalid_toml_gives_toml_parse_error() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Ok(Some(r#""garbage": { "foo", "bar" }"#.into()))
            }
        }
        let store = TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("".into()));
        let error = store.load().unwrap_err().to_string();
        assert!(error.starts_with("TOML parse error"));
    }

    #[test]
    fn load_of_empty_file_gives_missing_version_error() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Ok(Some("foo = 3\n".into()))
            }
        }
        let store = TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("".into()));
        assert_eq!(store.load().unwrap_err().to_string(), MISSING_VERSION);
    }

    #[test]
    fn load_of_file_without_version_gives_missing_version_error() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Ok(Some("foo = 3\n".into()))
            }
        }
        let store = TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("".into()));
        assert_eq!(store.load().unwrap_err().to_string(), MISSING_VERSION);
    }

    #[test]
    fn load_of_file_with_non_integer_version_gives_version_not_an_integer_error() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Ok(Some("version = \"v1\"\n".into()))
            }
        }
        let store = TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("".into()));
        assert_eq!(
            store.load().unwrap_err().to_string(),
            VERSION_NOT_AN_INTEGER
        );
    }

    #[test]
    fn load_of_file_with_old_version_gives_default_listing() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Ok(Some("version = 0\nfoo = \"bar\"\n".into()))
            }
        }
        let store = TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("".into()));
        assert_eq!(store.load().unwrap(), TestListing::default());
    }

    #[test]
    fn load_of_file_with_newer_version_gives_default_listing() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Ok(Some("version = 1000000\nfoo = \"bar\"\n".into()))
            }
        }
        let store = TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("".into()));
        assert_eq!(store.load().unwrap(), TestListing::default());
    }

    #[test]
    fn load_of_file_with_correct_version_gives_deserialized_listing() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Ok(Some(
                    indoc! {r#"
                        version = 2

                        [package-1."artifact-1.library"]
                        case-1-1L-1 = [0.01, 0.011]
                        case-1-1L-2 = [0.02]
                        case-1-1L-3 = []
                    "#}
                    .into(),
                ))
            }
        }
        let store = TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("".into()));
        let expected = TestListing::<StringArtifactKey>::from_iter([(
            "package-1",
            Package::from_iter([(
                StringArtifactKey::from("artifact-1.library"),
                Artifact::from_iter([
                    ("case-1-1L-1", vec![millis!(10), millis!(11)]),
                    ("case-1-1L-2", vec![millis!(20)]),
                    ("case-1-1L-3", vec![]),
                ]),
            )]),
        )]);
        assert_eq!(store.load().unwrap(), expected);
    }

    #[test]
    fn load_of_file_with_correct_version_but_bad_toml_gives_toml_parse_error() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Ok(Some(
                    indoc! {r#"
                        version = 2

                        [[frob.blah]]
                        foo = "package1"
                        bar = "Library"
                        baz = [
                            "case1",
                            "case2",
                        ]
                    "#}
                    .into(),
                ))
            }
        }
        let store = TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("".into()));
        let error = store.load().unwrap_err().to_string();
        assert!(error.starts_with("TOML parse error"));
    }

    #[test]
    fn error_creating_dir_in_save_propagates_error() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn create_dir_all(&self, _: impl AsRef<Path>) -> Result<()> {
                Err(anyhow!("error!"))
            }
        }
        let store =
            TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("state".into()));
        assert_eq!(
            store.save(TestListing::default()).unwrap_err().to_string(),
            "error!"
        );
    }

    #[test]
    fn error_writing_in_save_propagates_error() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn create_dir_all(&self, _: impl AsRef<Path>) -> Result<()> {
                Ok(())
            }
            fn write(&self, _: impl AsRef<Path>, _: impl AsRef<[u8]>) -> Result<()> {
                Err(anyhow!("error!"))
            }
        }
        let store =
            TestListingStore::<StringArtifactKey, _>::new(Deps, RootBuf::new("state".into()));
        assert_eq!(
            store.save(TestListing::default()).unwrap_err().to_string(),
            "error!"
        );
    }

    #[derive(Default)]
    struct LoggingDeps {
        create_dir_all: Option<String>,
        write: Option<(String, String)>,
    }

    impl TestListingStoreDeps for Rc<RefCell<LoggingDeps>> {
        fn create_dir_all(&self, path: impl AsRef<Path>) -> Result<()> {
            self.borrow_mut()
                .create_dir_all
                .replace(path.as_ref().to_str().unwrap().to_string())
                .assert_is_none();
            Ok(())
        }
        fn write(&self, path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> Result<()> {
            self.borrow_mut()
                .write
                .replace((
                    path.as_ref().to_str().unwrap().to_string(),
                    str::from_utf8(contents.as_ref()).unwrap().to_string(),
                ))
                .assert_is_none();
            Ok(())
        }
    }

    #[test]
    fn save_creates_parent_directory() {
        let deps = Rc::new(RefCell::new(LoggingDeps::default()));
        let store = TestListingStore::<StringArtifactKey, _>::new(
            deps.clone(),
            RootBuf::new("maelstrom/state/".into()),
        );
        store.save(TestListing::default()).unwrap();
        assert_eq!(deps.borrow().create_dir_all, Some("maelstrom/state".into()));
    }

    #[test]
    fn save_of_default() {
        let deps = Rc::new(RefCell::new(LoggingDeps::default()));
        let store = TestListingStore::<StringArtifactKey, _>::new(
            deps.clone(),
            RootBuf::new("maelstrom/state/".into()),
        );
        store.save(TestListing::default()).unwrap();
        assert_eq!(
            deps.borrow().write,
            Some((
                format!("maelstrom/state/{TEST_LISTING_FILE}"),
                "version = 2\n".into()
            ))
        );
    }

    #[test]
    fn save_of_listing() {
        let deps = Rc::new(RefCell::new(LoggingDeps::default()));
        let store = TestListingStore::<StringArtifactKey, _>::new(
            deps.clone(),
            RootBuf::new("maelstrom/state/".into()),
        );
        let listing = TestListing::<StringArtifactKey>::from_iter([
            (
                "package-2",
                Package::from_iter([(
                    StringArtifactKey::from("artifact-1.library"),
                    Artifact::from_iter([("case-2-1L-1", [millis!(10), millis!(12)])]),
                )]),
            ),
            (
                "package-1",
                Package::from_iter([
                    (
                        StringArtifactKey::from("artifact-1.binary"),
                        Artifact::from_iter([
                            ("case-1-1B-1", vec![millis!(15), millis!(16)]),
                            ("case-1-1B-2", vec![]),
                        ]),
                    ),
                    (
                        StringArtifactKey::from("artifact-1.library"),
                        Artifact::from_iter([
                            ("case-1-1L-1", vec![millis!(10), millis!(11)]),
                            ("case-1-1L-2", vec![millis!(20)]),
                            ("case-1-1L-3", vec![]),
                        ]),
                    ),
                ]),
            ),
        ]);
        store.save(listing).unwrap();
        let (actual_path, actual_contents) = deps.borrow_mut().write.take().unwrap();
        assert_eq!(actual_path, format!("maelstrom/state/{TEST_LISTING_FILE}"));
        assert_eq!(
            actual_contents,
            indoc! {r#"
                version = 2

                [package-1."artifact-1.binary"]
                case-1-1B-1 = [0.015, 0.016]
                case-1-1B-2 = []

                [package-1."artifact-1.library"]
                case-1-1L-1 = [0.01, 0.011]
                case-1-1L-2 = [0.02]
                case-1-1L-3 = []

                [package-2."artifact-1.library"]
                case-2-1L-1 = [0.01, 0.012]
            "#},
        );
    }
}
