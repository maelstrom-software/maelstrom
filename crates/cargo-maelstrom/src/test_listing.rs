use crate::pattern;
pub use crate::pattern::ArtifactKind;
use anyhow::{anyhow, bail, Result};
use cargo_metadata::Target as CargoTarget;
use maelstrom_client::StateDir;
use maelstrom_util::{
    fs::Fs,
    root::{Root, RootBuf},
};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive as _;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_with::{serde_as, FromInto};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
};

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
    pub fn new(name: impl Into<String>, kind: ArtifactKind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }
}

impl From<&CargoTarget> for ArtifactKey {
    fn from(target: &CargoTarget) -> Self {
        Self::new(&target.name, ArtifactKind::from_target(target))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Artifact {
    pub cases: HashSet<String>,
}

impl<A: Into<String>> FromIterator<A> for Artifact {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        Self {
            cases: HashSet::from_iter(iter.into_iter().map(Into::into)),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Package {
    pub artifacts: HashMap<ArtifactKey, Artifact>,
}

impl<K: Into<ArtifactKey>, V: Into<Artifact>> FromIterator<(K, V)> for Package {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            artifacts: HashMap::from_iter(iter.into_iter().map(|(k, v)| (k.into(), v.into()))),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TestListing {
    pub packages: HashMap<String, Package>,
}

impl<K: Into<String>, V: Into<Package>> FromIterator<(K, V)> for TestListing {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            packages: HashMap::from_iter(iter.into_iter().map(|(k, v)| (k.into(), v.into()))),
        }
    }
}

impl TestListing {
    pub fn update_artifact_cases<K, I, T>(&mut self, package_name: &str, artifact_key: K, cases: I)
    where
        K: Into<ArtifactKey>,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let package = self.packages.entry(package_name.into()).or_default();
        package
            .artifacts
            .insert(artifact_key.into(), Artifact::from_iter(cases));
    }

    pub fn retain_packages_and_artifacts<'a, PI, PN, AI, AK>(&mut self, existing_packages: PI)
    where
        PI: IntoIterator<Item = (PN, AI)>,
        PN: Into<&'a str>,
        AI: IntoIterator<Item = AK>,
        AK: Into<ArtifactKey>,
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

    pub fn expected_job_count(&self, filter: &pattern::Pattern) -> u64 {
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
            version: OnDiskTestListingVersion::V1,
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
                                            cases: {
                                                let mut cases = Vec::from_iter(artifact.cases);
                                                cases.sort();
                                                cases
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

impl From<OnDiskTestListing> for TestListing {
    fn from(on_disk: OnDiskTestListing) -> Self {
        Self::from_iter(on_disk.packages.into_iter().map(|(package_name, package)| {
            (
                package_name,
                Package::from_iter(package.artifacts.into_iter().map(
                    |(artifact_key, artifact)| (artifact_key, Artifact::from_iter(artifact.cases)),
                )),
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

pub struct TestListingStore<DepsT = Fs> {
    deps: DepsT,
    test_listing_file: RootBuf<TestListingFile>,
}

const MISSING_VERSION: &str = "missing version";
const VERSION_NOT_AN_INTEGER: &str = "version field is not an integer";
const TEST_LISTING_FILE: &str = "test-listing.toml";

impl<DepsT: TestListingStoreDeps> TestListingStore<DepsT> {
    pub fn new(deps: DepsT, state_dir: impl AsRef<Root<StateDir>>) -> Self {
        Self {
            deps,
            test_listing_file: state_dir.as_ref().join(TEST_LISTING_FILE),
        }
    }

    pub fn load(&self) -> Result<TestListing> {
        let Some(contents) = self
            .deps
            .read_to_string_if_exists(&self.test_listing_file)?
        else {
            return Ok(Default::default());
        };
        let mut table: toml::Table = toml::from_str(&contents)?;
        let version = table.remove("version").ok_or(anyhow!(MISSING_VERSION))?;
        let Some(version) = version.as_integer() else {
            bail!(VERSION_NOT_AN_INTEGER);
        };
        match OnDiskTestListingVersion::from_i64(version) {
            None => Ok(Default::default()),
            Some(OnDiskTestListingVersion::V1) => {
                Ok(toml::from_str::<OnDiskTestListing>(&contents)?.into())
            }
        }
    }

    pub fn save(&self, job_listing: TestListing) -> Result<()> {
        self.deps
            .create_dir_all(self.test_listing_file.parent().unwrap())?;
        self.deps.write(
            &self.test_listing_file,
            toml::to_string_pretty::<OnDiskTestListing>(&job_listing.into())?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use maelstrom_util::ext::OptionExt as _;
    use pretty_assertions::assert_eq;
    use std::{cell::RefCell, rc::Rc, str};

    #[test]
    fn update_artifact_cases() {
        let mut listing = TestListing::default();

        // Add some initial cases.
        listing.update_artifact_cases(
            "package-1",
            ArtifactKey::new("artifact-1", ArtifactKind::Library),
            ["case1", "case2", "case3"],
        );
        assert_eq!(
            listing,
            TestListing::from_iter([(
                "package-1",
                Package::from_iter([(
                    ArtifactKey::new("artifact-1", ArtifactKind::Library),
                    Artifact::from_iter(["case3", "case2", "case1"]),
                )])
            )])
        );

        // Add some more cases with the same artifact name in the same package.
        listing.update_artifact_cases(
            "package-1",
            ArtifactKey::new("artifact-1", ArtifactKind::Binary),
            ["case2", "case3", "case4"],
        );
        assert_eq!(
            listing,
            TestListing::from_iter([(
                "package-1",
                Package::from_iter([
                    (
                        ArtifactKey::new("artifact-1", ArtifactKind::Binary),
                        Artifact::from_iter(["case4", "case3", "case2"]),
                    ),
                    (
                        ArtifactKey::new("artifact-1", ArtifactKind::Library),
                        Artifact::from_iter(["case3", "case2", "case1"]),
                    )
                ])
            )])
        );

        // Add some more cases that partially overlap with previous ones. This should overwrite the
        // old value we had for that artifact.
        listing.update_artifact_cases(
            "package-1",
            ArtifactKey::new("artifact-1", ArtifactKind::Binary),
            ["case4", "case5", "case6"],
        );
        assert_eq!(
            listing,
            TestListing::from_iter([(
                "package-1",
                Package::from_iter([
                    (
                        ArtifactKey::new("artifact-1", ArtifactKind::Binary),
                        Artifact::from_iter(["case6", "case5", "case4"]),
                    ),
                    (
                        ArtifactKey::new("artifact-1", ArtifactKind::Library),
                        Artifact::from_iter(["case3", "case2", "case1"]),
                    )
                ])
            )])
        );

        // Add some more cases for a different package. They should be independent.
        listing.update_artifact_cases(
            "package-2",
            ArtifactKey::new("artifact-1", ArtifactKind::Library),
            ["case100", "case101"],
        );
        assert_eq!(
            listing,
            TestListing::from_iter([
                (
                    "package-2",
                    Package::from_iter([(
                        ArtifactKey::new("artifact-1", ArtifactKind::Library),
                        Artifact::from_iter(["case101", "case100"]),
                    )])
                ),
                (
                    "package-1",
                    Package::from_iter([
                        (
                            ArtifactKey::new("artifact-1", ArtifactKind::Binary),
                            Artifact::from_iter(["case6", "case5", "case4"]),
                        ),
                        (
                            ArtifactKey::new("artifact-1", ArtifactKind::Library),
                            Artifact::from_iter(["case3", "case2", "case1"]),
                        )
                    ])
                )
            ])
        );
    }

    #[test]
    fn retain_packages_and_artifacts() {
        let mut listing = TestListing::from_iter([
            (
                "package-1",
                Package::from_iter([
                    (
                        ArtifactKey::new("artifact-1-1", ArtifactKind::Library),
                        Artifact::from_iter(["case-1-1-1", "case-1-1-2"]),
                    ),
                    (
                        ArtifactKey::new("artifact-1-2", ArtifactKind::Binary),
                        Artifact::from_iter(["case-1-2-1", "case-1-2-2"]),
                    ),
                ]),
            ),
            (
                "package-2",
                Package::from_iter([(
                    ArtifactKey::new("artifact-2-1", ArtifactKind::Library),
                    Artifact::from_iter(["case-2-1-1", "case-2-1-2"]),
                )]),
            ),
        ]);

        listing.retain_packages_and_artifacts([
            (
                "package-1",
                vec![
                    ArtifactKey::new("artifact-1-1", ArtifactKind::Library),
                    ArtifactKey::new("artifact-1-3", ArtifactKind::Binary),
                ],
            ),
            (
                "package-3",
                vec![ArtifactKey::new("artifact-3-1", ArtifactKind::Library)],
            ),
        ]);

        assert_eq!(
            listing,
            TestListing::from_iter([(
                "package-1",
                Package::from_iter([(
                    ArtifactKey::new("artifact-1-1", ArtifactKind::Library),
                    Artifact::from_iter(["case-1-1-1", "case-1-1-2"]),
                ),]),
            ),])
        );
    }

    #[test]
    fn expected_job_count() {
        let listing = TestListing::from_iter([
            (
                "package-1",
                Package::from_iter([
                    (
                        ArtifactKey::new("artifact-1-1", ArtifactKind::Library),
                        Artifact::from_iter(["case-1-1-1", "case-1-1-2", "case-1-1-3"]),
                    ),
                    (
                        ArtifactKey::new("artifact-1-2", ArtifactKind::Binary),
                        Artifact::from_iter(["case-1-2-1", "case-1-2-2"]),
                    ),
                ]),
            ),
            (
                "package-2",
                Package::from_iter([(
                    ArtifactKey::new("artifact-2-1", ArtifactKind::Library),
                    Artifact::from_iter(["case-2-1-1"]),
                )]),
            ),
        ]);

        assert_eq!(listing.expected_job_count(&"all".parse().unwrap()), 6);
        assert_eq!(listing.expected_job_count(&"none".parse().unwrap()), 0);
        assert_eq!(
            listing.expected_job_count(&"package.eq(package-1)".parse().unwrap()),
            5
        );
        assert_eq!(listing.expected_job_count(&"library".parse().unwrap()), 4);
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
        let _ = TestListingStore::new(Deps, RootBuf::new("path/to/state".into()));
    }

    #[test]
    fn error_reading_in_load_propagates_error() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Err(anyhow!("error!"))
            }
        }
        let store = TestListingStore::new(Deps, RootBuf::new("".into()));
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
        let store = TestListingStore::new(Deps, RootBuf::new("".into()));
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
        let store = TestListingStore::new(Deps, RootBuf::new("".into()));
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
        let store = TestListingStore::new(Deps, RootBuf::new("".into()));
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
        let store = TestListingStore::new(Deps, RootBuf::new("".into()));
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
        let store = TestListingStore::new(Deps, RootBuf::new("".into()));
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
        let store = TestListingStore::new(Deps, RootBuf::new("".into()));
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
        let store = TestListingStore::new(Deps, RootBuf::new("".into()));
        assert_eq!(store.load().unwrap(), TestListing::default());
    }

    #[test]
    fn load_of_file_with_correct_version_gives_deserialized_listing() {
        struct Deps;
        impl TestListingStoreDeps for Deps {
            fn read_to_string_if_exists(&self, _: impl AsRef<Path>) -> Result<Option<String>> {
                Ok(Some(
                    indoc! {r#"
                        version = 1

                        [[package1.artifacts]]
                        name = "package1"
                        kind = "Library"
                        cases = [
                            "case1",
                            "case2",
                        ]
                    "#}
                    .into(),
                ))
            }
        }
        let store = TestListingStore::new(Deps, RootBuf::new("".into()));
        let expected = TestListing::from_iter([(
            "package1",
            Package::from_iter([(
                ArtifactKey::new("package1", ArtifactKind::Library),
                Artifact::from_iter(["case1", "case2"]),
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
                        version = 1

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
        let store = TestListingStore::new(Deps, RootBuf::new("".into()));
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
        let store = TestListingStore::new(Deps, RootBuf::new("state".into()));
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
        let store = TestListingStore::new(Deps, RootBuf::new("state".into()));
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
            Ok(self
                .borrow_mut()
                .create_dir_all
                .replace(path.as_ref().to_str().unwrap().to_string())
                .assert_is_none())
        }
        fn write(&self, path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> Result<()> {
            Ok(self
                .borrow_mut()
                .write
                .replace((
                    path.as_ref().to_str().unwrap().to_string(),
                    str::from_utf8(contents.as_ref()).unwrap().to_string(),
                ))
                .assert_is_none())
        }
    }

    #[test]
    fn save_creates_parent_directory() {
        let deps = Rc::new(RefCell::new(LoggingDeps::default()));
        let store = TestListingStore::new(deps.clone(), RootBuf::new("maelstrom/state/".into()));
        store.save(TestListing::default()).unwrap();
        assert_eq!(deps.borrow().create_dir_all, Some("maelstrom/state".into()));
    }

    #[test]
    fn save_of_default() {
        let deps = Rc::new(RefCell::new(LoggingDeps::default()));
        let store = TestListingStore::new(deps.clone(), RootBuf::new("maelstrom/state/".into()));
        store.save(TestListing::default()).unwrap();
        assert_eq!(
            deps.borrow().write,
            Some((
                format!("maelstrom/state/{TEST_LISTING_FILE}"),
                "version = 1\n".into()
            ))
        );
    }

    #[test]
    fn save_of_simple_listing() {
        let deps = Rc::new(RefCell::new(LoggingDeps::default()));
        let store = TestListingStore::new(deps.clone(), RootBuf::new("maelstrom/state/".into()));
        let listing = TestListing::from_iter([(
            "package1",
            Package::from_iter([(
                ArtifactKey::new("package1", ArtifactKind::Library),
                Artifact::from_iter(["case1", "case2"]),
            )]),
        )]);
        store.save(listing).unwrap();
        assert_eq!(
            deps.borrow().write,
            Some((
                format!("maelstrom/state/{TEST_LISTING_FILE}"),
                indoc! {r#"
                    version = 1

                    [[package1.artifacts]]
                    name = "package1"
                    kind = "Library"
                    cases = [
                        "case1",
                        "case2",
                    ]
                "#}
                .into()
            ))
        );
    }
}
