use crate::{metadata::Metadata, ui};
use anyhow::Result;
use maelstrom_base::Utf8PathBuf;
use maelstrom_client::spec::{ImageRef, LayerSpec};
use maelstrom_util::{process::ExitCode, template::TemplateVars};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::HashSet,
    fmt,
    hash::Hash,
    path::Path,
    str::{self, FromStr},
    sync::Arc,
};

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct WaitStatus {
    pub exit_code: ExitCode,
    pub output: String,
}

/// Wait for some asynchronous thing like a process to finish.
pub trait Wait {
    /// Block the current thread waiting for whatever thing to finish.
    fn wait(&self) -> Result<WaitStatus>;

    /// Kill the asynchronous thing waking up any threads in `wait`.
    fn kill(&self) -> Result<()>;
}

/// A helper that will call `Wait::kill` when it is dropped.
pub struct KillOnDrop<WaitT: Wait> {
    wait: Arc<WaitT>,
}

impl<WaitT: Wait> KillOnDrop<WaitT> {
    pub fn new(wait: Arc<WaitT>) -> Self {
        Self { wait }
    }
}

impl<WaitT: Wait> Drop for KillOnDrop<WaitT> {
    fn drop(&mut self) {
        let _ = self.wait.kill();
    }
}

/// A handle to an artifact
pub trait TestArtifactKey:
    fmt::Display + FromStr<Err = anyhow::Error> + Hash + Ord + Eq + Clone + Send + Sync + 'static
{
}

/// Metadata associated with a test case
pub trait TestCaseMetadata:
    Hash + Ord + Eq + Clone + Send + Sync + Serialize + DeserializeOwned + 'static
{
}

#[derive(Clone, Debug, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub struct NoCaseMetadata;

impl TestCaseMetadata for NoCaseMetadata {}

impl Serialize for NoCaseMetadata {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_none()
    }
}

impl<'de> Deserialize<'de> for NoCaseMetadata {
    fn deserialize<D>(_deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        Ok(Self)
    }
}

/// A handle to an artifact that is just a `String`
#[cfg(test)]
#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StringArtifactKey(String);

#[cfg(test)]
impl<'a> From<&'a str> for StringArtifactKey {
    fn from(s: &'a str) -> Self {
        Self(s.into())
    }
}

#[cfg(test)]
impl fmt::Display for StringArtifactKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
impl FromStr for StringArtifactKey {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(Self(s.into()))
    }
}

#[cfg(test)]
impl TestArtifactKey for StringArtifactKey {}

/// A handle to a package.
pub trait TestPackageId: Clone + Ord + fmt::Debug {}

/// An artifact is a file that contains and can be used to run test cases.
pub trait TestArtifact: fmt::Debug + Send + Sync + 'static {
    /// The key for this artifact.
    type ArtifactKey: TestArtifactKey;
    /// The handle to the package containing this artifact.
    type PackageId: TestPackageId;
    /// The case metadata this artifact uses.
    type CaseMetadata: TestCaseMetadata;

    /// The key for this artifact.
    fn to_key(&self) -> Self::ArtifactKey;

    /// The path to the artifact in the file-system.
    fn path(&self) -> &Path;

    /// Get the listing of test cases. This can potentially block.
    fn list_tests(&self) -> Result<Vec<(String, Self::CaseMetadata)>>;

    /// Get the listing of ignored test cases. This can potentially block.
    fn list_ignored_tests(&self) -> Result<Vec<String>>;

    /// The handle to the package that contains this artifact.
    fn package(&self) -> Self::PackageId;

    /// Build a command to run the given test cases. It returns a tuple of the program to run and
    /// vector of arguments to pass to it.
    fn build_command(
        &self,
        case_name: &str,
        case_metadata: &Self::CaseMetadata,
    ) -> (Utf8PathBuf, Vec<String>);

    /// Create string that represents the given test case. This should be used for display
    /// purposes.
    fn format_case(
        &self,
        package_name: &str,
        case_name: &str,
        case_metadata: &Self::CaseMetadata,
    ) -> String;

    /// Get any layers to add to the jobs using this artifact.
    fn get_test_layers(&self, metadata: &Metadata) -> Vec<LayerSpec>;
}

/// A package is something that contains artifacts.
pub trait TestPackage: Clone + fmt::Debug + Send + Sync + 'static {
    /// A handle to this package.
    type PackageId: TestPackageId;
    /// The type used for artifacts.
    type ArtifactKey: TestArtifactKey;

    /// The name of the package.
    fn name(&self) -> &str;

    /// The artifacts contained in this package.
    fn artifacts(&self) -> Vec<Self::ArtifactKey>;

    /// The handle to this package.
    fn id(&self) -> Self::PackageId;
}

pub trait CollectTests {
    /// This message is displayed in the UI when tests are being enqueued.
    const ENQUEUE_MESSAGE: &'static str;

    /// This type is used to filter packages and test cases.
    type TestFilter: TestFilter<
        Package = Self::Package,
        ArtifactKey = Self::ArtifactKey,
        CaseMetadata = Self::CaseMetadata,
    >;

    /// This type is used to wait for a background build to complete or to kill it.
    type BuildHandle: Wait + Send + Sync;

    /// A handle to a package.
    type PackageId: TestPackageId;

    /// A package contains artifats.
    type Package: TestPackage<PackageId = Self::PackageId, ArtifactKey = Self::ArtifactKey>;

    /// The handle to an artifact.
    type ArtifactKey: TestArtifactKey;

    /// An artifact contains test cases and can run them.
    type Artifact: TestArtifact<
        ArtifactKey = Self::ArtifactKey,
        PackageId = Self::PackageId,
        CaseMetadata = Self::CaseMetadata,
    >;

    /// An iterator of artifacts. Yielding one can block.
    type ArtifactStream: Iterator<Item = Result<Self::Artifact>> + Send;

    /// Metadata associated with a test case.
    type CaseMetadata: TestCaseMetadata;

    /// These options are used in `start`.
    type Options;

    /// Start collecting tests for the given packages. Kick off any building that may need to
    /// happen in the background.
    /// The given `color` controls if any build output sent to the UI has color codes. The given
    /// options is used to configure that collection.
    ///
    /// The returned `BuildHandle` gives a way to wait for or get the error for any build
    /// happening. The given `ArtifactStream` gives a way to get the artifacts as they are built.
    ///
    /// This function should not block for any long amount of time.
    fn start(
        &self,
        color: bool,
        options: &Self::Options,
        packages: Vec<&Self::Package>,
        ui: &ui::UiSender,
    ) -> Result<(Self::BuildHandle, Self::ArtifactStream)>;

    /// Strip any lines out of the given output which comes from the test fixture and would just be
    /// noise.
    fn remove_fixture_output(_case_str: &str, lines: Vec<String>) -> Vec<String> {
        lines
    }

    /// Look at the given test output lines and determine if the test didn't actually run.
    fn was_test_ignored(_case_str: &str, _lines: &[String]) -> bool {
        false
    }

    /// Get all the packages for the project. This function is allowed to block.
    fn get_packages(&self, ui: &ui::UiSender) -> Result<Vec<Self::Package>>;

    /// Build any test layers that might want to be used later. This function is allowed to block.
    fn build_test_layers(&self, _images: HashSet<ImageRef>, _ui: &ui::UiSender) -> Result<()> {
        Ok(())
    }
}

/// This filter is something which describes a set of test cases.
pub trait TestFilter: Sized + FromStr<Err = anyhow::Error> {
    type Package: TestPackage;
    type ArtifactKey: TestArtifactKey;
    type CaseMetadata: TestCaseMetadata;

    fn compile(include: &[String], exclude: &[String]) -> Result<Self>;
    fn filter(
        &self,
        package: &Self::Package,
        artifact: Option<&Self::ArtifactKey>,
        case: Option<(&str, &Self::CaseMetadata)>,
    ) -> Option<bool>;
}

pub fn maybe_not(a: Option<bool>) -> Option<bool> {
    a.map(|v| !v)
}

pub fn maybe_and(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a && b),
        (None, Some(true)) => None,
        (None, Some(false)) => Some(false),
        (Some(true), None) => None,
        (Some(false), None) => Some(false),
        (None, None) => None,
    }
}

pub fn maybe_or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a || b),
        (None, Some(true)) => Some(true),
        (None, Some(false)) => None,
        (Some(true), None) => Some(true),
        (Some(false), None) => None,
        (None, None) => None,
    }
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimpleFilter {
    All,
    None,
    Name(String),
    Package(String),
    ArtifactEndsWith(String),
    Not(Box<SimpleFilter>),
    And(Vec<SimpleFilter>),
    Or(Vec<SimpleFilter>),
}

#[cfg(test)]
impl FromStr for SimpleFilter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        if s == "all" {
            Ok(Self::All)
        } else if s == "none" {
            Ok(Self::None)
        } else {
            Ok(toml::from_str(s)?)
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StringPackageId(pub String);

#[cfg(test)]
impl<'a> From<&'a str> for StringPackageId {
    fn from(s: &'a str) -> Self {
        Self(s.into())
    }
}

#[cfg(test)]
impl TestPackageId for StringPackageId {}

#[cfg(test)]
#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StringPackage(pub String);

#[cfg(test)]
impl<'a> From<&'a str> for StringPackage {
    fn from(s: &'a str) -> Self {
        Self(s.into())
    }
}

#[cfg(test)]
impl TestPackage for StringPackage {
    type PackageId = StringPackageId;
    type ArtifactKey = StringArtifactKey;

    fn name(&self) -> &str {
        &self.0
    }

    fn artifacts(&self) -> Vec<Self::ArtifactKey> {
        vec![]
    }

    fn id(&self) -> Self::PackageId {
        StringPackageId(self.0.clone())
    }
}

#[cfg(test)]
impl TestFilter for SimpleFilter {
    type Package = StringPackage;
    type ArtifactKey = StringArtifactKey;
    type CaseMetadata = NoCaseMetadata;

    fn compile(include: &[String], exclude: &[String]) -> Result<Self> {
        let include = include
            .iter()
            .map(|p| Self::from_str(p))
            .collect::<Result<Vec<_>>>()?;
        if exclude.is_empty() {
            return Ok(Self::Or(include));
        }
        let exclude = exclude
            .iter()
            .map(|p| Self::from_str(p))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::And(vec![
            Self::Or(include),
            Self::Not(Box::new(Self::And(exclude))),
        ]))
    }

    fn filter(
        &self,
        package: &StringPackage,
        artifact: Option<&Self::ArtifactKey>,
        case: Option<(&str, &NoCaseMetadata)>,
    ) -> Option<bool> {
        match self {
            Self::All => Some(true),
            Self::None => Some(false),
            Self::Name(m) => case.map(|(c, _)| c == m),
            Self::Package(m) => Some(&package.0 == m),
            Self::ArtifactEndsWith(m) => artifact.map(|a| a.0.ends_with(m)),
            Self::Not(f) => f.filter(package, artifact, case).map(|v| !v),
            #[allow(clippy::manual_try_fold)]
            Self::Or(p) => p.iter().fold(Some(false), |acc, x| {
                maybe_or(acc, x.filter(package, artifact, case))
            }),
            #[allow(clippy::manual_try_fold)]
            Self::And(p) => p.iter().fold(Some(true), |acc, x| {
                maybe_and(acc, x.filter(package, artifact, case))
            }),
        }
    }
}

pub trait MainAppDeps: Sync {
    fn client(&self) -> &maelstrom_client::Client;

    type TestCollector: CollectTests;
    fn test_collector(&self) -> &Self::TestCollector;

    fn get_template_vars(
        &self,
        options: &<Self::TestCollector as CollectTests>::Options,
    ) -> Result<TemplateVars>;

    const TEST_METADATA_FILE_NAME: &'static str;
    const DEFAULT_TEST_METADATA_CONTENTS: &'static str;
}
