use crate::{metadata::TestMetadata, BuildDir};
use anyhow::Result;
use maelstrom_base::{ArtifactType, ClientJobId, JobOutcomeResult, Sha256Digest, Utf8PathBuf};
use maelstrom_client::{
    spec::{JobSpec, Layer},
    IntrospectResponse,
};
use maelstrom_util::{root::Root, template::TemplateVars};
use std::{
    fmt,
    hash::Hash,
    path::Path,
    str::{self, FromStr},
};

pub trait Wait {
    fn wait(self) -> Result<()>;
}

pub trait ClientTrait: Sync {
    fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)>;
    fn introspect(&self) -> Result<IntrospectResponse>;
    fn add_job(
        &self,
        spec: JobSpec,
        handler: impl FnOnce(Result<(ClientJobId, JobOutcomeResult)>) + Send + Sync + 'static,
    ) -> Result<()>;
}

impl ClientTrait for maelstrom_client::Client {
    fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        maelstrom_client::Client::add_layer(self, layer)
    }

    fn introspect(&self) -> Result<IntrospectResponse> {
        maelstrom_client::Client::introspect(self)
    }

    fn add_job(
        &self,
        spec: JobSpec,
        handler: impl FnOnce(Result<(ClientJobId, JobOutcomeResult)>) + Send + Sync + 'static,
    ) -> Result<()> {
        maelstrom_client::Client::add_job(self, spec, handler)
    }
}

pub trait TestArtifactKey:
    fmt::Display + FromStr<Err = anyhow::Error> + Hash + Ord + Eq + Clone + Send + Sync + 'static
{
}

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

pub trait TestPackageId: Clone + Ord + fmt::Debug {}

pub enum TestLayers {
    GenerateForBinary,
    Provided(Vec<Layer>),
}

pub trait TestArtifact: fmt::Debug {
    type ArtifactKey: TestArtifactKey;
    type PackageId: TestPackageId;
    fn to_key(&self) -> Self::ArtifactKey;
    fn path(&self) -> &Path;
    fn list_tests(&self) -> Result<Vec<String>>;
    fn list_ignored_tests(&self) -> Result<Vec<String>>;
    fn name(&self) -> &str;
    fn package(&self) -> Self::PackageId;
    fn build_command(&self, case: &str) -> (Utf8PathBuf, Vec<String>);
}

pub trait TestPackage: Clone + fmt::Debug {
    type PackageId: TestPackageId;
    type ArtifactKey: TestArtifactKey;
    fn name(&self) -> &str;
    fn version(&self) -> &impl fmt::Display;
    fn artifacts(&self) -> Vec<Self::ArtifactKey>;
    fn id(&self) -> Self::PackageId;
}

pub trait CollectTests {
    type TestFilter: TestFilter<ArtifactKey = Self::ArtifactKey>;

    type BuildHandle: Wait;
    type PackageId: TestPackageId;
    type Package: TestPackage<PackageId = Self::PackageId, ArtifactKey = Self::ArtifactKey>;
    type ArtifactKey: TestArtifactKey;
    type Artifact: TestArtifact<ArtifactKey = Self::ArtifactKey, PackageId = Self::PackageId>;
    type ArtifactStream: Iterator<Item = Result<Self::Artifact>>;

    type Options;
    fn start(
        &self,
        color: bool,
        options: &Self::Options,
        packages: Vec<String>,
    ) -> Result<(Self::BuildHandle, Self::ArtifactStream)>;

    fn get_test_layers(&self, metadata: &TestMetadata) -> Result<TestLayers>;

    fn remove_fixture_output(_case_str: &str, lines: Vec<String>) -> Vec<String> {
        lines
    }
}

pub trait TestFilter: Sized + FromStr<Err = anyhow::Error> {
    type ArtifactKey: TestArtifactKey;
    fn compile(include: &[String], exclude: &[String]) -> Result<Self>;
    fn filter(
        &self,
        package: &str,
        artifact: Option<&Self::ArtifactKey>,
        case: Option<&str>,
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
impl TestFilter for SimpleFilter {
    type ArtifactKey = StringArtifactKey;

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
        package: &str,
        artifact: Option<&Self::ArtifactKey>,
        case: Option<&str>,
    ) -> Option<bool> {
        match self {
            Self::All => Some(true),
            Self::None => Some(false),
            Self::Name(m) => case.map(|c| c == m),
            Self::Package(m) => Some(package == m),
            Self::ArtifactEndsWith(m) => artifact.map(|a| a.0.ends_with(m)),
            Self::Not(f) => f.filter(package, artifact, case).map(|v| !v),
            Self::Or(p) => p.into_iter().fold(Some(false), |acc, x| {
                maybe_or(acc, x.filter(package, artifact, case))
            }),
            Self::And(p) => p.into_iter().fold(Some(true), |acc, x| {
                maybe_and(acc, x.filter(package, artifact, case))
            }),
        }
    }
}

pub trait MainAppDeps: Sync {
    type Client: ClientTrait;
    fn client(&self) -> &Self::Client;

    type TestCollector: CollectTests;
    fn test_collector(&self) -> &Self::TestCollector;

    fn get_template_vars(
        &self,
        options: &<Self::TestCollector as CollectTests>::Options,
        target_dir: &Root<BuildDir>,
    ) -> Result<TemplateVars>;

    const MAELSTROM_TEST_TOML: &'static str;
}
