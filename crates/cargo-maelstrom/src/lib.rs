mod alternative_mains;
pub mod cargo;
pub mod cli;
pub mod config;
mod pattern;

pub use maelstrom_test_runner::log::LoggerBuilder;

use anyhow::{anyhow, Result};
use cargo_metadata::{Metadata as CargoMetadata, Target as CargoTarget};
use cli::{ExtraCommandLineOptions, ListOptions};
use config::Config;
use maelstrom_base::{Utf8Path, Utf8PathBuf};
use maelstrom_client::{
    shared_library_dependencies_layer_spec,
    spec::{LayerSpec, PathsLayerSpec, PrefixOptions},
    Client,
};
use maelstrom_test_runner::{
    config::IntoParts, metadata::Metadata, ui::UiSender, CollectTests, Directories, ListingType,
    NoCaseMetadata, TestArtifact, TestArtifactKey, TestFilter, TestPackage, TestPackageId, Wait,
    WaitStatus,
};
use maelstrom_util::{process::ExitCode, root::Root, template::TemplateVars};
use pattern::ArtifactKind;
use std::{
    fmt, io,
    path::{Path, PathBuf},
    str::FromStr,
};

/// The Maelstrom target directory is `<target-dir>/maelstrom`.
pub struct MaelstromTargetDir;

const MISSING_RIGHT_PAREN: &str = "last character was not ')'";
const MISSING_LEFT_PAREN: &str = "could not find opening '('";

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CargoArtifactKey {
    pub name: String,
    pub kind: ArtifactKind,
}

impl TestArtifactKey for CargoArtifactKey {}

impl CargoArtifactKey {
    pub fn new(name: impl Into<String>, kind: ArtifactKind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }
}

impl From<&CargoTarget> for CargoArtifactKey {
    fn from(target: &CargoTarget) -> Self {
        Self::new(&target.name, ArtifactKind::from_target(target))
    }
}

impl fmt::Display for CargoArtifactKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format!("{}({})", self.name, self.kind).fmt(f)
    }
}

impl FromStr for CargoArtifactKey {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let Some(s) = s.strip_suffix(')') else {
            return Err(anyhow!("{MISSING_RIGHT_PAREN}"));
        };
        let Some((name, kind)) = s.rsplit_once('(') else {
            return Err(anyhow!("{MISSING_LEFT_PAREN}"));
        };
        let kind = ArtifactKind::from_str(kind)?;
        let name = name.to_owned();
        Ok(Self { name, kind })
    }
}

impl TestFilter for pattern::Pattern {
    type Package = CargoPackage;
    type ArtifactKey = CargoArtifactKey;
    type CaseMetadata = NoCaseMetadata;

    fn compile(include: &[String], exclude: &[String]) -> Result<Self> {
        pattern::compile_filter(include, exclude)
    }

    fn filter(
        &self,
        package: &CargoPackage,
        artifact: Option<&CargoArtifactKey>,
        case: Option<(&str, &NoCaseMetadata)>,
    ) -> Option<bool> {
        let c = pattern::Context {
            package: package.name().into(),
            artifact: artifact.map(|a| pattern::Artifact {
                name: a.name.clone(),
                kind: a.kind,
            }),
            case: case.map(|(case, _)| pattern::Case { name: case.into() }),
        };
        pattern::interpret_pattern(self, &c)
    }
}

pub struct CargoOptions {
    feature_selection_options: cargo::FeatureSelectionOptions,
    compilation_options: cargo::CompilationOptions,
    manifest_options: cargo::ManifestOptions,
    extra_test_binary_args: Vec<String>,
}

pub struct CargoTestCollector {
    log: slog::Logger,
    packages: Vec<CargoPackage>,
}

#[derive(Debug)]
pub struct CargoTestArtifact {
    artifact: cargo_metadata::Artifact,
    extra_test_binary_args: Vec<String>,
}

impl CargoTestArtifact {
    fn utf8_path(&self) -> &Utf8Path {
        self.artifact.executable.as_ref().unwrap().as_ref()
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct CargoPackageId(cargo_metadata::PackageId);

impl TestPackageId for CargoPackageId {}

impl TestArtifact for CargoTestArtifact {
    type ArtifactKey = CargoArtifactKey;
    type PackageId = CargoPackageId;
    type CaseMetadata = NoCaseMetadata;

    fn package(&self) -> CargoPackageId {
        CargoPackageId(self.artifact.package_id.clone())
    }

    fn to_key(&self) -> CargoArtifactKey {
        CargoArtifactKey::from(&self.artifact.target)
    }

    fn path(&self) -> &Path {
        self.utf8_path().as_ref()
    }

    fn list_tests(&self) -> Result<Vec<(String, NoCaseMetadata)>> {
        Ok(cargo::get_cases_from_binary(self.path(), &None)?
            .into_iter()
            .map(|case| (case, NoCaseMetadata))
            .collect())
    }

    fn list_ignored_tests(&self) -> Result<Vec<String>> {
        cargo::get_cases_from_binary(self.path(), &Some("--ignored".into()))
    }

    fn build_command(
        &self,
        case_name: &str,
        _case_metadata: &NoCaseMetadata,
    ) -> (Utf8PathBuf, Vec<String>) {
        let binary_name = self.path().file_name().unwrap().to_str().unwrap();
        let mut args = vec!["--exact".into(), "--nocapture".into()];
        args.extend(self.extra_test_binary_args.clone());
        args.push(case_name.into());
        (format!("/{binary_name}").into(), args)
    }

    fn format_case(
        &self,
        package_name: &str,
        case_name: &str,
        _case_metadata: &NoCaseMetadata,
    ) -> String {
        let mut s = package_name.to_string();
        s += " ";

        let artifact_name = &self.artifact.target.name;
        if artifact_name.replace('_', "-") != package_name {
            s += artifact_name;
            s += " ";
        }
        s += case_name;
        s
    }

    fn get_test_layers(&self, metadata: &Metadata) -> Vec<LayerSpec> {
        let mut layers = vec![path_layer_for_binary(self.utf8_path())];

        if metadata.include_shared_libraries {
            layers.push(so_layer_for_binary(self.utf8_path()));
        }

        layers
    }
}

pub struct CargoTestArtifactStream {
    stream: cargo::TestArtifactStream,
    extra_test_binary_args: Vec<String>,
}

impl Iterator for CargoTestArtifactStream {
    type Item = Result<CargoTestArtifact>;

    fn next(&mut self) -> Option<Result<CargoTestArtifact>> {
        match self.stream.next() {
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(artifact)) => Some(Ok(CargoTestArtifact {
                artifact,
                extra_test_binary_args: self.extra_test_binary_args.clone(),
            })),
            None => None,
        }
    }
}

fn path_layer_for_binary(binary_path: &Utf8Path) -> LayerSpec {
    LayerSpec::Paths(PathsLayerSpec {
        paths: vec![binary_path.to_path_buf()],
        prefix_options: PrefixOptions {
            strip_prefix: Some(binary_path.parent().unwrap().to_path_buf()),
            ..Default::default()
        },
    })
}

fn so_layer_for_binary(binary_path: &Utf8Path) -> LayerSpec {
    shared_library_dependencies_layer_spec! {
        [binary_path],
        follow_symlinks: true,
    }
}

#[derive(Clone, Debug)]
pub struct CargoPackage(cargo_metadata::Package);

impl TestPackage for CargoPackage {
    type PackageId = CargoPackageId;
    type ArtifactKey = CargoArtifactKey;

    fn name(&self) -> &str {
        &self.0.name
    }

    fn artifacts(&self) -> Vec<CargoArtifactKey> {
        self.0.targets.iter().map(CargoArtifactKey::from).collect()
    }

    fn id(&self) -> CargoPackageId {
        CargoPackageId(self.0.id.clone())
    }
}

impl CollectTests for CargoTestCollector {
    const ENQUEUE_MESSAGE: &'static str = "building artifacts...";

    type BuildHandle = cargo::WaitHandle;
    type Artifact = CargoTestArtifact;
    type ArtifactStream = CargoTestArtifactStream;
    type TestFilter = pattern::Pattern;
    type PackageId = CargoPackageId;
    type Package = CargoPackage;
    type ArtifactKey = CargoArtifactKey;
    type Options = CargoOptions;
    type CaseMetadata = NoCaseMetadata;

    fn start(
        &self,
        color: bool,
        options: &CargoOptions,
        packages: Vec<&CargoPackage>,
        ui: &UiSender,
    ) -> Result<(cargo::WaitHandle, CargoTestArtifactStream)> {
        let packages: Vec<_> = packages.into_iter().map(|p| &p.0).collect();
        let (handle, stream) = cargo::run_cargo_test(
            color,
            &options.feature_selection_options,
            &options.compilation_options,
            &options.manifest_options,
            packages,
            ui.downgrade(),
            self.log.clone(),
        )?;
        Ok((
            handle,
            CargoTestArtifactStream {
                stream,
                extra_test_binary_args: options.extra_test_binary_args.clone(),
            },
        ))
    }

    fn get_packages(&self, _ui: &UiSender) -> Result<Vec<CargoPackage>> {
        Ok(self.packages.clone())
    }

    /// The Rust std test fixture prints out some output like "running 1 test" etc. This isn't very
    /// useful, so we want to strip it out.
    fn remove_fixture_output(case_str: &str, mut lines: Vec<String>) -> Vec<String> {
        if let Some(pos) = lines.iter().position(|s| s.as_str() == "running 1 test") {
            lines = lines[(pos + 1)..].to_vec();
        }
        if let Some(pos) = lines
            .iter()
            .rposition(|s| s.as_str().starts_with(&format!("test {case_str} ... ")))
        {
            lines = lines[..pos].to_vec();
        }
        lines
    }
}

impl Wait for cargo::WaitHandle {
    fn wait(&self) -> Result<WaitStatus> {
        cargo::WaitHandle::wait(self)
    }

    fn kill(&self) -> Result<()> {
        cargo::WaitHandle::kill(self)
    }
}

pub struct TestRunner;

impl TestRunner {
    fn get_cargo_metadata(config: &Config) -> Result<CargoMetadata> {
        cargo::read_metadata(
            &config.cargo_feature_selection_options,
            &config.cargo_manifest_options,
        )
    }
}

impl maelstrom_test_runner::TestRunner for TestRunner {
    type Config = Config;
    type ExtraCommandLineOptions = ExtraCommandLineOptions;
    type Metadata = CargoMetadata;
    type TestCollector<'client> = CargoTestCollector;
    type CollectorOptions = CargoOptions;

    const BASE_DIRECTORIES_PREFIX: &'static str = "maelstrom/cargo-maelstrom";
    const ENVIRONMENT_VARIABLE_PREFIX: &'static str = "CARGO_MAELSTROM";
    const TEST_METADATA_FILE_NAME: &'static str = "cargo-maelstrom.toml";
    const DEFAULT_TEST_METADATA_FILE_CONTENTS: &'static str =
        include_str!("default-test-metadata.toml");

    fn get_listing_type(extra_options: &ExtraCommandLineOptions) -> ListingType {
        match &extra_options.list {
            ListOptions {
                tests: true,
                binaries: false,
                packages: false,
            } => ListingType::Tests,
            ListOptions {
                tests: false,
                binaries: true,
                packages: false,
            }
            | ListOptions {
                tests: false,
                binaries: false,
                packages: true,
            } => ListingType::OtherWithoutUi,
            ListOptions {
                tests: false,
                binaries: false,
                packages: false,
            } => ListingType::None,
            options => {
                panic!("invalid ListOptions {options:?}, clap should have disallowed");
            }
        }
    }

    fn get_directories_and_metadata(config: &Config) -> Result<(Directories, CargoMetadata)> {
        let cargo_metadata = Self::get_cargo_metadata(config)?;
        let project = Root::new(cargo_metadata.workspace_root.as_std_path()).to_owned();
        let build = Root::new(cargo_metadata.target_directory.as_std_path()).to_owned();
        let maelstrom_target = build.join::<MaelstromTargetDir>("maelstrom");
        let state = maelstrom_target.join("state");
        let cache = maelstrom_target.join("cache");
        Ok((
            Directories {
                build,
                cache,
                state,
                project,
            },
            cargo_metadata,
        ))
    }

    fn execute_listing_without_ui(
        config: &Config,
        extra_options: &ExtraCommandLineOptions,
    ) -> Result<ExitCode> {
        assert!(!extra_options.list.tests);
        assert!(extra_options.list.binaries || extra_options.list.packages);
        assert!(!(extra_options.list.binaries && extra_options.list.packages));

        match &extra_options.list {
            ListOptions {
                tests: false,
                binaries: true,
                packages: false,
            } => alternative_mains::list_binaries(
                &Self::get_cargo_metadata(config)?.workspace_packages(),
                &extra_options.parent.include,
                &extra_options.parent.exclude,
                &mut io::stdout().lock(),
            ),
            ListOptions {
                tests: false,
                binaries: false,
                packages: true,
            } => alternative_mains::list_packages(
                &Self::get_cargo_metadata(config)?.workspace_packages(),
                &extra_options.parent.include,
                &extra_options.parent.exclude,
                &mut io::stdout().lock(),
            ),
            options => {
                panic!("invalid ListOptions {options:?}");
            }
        }
    }

    fn get_test_collector(
        _client: &Client,
        _directories: &Directories,
        log: &slog::Logger,
        metadata: CargoMetadata,
    ) -> Result<CargoTestCollector> {
        Ok(CargoTestCollector {
            log: log.clone(),
            packages: metadata
                .workspace_packages()
                .into_iter()
                .map(|p| CargoPackage(p.clone()))
                .collect(),
        })
    }

    fn get_watch_exclude_paths(directories: &Directories) -> Vec<PathBuf> {
        vec![directories.build.to_owned().into_path_buf()]
    }

    fn split_config(config: Config) -> (maelstrom_test_runner::config::Config, CargoOptions) {
        config.into_parts()
    }

    fn extra_options_into_parent(
        extra_options: ExtraCommandLineOptions,
    ) -> maelstrom_test_runner::config::ExtraCommandLineOptions {
        extra_options.parent
    }

    fn get_template_vars(
        cargo_options: &CargoOptions,
        directories: &Directories,
    ) -> Result<TemplateVars> {
        let profile = cargo_options
            .compilation_options
            .profile
            .clone()
            .unwrap_or("dev".into());
        let target = match profile.as_str() {
            "dev" => directories.build.join::<()>("debug"),
            other => directories.build.join::<()>(other),
        };
        let build_dir = target
            .to_str()
            .ok_or_else(|| anyhow!("{} contains non-UTF8", target.display()))?;
        Ok(TemplateVars::new([("build-dir", build_dir)]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maelstrom_test_runner::TestRunner as _;

    #[test]
    fn cargo_artifact_key_display() {
        let key = CargoArtifactKey::new("foo", ArtifactKind::Library);
        assert_eq!(format!("{key}"), "foo(library)");
    }

    #[test]
    fn cargo_artifact_key_from_str_empty_string() {
        let err = CargoArtifactKey::from_str("").unwrap_err();
        assert_eq!(err.to_string(), MISSING_RIGHT_PAREN);
    }

    #[test]
    fn cargo_artifact_key_from_str_no_right_paren() {
        let err = CargoArtifactKey::from_str("foo bar").unwrap_err();
        assert_eq!(err.to_string(), MISSING_RIGHT_PAREN);
    }

    #[test]
    fn cargo_artifact_key_from_str_no_left_paren() {
        let err = CargoArtifactKey::from_str("bar)").unwrap_err();
        assert_eq!(err.to_string(), MISSING_LEFT_PAREN);
    }

    #[test]
    fn cargo_artifact_key_from_str_bad_kind() {
        let err = CargoArtifactKey::from_str("foo(not-a-valid-kind)").unwrap_err();
        assert_eq!(err.to_string(), "Matching variant not found");
    }

    #[test]
    fn cargo_artifact_key_from_str_good() {
        let key = CargoArtifactKey::from_str("foo(library)").unwrap();
        assert_eq!(key, CargoArtifactKey::new("foo", ArtifactKind::Library));
    }

    #[test]
    fn remove_fixture_output_basic_case() {
        let example = "\n\
            running 1 test\n\
            this is some output from the test\n\
            this is too\n\
            test tests::i_be_failing ... FAILED\n\
            \n\
            failures:\n\
            \n\
            failures:\n\
                tests::i_be_failing\n\
                \n\
            test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 155 filtered out; \
            finished in 0.01s\n\
            \n\
            ";
        let cleansed = CargoTestCollector::remove_fixture_output(
            "tests::i_be_failing",
            example.split('\n').map(ToOwned::to_owned).collect(),
        );
        assert_eq!(
            cleansed.join("\n") + "\n",
            "\
            this is some output from the test\n\
            this is too\n\
            "
        );
    }

    #[test]
    fn remove_fixture_output_confusing_trailer() {
        let example = "\n\
            running 1 test\n\
            this is some output from the test\n\
            test tests::i_be_failing ... this is the test's own weird output\n\
            this is too\n\
            test tests::i_be_failing ... FAILED\n\
            \n\
            failures:\n\
            \n\
            failures:\n\
                tests::i_be_failing\n\
                \n\
            test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 155 filtered out; \
            finished in 0.01s\n\
            \n\
            ";
        let cleansed = CargoTestCollector::remove_fixture_output(
            "tests::i_be_failing",
            example.split('\n').map(ToOwned::to_owned).collect(),
        );
        assert_eq!(
            cleansed.join("\n") + "\n",
            "\
            this is some output from the test\n\
            test tests::i_be_failing ... this is the test's own weird output\n\
            this is too\n\
            "
        );
    }

    #[test]
    fn default_test_metadata_parses() {
        maelstrom_test_runner::metadata::Store::<pattern::Pattern>::load(
            TestRunner::DEFAULT_TEST_METADATA_FILE_CONTENTS,
            &Default::default(),
        )
        .unwrap();
    }
}
