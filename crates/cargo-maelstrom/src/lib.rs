pub mod alternative_mains;
pub mod cargo;
pub mod cli;
pub mod config;
pub mod pattern;

use anyhow::{anyhow, bail, Context as _, Result};
use cargo_metadata::{Metadata as CargoMetadata, Target as CargoTarget};
use maelstrom_base::{Timeout, Utf8PathBuf};
use maelstrom_client::{
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, Client, ClientBgProcess,
    ContainerImageDepotDir, ProjectDir, StateDir,
};
use maelstrom_test_runner::{
    metadata::TestMetadata, run_app_with_ui_multithreaded, ui::Ui, ui::UiSender, BuildDir,
    CollectTests, ListAction, LoggingOutput, MainAppDeps, MainAppState, NoCaseMetadata,
    TestArtifact, TestArtifactKey, TestFilter, TestLayers, TestPackage, TestPackageId, Wait,
};
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, Slots},
    fs::Fs,
    process::ExitCode,
    root::{Root, RootBuf},
    template::TemplateVars,
};
use pattern::ArtifactKind;
use std::path::Path;
use std::str::FromStr;
use std::{fmt, io};

pub use maelstrom_test_runner::Logger;

pub const MAELSTROM_TEST_TOML: &str = "cargo-maelstrom.toml";

/// The Maelstrom target directory is <target-dir>/maelstrom.
pub struct MaelstromTargetDir;

struct DefaultMainAppDeps {
    client: Client,
    test_collector: CargoTestCollector,
    target_dir: RootBuf<BuildDir>,
}

impl DefaultMainAppDeps {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bg_proc: ClientBgProcess,
        broker_addr: Option<BrokerAddr>,
        project_dir: impl AsRef<Root<ProjectDir>>,
        state_dir: impl AsRef<Root<StateDir>>,
        container_image_depot_dir: impl AsRef<Root<ContainerImageDepotDir>>,
        cache_dir: impl AsRef<Root<CacheDir>>,
        target_dir: impl AsRef<Root<BuildDir>>,
        cache_size: CacheSize,
        inline_limit: InlineLimit,
        slots: Slots,
        accept_invalid_remote_container_tls_certs: AcceptInvalidRemoteContainerTlsCerts,
        log: slog::Logger,
    ) -> Result<Self> {
        let project_dir = project_dir.as_ref();
        let state_dir = state_dir.as_ref();
        let container_image_depot_dir = container_image_depot_dir.as_ref();
        let cache_dir = cache_dir.as_ref();
        slog::debug!(
            log, "creating app dependencies";
            "broker_addr" => ?broker_addr,
            "project_dir" => ?project_dir,
            "state_dir" => ?state_dir,
            "container_image_depot_dir" => ?container_image_depot_dir,
            "cache_dir" => ?cache_dir,
            "cache_size" => ?cache_size,
            "inline_limit" => ?inline_limit,
            "slots" => ?slots,
        );
        let client = Client::new(
            bg_proc,
            broker_addr,
            project_dir,
            state_dir,
            container_image_depot_dir,
            cache_dir,
            cache_size,
            inline_limit,
            slots,
            accept_invalid_remote_container_tls_certs,
            log,
        )?;
        Ok(Self {
            client,
            test_collector: CargoTestCollector,
            target_dir: target_dir.as_ref().to_owned(),
        })
    }
}

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

impl TestFilter for pattern::Pattern {
    type ArtifactKey = CargoArtifactKey;
    type CaseMetadata = NoCaseMetadata;

    fn compile(include: &[String], exclude: &[String]) -> Result<Self> {
        pattern::compile_filter(include, exclude)
    }

    fn filter(
        &self,
        package: &str,
        artifact: Option<&CargoArtifactKey>,
        case: Option<(&str, &NoCaseMetadata)>,
    ) -> Option<bool> {
        let c = pattern::Context {
            package: package.into(),
            artifact: artifact.map(|a| pattern::Artifact {
                name: a.name.clone(),
                kind: a.kind,
            }),
            case: case.map(|(case, _)| pattern::Case { name: case.into() }),
        };
        pattern::interpret_pattern(self, &c)
    }
}

struct CargoOptions {
    feature_selection_options: cargo::FeatureSelectionOptions,
    compilation_options: cargo::CompilationOptions,
    manifest_options: cargo::ManifestOptions,
}

struct CargoTestCollector;

#[derive(Debug)]
struct CargoTestArtifact(cargo_metadata::Artifact);

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
struct CargoPackageId(cargo_metadata::PackageId);

impl TestPackageId for CargoPackageId {}

impl TestArtifact for CargoTestArtifact {
    type ArtifactKey = CargoArtifactKey;
    type PackageId = CargoPackageId;
    type CaseMetadata = NoCaseMetadata;

    fn package(&self) -> CargoPackageId {
        CargoPackageId(self.0.package_id.clone())
    }

    fn to_key(&self) -> CargoArtifactKey {
        CargoArtifactKey::from(&self.0.target)
    }

    fn path(&self) -> &Path {
        self.0.executable.as_ref().unwrap().as_ref()
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

    fn name(&self) -> &str {
        &self.0.target.name
    }

    fn build_command(
        &self,
        case_name: &str,
        _case_metadata: &NoCaseMetadata,
    ) -> (Utf8PathBuf, Vec<String>) {
        let binary_name = self.path().file_name().unwrap().to_str().unwrap();
        (
            format!("/{binary_name}").into(),
            vec!["--exact".into(), "--nocapture".into(), case_name.into()],
        )
    }

    fn format_case(
        &self,
        package_name: &str,
        case_name: &str,
        _case_metadata: &NoCaseMetadata,
    ) -> String {
        let mut s = package_name.to_string();
        s += " ";

        let artifact_name = self.name();
        if artifact_name.replace('_', "-") != package_name {
            s += artifact_name;
            s += " ";
        }
        s += case_name;
        s
    }
}

struct CargoTestArtifactStream(cargo::TestArtifactStream);

impl Iterator for CargoTestArtifactStream {
    type Item = Result<CargoTestArtifact>;

    fn next(&mut self) -> Option<Result<CargoTestArtifact>> {
        match self.0.next() {
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(v)) => Some(Ok(CargoTestArtifact(v))),
            None => None,
        }
    }
}

#[derive(Clone, Debug)]
struct CargoPackage(cargo_metadata::Package);

impl CargoPackage {
    fn version(&self) -> &impl fmt::Display {
        &self.0.version
    }
}

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
        let packages: Vec<_> = packages
            .into_iter()
            .map(|p| format!("{}@{}", p.name(), p.version()))
            .collect();

        let (handle, stream) = cargo::run_cargo_test(
            color,
            &options.feature_selection_options,
            &options.compilation_options,
            &options.manifest_options,
            packages,
            ui.clone(),
        )?;
        Ok((handle, CargoTestArtifactStream(stream)))
    }

    fn get_test_layers(&self, _metadata: &TestMetadata, _ind: &UiSender) -> Result<TestLayers> {
        Ok(TestLayers::GenerateForBinary)
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

impl MainAppDeps for DefaultMainAppDeps {
    type Client = Client;

    fn client(&self) -> &Client {
        &self.client
    }

    type TestCollector = CargoTestCollector;

    fn test_collector(&self) -> &CargoTestCollector {
        &self.test_collector
    }

    fn get_template_vars(&self, cargo_options: &CargoOptions) -> Result<TemplateVars> {
        let profile = cargo_options
            .compilation_options
            .profile
            .clone()
            .unwrap_or("dev".into());
        let mut target = (**self.target_dir).to_owned();
        match profile.as_str() {
            "dev" => target.push("debug"),
            other => target.push(other),
        }
        let build_dir = target
            .to_str()
            .ok_or_else(|| anyhow!("{} contains non-UTF8", target.display()))?;
        Ok(TemplateVars::new()
            .with_var("build-dir", build_dir)
            .unwrap())
    }

    const MAELSTROM_TEST_TOML: &'static str = MAELSTROM_TEST_TOML;
}

impl Wait for cargo::WaitHandle {
    fn wait(self) -> Result<()> {
        cargo::WaitHandle::wait(self)
    }
}

fn maybe_print_build_error(res: Result<ExitCode>) -> Result<ExitCode> {
    if let Err(e) = &res {
        if let Some(e) = e.downcast_ref::<cargo::CargoBuildError>() {
            eprintln!("{}", &e.stderr);
            return Ok(e.exit_code);
        }
    }
    res
}

fn read_cargo_metadata(config: &config::Config) -> Result<CargoMetadata> {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version=1"])
        .args(config.cargo_feature_selection_options.iter())
        .args(config.cargo_manifest_options.iter())
        .output()
        .context("getting cargo metadata")?;
    if !output.status.success() {
        bail!(String::from_utf8(output.stderr)
            .context("reading stderr")?
            .trim_end()
            .trim_start_matches("error: ")
            .to_owned());
    }
    let cargo_metadata: CargoMetadata =
        serde_json::from_slice(&output.stdout).context("parsing cargo metadata")?;
    Ok(cargo_metadata)
}

pub fn main(
    config: config::Config,
    extra_options: cli::ExtraCommandLineOptions,
    bg_proc: ClientBgProcess,
    logger: Logger,
    stderr_is_tty: bool,
    ui: impl Ui,
) -> Result<ExitCode> {
    if extra_options.parent.init {
        let cargo_metadata = read_cargo_metadata(&config)?;
        alternative_mains::init(&cargo_metadata.workspace_root)
    } else if extra_options.list.packages {
        let cargo_metadata = read_cargo_metadata(&config)?;
        alternative_mains::list_packages(
            &cargo_metadata.workspace_packages(),
            &extra_options.parent.include,
            &extra_options.parent.exclude,
            &mut io::stdout().lock(),
        )
    } else if extra_options.list.binaries {
        let cargo_metadata = read_cargo_metadata(&config)?;
        alternative_mains::list_binaries(
            &cargo_metadata.workspace_packages(),
            &extra_options.parent.include,
            &extra_options.parent.exclude,
            &mut io::stdout().lock(),
        )
    } else {
        let cargo_metadata = read_cargo_metadata(&config)?;
        let workspace_dir = Root::<ProjectDir>::new(cargo_metadata.workspace_root.as_std_path());
        let logging_output = LoggingOutput::default();
        let log = logger.build(logging_output.clone());

        let list_action = extra_options.list.tests.then_some(ListAction::ListTests);
        let target_dir = Root::<BuildDir>::new(cargo_metadata.target_directory.as_std_path());
        let maelstrom_target_dir = target_dir.join::<MaelstromTargetDir>("maelstrom");
        let state_dir = maelstrom_target_dir.join::<StateDir>("state");
        let cache_dir = maelstrom_target_dir.join::<CacheDir>("cache");

        Fs.create_dir_all(&state_dir)?;
        Fs.create_dir_all(&cache_dir)?;

        let deps = DefaultMainAppDeps::new(
            bg_proc,
            config.parent.broker,
            workspace_dir.transmute::<ProjectDir>(),
            &state_dir,
            config.parent.container_image_depot_root,
            cache_dir,
            target_dir,
            config.parent.cache_size,
            config.parent.inline_limit,
            config.parent.slots,
            config.parent.accept_invalid_remote_container_tls_certs,
            log.clone(),
        )?;

        let cargo_options = CargoOptions {
            feature_selection_options: config.cargo_feature_selection_options,
            compilation_options: config.cargo_compilation_options,
            manifest_options: config.cargo_manifest_options,
        };
        let state = MainAppState::new(
            deps,
            extra_options.parent.include,
            extra_options.parent.exclude,
            list_action,
            stderr_is_tty,
            workspace_dir,
            &cargo_metadata
                .workspace_packages()
                .into_iter()
                .map(|p| CargoPackage(p.clone()))
                .collect::<Vec<_>>(),
            &state_dir,
            cargo_options,
            logging_output,
            log,
        )?;

        let res = run_app_with_ui_multithreaded(state, config.parent.timeout.map(Timeout::new), ui);
        maybe_print_build_error(res)
    }
}
