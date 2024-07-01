mod alternative_mains;
pub mod cli;
mod go_test;
pub mod pattern;

use anyhow::{Context as _, Result};
use maelstrom_base::{Timeout, Utf8PathBuf};
use maelstrom_client::{
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, Client, ClientBgProcess,
    ContainerImageDepotDir, ProjectDir, StateDir,
};
use maelstrom_macro::Config;
use maelstrom_test_runner::{
    metadata::TestMetadata, run_app_with_ui_multithreaded, ui::Ui, ui::UiSender, BuildDir,
    CollectTests, ListAction, LoggingOutput, MainAppDeps, MainAppState, NoCaseMetadata,
    TestArtifact, TestArtifactKey, TestFilter, TestLayers, TestPackage, TestPackageId, Wait,
};
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, Slots},
    fs::Fs,
    process::ExitCode,
    root::Root,
    template::TemplateVars,
};
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::{fmt, io};

pub use maelstrom_test_runner::Logger;

#[derive(Config, Debug)]
pub struct Config {
    #[config(flatten)]
    pub parent: maelstrom_test_runner::config::Config,
}

impl AsRef<maelstrom_test_runner::config::Config> for Config {
    fn as_ref(&self) -> &maelstrom_test_runner::config::Config {
        &self.parent
    }
}

pub const MAELSTROM_TEST_TOML: &str = "maelstrom-go-test.toml";

/// The Maelstrom target directory is <target-dir>/maelstrom.
pub struct MaelstromTargetDir;

#[allow(clippy::too_many_arguments)]
fn create_client(
    bg_proc: ClientBgProcess,
    broker_addr: Option<BrokerAddr>,
    project_dir: impl AsRef<Root<ProjectDir>>,
    state_dir: impl AsRef<Root<StateDir>>,
    container_image_depot_dir: impl AsRef<Root<ContainerImageDepotDir>>,
    cache_dir: impl AsRef<Root<CacheDir>>,
    cache_size: CacheSize,
    inline_limit: InlineLimit,
    slots: Slots,
    accept_invalid_remote_container_tls_certs: AcceptInvalidRemoteContainerTlsCerts,
    log: slog::Logger,
) -> Result<Client> {
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
    Client::new(
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
    )
}

struct DefaultMainAppDeps<'client> {
    client: &'client Client,
    test_collector: GoTestCollector,
}

impl<'client> DefaultMainAppDeps<'client> {
    pub fn new(client: &'client Client) -> Result<Self> {
        Ok(Self {
            client,
            test_collector: GoTestCollector,
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GoTestArtifactKey {
    name: String,
}

impl TestArtifactKey for GoTestArtifactKey {}

impl fmt::Display for GoTestArtifactKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)
    }
}

impl FromStr for GoTestArtifactKey {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(Self { name: s.into() })
    }
}

impl TestFilter for pattern::Pattern {
    type ArtifactKey = GoTestArtifactKey;
    type CaseMetadata = NoCaseMetadata;

    fn compile(include: &[String], exclude: &[String]) -> Result<Self> {
        pattern::compile_filter(include, exclude)
    }

    fn filter(
        &self,
        package: &str,
        _artifact: Option<&GoTestArtifactKey>,
        case: Option<(&str, &NoCaseMetadata)>,
    ) -> Option<bool> {
        let c = pattern::Context {
            package: package.into(),
            file: None,
            case: case.map(|(case, _)| pattern::Case { name: case.into() }),
        };
        pattern::interpret_pattern(self, &c)
    }
}

struct GoTestCollector;

#[derive(Debug)]
pub(crate) struct GoTestArtifact {
    id: GoPackageId,
    path: PathBuf,
}

#[derive(Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub(crate) struct GoPackageId(String);

impl GoPackageId {
    fn short_name(&self) -> &str {
        let mut comp = self.0.split('/').collect::<Vec<&str>>().into_iter().rev();
        let last = comp.next().unwrap();

        let version_re = regex::Regex::new("^v[0-9]*$").unwrap();
        if version_re.is_match(last) {
            comp.next().unwrap()
        } else {
            last
        }
    }
}

#[test]
fn short_name() {
    assert_eq!(GoPackageId("github.com/foo/bar".into()).short_name(), "bar");
    assert_eq!(GoPackageId("github.com/foo/v1".into()).short_name(), "foo");
    assert_eq!(GoPackageId("github.com/foo/v1a".into()).short_name(), "v1a");
}

impl TestPackageId for GoPackageId {}

impl TestArtifact for GoTestArtifact {
    type ArtifactKey = GoTestArtifactKey;
    type PackageId = GoPackageId;
    type CaseMetadata = NoCaseMetadata;

    fn package(&self) -> GoPackageId {
        self.id.clone()
    }

    fn to_key(&self) -> GoTestArtifactKey {
        GoTestArtifactKey {
            name: "test".into(),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn list_tests(&self) -> Result<Vec<(String, NoCaseMetadata)>> {
        Ok(go_test::get_cases_from_binary(self.path(), &None)?
            .into_iter()
            .map(|case| (case, NoCaseMetadata))
            .collect())
    }

    fn list_ignored_tests(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }

    fn name(&self) -> &str {
        self.id.short_name()
    }

    fn build_command(
        &self,
        case_name: &str,
        _case_metadata: &NoCaseMetadata,
    ) -> (Utf8PathBuf, Vec<String>) {
        let binary_name = self.path().file_name().unwrap().to_str().unwrap();
        (
            format!("/{binary_name}").into(),
            vec!["-test.run".into(), case_name.into()],
        )
    }

    fn format_case(
        &self,
        package_name: &str,
        case_name: &str,
        _case_metadata: &NoCaseMetadata,
    ) -> String {
        format!("{package_name} {case_name}")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GoPackage {
    id: GoPackageId,
    package_dir: PathBuf,
}

impl TestPackage for GoPackage {
    type PackageId = GoPackageId;
    type ArtifactKey = GoTestArtifactKey;

    fn name(&self) -> &str {
        &self.id.0
    }

    fn artifacts(&self) -> Vec<GoTestArtifactKey> {
        vec![GoTestArtifactKey {
            name: "test".into(),
        }]
    }

    fn id(&self) -> GoPackageId {
        self.id.clone()
    }
}

struct GoTestOptions;

impl CollectTests for GoTestCollector {
    const ENQUEUE_MESSAGE: &'static str = "building artifacts...";

    type BuildHandle = go_test::WaitHandle;
    type Artifact = GoTestArtifact;
    type ArtifactStream = go_test::TestArtifactStream;
    type TestFilter = pattern::Pattern;
    type PackageId = GoPackageId;
    type Package = GoPackage;
    type ArtifactKey = GoTestArtifactKey;
    type Options = GoTestOptions;
    type CaseMetadata = NoCaseMetadata;

    fn start(
        &self,
        color: bool,
        _options: &GoTestOptions,
        packages: Vec<&GoPackage>,
        _ui: &UiSender,
    ) -> Result<(go_test::WaitHandle, go_test::TestArtifactStream)> {
        go_test::build_and_collect(color, packages)
    }

    fn get_test_layers(&self, _metadata: &TestMetadata, _ind: &UiSender) -> Result<TestLayers> {
        Ok(TestLayers::GenerateForBinary)
    }

    fn remove_fixture_output(_case_str: &str, lines: Vec<String>) -> Vec<String> {
        lines
    }
}

impl<'client> MainAppDeps for DefaultMainAppDeps<'client> {
    type Client = Client;

    fn client(&self) -> &Client {
        self.client
    }

    type TestCollector = GoTestCollector;

    fn test_collector(&self) -> &GoTestCollector {
        &self.test_collector
    }

    fn get_template_vars(&self, _: &GoTestOptions) -> Result<TemplateVars> {
        Ok(TemplateVars::new())
    }

    const MAELSTROM_TEST_TOML: &'static str = MAELSTROM_TEST_TOML;
}

impl Wait for go_test::WaitHandle {
    fn wait(self) -> Result<()> {
        go_test::WaitHandle::wait(self)
    }
}

fn maybe_print_build_error(stderr: &mut impl io::Write, res: Result<ExitCode>) -> Result<ExitCode> {
    if let Err(e) = &res {
        if let Some(e) = e.downcast_ref::<go_test::BuildError>() {
            io::copy(&mut e.stderr.as_bytes(), stderr)?;
            return Ok(e.exit_code);
        }
    }
    res
}

pub fn main(
    config: Config,
    extra_options: cli::ExtraCommandLineOptions,
    bg_proc: ClientBgProcess,
    logger: Logger,
    stderr_is_tty: bool,
    ui: impl Ui,
) -> Result<ExitCode> {
    let cwd = Path::new(".").canonicalize()?;
    let project_dir = Root::<ProjectDir>::new(&cwd);
    main_with_stderr_and_project_dir(
        config,
        extra_options,
        bg_proc,
        logger,
        stderr_is_tty,
        ui,
        std::io::stderr(),
        project_dir,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn main_with_stderr_and_project_dir(
    config: Config,
    extra_options: cli::ExtraCommandLineOptions,
    bg_proc: ClientBgProcess,
    logger: Logger,
    stderr_is_tty: bool,
    ui: impl Ui,
    mut stderr: impl io::Write,
    project_dir: &Root<ProjectDir>,
) -> Result<ExitCode> {
    if extra_options.parent.client_bg_proc {
        return alternative_mains::client_bg_proc();
    }

    let logging_output = LoggingOutput::default();
    let log = logger.build(logging_output.clone());

    let list_action = extra_options.list.then_some(ListAction::ListTests);
    let build_dir = AsRef::<Path>::as_ref(project_dir).join(".maelstrom-go-test");
    let build_dir = Root::<BuildDir>::new(&build_dir);
    let state_dir = build_dir.join::<StateDir>("state");
    let cache_dir = build_dir.join::<CacheDir>("cache");

    Fs.create_dir_all(&state_dir)?;
    Fs.create_dir_all(&cache_dir)?;

    let client = create_client(
        bg_proc,
        config.parent.broker,
        project_dir,
        &state_dir,
        config.parent.container_image_depot_root,
        &cache_dir,
        config.parent.cache_size,
        config.parent.inline_limit,
        config.parent.slots,
        config.parent.accept_invalid_remote_container_tls_certs,
        log.clone(),
    )?;
    let deps = DefaultMainAppDeps::new(&client)?;

    let packages = go_test::find_packages(project_dir.as_ref()).context("finding go packages")?;

    let state = MainAppState::new(
        deps,
        extra_options.parent.include,
        extra_options.parent.exclude,
        list_action,
        stderr_is_tty,
        project_dir,
        &packages,
        &state_dir,
        GoTestOptions,
        logging_output,
        log,
    )?;

    let res = run_app_with_ui_multithreaded(state, config.parent.timeout.map(Timeout::new), ui);
    maybe_print_build_error(&mut stderr, res)
}
