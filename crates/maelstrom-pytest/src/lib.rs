pub mod cli;
pub mod pattern;
mod pytest;

use anyhow::Result;
use indicatif::TermLike;
use maelstrom_base::{Timeout, Utf8PathBuf};
use maelstrom_client::{
    CacheDir, Client, ClientBgProcess, ContainerImageDepotDir, ProjectDir, StateDir,
};
pub use maelstrom_test_runner::config::Config;
use maelstrom_test_runner::{
    main_app_new, progress, BuildDir, CollectTests, LoggingOutput, MainAppDeps, MainAppState,
    TestArtifact, TestArtifactKey, TestFilter, TestLayers, TestPackage, TestPackageId, Wait,
};
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, Slots},
    fs::Fs,
    process::ExitCode,
    root::Root,
    template::TemplateVars,
};
use std::fmt;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub use maelstrom_test_runner::Logger;

/// The Maelstrom target directory is <target-dir>/maelstrom.
pub struct MaelstromTargetDir;

struct DefaultMainAppDeps {
    client: Client,
    test_collector: PytestTestCollector,
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
        cache_size: CacheSize,
        inline_limit: InlineLimit,
        slots: Slots,
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
            log,
        )?;
        Ok(Self {
            client,
            test_collector: PytestTestCollector,
        })
    }
}
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PytestArtifactKey {
    path: PathBuf,
}

impl TestArtifactKey for PytestArtifactKey {}

impl fmt::Display for PytestArtifactKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.display().fmt(f)
    }
}

impl FromStr for PytestArtifactKey {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(Self { path: s.into() })
    }
}

impl TestFilter for pattern::Pattern {
    type ArtifactKey = PytestArtifactKey;

    fn compile(include: &[String], exclude: &[String]) -> Result<Self> {
        pattern::compile_filter(include, exclude)
    }

    fn filter(
        &self,
        package: &str,
        artifact: Option<&PytestArtifactKey>,
        case: Option<&str>,
    ) -> Option<bool> {
        let c = pattern::Context {
            package: package.into(),
            artifact: artifact.map(|a| pattern::Artifact {
                name: a.path.display().to_string(),
                kind: pattern::ArtifactKind::Library,
            }),
            case: case.map(|case| pattern::Case { name: case.into() }),
        };
        pattern::interpret_pattern(self, &c)
    }
}

struct PytestOptions;

struct PytestTestCollector;

#[derive(Debug)]
pub(crate) struct PytestTestArtifact {
    name: String,
    path: PathBuf,
    tests: Vec<String>,
    ignored_tests: Vec<String>,
    package: PytestPackageId,
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
struct PytestPackageId(String);

impl TestPackageId for PytestPackageId {}

impl TestArtifact for PytestTestArtifact {
    type ArtifactKey = PytestArtifactKey;
    type PackageId = PytestPackageId;

    fn package(&self) -> PytestPackageId {
        self.package.clone()
    }

    fn to_key(&self) -> PytestArtifactKey {
        PytestArtifactKey {
            path: self.path.clone(),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn list_tests(&self) -> Result<Vec<String>> {
        Ok(self.tests.clone())
    }

    fn list_ignored_tests(&self) -> Result<Vec<String>> {
        Ok(self.ignored_tests.clone())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn test_layers(&self) -> TestLayers {
        TestLayers::Provided(vec![])
    }

    fn build_command(&self, case: &str) -> (Utf8PathBuf, Vec<String>) {
        let path = self.path().display();
        (
            "/usr/local/bin/python".into(),
            vec![
                "-m".into(),
                "pytest".into(),
                "--verbose".into(),
                format!("{path}::{case}"),
            ],
        )
    }
}

#[derive(Clone, Debug)]
struct PytestPackage {
    name: String,
    version: String,
    id: PytestPackageId,
    artifacts: Vec<PytestArtifactKey>,
}

impl TestPackage for PytestPackage {
    type PackageId = PytestPackageId;
    type ArtifactKey = PytestArtifactKey;

    fn name(&self) -> &str {
        &self.name
    }

    fn version(&self) -> &impl fmt::Display {
        &self.version
    }

    fn artifacts(&self) -> Vec<PytestArtifactKey> {
        self.artifacts.clone()
    }

    fn id(&self) -> PytestPackageId {
        self.id.clone()
    }
}

impl CollectTests for PytestTestCollector {
    type BuildHandle = pytest::WaitHandle;
    type Artifact = PytestTestArtifact;
    type ArtifactStream = pytest::TestArtifactStream;
    type TestFilter = pattern::Pattern;
    type PackageId = PytestPackageId;
    type Package = PytestPackage;
    type ArtifactKey = PytestArtifactKey;
    type Options = PytestOptions;

    fn start(
        &self,
        color: bool,
        _options: &PytestOptions,
        packages: Vec<String>,
    ) -> Result<(pytest::WaitHandle, pytest::TestArtifactStream)> {
        let (handle, stream) = pytest::pytest_collect_tests(color, packages)?;
        Ok((handle, stream))
    }
}

impl MainAppDeps for DefaultMainAppDeps {
    type Client = Client;

    fn client(&self) -> &Client {
        &self.client
    }

    type TestCollector = PytestTestCollector;

    fn test_collector(&self) -> &PytestTestCollector {
        &self.test_collector
    }

    fn get_template_vars(
        &self,
        _pytest_options: &PytestOptions,
        _target_dir: &Root<BuildDir>,
    ) -> Result<TemplateVars> {
        Ok(TemplateVars::new())
    }

    const MAELSTROM_TEST_TOML: &'static str = "maelstrom-pytest.toml";
}

impl Wait for pytest::WaitHandle {
    fn wait(self) -> Result<()> {
        pytest::WaitHandle::wait(self)
    }
}

fn maybe_print_collect_error(res: Result<ExitCode>) -> Result<ExitCode> {
    if let Err(e) = &res {
        if let Some(e) = e.downcast_ref::<pytest::PytestCollectError>() {
            eprintln!("{}", &e.stderr);
            return Ok(e.exit_code);
        }
    }
    res
}

fn find_python() -> Result<Vec<PytestArtifactKey>> {
    let mut glob_builder = globset::GlobSet::builder();
    glob_builder.add(globset::Glob::new("*.py")?);
    Ok(Fs
        .glob_walk(".", &glob_builder.build()?)
        .filter_map(|path| path.ok().map(|path| PytestArtifactKey { path }))
        .collect())
}

pub fn main<TermT>(
    config: Config,
    extra_options: cli::ExtraCommandLineOptions,
    bg_proc: ClientBgProcess,
    logger: Logger,
    stderr_is_tty: bool,
    stdout_is_tty: bool,
    terminal: TermT,
) -> Result<ExitCode>
where
    TermT: TermLike + Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static,
{
    let workspace_dir = Root::<ProjectDir>::new(Path::new("."));
    let logging_output = LoggingOutput::default();
    let log = logger.build(logging_output.clone());

    let target_dir = Root::<BuildDir>::new(Path::new("target"));
    let maelstrom_target_dir = target_dir.join::<MaelstromTargetDir>("maelstrom_pytest");
    let state_dir = maelstrom_target_dir.join::<StateDir>("state");
    let cache_dir = maelstrom_target_dir.join::<CacheDir>("cache");

    Fs.create_dir_all(&state_dir)?;
    Fs.create_dir_all(&cache_dir)?;

    let deps = DefaultMainAppDeps::new(
        bg_proc,
        config.broker,
        workspace_dir.transmute::<ProjectDir>(),
        &state_dir,
        config.container_image_depot_root,
        cache_dir,
        config.cache_size,
        config.inline_limit,
        config.slots,
        log.clone(),
    )?;

    let packages = vec![PytestPackage {
        name: "default".into(),
        version: "0".into(),
        id: PytestPackageId("default".into()),
        artifacts: find_python()?,
    }];

    let state = MainAppState::new(
        deps,
        extra_options.include,
        extra_options.exclude,
        None,
        stderr_is_tty,
        workspace_dir,
        &packages,
        &state_dir,
        target_dir,
        PytestOptions,
        logging_output,
        log,
    )?;

    let res = std::thread::scope(|scope| {
        let mut app = main_app_new(
            &state,
            stdout_is_tty,
            config.quiet,
            terminal,
            progress::DefaultProgressDriver::new(scope),
            config.timeout.map(Timeout::new),
        )?;
        while !app.enqueue_one()?.is_done() {}
        app.drain()?;
        app.finish()
    });
    drop(state);
    maybe_print_collect_error(res)
}
