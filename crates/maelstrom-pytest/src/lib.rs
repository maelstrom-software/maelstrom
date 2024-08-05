pub mod cli;
mod config;
pub mod pattern;
mod pytest;

use anyhow::{anyhow, bail, Result};
use maelstrom_base::{
    enum_set, JobDevice, JobMount, JobNetwork, JobOutcome, JobRootOverlay, JobStatus, Timeout,
    Utf8PathBuf,
};
use maelstrom_client::{
    spec::{Layer, PrefixOptions},
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, Client, ClientBgProcess,
    ContainerImageDepotDir, ImageSpec, JobSpec, ProjectDir, StateDir,
};
use maelstrom_container::{DockerReference, ImageName};
use maelstrom_test_runner::{
    metadata::TestMetadata, run_app_with_ui_multithreaded, ui::Ui, ui::UiSender, BuildDir,
    CollectTests, ListAction, LoggingOutput, MainAppDeps, MainAppState, TestArtifact,
    TestArtifactKey, TestCaseMetadata, TestFilter, TestLayers, TestPackage, TestPackageId, Wait,
};
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, Slots},
    fs::Fs,
    process::ExitCode,
    root::{Root, RootBuf},
    template::TemplateVars,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::{fmt, io};

pub use config::{Config, PytestConfigValues};
pub use maelstrom_test_runner::Logger;

pub const MAELSTROM_TEST_TOML: &str = "maelstrom-pytest.toml";
pub const ADDED_DEFAULT_TEST_METADATA: &str = include_str!("added-default-test-metadata.toml");

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
    test_collector: PytestTestCollector<'client>,
}

impl<'client> DefaultMainAppDeps<'client> {
    pub fn new(
        project_dir: &Root<ProjectDir>,
        build_dir: &Root<BuildDir>,
        cache_dir: &Root<CacheDir>,
        client: &'client Client,
    ) -> Result<Self> {
        Ok(Self {
            client,
            test_collector: PytestTestCollector {
                client,
                project_dir: project_dir.to_owned(),
                build_dir: build_dir.to_owned(),
                cache_dir: cache_dir.to_owned(),
            },
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
    type Package = PytestPackage;
    type ArtifactKey = PytestArtifactKey;
    type CaseMetadata = PytestCaseMetadata;

    fn compile(include: &[String], exclude: &[String]) -> Result<Self> {
        pattern::compile_filter(include, exclude)
    }

    fn filter(
        &self,
        package: &PytestPackage,
        artifact: Option<&PytestArtifactKey>,
        case: Option<(&str, &PytestCaseMetadata)>,
    ) -> Option<bool> {
        let c = pattern::Context {
            package: package.name().into(),
            file: artifact.map(|a| a.path.display().to_string()),
            case: case.map(|(name, metadata)| pattern::Case {
                name: name.into(),
                node_id: metadata.node_id.clone(),
                markers: metadata.markers.clone(),
            }),
        };
        pattern::interpret_pattern(self, &c)
    }
}

struct PytestTestCollector<'client> {
    project_dir: RootBuf<ProjectDir>,
    build_dir: RootBuf<BuildDir>,
    cache_dir: RootBuf<CacheDir>,
    client: &'client Client,
}

impl<'client> PytestTestCollector<'client> {
    fn get_pip_packages(
        &self,
        image_spec: ImageSpec,
        ref_: &DockerReference,
        ind: &UiSender,
    ) -> Result<Utf8PathBuf> {
        let fs = Fs::new();

        // Build some paths
        let cache_dir: &Path = self.cache_dir.as_ref();
        let project_dir: &Path = self.project_dir.as_ref();
        let packages_path: PathBuf = cache_dir.join(format!("pip_packages/{ref_}"));
        if !fs.exists(&packages_path) {
            fs.create_dir_all(&packages_path)?;
        }
        let source_req_path = project_dir.join("test-requirements.txt");
        let saved_req_path = packages_path.join("requirements.txt");
        let upper = packages_path.join("root");

        // Are the existing packages up-to-date
        let source_req = fs.read_to_string(&source_req_path)?;
        let saved_req = fs.read_to_string_if_exists(&saved_req_path)?;
        if Some(source_req) == saved_req {
            return Ok(upper.try_into()?);
        }

        ind.update_enqueue_status("installing pip packages");

        // Delete the work dir in case we have leaked it
        let work = packages_path.join("work");
        if fs.exists(&work) {
            let inner_work = work.join("work");
            if fs.exists(&inner_work) {
                let mut work_perm = fs.metadata(&inner_work)?.permissions();
                work_perm.set_mode(0o777);
                fs.set_permissions(work.join("work"), work_perm)?;
            }
            fs.remove_dir_all(&work)?;
        }

        // Ensure the work and upper exist now
        fs.create_dir_all(&work)?;
        if !fs.exists(&upper) {
            fs.create_dir_all(&upper)?;
        }

        // We need to install our own resolv.conf to get internet access
        // These two addresses are for cloudflare's DNS server
        let resolv_conf = packages_path.join("resolv.conf");
        fs.write(&resolv_conf, b"nameserver 1.1.1.1\nnameserver 1.0.0.1")?;

        // Run a local job to install the packages
        let layers = vec![
            self.client.add_layer(Layer::Paths {
                paths: vec![source_req_path.clone().try_into()?],
                prefix_options: Default::default(),
            })?,
            self.client.add_layer(Layer::Stubs {
                stubs: vec!["/dev/null".into()],
            })?,
            self.client.add_layer(Layer::Paths {
                paths: vec![resolv_conf.clone().try_into()?],
                prefix_options: PrefixOptions {
                    strip_prefix: Some(resolv_conf.parent().unwrap().to_owned().try_into()?),
                    prepend_prefix: Some("/etc/".into()),
                    canonicalize: false,
                    follow_symlinks: false,
                },
            })?,
        ];
        let (sender, receiver) = std::sync::mpsc::channel();
        self.client.add_job(
            JobSpec::new("/bin/sh", layers)
                .arguments([
                    "-c".to_owned(),
                    format!(
                        "
                        set -ex
                        pip install --requirement {}
                        python -m compileall /usr/lib/python* /usr/local/lib/python*
                        ",
                        source_req_path
                            .to_str()
                            .ok_or_else(|| anyhow!("non-UTF8 path"))?
                    ),
                ])
                .image(image_spec)
                .network(JobNetwork::Local)
                .root_overlay(JobRootOverlay::Local {
                    upper: upper.clone().try_into()?,
                    work: work.clone().try_into()?,
                })
                .mounts([JobMount::Devices {
                    devices: enum_set![JobDevice::Null],
                }]),
            move |res| drop(sender.send(res)),
        )?;
        let (_, outcome) = receiver.recv()??;
        let outcome = outcome.map_err(|err| anyhow!("error installing pip packages: {err:?}"))?;
        match outcome {
            JobOutcome::Completed(completed) => {
                if completed.status != JobStatus::Exited(0) {
                    bail!(
                        "pip install failed:\nstderr: {}\nstdout{}",
                        completed.effects.stderr,
                        completed.effects.stdout
                    )
                }
            }
            JobOutcome::TimedOut(_) => bail!("pip install timed out"),
        }

        // Delete any special character files
        for path in fs.walk(&upper) {
            let path = path?;
            let meta = fs.symlink_metadata(&path)?;
            if !(meta.is_file() || meta.is_dir() || meta.is_symlink()) {
                fs.remove_file(path)?;
            }
        }

        // Remove work
        let mut work_perm = fs.metadata(work.join("work"))?.permissions();
        work_perm.set_mode(0o777);
        fs.set_permissions(work.join("work"), work_perm)?;
        fs.remove_dir_all(work)?;

        // Save requirements
        fs.copy(source_req_path, saved_req_path)?;

        Ok(upper.try_into()?)
    }
}

#[derive(Debug)]
pub(crate) struct PytestTestArtifact {
    path: PathBuf,
    tests: Vec<(String, PytestCaseMetadata)>,
    ignored_tests: Vec<String>,
    package: PytestPackageId,
    pytest_options: PytestConfigValues,
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
struct PytestPackageId(String);

impl TestPackageId for PytestPackageId {}

#[derive(Clone, Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct PytestCaseMetadata {
    node_id: String,
    markers: Vec<String>,
}

impl TestCaseMetadata for PytestCaseMetadata {}

impl TestArtifact for PytestTestArtifact {
    type ArtifactKey = PytestArtifactKey;
    type PackageId = PytestPackageId;
    type CaseMetadata = PytestCaseMetadata;

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

    fn list_tests(&self) -> Result<Vec<(String, PytestCaseMetadata)>> {
        Ok(self.tests.clone())
    }

    fn list_ignored_tests(&self) -> Result<Vec<String>> {
        Ok(self.ignored_tests.clone())
    }

    fn build_command(
        &self,
        _case_name: &str,
        case_metadata: &PytestCaseMetadata,
    ) -> (Utf8PathBuf, Vec<String>) {
        let mut args = vec!["-m".into(), "pytest".into(), "--verbose".into()];
        args.extend(self.pytest_options.extra_pytest_args.clone());
        args.push(case_metadata.node_id.clone());
        ("/usr/local/bin/python".into(), args)
    }

    fn format_case(
        &self,
        _package_name: &str,
        _case_name: &str,
        case_metadata: &PytestCaseMetadata,
    ) -> String {
        case_metadata.node_id.clone()
    }
}

#[derive(Clone, Debug)]
struct PytestPackage {
    name: String,
    id: PytestPackageId,
    artifacts: Vec<PytestArtifactKey>,
}

impl TestPackage for PytestPackage {
    type PackageId = PytestPackageId;
    type ArtifactKey = PytestArtifactKey;

    fn name(&self) -> &str {
        &self.name
    }

    fn artifacts(&self) -> Vec<PytestArtifactKey> {
        self.artifacts.clone()
    }

    fn id(&self) -> PytestPackageId {
        self.id.clone()
    }
}

impl<'client> CollectTests for PytestTestCollector<'client> {
    const ENQUEUE_MESSAGE: &'static str = "collecting tests...";

    type BuildHandle = pytest::WaitHandle;
    type Artifact = PytestTestArtifact;
    type ArtifactStream = pytest::TestArtifactStream;
    type TestFilter = pattern::Pattern;
    type PackageId = PytestPackageId;
    type Package = PytestPackage;
    type ArtifactKey = PytestArtifactKey;
    type Options = PytestConfigValues;
    type CaseMetadata = PytestCaseMetadata;

    fn start(
        &self,
        color: bool,
        options: &PytestConfigValues,
        _packages: Vec<&PytestPackage>,
        _ui: &UiSender,
    ) -> Result<(pytest::WaitHandle, pytest::TestArtifactStream)> {
        let (handle, stream) =
            pytest::pytest_collect_tests(color, options, &self.project_dir, &self.build_dir)?;
        Ok((handle, stream))
    }

    fn get_test_layers(&self, metadata: &TestMetadata, ind: &UiSender) -> Result<TestLayers> {
        match &metadata.image {
            Some(image) => {
                let image_name: ImageName = image.name.parse()?;
                let ImageName::Docker(ref_) = image_name else {
                    return Ok(TestLayers::Provided(vec![]));
                };
                if ref_.name() != "python" {
                    return Ok(TestLayers::Provided(vec![]));
                }

                let packages_path = self.get_pip_packages(image.clone(), &ref_, ind)?;
                let packages_path = packages_path.strip_prefix(&self.project_dir).unwrap();
                Ok(TestLayers::Provided(vec![Layer::Glob {
                    glob: format!("{packages_path}/**"),
                    prefix_options: PrefixOptions {
                        strip_prefix: Some(packages_path.into()),
                        ..Default::default()
                    },
                }]))
            }
            _ => Ok(TestLayers::Provided(vec![])),
        }
    }

    fn get_packages(&self, _ui: &UiSender) -> Result<Vec<PytestPackage>> {
        Ok(vec![PytestPackage {
            name: "default".into(),
            id: PytestPackageId("default".into()),
            artifacts: find_artifacts(self.project_dir.as_ref())?,
        }])
    }

    fn remove_fixture_output(_case_str: &str, mut lines: Vec<String>) -> Vec<String> {
        let start_re = Regex::new("=+ FAILURES =+").unwrap();
        let end_re = Regex::new("=+ short test summary info =+").unwrap();

        if let Some(pos) = lines.iter().position(|s| start_re.is_match(s.as_str())) {
            lines = lines[(pos + 2)..].to_vec();
        }
        if let Some(pos) = lines.iter().rposition(|s| end_re.is_match(s.as_str())) {
            lines = lines[..pos].to_vec();
        }
        lines
    }
}

#[test]
fn remove_fixture_output_basic_case() {
    let example = indoc::indoc!(
        "
        ============================= test session starts ==============================
        platform linux -- Python 3.12.3, pytest-8.1.1, pluggy-1.4.0 -- /usr/local/bin/python
        cachedir: .pytest_cache
        rootdir: /
        configfile: pyproject.toml
        plugins: cov-4.1.0, xdist-3.3.1
        created: 1/1 worker
        1 worker [1 item]

        scheduling tests via LoadScheduling

        mypyc/test/test_commandline.py::TestCommandLine::testCompileMypyc
        [gw0] [100%] FAILED mypyc/test/test_commandline.py::TestCommandLine::testCompileMypyc

        =================================== FAILURES ===================================
        _______________________________ testCompileMypyc _______________________________
        [gw0] linux -- Python 3.12.3 /usr/local/bin/python
        data: /mypyc/test-data/commandline.test:5:
        Failed: Invalid output (/mypyc/test-data/commandline.test, line 5)
        ----------------------------- Captured stderr call -----------------------------
        this is the stderr of the test
        this is also test output
        =========================== short test summary info ============================
        FAILED mypyc/test/test_commandline.py::TestCommandLine::testCompileMypyc
        ============================== 1 failed in 2.22s ===============================
        "
    );
    let cleansed = PytestTestCollector::remove_fixture_output(
        "tests::i_be_failing",
        example.split('\n').map(ToOwned::to_owned).collect(),
    );
    assert_eq!(
        cleansed.join("\n"),
        indoc::indoc!(
            "
            [gw0] linux -- Python 3.12.3 /usr/local/bin/python
            data: /mypyc/test-data/commandline.test:5:
            Failed: Invalid output (/mypyc/test-data/commandline.test, line 5)
            ----------------------------- Captured stderr call -----------------------------
            this is the stderr of the test
            this is also test output\
            "
        )
    );
}

impl<'client> MainAppDeps for DefaultMainAppDeps<'client> {
    type Client = Client;

    fn client(&self) -> &Client {
        self.client
    }

    type TestCollector = PytestTestCollector<'client>;

    fn test_collector(&self) -> &PytestTestCollector<'client> {
        &self.test_collector
    }

    fn get_template_vars(&self, _pytest_options: &PytestConfigValues) -> Result<TemplateVars> {
        Ok(TemplateVars::new())
    }

    const MAELSTROM_TEST_TOML: &'static str = MAELSTROM_TEST_TOML;
}

impl Wait for pytest::WaitHandle {
    fn wait(self) -> Result<()> {
        pytest::WaitHandle::wait(self)
    }
}

fn maybe_print_collect_error(
    stderr: &mut impl io::Write,
    res: Result<ExitCode>,
) -> Result<ExitCode> {
    if let Err(e) = &res {
        if let Some(e) = e.downcast_ref::<pytest::PytestCollectError>() {
            io::copy(&mut e.stderr.as_bytes(), stderr)?;
            return Ok(e.exit_code);
        }
    }
    res
}

fn find_artifacts(path: &Path) -> Result<Vec<PytestArtifactKey>> {
    let cwd = path.canonicalize()?;
    Ok(Fs
        .walk(&cwd)
        .filter_map(|path| {
            path.ok().map(|path| PytestArtifactKey {
                path: path.strip_prefix(&cwd).unwrap().into(),
            })
        })
        .collect())
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
    let logging_output = LoggingOutput::default();
    let log = logger.build(logging_output.clone());

    let list_action = extra_options.list.then_some(ListAction::ListTests);
    let build_dir = AsRef::<Path>::as_ref(project_dir).join(".maelstrom-pytest");
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
    let deps = DefaultMainAppDeps::new(project_dir, build_dir, &cache_dir, &client)?;

    let state = MainAppState::new(
        deps,
        extra_options.parent.include,
        extra_options.parent.exclude,
        list_action,
        config.parent.repeat,
        stderr_is_tty,
        project_dir,
        &state_dir,
        config.pytest_options,
        log,
    )?;

    let res = run_app_with_ui_multithreaded(
        state,
        logging_output,
        config.parent.timeout.map(Timeout::new),
        ui,
    );
    maybe_print_collect_error(&mut stderr, res)
}
