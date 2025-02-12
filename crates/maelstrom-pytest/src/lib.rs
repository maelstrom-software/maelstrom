pub mod cli;
mod config;
pub mod pattern;
mod pytest;

pub use config::{Config, PytestConfigValues};
pub use maelstrom_test_runner::log::LoggerBuilder;

use anyhow::{anyhow, bail, Result};
use cli::ExtraCommandLineOptions;
use maelstrom_base::{
    enum_set, CaptureFileSystemChanges, JobDevice, JobMount, JobNetwork, JobOutcome,
    JobTerminationStatus, Timeout, Utf8PathBuf,
};
use maelstrom_client::{
    glob_layer_spec, job_spec,
    spec::{ContainerParent, ImageRef, LayerSpec, PathsLayerSpec, PrefixOptions, StubsLayerSpec},
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, Client, ClientBgProcess,
    ContainerImageDepotDir, ProjectDir, StateDir,
};
use maelstrom_container::{DockerReference, ImageName};
use maelstrom_test_runner::{
    log::LogDestination,
    metadata::Metadata,
    run_app_with_ui_multithreaded,
    ui::{Ui, UiMessage, UiSender},
    BuildDir, CollectTests, Directories, ListingType, TestArtifact, TestArtifactKey,
    TestCaseMetadata, TestFilter, TestPackage, TestPackageId, TestRunner as _, Wait, WaitStatus,
};
use maelstrom_util::{
    config::common::{ArtifactTransferStrategy, BrokerAddr, CacheSize, InlineLimit, Slots},
    fs::Fs,
    process::ExitCode,
    root::{Root, RootBuf},
    template::TemplateVars,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{
        HashSet,
        {hash_map::Entry, HashMap},
    },
    fmt, io,
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Mutex,
};

#[allow(clippy::too_many_arguments)]
fn create_client_for_test(
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
    artifact_transfer_strategy: ArtifactTransferStrategy,
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
        artifact_transfer_strategy,
        log,
    )
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

pub struct PytestTestCollector<'client> {
    directories: Directories,
    client: &'client Client,
    test_layers: Mutex<HashMap<ImageRef, LayerSpec>>,
}

impl PytestTestCollector<'_> {
    fn get_pip_packages(
        &self,
        image_spec: ImageRef,
        ref_: &DockerReference,
        ui: &UiSender,
    ) -> Result<Utf8PathBuf> {
        let fs = Fs::new();

        // Build some paths
        let cache_dir: &Path = self.directories.cache.as_ref();
        let project_dir: &Path = self.directories.project.as_ref();
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

        ui.send(UiMessage::UpdateEnqueueStatus(
            "installing pip packages".into(),
        ));

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
            LayerSpec::Paths(PathsLayerSpec {
                paths: vec![source_req_path.clone().try_into()?],
                prefix_options: Default::default(),
            }),
            LayerSpec::Stubs(StubsLayerSpec {
                stubs: vec!["/dev/null".into()],
            }),
            LayerSpec::Paths(PathsLayerSpec {
                paths: vec![resolv_conf.clone().try_into()?],
                prefix_options: PrefixOptions {
                    strip_prefix: Some(resolv_conf.parent().unwrap().to_owned().try_into()?),
                    prepend_prefix: Some("/etc/".into()),
                    canonicalize: false,
                    follow_symlinks: false,
                },
            }),
        ];
        let (_, outcome) = self.client.run_job(job_spec! {
            "/bin/sh",
            layers: layers,
            arguments: [
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
            ],
            parent: ContainerParent::Image(image_spec),
            network: JobNetwork::Local,
            mounts: [
                JobMount::Devices {
                    devices: enum_set![JobDevice::Null],
                },
            ],
            capture_file_system_changes: CaptureFileSystemChanges {
                upper: upper.clone().try_into()?,
                work: work.clone().try_into()?,
            },
        })?;
        let outcome = outcome.map_err(|err| anyhow!("error installing pip packages: {err:?}"))?;
        match outcome {
            JobOutcome::Completed(completed) => {
                if completed.status != JobTerminationStatus::Exited(0) {
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

    fn build_test_layer(&self, image: ImageRef, ui: &UiSender) -> Result<Option<LayerSpec>> {
        let image_name: ImageName = image.name.parse()?;
        let ImageName::Docker(ref_) = image_name else {
            return Ok(None);
        };
        if ref_.name() != "python" {
            return Ok(None);
        }

        let packages_path = self.get_pip_packages(image, &ref_, ui)?;
        let packages_path = packages_path
            .strip_prefix(&self.directories.project)
            .unwrap();
        Ok(Some(glob_layer_spec! {
            format!("{packages_path}/**"),
            strip_prefix: packages_path,
        }))
    }
}

#[derive(Debug)]
pub struct PytestTestArtifact {
    path: PathBuf,
    tests: Vec<(String, PytestCaseMetadata)>,
    ignored_tests: Vec<String>,
    package: PytestPackageId,
    pytest_options: PytestConfigValues,
    test_layers: HashMap<ImageRef, LayerSpec>,
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct PytestPackageId(String);

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
        args.extend(self.pytest_options.extra_pytest_test_args.clone());
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

    fn get_test_layers(&self, metadata: &Metadata) -> Vec<LayerSpec> {
        match &metadata.container.parent {
            Some(ContainerParent::Image(image_spec)) => self
                .test_layers
                .get(image_spec)
                .into_iter()
                .cloned()
                .collect(),
            _ => vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub struct PytestPackage {
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

impl CollectTests for PytestTestCollector<'_> {
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
        let test_layers = self.test_layers.lock().unwrap().clone();
        let (handle, stream) = pytest::pytest_collect_tests(
            color,
            options,
            &self.directories.project,
            &self.directories.build,
            test_layers,
        )?;
        Ok((handle, stream))
    }

    fn build_test_layers(&self, images: HashSet<ImageRef>, ui: &UiSender) -> Result<()> {
        let mut test_layers = self.test_layers.lock().unwrap();
        for image in images {
            if let Entry::Vacant(e) = test_layers.entry(image.clone()) {
                if let Some(layer) = self.build_test_layer(image, ui)? {
                    e.insert(layer);
                }
            }
        }
        Ok(())
    }

    fn get_packages(&self, _ui: &UiSender) -> Result<Vec<PytestPackage>> {
        Ok(vec![PytestPackage {
            name: "default".into(),
            id: PytestPackageId("default".into()),
            artifacts: find_artifacts(self.directories.project.as_ref())?,
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

#[test]
fn default_test_metadata_parses() {
    maelstrom_test_runner::metadata::Store::<pattern::Pattern>::load(
        TestRunner::DEFAULT_TEST_METADATA_FILE_CONTENTS,
        &Default::default(),
    )
    .unwrap();
}

impl Wait for pytest::WaitHandle {
    fn wait(&self) -> Result<WaitStatus> {
        pytest::WaitHandle::wait(self)
    }

    fn kill(&self) -> Result<()> {
        pytest::WaitHandle::kill(self)
    }
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

#[allow(clippy::too_many_arguments)]
pub fn main_for_test(
    config: Config,
    extra_options: cli::ExtraCommandLineOptions,
    bg_proc: ClientBgProcess,
    logger: LoggerBuilder,
    stdout_is_tty: bool,
    ui: impl Ui,
    _stderr: impl io::Write,
    project_dir: &Root<ProjectDir>,
) -> Result<ExitCode> {
    let log_destination = LogDestination::default();
    let log = logger.build(log_destination.clone());

    let list_tests = extra_options.list.into();
    let project = project_dir.to_owned();
    let build = project.join(".maelstrom-pytest");
    let cache = build.join("cache");
    let state = build.join("state");
    let directories = Directories {
        build,
        cache,
        project,
        state,
    };

    Fs.create_dir_all(&directories.state)?;
    Fs.create_dir_all(&directories.cache)?;

    let client = create_client_for_test(
        bg_proc,
        config.parent.broker,
        project_dir,
        &directories.state,
        config.parent.container_image_depot_root,
        &directories.cache,
        config.parent.cache_size,
        config.parent.inline_limit,
        config.parent.slots,
        config.parent.accept_invalid_remote_container_tls_certs,
        config.parent.artifact_transfer_strategy,
        log.clone(),
    )?;
    let test_collector = PytestTestCollector {
        client: &client,
        directories: directories.clone(),
        test_layers: Mutex::new(HashMap::new()),
    };
    let template_vars = TestRunner::get_template_vars(&config.pytest_options, &directories)?;

    run_app_with_ui_multithreaded(
        log_destination,
        config.parent.timeout.map(Timeout::new),
        ui,
        &test_collector,
        extra_options.parent.include,
        extra_options.parent.exclude,
        list_tests,
        config.parent.repeat,
        config.parent.stop_after,
        extra_options.parent.watch,
        stdout_is_tty,
        project_dir,
        &directories.state,
        vec![directories.build.into_path_buf()],
        config.pytest_options,
        log,
        &client,
        TestRunner::TEST_METADATA_FILE_NAME,
        TestRunner::DEFAULT_TEST_METADATA_FILE_CONTENTS,
        template_vars,
    )
}

pub struct TestRunner;

impl maelstrom_test_runner::TestRunner for TestRunner {
    type Config = Config;
    type ExtraCommandLineOptions = ExtraCommandLineOptions;
    type Metadata = ();
    type TestCollector<'client> = PytestTestCollector<'client>;
    type CollectorOptions = PytestConfigValues;

    const BASE_DIRECTORIES_PREFIX: &'static str = "maelstrom/maelstrom-pytest";
    const ENVIRONMENT_VARIABLE_PREFIX: &'static str = "MAELSTROM_PYTEST";
    const TEST_METADATA_FILE_NAME: &'static str = "maelstrom-pytest.toml";
    const DEFAULT_TEST_METADATA_FILE_CONTENTS: &'static str =
        include_str!("default-test-metadata.toml");

    fn get_listing_type(extra_options: &ExtraCommandLineOptions) -> ListingType {
        if extra_options.list {
            ListingType::Tests
        } else {
            ListingType::None
        }
    }

    fn get_directories_and_metadata(_config: &Config) -> Result<(Directories, ())> {
        let project = RootBuf::new(Path::new(".").canonicalize()?);
        let build = project.join(".maelstrom-pytest");
        let cache = build.join("cache");
        let state = build.join("state");
        Ok((
            Directories {
                build,
                cache,
                project,
                state,
            },
            (),
        ))
    }

    fn get_test_collector<'client>(
        client: &'client Client,
        directories: &Directories,
        _log: &slog::Logger,
        _metadata: (),
    ) -> Result<PytestTestCollector<'client>> {
        Ok(PytestTestCollector {
            client,
            directories: directories.clone(),
            test_layers: Mutex::new(HashMap::new()),
        })
    }

    fn get_watch_exclude_paths(directories: &Directories) -> Vec<PathBuf> {
        vec![directories.build.to_owned().into_path_buf()]
    }

    fn get_template_vars(
        _collector_options: &Self::CollectorOptions,
        _directories: &Directories,
    ) -> Result<TemplateVars> {
        Ok(TemplateVars::default())
    }
}
