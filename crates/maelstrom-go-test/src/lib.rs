pub mod alternative_mains;
pub mod cli;
mod config;
mod go_test;
mod pattern;

use anyhow::{Context as _, Result};
use config::GoTestOptions;
use maelstrom_base::{Timeout, Utf8Path, Utf8PathBuf};
use maelstrom_client::{
    shared_library_dependencies_layer_spec,
    spec::{LayerSpec, PathsLayerSpec, PrefixOptions},
    AcceptInvalidRemoteContainerTlsCerts, CacheDir, Client, ClientBgProcess,
    ContainerImageDepotDir, ProjectDir, StateDir,
};
use maelstrom_test_runner::{
    metadata::Metadata,
    run_app_with_ui_multithreaded,
    ui::{Ui, UiSender},
    BuildDir, CollectTests, ListAction, LoggingOutput, MainAppCombinedDeps, MainAppDeps,
    NoCaseMetadata, TestArtifact, TestArtifactKey, TestFilter, TestPackage, TestPackageId, Wait,
    WaitStatus,
};
use maelstrom_util::{
    config::common::{ArtifactTransferStrategy, BrokerAddr, CacheSize, InlineLimit, Slots},
    fs::Fs,
    process::ExitCode,
    root::{Root, RootBuf},
    template::TemplateVars,
};
use std::{fmt, io, path::Path, str::FromStr};

pub use config::Config;
pub use maelstrom_test_runner::Logger;

pub const TEST_METADATA_FILE_NAME: &str = "maelstrom-go-test.toml";
pub const DEFAULT_TEST_METADATA_CONTENTS: &str = include_str!("default-test-metadata.toml");

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

struct DefaultMainAppDeps<'client> {
    client: &'client Client,
    test_collector: GoTestCollector,
}

impl<'client> DefaultMainAppDeps<'client> {
    pub fn new(
        client: &'client Client,
        project_dir: &Root<ProjectDir>,
        cache_dir: &Root<CacheDir>,
    ) -> Result<Self> {
        Ok(Self {
            client,
            test_collector: GoTestCollector::new(project_dir, cache_dir),
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
    type Package = GoPackage;
    type ArtifactKey = GoTestArtifactKey;
    type CaseMetadata = NoCaseMetadata;

    fn compile(include: &[String], exclude: &[String]) -> Result<Self> {
        pattern::compile_filter(include, exclude)
    }

    fn filter(
        &self,
        package: &GoPackage,
        _artifact: Option<&GoTestArtifactKey>,
        case: Option<(&str, &NoCaseMetadata)>,
    ) -> Option<bool> {
        let c = pattern::Context {
            package_import_path: package.0.import_path.clone(),
            package_path: package.0.root_relative_path().display().to_string(),
            package_name: package.0.name.clone(),
            case: case.map(|(case, _)| pattern::Case { name: case.into() }),
        };
        pattern::interpret_pattern(self, &c)
    }
}

struct GoTestCollector {
    project_dir: RootBuf<ProjectDir>,
    cache_dir: RootBuf<CacheDir>,
}

impl GoTestCollector {
    fn new(project_dir: &Root<ProjectDir>, cache_dir: &Root<CacheDir>) -> Self {
        Self {
            project_dir: project_dir.to_owned(),
            cache_dir: cache_dir.to_owned(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct GoTestArtifact {
    id: GoImportPath,
    path: Utf8PathBuf,
    options: GoTestOptions,
}

impl TryFrom<go_test::GoTestArtifact> for GoTestArtifact {
    type Error = anyhow::Error;

    fn try_from(a: go_test::GoTestArtifact) -> Result<Self> {
        Ok(Self {
            id: GoImportPath(a.package.import_path),
            path: a
                .path
                .try_into()
                .with_context(|| "path contains non-UTF8 character")?,
            options: a.options,
        })
    }
}

#[derive(Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub(crate) struct GoImportPath(String);

impl GoImportPath {
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
    assert_eq!(
        GoImportPath("github.com/foo/bar".into()).short_name(),
        "bar"
    );
    assert_eq!(GoImportPath("github.com/foo/v1".into()).short_name(), "foo");
    assert_eq!(
        GoImportPath("github.com/foo/v1a".into()).short_name(),
        "v1a"
    );
}

impl TestPackageId for GoImportPath {}

impl TestArtifact for GoTestArtifact {
    type ArtifactKey = GoTestArtifactKey;
    type PackageId = GoImportPath;
    type CaseMetadata = NoCaseMetadata;

    fn package(&self) -> GoImportPath {
        self.id.clone()
    }

    fn to_key(&self) -> GoTestArtifactKey {
        GoTestArtifactKey {
            name: "test".into(),
        }
    }

    fn path(&self) -> &Path {
        self.path.as_ref()
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

    fn build_command(
        &self,
        case_name: &str,
        _case_metadata: &NoCaseMetadata,
    ) -> (Utf8PathBuf, Vec<String>) {
        let binary_name = self.path().file_name().unwrap().to_str().unwrap();
        let mut args = vec![
            "-test.run".into(),
            // This argument is a regular expression and we want an exact match for our test
            // name. We shouldn't have to worry about escaping the test name.
            format!("^{case_name}$"),
            // We have our own mechanism for timeouts, so we disable the one built into the
            // test binary.
            "-test.timeout=0".into(),
            // Print out more information, in particular this include whether or not the test
            // was skipped.
            "-test.v".into(),
            // Plumb these options through
            format!("-test.short={}", self.options.short),
            format!("-test.fullpath={}", self.options.fullpath),
        ];
        args.extend(self.options.extra_test_binary_args.clone());
        (format!("/{binary_name}").into(), args)
    }

    fn format_case(
        &self,
        import_path: &str,
        case_name: &str,
        _case_metadata: &NoCaseMetadata,
    ) -> String {
        format!("{import_path} {case_name}")
    }

    fn get_test_layers(&self, metadata: &Metadata) -> Vec<LayerSpec> {
        let mut layers = vec![path_layer_for_binary(&self.path)];

        if metadata.include_shared_libraries() {
            // Go binaries usually are statically linked, but on the off-chance they use some OS
            // library or something, doesn't hurt to check.
            layers.push(so_layer_for_binary(&self.path));
        }

        layers
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GoPackage(go_test::GoPackage);

impl TestPackage for GoPackage {
    type PackageId = GoImportPath;
    type ArtifactKey = GoTestArtifactKey;

    #[allow(clippy::misnamed_getters)]
    fn name(&self) -> &str {
        &self.0.import_path
    }

    fn artifacts(&self) -> Vec<GoTestArtifactKey> {
        vec![GoTestArtifactKey {
            name: "test".into(),
        }]
    }

    fn id(&self) -> GoImportPath {
        GoImportPath(self.0.import_path.clone())
    }
}

struct TestArtifactStream(go_test::TestArtifactStream);

impl Iterator for TestArtifactStream {
    type Item = Result<GoTestArtifact>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|r| GoTestArtifact::try_from(r?))
    }
}

impl GoTestCollector {
    fn remove_fixture_output_test(case_str: &str, mut lines: Vec<String>) -> Vec<String> {
        if let Some(pos) = lines
            .iter()
            .position(|s| s == &format!("=== RUN   {case_str}"))
        {
            lines = lines[(pos + 1)..].to_vec();
        }
        if let Some(pos) = lines
            .iter()
            .rposition(|s| s.starts_with(&format!("--- FAIL: {case_str} ")))
        {
            lines = lines[..pos].to_vec();
        }
        lines
    }

    fn remove_fixture_output_fuzz(_case_str: &str, mut lines: Vec<String>) -> Vec<String> {
        if let Some(pos) = lines.iter().rposition(|s| s == "FAIL") {
            lines = lines[..pos].to_vec();
        }
        lines
    }

    fn remove_fixture_output_example(case_str: &str, mut lines: Vec<String>) -> Vec<String> {
        if let Some(pos) = lines
            .iter()
            .position(|s| s.starts_with(&format!("--- FAIL: {case_str} ")))
        {
            lines = lines[(pos + 1)..].to_vec();
        }
        if let Some(pos) = lines.iter().rposition(|s| s == "FAIL") {
            lines = lines[..pos].to_vec();
        }
        lines
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

impl CollectTests for GoTestCollector {
    const ENQUEUE_MESSAGE: &'static str = "building artifacts...";

    type BuildHandle = go_test::WaitHandle;
    type Artifact = GoTestArtifact;
    type ArtifactStream = TestArtifactStream;
    type TestFilter = pattern::Pattern;
    type PackageId = GoImportPath;
    type Package = GoPackage;
    type ArtifactKey = GoTestArtifactKey;
    type Options = GoTestOptions;
    type CaseMetadata = NoCaseMetadata;

    fn start(
        &self,
        _color: bool,
        options: &GoTestOptions,
        packages: Vec<&GoPackage>,
        ui: &UiSender,
    ) -> Result<(go_test::WaitHandle, TestArtifactStream)> {
        let packages = packages.into_iter().map(|m| &m.0).collect();

        let build_dir = self.cache_dir.join::<BuildDir>("test-binaries");
        let (wait, stream) =
            go_test::build_and_collect(options, packages, &build_dir, ui.downgrade())?;
        Ok((wait, TestArtifactStream(stream)))
    }

    fn get_packages(&self, ui: &UiSender) -> Result<Vec<GoPackage>> {
        Ok(go_test::go_list(self.project_dir.as_ref(), ui.downgrade())
            .with_context(|| "running go list")?
            .into_iter()
            .map(GoPackage)
            .collect())
    }

    fn remove_fixture_output(case_str: &str, lines: Vec<String>) -> Vec<String> {
        if case_str.starts_with("Fuzz") {
            Self::remove_fixture_output_fuzz(case_str, lines)
        } else if case_str.starts_with("Example") {
            Self::remove_fixture_output_example(case_str, lines)
        } else {
            Self::remove_fixture_output_test(case_str, lines)
        }
    }

    fn was_test_ignored(case_str: &str, lines: &[String]) -> bool {
        if let Some(last) = lines.iter().rposition(|s| !s.is_empty()) {
            if last == 0 {
                return false;
            }
            lines[last - 1].starts_with(&format!("--- SKIP: {case_str} ")) && lines[last] == "PASS"
        } else {
            false
        }
    }
}

#[test]
fn test_regular_output_not_skipped() {
    let example = indoc::indoc! {"
    === RUN   TestAdd
    test output
        foo_test.go:9: 1 + 2 != 3
    --- FAIL: TestAdd (0.00s)
    FAIL
    "};
    let ignored = GoTestCollector::was_test_ignored(
        "TestAdd",
        &example
            .split('\n')
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>(),
    );
    assert!(!ignored);
}

#[test]
fn test_empty_output_not_skipped() {
    let ignored = GoTestCollector::was_test_ignored("TestAdd", &[]);
    assert!(!ignored);
}

#[test]
fn test_single_line_not_skipped() {
    let ignored =
        GoTestCollector::was_test_ignored("TestAdd", &["--- SKIP: TestAdd (0.00s)".into()]);
    assert!(!ignored);
}

#[test]
fn test_skip_output() {
    let example = indoc::indoc! {"
    === RUN   TestAdd
    test output
        foo_test.go:11: HELLO
    --- SKIP: TestAdd (0.00s)
    PASS
    "};
    let ignored = GoTestCollector::was_test_ignored(
        "TestAdd",
        &example
            .split('\n')
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>(),
    );
    assert!(ignored);
}

#[test]
fn test_skip_output_different_case_str() {
    let example = indoc::indoc! {"
    === RUN   TestAdd2
    test output
        foo_test.go:11: HELLO
    --- SKIP: TestAdd2 (0.00s)
    PASS
    "};
    let ignored = GoTestCollector::was_test_ignored(
        "TestAdd",
        &example
            .split('\n')
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>(),
    );
    assert!(!ignored);
}

#[test]
fn remove_fixture_output_basic_case() {
    let example = indoc::indoc! {"
    === RUN   TestAdd
    test output
        foo_test.go:9: 1 + 2 != 3
    --- FAIL: TestAdd (0.00s)
    FAIL
    "};
    let cleansed = GoTestCollector::remove_fixture_output(
        "TestAdd",
        example.split('\n').map(ToOwned::to_owned).collect(),
    );
    assert_eq!(
        cleansed.join("\n") + "\n",
        indoc::indoc! {"
        test output
            foo_test.go:9: 1 + 2 != 3
        "}
    );
}

#[test]
fn remove_fixture_output_different_case_str_beginning() {
    let example = indoc::indoc! {"
    === RUN   TestAdd2
    test output
        foo_test.go:9: 1 + 2 != 3
    --- FAIL: TestAdd (0.00s)
    FAIL
    "};
    let cleansed = GoTestCollector::remove_fixture_output(
        "TestAdd",
        example.split('\n').map(ToOwned::to_owned).collect(),
    );
    assert_eq!(
        cleansed.join("\n") + "\n",
        indoc::indoc! {"
        === RUN   TestAdd2
        test output
            foo_test.go:9: 1 + 2 != 3
        "}
    );
}

#[test]
fn remove_fixture_output_different_case_str_end() {
    let example = indoc::indoc! {"
    === RUN   TestAdd
    test output
        foo_test.go:9: 1 + 2 != 3
    --- FAIL: TestAdd2 (0.00s)
    FAIL
    "};
    let cleansed = GoTestCollector::remove_fixture_output(
        "TestAdd",
        example.split('\n').map(ToOwned::to_owned).collect(),
    );
    assert_eq!(
        cleansed.join("\n"),
        indoc::indoc! {"
        test output
            foo_test.go:9: 1 + 2 != 3
        --- FAIL: TestAdd2 (0.00s)
        FAIL
        "}
    );
}

#[test]
fn remove_fixture_output_multiple_matching_lines() {
    let example = indoc::indoc! {"
    === RUN   TestAdd
    === RUN   TestAdd
    test output
        foo_test.go:9: 1 + 2 != 3
    --- FAIL: TestAdd (0.00s)
    --- FAIL: TestAdd (0.00s)
    FAIL
    "};
    let cleansed = GoTestCollector::remove_fixture_output(
        "TestAdd",
        example.split('\n').map(ToOwned::to_owned).collect(),
    );
    assert_eq!(
        cleansed.join("\n") + "\n",
        indoc::indoc! {"
        === RUN   TestAdd
        test output
            foo_test.go:9: 1 + 2 != 3
        --- FAIL: TestAdd (0.00s)
        "}
    );
}

#[test]
fn remove_fixture_output_fuzz_test() {
    let example = indoc::indoc! {"
        === RUN   FuzzAdd2
        === RUN   FuzzAdd2/seed#0
            foo_test.go:47: 1 + 2 != 3
        === RUN   FuzzAdd2/seed#1
            foo_test.go:47: 2 + 2 != 4
        === RUN   FuzzAdd2/seed#2
            foo_test.go:47: 3 + 2 != 5
        === RUN   FuzzAdd2/simple.fuzz
            foo_test.go:47: 100 + 2 != 102
        --- FAIL: FuzzAdd2 (0.00s)
            --- FAIL: FuzzAdd2/seed#0 (0.00s)
            --- FAIL: FuzzAdd2/seed#1 (0.00s)
            --- FAIL: FuzzAdd2/seed#2 (0.00s)
            --- FAIL: FuzzAdd2/simple.fuzz (0.00s)
        FAIL
    "};
    let cleansed = GoTestCollector::remove_fixture_output(
        "FuzzAdd2",
        example.split('\n').map(ToOwned::to_owned).collect(),
    );
    assert_eq!(
        cleansed.join("\n") + "\n",
        indoc::indoc! {"
        === RUN   FuzzAdd2
        === RUN   FuzzAdd2/seed#0
            foo_test.go:47: 1 + 2 != 3
        === RUN   FuzzAdd2/seed#1
            foo_test.go:47: 2 + 2 != 4
        === RUN   FuzzAdd2/seed#2
            foo_test.go:47: 3 + 2 != 5
        === RUN   FuzzAdd2/simple.fuzz
            foo_test.go:47: 100 + 2 != 102
        --- FAIL: FuzzAdd2 (0.00s)
            --- FAIL: FuzzAdd2/seed#0 (0.00s)
            --- FAIL: FuzzAdd2/seed#1 (0.00s)
            --- FAIL: FuzzAdd2/seed#2 (0.00s)
            --- FAIL: FuzzAdd2/simple.fuzz (0.00s)
        "}
    );
}

#[test]
fn remove_fixture_output_example_test() {
    let example = indoc::indoc! {"
        --- FAIL: ExamplePrintln (0.00s)
        got:
        The output of
        this example.
        want:
        Thej output of
        this example.
        FAIL
    "};
    let cleansed = GoTestCollector::remove_fixture_output(
        "ExamplePrintln",
        example.split('\n').map(ToOwned::to_owned).collect(),
    );
    assert_eq!(
        cleansed.join("\n") + "\n",
        indoc::indoc! {"
            got:
            The output of
            this example.
            want:
            Thej output of
            this example.
        "}
    );
}

impl MainAppDeps for DefaultMainAppDeps<'_> {
    fn client(&self) -> &Client {
        self.client
    }

    type TestCollector = GoTestCollector;

    fn test_collector(&self) -> &GoTestCollector {
        &self.test_collector
    }

    fn get_template_vars(&self, _: &GoTestOptions) -> Result<TemplateVars> {
        Ok(TemplateVars::default())
    }

    const TEST_METADATA_FILE_NAME: &'static str = TEST_METADATA_FILE_NAME;
    const DEFAULT_TEST_METADATA_CONTENTS: &'static str = DEFAULT_TEST_METADATA_CONTENTS;
}

#[test]
fn default_test_metadata_parses() {
    maelstrom_test_runner::metadata::Store::<pattern::Pattern>::load(
        DEFAULT_TEST_METADATA_CONTENTS,
        &Default::default(),
    )
    .unwrap();
}

impl Wait for go_test::WaitHandle {
    fn wait(&self) -> Result<WaitStatus> {
        go_test::WaitHandle::wait(self)
    }

    fn kill(&self) -> Result<()> {
        go_test::WaitHandle::kill(self)
    }
}

pub fn main(
    config: Config,
    extra_options: cli::ExtraCommandLineOptions,
    bg_proc: ClientBgProcess,
    logger: Logger,
    stdout_is_tty: bool,
    ui: impl Ui,
) -> Result<ExitCode> {
    let project_root = go_test::get_module_root()?;
    let project_dir = Root::<ProjectDir>::new(project_root.as_ref());
    main_with_stderr_and_project_dir(
        config,
        extra_options,
        bg_proc,
        logger,
        stdout_is_tty,
        ui,
        std::io::stderr(),
        project_dir,
    )
}

/// This is the `.maelstrom-go-test` directory.
pub struct HiddenDir;

#[allow(clippy::too_many_arguments)]
pub fn main_with_stderr_and_project_dir(
    config: Config,
    extra_options: cli::ExtraCommandLineOptions,
    bg_proc: ClientBgProcess,
    logger: Logger,
    stdout_is_tty: bool,
    ui: impl Ui,
    _stderr: impl io::Write,
    project_dir: &Root<ProjectDir>,
) -> Result<ExitCode> {
    let hidden_dir = AsRef::<Path>::as_ref(project_dir).join(".maelstrom-go-test");
    let hidden_dir = Root::<HiddenDir>::new(&hidden_dir);
    let state_dir = hidden_dir.join::<StateDir>("state");
    let cache_dir = hidden_dir.join::<CacheDir>("cache");

    Fs.create_dir_all(&state_dir)?;
    Fs.create_dir_all(&cache_dir)?;

    let logging_output = LoggingOutput::default();
    let log = logger.build(logging_output.clone());

    if extra_options.list.packages {
        let (ui_handle, ui) = ui.start_ui_thread(logging_output, log);

        let list_res = alternative_mains::list_packages(
            ui,
            project_dir,
            &cache_dir,
            &extra_options.parent.include,
            &extra_options.parent.exclude,
        );
        let ui_res = ui_handle.join();
        let exit_code = list_res?;
        ui_res?;
        Ok(exit_code)
    } else {
        let list_action = extra_options.list.tests.then_some(ListAction::ListTests);

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
            config.parent.artifact_transfer_strategy,
            log.clone(),
        )?;
        let deps = DefaultMainAppDeps::new(&client, project_dir, &cache_dir)?;

        let watch_exclude_paths = vec![];
        let deps = MainAppCombinedDeps::new(
            deps,
            extra_options.parent.include,
            extra_options.parent.exclude,
            list_action,
            config.parent.repeat,
            config.parent.stop_after,
            extra_options.parent.watch,
            stdout_is_tty,
            project_dir,
            &state_dir,
            watch_exclude_paths,
            config.go_test_options,
            log,
        )?;

        run_app_with_ui_multithreaded(
            deps,
            logging_output,
            config.parent.timeout.map(Timeout::new),
            ui,
        )
    }
}
