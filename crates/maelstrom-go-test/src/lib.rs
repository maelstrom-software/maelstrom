pub mod alternative_mains;
pub mod cli;
mod config;
mod go_test;
mod pattern;

pub use config::Config;
pub use maelstrom_test_runner::log::LoggerBuilder;

use anyhow::{Context as _, Result};
use cli::{ExtraCommandLineOptions, ListOptions};
use config::GoTestConfig;
use maelstrom_base::{Utf8Path, Utf8PathBuf};
use maelstrom_client::{
    shared_library_dependencies_layer_spec,
    spec::{LayerSpec, PathsLayerSpec, PrefixOptions},
    CacheDir, Client, ProjectDir,
};
use maelstrom_test_runner::{
    metadata::Metadata, ui::UiSender, util::UseColor, BuildDir, Directories, ListingMode,
    NoCaseMetadata, TestArtifact, TestArtifactKey, TestCollector, TestFilter, TestPackage,
    TestPackageId, Wait, WaitStatus,
};
use maelstrom_util::{
    fs::Fs,
    process::ExitCode,
    root::{Root, RootBuf},
};
use regex_macro::regex;
use std::{fmt, path::Path, str::FromStr};

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

pub struct GoTestCollector {
    cache_dir: RootBuf<CacheDir>,
    config: GoTestConfig,
    project_dir: RootBuf<ProjectDir>,
}

impl GoTestCollector {
    fn new(
        cache_dir: &Root<CacheDir>,
        config: GoTestConfig,
        project_dir: &Root<ProjectDir>,
    ) -> Self {
        Self {
            cache_dir: cache_dir.to_owned(),
            config,
            project_dir: project_dir.to_owned(),
        }
    }
}

#[derive(Debug)]
pub struct GoTestArtifact {
    id: GoImportPath,
    path: Utf8PathBuf,
    options: GoTestConfig,
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
pub struct GoImportPath(String);

impl GoImportPath {
    fn short_name(&self) -> &str {
        let mut comp = self.0.split('/').collect::<Vec<&str>>().into_iter().rev();
        let last = comp.next().unwrap();

        let version_re = regex!("^v[0-9]*$");
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

        if metadata.include_shared_libraries {
            // Go binaries usually are statically linked, but on the off-chance they use some OS
            // library or something, doesn't hurt to check.
            layers.push(so_layer_for_binary(&self.path));
        }

        layers
    }
}

#[derive(Clone, Debug)]
pub struct GoPackage(go_test::GoPackage);

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

pub struct TestArtifactStream(go_test::TestArtifactStream);

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

impl TestCollector for GoTestCollector {
    const ENQUEUE_MESSAGE: &'static str = "building artifacts...";

    type BuildHandle = go_test::WaitHandle;
    type Artifact = GoTestArtifact;
    type ArtifactStream = TestArtifactStream;
    type TestFilter = pattern::Pattern;
    type PackageId = GoImportPath;
    type Package = GoPackage;
    type ArtifactKey = GoTestArtifactKey;
    type CaseMetadata = NoCaseMetadata;

    fn start(
        &self,
        _use_color: UseColor,
        packages: Vec<&GoPackage>,
        ui: &UiSender,
    ) -> Result<(go_test::WaitHandle, TestArtifactStream)> {
        let packages = packages.into_iter().map(|m| &m.0).collect();

        let build_dir = self.cache_dir.join::<BuildDir>("test-binaries");
        let (wait, stream) =
            go_test::build_and_collect(&self.config, packages, &build_dir, ui.downgrade())?;
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

#[test]
fn default_test_metadata_parses() {
    use maelstrom_test_runner::TestRunner as _;
    maelstrom_test_runner::metadata::Store::<pattern::Pattern>::load(
        TestRunner::DEFAULT_TEST_METADATA_FILE_CONTENTS,
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

/// This is the `.maelstrom-go-test` directory.
pub struct HiddenDir;

pub struct TestRunner;

impl maelstrom_test_runner::TestRunner for TestRunner {
    type Config = Config;
    type ExtraCommandLineOptions = ExtraCommandLineOptions;
    type Metadata = ();
    type TestCollector<'client> = GoTestCollector;
    type TestCollectorConfig = GoTestConfig;

    const BASE_DIRECTORIES_PREFIX: &'static str = "maelstrom/maelstrom-go-test";
    const ENVIRONMENT_VARIABLE_PREFIX: &'static str = "MAELSTROM_GO_TEST";
    const TEST_METADATA_FILE_NAME: &'static str = "maelstrom-go-test.toml";
    const DEFAULT_TEST_METADATA_FILE_CONTENTS: &'static str =
        include_str!("default-test-metadata.toml");

    fn get_listing_mode(extra_options: &ExtraCommandLineOptions) -> ListingMode {
        match &extra_options.list {
            ListOptions {
                tests: true,
                packages: false,
            } => ListingMode::Tests,
            ListOptions {
                tests: false,
                packages: true,
            } => ListingMode::OtherWithUi,
            ListOptions {
                tests: false,
                packages: false,
            } => ListingMode::None,
            options => {
                panic!("invalid ListOptions {options:?}, clap should have disallowed");
            }
        }
    }

    fn execute_listing_with_ui(
        config: &Config,
        extra_options: &ExtraCommandLineOptions,
        ui_sender: UiSender,
    ) -> Result<ExitCode> {
        assert!(!extra_options.list.tests);
        assert!(extra_options.list.packages);

        let (metadata, project_dir) = Self::get_metadata_and_project_directory(config)?;
        let directories = Self::get_directories(&metadata, project_dir);

        Fs.create_dir_all(&directories.cache)?;

        alternative_mains::list_packages(
            ui_sender,
            &directories.project,
            &directories.cache,
            &extra_options.parent.include,
            &extra_options.parent.exclude,
        )
    }

    fn get_metadata_and_project_directory(_config: &Config) -> Result<((), RootBuf<ProjectDir>)> {
        Ok(((), RootBuf::new(go_test::get_module_root()?)))
    }

    fn get_directories(_metadata: &(), project: RootBuf<ProjectDir>) -> Directories {
        let hidden = project.join::<HiddenDir>(".maelstrom-go-test");
        let cache = hidden.join("cache");
        let build = cache.join("test-binaries");
        let state = hidden.join("state");
        Directories {
            build,
            cache,
            project,
            state,
        }
    }

    fn build_test_collector(
        _client: &Client,
        config: &GoTestConfig,
        directories: &Directories,
        _log: &slog::Logger,
        _metadata: (),
    ) -> Result<GoTestCollector> {
        Ok(GoTestCollector::new(
            &directories.cache,
            config.clone(),
            &directories.project,
        ))
    }
}
