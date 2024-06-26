use crate::{
    config::Quiet,
    introspect_driver::IntrospectDriver,
    metadata::TestMetadata,
    test_listing::{TestListing, TestListingStore},
    ui, BuildDir, ClientTrait, CollectTests, EnqueueResult, ListAction, LoggingOutput, MainApp,
    MainAppDeps, MainAppState, NoCaseMetadata, SimpleFilter, StringArtifactKey, TestArtifact,
    TestLayers, TestPackage, TestPackageId, Wait,
};
use anyhow::Result;
use indicatif::InMemoryTerm;
use indoc::indoc;
use maelstrom_base::{
    stats::{JobState, JobStateCounts},
    ArtifactType, ClientJobId, JobCompleted, JobEffects, JobOutcome, JobOutcomeResult,
    JobOutputResult, JobStatus, Sha256Digest, Utf8PathBuf,
};
use maelstrom_client::{
    spec::{JobSpec, Layer},
    IntrospectResponse, ProjectDir, StateDir,
};
use maelstrom_test::digest;
use maelstrom_util::{
    fs::Fs,
    log::test_logger,
    root::{Root, RootBuf},
    template::TemplateVars,
};
use pretty_assertions::assert_eq;
use std::{
    cell::RefCell,
    collections::HashSet,
    path::{Path, PathBuf},
    rc::Rc,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use tempfile::tempdir;

#[derive(Clone)]
struct FakeTestCase {
    name: String,
    ignored: bool,
    desired_state: JobState,
    expected_estimated_duration: Option<Duration>,
    outcome: JobOutcome,
}

impl FakeTestCase {
    fn timing(&self) -> Duration {
        let (JobOutcome::TimedOut(JobEffects { duration, .. })
        | JobOutcome::Completed(JobCompleted {
            effects: JobEffects { duration, .. },
            ..
        })) = self.outcome;
        duration
    }
}

impl Default for FakeTestCase {
    fn default() -> Self {
        Self {
            name: "".into(),
            ignored: false,
            desired_state: JobState::Complete,
            expected_estimated_duration: None,
            outcome: JobOutcome::Completed(JobCompleted {
                status: JobStatus::Exited(0),
                effects: JobEffects {
                    stdout: JobOutputResult::None,
                    stderr: JobOutputResult::Inline(Box::new(*b"this output should be ignored")),
                    duration: Duration::from_secs(1),
                },
            }),
        }
    }
}

#[derive(Clone, Default)]
struct FakeTestBinary {
    name: String,
    tests: Vec<FakeTestCase>,
}

impl FakeTestBinary {
    fn artifact_key(&self) -> StringArtifactKey {
        self.name.as_str().into()
    }
}

#[derive(Clone)]
struct FakeTests {
    test_binaries: Vec<FakeTestBinary>,
}

impl FakeTests {
    fn create_binaries(&self, fs: &Fs, bin_path: &Path) {
        for bin in &self.test_binaries {
            let dest = bin_path.join(&bin.name);
            if !fs.exists(&dest) {
                fs.symlink("/proc/self/exe", dest).unwrap();
            }
        }
    }

    fn update_listing(&self, listing: &mut TestListing<StringArtifactKey, NoCaseMetadata>) {
        listing.retain_packages_and_artifacts(
            self.test_binaries
                .iter()
                .map(|binary| (binary.name.as_str(), [binary.artifact_key()])),
        );
        for binary in &self.test_binaries {
            listing.update_artifact_cases(
                &binary.name,
                binary.artifact_key(),
                binary
                    .tests
                    .iter()
                    .map(|case| (case.name.clone(), NoCaseMetadata)),
            );
            for case in &binary.tests {
                listing.add_timing(
                    &binary.name,
                    binary.artifact_key(),
                    &case.name,
                    case.timing(),
                );
            }
        }
    }

    fn listing(&self) -> TestListing<StringArtifactKey, NoCaseMetadata> {
        let mut listing = TestListing::default();
        self.update_listing(&mut listing);
        listing
    }

    fn packages(&self) -> Vec<FakeTestPackage> {
        self.test_binaries
            .iter()
            .map(|b| FakeTestPackage {
                name: b.name.clone(),
                artifacts: vec![StringArtifactKey::from(b.name.as_ref())],
                id: FakePackageId(format!("{} 1.0.0", b.name)),
            })
            .collect()
    }

    fn artifacts(
        &self,
        bin_path: &Path,
        packages: Vec<&FakeTestPackage>,
    ) -> Vec<Result<FakeTestArtifact>> {
        let packages: HashSet<_> = packages.iter().map(|p| p.name()).collect();
        self.test_binaries
            .iter()
            .filter_map(|b| {
                if !packages.contains(b.name.as_str()) {
                    return None;
                }

                let exe = bin_path.join(&b.name);
                Some(Ok(FakeTestArtifact {
                    name: b.name.clone(),
                    tests: self.cases(&exe),
                    ignored_tests: self.ignored_cases(&exe),
                    path: exe,
                    package: FakePackageId(format!("{} 1.0.0", b.name)),
                }))
            })
            .collect()
    }

    fn cases(&self, binary: &Path) -> Vec<String> {
        let binary_name = binary.file_name().unwrap().to_str().unwrap();
        let binary = self.find_binary(binary_name);
        binary.tests.iter().map(|t| t.name.to_owned()).collect()
    }

    fn ignored_cases(&self, binary: &Path) -> Vec<String> {
        let binary_name = binary.file_name().unwrap().to_str().unwrap();
        let binary = self.find_binary(binary_name);
        binary
            .tests
            .iter()
            .filter(|&t| t.ignored)
            .map(|t| t.name.to_owned())
            .collect()
    }

    fn find_outcome(&self, spec: JobSpec) -> Option<JobOutcome> {
        let binary_name = spec.program.file_name().unwrap();
        let binary = self.find_binary(binary_name);
        let case_name = spec
            .arguments
            .iter()
            .find(|a| !a.starts_with("--"))
            .unwrap();
        let case = binary.tests.iter().find(|c| &c.name == case_name).unwrap();
        assert_eq!(&spec.estimated_duration, &case.expected_estimated_duration);
        (case.desired_state == JobState::Complete).then(|| case.outcome.clone())
    }

    fn find_binary(&self, binary_name: &str) -> &FakeTestBinary {
        self.test_binaries
            .iter()
            .find(|b| b.name == binary_name)
            .unwrap_or_else(|| panic!("binary {binary_name} not found"))
    }

    fn find_case(&self, binary_name: &str, case: &str) -> &FakeTestCase {
        let binary = self.find_binary(binary_name);
        binary.tests.iter().find(|c| &c.name == case).unwrap()
    }
}

#[derive(Default, Clone)]
struct TestIntrospectDriver<'scope> {
    #[allow(clippy::type_complexity)]
    update_func: Rc<RefCell<Option<Box<dyn FnMut(IntrospectResponse) + 'scope>>>>,
}

impl<'scope> IntrospectDriver<'scope> for TestIntrospectDriver<'scope> {
    fn drive<'client>(&mut self, _client: &'client impl ClientTrait, ind: ui::UiSender)
    where
        'client: 'scope,
    {
        *self.update_func.borrow_mut() =
            Some(Box::new(move |resp| ind.update_introspect_state(resp)));
    }

    fn stop(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<'scope> TestIntrospectDriver<'scope> {
    fn update(&self, job_state_counts: JobStateCounts) {
        let resp = IntrospectResponse {
            job_state_counts,
            artifact_uploads: vec![],
            image_downloads: vec![],
        };
        (self.update_func.borrow_mut().as_mut().unwrap())(resp)
    }
}

struct WaitForNothing;

impl Wait for WaitForNothing {
    fn wait(self) -> Result<()> {
        Ok(())
    }
}

struct BinDir;
struct TmpDir;

struct TestMainAppDeps {
    client: TestClient,
    test_collector: TestCollector,
}

impl TestMainAppDeps {
    fn new(tests: FakeTests, bin_path: RootBuf<BinDir>, target_dir: RootBuf<BuildDir>) -> Self {
        Self {
            client: TestClient {
                next_job_id: AtomicU32::new(1),
                tests: tests.clone(),
            },
            test_collector: TestCollector {
                tests,
                bin_path,
                target_dir,
            },
        }
    }
}

struct TestOptions;

struct TestClient {
    next_job_id: AtomicU32,
    tests: FakeTests,
}

impl ClientTrait for TestClient {
    fn add_layer(&self, _layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        Ok((digest!(42), ArtifactType::Manifest))
    }

    fn introspect(&self) -> Result<IntrospectResponse> {
        todo!()
    }

    fn add_job(
        &self,
        spec: JobSpec,
        handler: impl FnOnce(Result<(ClientJobId, JobOutcomeResult)>) + Send + Sync + 'static,
    ) -> Result<()> {
        let cjid = ClientJobId::from_u32(self.next_job_id.fetch_add(1, Ordering::AcqRel));
        if let Some(outcome) = self.tests.find_outcome(spec) {
            handler(Ok((cjid, Ok(outcome))));
        }
        Ok(())
    }
}

struct TestCollector {
    tests: FakeTests,
    bin_path: RootBuf<BinDir>,
    target_dir: RootBuf<BuildDir>,
}

#[derive(Clone, Debug)]
struct FakeTestArtifact {
    name: String,
    tests: Vec<String>,
    ignored_tests: Vec<String>,
    path: PathBuf,
    package: FakePackageId,
}

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq)]
struct FakePackageId(String);

impl TestPackageId for FakePackageId {}

impl TestArtifact for FakeTestArtifact {
    type ArtifactKey = StringArtifactKey;
    type PackageId = FakePackageId;
    type CaseMetadata = NoCaseMetadata;

    fn package(&self) -> FakePackageId {
        self.package.clone()
    }

    fn to_key(&self) -> StringArtifactKey {
        StringArtifactKey::from(self.name.as_ref())
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn list_tests(&self) -> Result<Vec<(String, NoCaseMetadata)>> {
        Ok(self
            .tests
            .iter()
            .map(|name| (name.clone(), NoCaseMetadata))
            .collect())
    }

    fn list_ignored_tests(&self) -> Result<Vec<String>> {
        Ok(self.ignored_tests.clone())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn build_command(
        &self,
        case_name: &str,
        _case_metadata: &NoCaseMetadata,
    ) -> (Utf8PathBuf, Vec<String>) {
        let binary_name = self.path().file_name().unwrap().to_str().unwrap();
        (format!("/{binary_name}").into(), vec![case_name.into()])
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
struct FakeTestPackage {
    name: String,
    artifacts: Vec<StringArtifactKey>,
    id: FakePackageId,
}

impl TestPackage for FakeTestPackage {
    type PackageId = FakePackageId;
    type ArtifactKey = StringArtifactKey;

    fn name(&self) -> &str {
        &self.name
    }

    fn artifacts(&self) -> Vec<Self::ArtifactKey> {
        self.artifacts.clone()
    }

    fn id(&self) -> Self::PackageId {
        self.id.clone()
    }
}

impl CollectTests for TestCollector {
    const ENQUEUE_MESSAGE: &'static str = "building artifacts...";

    type BuildHandle = WaitForNothing;
    type Artifact = FakeTestArtifact;
    type ArtifactStream = std::vec::IntoIter<Result<FakeTestArtifact>>;
    type Options = TestOptions;
    type TestFilter = SimpleFilter;
    type ArtifactKey = StringArtifactKey;
    type PackageId = FakePackageId;
    type Package = FakeTestPackage;
    type CaseMetadata = NoCaseMetadata;

    fn start(
        &self,
        _color: bool,
        _options: &TestOptions,
        packages: Vec<&FakeTestPackage>,
    ) -> Result<(Self::BuildHandle, Self::ArtifactStream)> {
        let fs = Fs::new();
        fs.create_dir_all(&self.target_dir).unwrap();
        fs.write((**self.target_dir).join("test_run"), "").unwrap();

        let artifacts: Vec<_> = self.tests.artifacts(&self.bin_path, packages);
        Ok((WaitForNothing, artifacts.into_iter()))
    }

    fn get_test_layers(&self, _metadata: &TestMetadata, _ind: &ui::UiSender) -> Result<TestLayers> {
        Ok(TestLayers::GenerateForBinary)
    }

    fn remove_fixture_output(case_str: &str, lines: Vec<String>) -> Vec<String> {
        lines
            .into_iter()
            .filter(|line| {
                !(line.starts_with("fixture") || line.starts_with(&format!("{case_str} FAILED")))
            })
            .collect()
    }
}

impl MainAppDeps for TestMainAppDeps {
    type Client = TestClient;

    fn client(&self) -> &TestClient {
        &self.client
    }

    type TestCollector = TestCollector;
    fn test_collector(&self) -> &TestCollector {
        &self.test_collector
    }

    fn get_template_vars(&self, _options: &TestOptions) -> Result<TemplateVars> {
        Ok(TemplateVars::new())
    }

    const MAELSTROM_TEST_TOML: &'static str = "maelstrom-test.toml";
}

fn counts_from_states(states: &[JobState]) -> JobStateCounts {
    let mut counts = JobStateCounts::default();
    for state in states {
        counts[*state] += 1;
    }
    counts
}

#[allow(clippy::too_many_arguments)]
fn run_app(
    bin_dir: &Root<BinDir>,
    term: InMemoryTerm,
    fake_tests: FakeTests,
    project_dir: &Root<ProjectDir>,
    stdout_tty: bool,
    quiet: Quiet,
    include_filter: Vec<String>,
    exclude_filter: Vec<String>,
    list: Option<ListAction>,
    finish: bool,
) -> String {
    let fs = Fs::new();
    let log = test_logger();
    let packages = fake_tests.packages();
    slog::info!(
        log, "doing test";
        "quiet" => ?quiet,
        "include_filter" => ?include_filter,
        "exclude_filter" => ?exclude_filter,
        "list" => ?list,
        "packages" => ?packages
    );

    fs.create_dir_all(bin_dir).unwrap();
    fake_tests.create_binaries(&fs, bin_dir);

    let target_directory = project_dir.join::<BuildDir>("target");
    let deps = TestMainAppDeps::new(
        fake_tests.clone(),
        bin_dir.to_owned(),
        target_directory.clone(),
    );

    let is_list = list.is_some();
    let state = MainAppState::new(
        deps,
        include_filter,
        exclude_filter,
        list,
        false, // stderr_color
        project_dir,
        &packages,
        target_directory.join::<StateDir>("maelstrom/state"),
        TestOptions,
        LoggingOutput::default(),
        log.clone(),
    )
    .unwrap();
    let (ui_send, ui_recv) = std::sync::mpsc::channel();
    let ui_sender = ui::UiSender::new(ui_send);
    let introspect_driver = TestIntrospectDriver::default();
    let mut app = MainApp::new(&state, ui_sender, introspect_driver.clone(), None).unwrap();

    let mut running = vec![];
    loop {
        let res = app.enqueue_one().unwrap();
        let (package_name, case) = match res {
            EnqueueResult::Done => break,
            EnqueueResult::Ignored | EnqueueResult::Listed => continue,
            EnqueueResult::Enqueued { package_name, case } => (package_name, case),
        };
        let test = fake_tests.find_case(&package_name, &case);
        running.push(test.desired_state);

        introspect_driver.update(counts_from_states(&running));
    }

    app.drain().unwrap();

    if finish {
        app.finish().unwrap();
    }

    drop(app);
    drop(introspect_driver);
    drop(state);

    let mut ui = ui::UiImpl::new(
        ui::UiKind::Simple,
        TestCollector::ENQUEUE_MESSAGE,
        is_list,
        stdout_tty,
        quiet,
        term.clone(),
    );
    ui.run(ui_recv).unwrap();

    slog::info!(log, "test complete");

    term.contents()
}

fn run_or_list_all_tests_sync(
    tmp_dir: &Root<TmpDir>,
    fake_tests: FakeTests,
    quiet: Quiet,
    include_filter: Vec<String>,
    exclude_filter: Vec<String>,
    list: Option<ListAction>,
) -> String {
    let bin_dir = tmp_dir.join::<BinDir>("bin");
    let project_dir = tmp_dir.join::<ProjectDir>("project");

    let term = InMemoryTerm::new(50, 50);
    run_app(
        &bin_dir,
        term.clone(),
        fake_tests,
        &project_dir,
        false, // stdout_tty
        quiet,
        include_filter,
        exclude_filter,
        list,
        true, // finish
    )
}

fn run_all_tests_sync(
    tmp_dir: &Root<TmpDir>,
    fake_tests: FakeTests,
    quiet: Quiet,
    include_filter: Vec<String>,
    exclude_filter: Vec<String>,
) -> String {
    run_or_list_all_tests_sync(
        tmp_dir,
        fake_tests,
        quiet,
        include_filter,
        exclude_filter,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn list_all_tests_sync(
    tmp_dir: &Root<TmpDir>,
    fake_tests: FakeTests,
    quiet: Quiet,
    include_filter: Vec<String>,
    exclude_filter: Vec<String>,
    expected_tests: &str,
) {
    let listing = run_or_list_all_tests_sync(
        tmp_dir,
        fake_tests.clone(),
        quiet.clone(),
        include_filter.clone(),
        exclude_filter.clone(),
        Some(ListAction::ListTests),
    );
    assert_eq!(listing, expected_tests);
}

#[test]
fn no_tests_all_tests_sync() {
    let tmp_dir = tempdir().unwrap();
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            tests: vec![],
        }],
    };
    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            fake_tests,
            false.into(),
            vec!["all".into()],
            vec![]
        ),
        "\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         0\n\
        Failed Tests    :         0\
        "
    );
}

#[test]
fn no_tests_all_tests_sync_listing() {
    let tmp_dir = tempdir().unwrap();
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            tests: vec![],
        }],
    };
    list_all_tests_sync(
        Root::new(tmp_dir.path()),
        fake_tests,
        false.into(),
        vec!["all".into()],
        vec![],
        "",
    );
}

#[test]
fn two_tests_all_tests_sync() {
    let tmp_dir = tempdir().unwrap();
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
        ],
    };
    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            fake_tests,
            false.into(),
            vec!["all".into()],
            vec![]
        ),
        "\
        bar test_it............................OK   1.000s\n\
        foo test_it............................OK   1.000s\n\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         2\n\
        Failed Tests    :         0\
        "
    );
}

#[test]
fn two_tests_all_tests_sync_listing() {
    let tmp_dir = tempdir().unwrap();
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
        ],
    };
    list_all_tests_sync(
        Root::new(tmp_dir.path()),
        fake_tests,
        false.into(),
        vec!["all".into()],
        vec![],
        indoc! {"
            bar test_it
            foo test_it\
        "},
    );
}

#[test]
fn four_tests_filtered_sync() {
    let tmp_dir = tempdir().unwrap();
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it2".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                tests: vec![FakeTestCase {
                    name: "testy".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bin".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
        ],
    };
    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            fake_tests,
            false.into(),
            vec!["name = \"test_it\"".into(), "name = \"test_it2\"".into()],
            vec!["package = \"bin\"".into()]
        ),
        "\
        bar test_it2...........................OK   1.000s\n\
        foo test_it............................OK   1.000s\n\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         2\n\
        Failed Tests    :         0\
        "
    );
}

#[test]
fn four_tests_filtered_sync_listing() {
    let tmp_dir = tempdir().unwrap();
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it2".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                tests: vec![FakeTestCase {
                    name: "testy".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bin".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
        ],
    };
    list_all_tests_sync(
        Root::new(tmp_dir.path()),
        fake_tests,
        false.into(),
        vec!["name = \"test_it\"".into(), "name = \"test_it2\"".into()],
        vec!["package = \"bin\"".into()],
        indoc! {"
            bar test_it2
            foo test_it\
        "},
    );
}

#[test]
fn three_tests_single_package_sync() {
    let tmp_dir = tempdir().unwrap();
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
        ],
    };
    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            fake_tests,
            false.into(),
            vec!["package = \"foo\"".into()],
            vec![]
        ),
        "\
        foo test_it............................OK   1.000s\n\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         1\n\
        Failed Tests    :         0\
        "
    );
}

#[test]
fn three_tests_single_package_filtered_sync() {
    let tmp_dir = tempdir().unwrap();
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![
                    FakeTestCase {
                        name: "test_it".into(),
                        ..Default::default()
                    },
                    FakeTestCase {
                        name: "testy".into(),
                        ..Default::default()
                    },
                ],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
        ],
    };
    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            fake_tests,
            false.into(),
            vec!["and = [{ package = \"foo\" }, { name = \"test_it\" }]".into()],
            vec![]
        ),
        "\
        foo test_it............................OK   1.000s\n\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         1\n\
        Failed Tests    :         0\
        "
    );
}

#[test]
fn ignored_test_sync() {
    let tmp_dir = tempdir().unwrap();
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ignored: true,
                    ..Default::default()
                }],
            },
        ],
    };
    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            fake_tests,
            false.into(),
            vec!["all".into()],
            vec![]
        ),
        "\
        bar test_it............................OK   1.000s\n\
        baz test_it............................OK   1.000s\n\
        foo test_it.......................IGNORED\n\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         2\n\
        Failed Tests    :         0\n\
        Ignored Tests   :         1\n\
        \x20\x20\x20\x20foo test_it: ignored\
        "
    );
}

#[test]
fn two_tests_all_tests_sync_quiet() {
    let tmp_dir = tempdir().unwrap();
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
        ],
    };
    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            fake_tests,
            true.into(),
            vec!["all".into()],
            vec![]
        ),
        "\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         2\n\
        Failed Tests    :         0\
        "
    );
}

fn run_failed_tests(fake_tests: FakeTests) -> String {
    let tmp_dir = tempdir().unwrap();
    let project_dir = RootBuf::<ProjectDir>::new(tmp_dir.path().join("project"));

    let term = InMemoryTerm::new(50, 50);
    run_app(
        Root::new(tmp_dir.path()),
        term.clone(),
        fake_tests,
        &project_dir,
        false, // stdout_tty
        Quiet::from(false),
        vec!["all".into()],
        vec![],
        None,
        true, // finish
    );

    term.contents()
}

#[test]
fn failed_tests() {
    let failed_outcome = JobOutcome::Completed(JobCompleted {
        status: JobStatus::Exited(1),
        effects: JobEffects {
            stdout: JobOutputResult::Inline(Box::new(
                *b"\
                this is some output from the test\n\
                this is too\n\
                test_it FAILED\n\
                fixture summary\
                ",
            )),
            stderr: JobOutputResult::Inline(Box::new(*b"error output")),
            duration: Duration::from_secs(1),
        },
    });
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    outcome: failed_outcome.clone(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    outcome: failed_outcome.clone(),
                    ..Default::default()
                }],
            },
        ],
    };
    assert_eq!(
        run_failed_tests(fake_tests),
        "\
        bar test_it..........................FAIL   1.000s\n\
        this is some output from the test\n\
        this is too\n\
        stderr: error output\n\
        foo test_it..........................FAIL   1.000s\n\
        this is some output from the test\n\
        this is too\n\
        stderr: error output\n\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         0\n\
        Failed Tests    :         2\n\
        \x20\x20\x20\x20bar test_it: failure\n\
        \x20\x20\x20\x20foo test_it: failure\
        "
    );
}

fn run_in_progress_test(fake_tests: FakeTests, quiet: Quiet, expected_output: &str) {
    let tmp_dir = tempdir().unwrap();
    let project_dir = RootBuf::<ProjectDir>::new(tmp_dir.path().join("project"));

    let term = InMemoryTerm::new(50, 50);
    let term_clone = term.clone();
    let contents = run_app(
        Root::new(tmp_dir.path()),
        term_clone,
        fake_tests,
        &project_dir,
        true, // stdout_tty
        quiet,
        vec!["all".into()],
        vec![],
        None,
        false, // finish
    );
    assert_eq!(contents, expected_output);
}

#[test]
fn waiting_for_artifacts() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::WaitingForArtifacts,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::WaitingForArtifacts,
                    ..Default::default()
                }],
            },
        ],
    };
    run_in_progress_test(
        fake_tests,
        false.into(),
        "\
        ######################## 2/2 waiting for artifacts\n\
        ------------------------ 0/2 pending\n\
        ------------------------ 0/2 running\n\
        ------------------------ 0/2 complete\
        ",
    );
}

#[test]
fn pending() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Pending,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Pending,
                    ..Default::default()
                }],
            },
        ],
    };
    run_in_progress_test(
        fake_tests,
        false.into(),
        "\
        ######################## 2/2 waiting for artifacts\n\
        ######################## 2/2 pending\n\
        ------------------------ 0/2 running\n\
        ------------------------ 0/2 complete\
        ",
    );
}

#[test]
fn running() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Running,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Running,
                    ..Default::default()
                }],
            },
        ],
    };
    run_in_progress_test(
        fake_tests,
        false.into(),
        "\
        ######################## 2/2 waiting for artifacts\n\
        ######################## 2/2 pending\n\
        ######################## 2/2 running\n\
        ------------------------ 0/2 complete\
        ",
    );
}

#[test]
fn complete() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Complete,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Running,
                    ..Default::default()
                }],
            },
        ],
    };
    run_in_progress_test(
        fake_tests,
        false.into(),
        "\
        foo test_it............................OK   1.000s\n\
        ######################## 2/2 waiting for artifacts\n\
        ######################## 2/2 pending\n\
        ######################## 2/2 running\n\
        #############----------- 1/2 complete\
        ",
    );
}

#[test]
fn complete_quiet() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Complete,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Running,
                    ..Default::default()
                }],
            },
        ],
    };
    run_in_progress_test(
        fake_tests,
        true.into(),
        "#####################-------------------- 1/2 jobs",
    );
}

#[test]
fn expected_count_updates_packages() {
    let tmp_dir = tempdir().unwrap();
    let tmp_dir = Root::new(tmp_dir.path());
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    outcome: JobOutcome::TimedOut(JobEffects {
                        stdout: JobOutputResult::None,
                        stderr: JobOutputResult::None,
                        duration: Duration::from_secs(1),
                    }),
                    ..Default::default()
                }],
            },
        ],
    };
    run_all_tests_sync(
        tmp_dir,
        fake_tests.clone(),
        false.into(),
        vec!["all".into()],
        vec![],
    );

    let test_listing_store = TestListingStore::new(
        Fs::new(),
        tmp_dir.join::<StateDir>("project/target/maelstrom/state"),
    );
    let listing = test_listing_store.load().unwrap();
    let mut expected_listing = fake_tests.listing();
    assert_eq!(listing, expected_listing);

    // remove bar
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            tests: vec![FakeTestCase {
                name: "test_it".into(),
                expected_estimated_duration: Some(Duration::from_secs(1)),
                ..Default::default()
            }],
        }],
    };

    run_all_tests_sync(
        tmp_dir,
        fake_tests.clone(),
        false.into(),
        vec!["all".into()],
        vec![],
    );

    // new listing should match
    let listing = test_listing_store.load().unwrap();
    fake_tests.update_listing(&mut expected_listing);
    assert_eq!(listing, expected_listing);
}

#[test]
fn expected_count_updates_cases() {
    let tmp_dir = tempdir().unwrap();
    let tmp_dir = Root::new(tmp_dir.path());
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            tests: vec![FakeTestCase {
                name: "test_it".into(),
                ..Default::default()
            }],
        }],
    };
    run_all_tests_sync(
        tmp_dir,
        fake_tests.clone(),
        false.into(),
        vec!["all".into()],
        vec![],
    );

    let test_listing_store = TestListingStore::new(
        Fs::new(),
        tmp_dir.join::<StateDir>("project/target/maelstrom/state"),
    );
    let listing = test_listing_store.load().unwrap();
    let mut expected_listing = fake_tests.listing();
    assert_eq!(listing, expected_listing);

    // remove the test
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            tests: vec![],
        }],
    };

    run_all_tests_sync(
        tmp_dir,
        fake_tests.clone(),
        false.into(),
        vec!["all".into()],
        vec![],
    );

    // new listing should match
    let listing = test_listing_store.load().unwrap();
    fake_tests.update_listing(&mut expected_listing);
    assert_eq!(listing, expected_listing);
}

#[test]
fn filtering_none_does_not_build() {
    let tmp_dir = tempdir().unwrap();
    let tmp_dir = Root::new(tmp_dir.path());
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            tests: vec![FakeTestCase {
                name: "test_it".into(),
                ..Default::default()
            }],
        }],
    };
    run_all_tests_sync(
        tmp_dir,
        fake_tests.clone(),
        false.into(),
        vec!["none".into()],
        vec![],
    );

    let entries: Vec<_> = Fs::new()
        .read_dir((**tmp_dir).join("project/target"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(entries, vec!["maelstrom"]);
}
