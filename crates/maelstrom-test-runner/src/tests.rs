use crate::{
    alternative_mains,
    config::Quiet,
    main_app_new,
    progress::{ProgressDriver, ProgressIndicator},
    test_listing::{ArtifactKey, ArtifactKind, TestListing, TestListingStore},
    ClientTrait, CollectTests, EnqueueResult, ListAction, LoggingOutput, MainAppDeps, MainAppState,
    TargetDir, TestArtifact, Wait, WorkspaceDir,
};
use anyhow::Result;
use cargo_metadata::{Artifact as CargoArtifact, Package as CargoPackage};
use indicatif::InMemoryTerm;
use indoc::indoc;
use maelstrom_base::{
    stats::{JobState, JobStateCounts},
    ArtifactType, ClientJobId, JobCompleted, JobEffects, JobOutcome, JobOutcomeResult,
    JobOutputResult, JobStatus, Sha256Digest,
};
use maelstrom_client::{
    spec::{JobSpec, Layer},
    IntrospectResponse, StateDir,
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
    path::Path,
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

#[derive(Clone)]
struct FakeTestBinary {
    name: String,
    kind: ArtifactKind,
    tests: Vec<FakeTestCase>,
}

impl FakeTestBinary {
    fn artifact_key(&self) -> ArtifactKey {
        ArtifactKey::new(&self.name, self.kind)
    }
}

impl Default for FakeTestBinary {
    fn default() -> Self {
        Self {
            name: Default::default(),
            kind: ArtifactKind::Library,
            tests: Default::default(),
        }
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

    fn update_listing(&self, listing: &mut TestListing) {
        listing.retain_packages_and_artifacts(
            self.test_binaries
                .iter()
                .map(|binary| (binary.name.as_str(), [binary.artifact_key()])),
        );
        for binary in &self.test_binaries {
            listing.update_artifact_cases(
                &binary.name,
                binary.artifact_key(),
                binary.tests.iter().map(|case| &case.name),
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

    fn listing(&self) -> TestListing {
        let mut listing = TestListing::default();
        self.update_listing(&mut listing);
        listing
    }

    fn packages(&self) -> Vec<CargoPackage> {
        self.test_binaries
            .iter()
            .map(|b| {
                serde_json::from_value(serde_json::json! {{
                    "name": &b.name,
                    "version": "1.0.0",
                    "id": &format!("{} 1.0.0", &b.name),
                    "dependencies": [],
                    "targets": [
                        {
                          "kind": [
                            b.kind.short_name(),
                          ],
                          "crate_types": [
                            b.kind.short_name(),
                          ],
                          "name": &b.name,
                          "src_path": "foo.rs",
                          "test": true
                        }
                    ],
                    "features": {},
                    "manifest_path": "Cargo.toml"
                }})
                .unwrap()
            })
            .collect()
    }

    fn artifacts(&self, bin_path: &Path, packages: &[String]) -> Vec<Result<CargoArtifact>> {
        let packages: HashSet<_> = packages
            .iter()
            .map(|p| p.split('@').next().unwrap())
            .collect();
        self.test_binaries
            .iter()
            .filter_map(|b| {
                if !packages.contains(b.name.as_str()) {
                    return None;
                }

                let exe = bin_path.join(&b.name);
                Some(Ok(serde_json::from_value(serde_json::json! {{
                    "package_id": &format!("{} 1.0.0", &b.name),
                    "executable": exe.to_str().unwrap(),
                    "target": {
                        "name": &b.name,
                        "kind": [b.kind.short_name()],
                        "src_path": "foo.rs"
                    },
                    "profile": {
                        "opt_level": "",
                        "debug_assertions": true,
                        "overflow_checks": true,
                        "test": true
                    },
                    "features": [],
                    "filenames": [],
                    "fresh": true

                }})
                .unwrap()))
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
struct TestProgressDriver<'scope> {
    #[allow(clippy::type_complexity)]
    update_func: Rc<RefCell<Option<Box<dyn FnMut(JobStateCounts) -> Result<bool> + 'scope>>>>,
}

impl<'scope> ProgressDriver<'scope> for TestProgressDriver<'scope> {
    fn drive<'client>(&mut self, _client: &'client impl ClientTrait, ind: impl ProgressIndicator)
    where
        'client: 'scope,
    {
        *self.update_func.borrow_mut() = Some(Box::new(move |state| ind.update_job_states(state)));
    }

    fn stop(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<'scope> TestProgressDriver<'scope> {
    fn update(&self, states: JobStateCounts) -> Result<bool> {
        (self.update_func.borrow_mut().as_mut().unwrap())(states)
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
    fn new(tests: FakeTests, bin_path: RootBuf<BinDir>, target_dir: RootBuf<TargetDir>) -> Self {
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
    target_dir: RootBuf<TargetDir>,
}

#[derive(Debug)]
struct FakeTestArtifact {
    cargo_artifact: CargoArtifact,
    tests: Vec<String>,
    ignored_tests: Vec<String>,
}

impl TestArtifact for FakeTestArtifact {
    fn path(&self) -> &Path {
        self.cargo_artifact.executable.as_ref().unwrap().as_ref()
    }

    fn list_tests(&self) -> Result<Vec<String>> {
        Ok(self.tests.clone())
    }

    fn list_ignored_tests(&self) -> Result<Vec<String>> {
        Ok(self.ignored_tests.clone())
    }

    fn cargo_artifact(&self) -> &CargoArtifact {
        &self.cargo_artifact
    }
}

impl CollectTests for TestCollector {
    type BuildHandle = WaitForNothing;
    type Artifact = FakeTestArtifact;
    type ArtifactStream = std::vec::IntoIter<Result<FakeTestArtifact>>;
    type Options = TestOptions;

    fn start(
        &self,
        _color: bool,
        _cargo_options: &TestOptions,
        packages: Vec<String>,
    ) -> Result<(Self::BuildHandle, Self::ArtifactStream)> {
        let fs = Fs::new();
        fs.create_dir_all(&self.target_dir).unwrap();
        fs.write((**self.target_dir).join("cargo_test_run"), "")
            .unwrap();

        let artifacts: Vec<_> = self
            .tests
            .artifacts(&self.bin_path, &packages)
            .into_iter()
            .map(|a| {
                a.map(|a| FakeTestArtifact {
                    tests: self.tests.cases(a.executable.as_ref().unwrap().as_ref()),
                    ignored_tests: self
                        .tests
                        .ignored_cases(a.executable.as_ref().unwrap().as_ref()),
                    cargo_artifact: a,
                })
            })
            .collect();
        Ok((WaitForNothing, artifacts.into_iter()))
    }
}

impl MainAppDeps for TestMainAppDeps {
    type Client = TestClient;

    fn client(&self) -> &TestClient {
        &self.client
    }

    type TestCollectorOptions = TestOptions;
    type TestCollector = TestCollector;
    fn test_collector(&self) -> &TestCollector {
        &self.test_collector
    }

    fn get_template_vars(
        &self,
        _options: &TestOptions,
        _target_dir: &Root<TargetDir>,
    ) -> Result<TemplateVars> {
        Ok(TemplateVars::new())
    }
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
    workspace_root: &Root<WorkspaceDir>,
    stdout_tty: bool,
    quiet: Quiet,
    include_filter: Vec<String>,
    exclude_filter: Vec<String>,
    list: Option<ListAction>,
    finish: bool,
) -> String {
    let fs = Fs::new();
    let log = test_logger();
    slog::info!(
        log, "doing test";
        "quiet" => ?quiet,
        "include_filter" => ?include_filter,
        "exclude_filter" => ?exclude_filter,
        "list" => ?list
    );
    let packages = fake_tests.packages();

    fs.create_dir_all(bin_dir).unwrap();
    fake_tests.create_binaries(&fs, bin_dir);

    let target_directory = workspace_root.join::<TargetDir>("target");
    let deps = TestMainAppDeps::new(
        fake_tests.clone(),
        bin_dir.to_owned(),
        target_directory.clone(),
    );

    let state = MainAppState::new(
        deps,
        include_filter,
        exclude_filter,
        list,
        false, // stderr_color
        workspace_root,
        &Vec::from_iter(packages.iter()),
        target_directory.join::<StateDir>("maelstrom/state"),
        &target_directory,
        TestOptions,
        LoggingOutput::default(),
        log.clone(),
    )
    .unwrap();
    let prog_driver = TestProgressDriver::default();
    let mut app = main_app_new(
        &state,
        stdout_tty,
        quiet,
        term.clone(),
        prog_driver.clone(),
        None,
    )
    .unwrap();

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

        prog_driver.update(counts_from_states(&running)).unwrap();
    }

    app.drain().unwrap();

    if finish {
        app.finish().unwrap();
    }

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
    let workspace_dir = tmp_dir.join::<WorkspaceDir>("workspace");

    let term = InMemoryTerm::new(50, 50);
    run_app(
        &bin_dir,
        term.clone(),
        fake_tests,
        &workspace_dir,
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

fn list_packages_sync(
    fake_tests: &FakeTests,
    include_filter: &Vec<String>,
    exclude_filter: &Vec<String>,
) -> String {
    let mut out = Vec::new();
    alternative_mains::list_packages(
        &fake_tests.packages().iter().collect::<Vec<_>>(),
        include_filter,
        exclude_filter,
        &mut out,
    )
    .unwrap();
    String::from_utf8(out).unwrap()
}

fn list_binaries_sync(
    fake_tests: &FakeTests,
    include_filter: &Vec<String>,
    exclude_filter: &Vec<String>,
) -> String {
    let mut out = Vec::new();
    alternative_mains::list_binaries(
        &fake_tests.packages().iter().collect::<Vec<_>>(),
        include_filter,
        exclude_filter,
        &mut out,
    )
    .unwrap();
    String::from_utf8(out).unwrap()
}

#[allow(clippy::too_many_arguments)]
fn list_all_tests_sync(
    tmp_dir: &Root<TmpDir>,
    fake_tests: FakeTests,
    quiet: Quiet,
    include_filter: Vec<String>,
    exclude_filter: Vec<String>,
    expected_packages: &str,
    expected_binaries: &str,
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

    let listing = list_binaries_sync(&fake_tests, &include_filter, &exclude_filter);
    assert_eq!(listing, expected_binaries);

    let listing = list_packages_sync(&fake_tests, &include_filter, &exclude_filter);
    assert_eq!(listing, expected_packages);
}

#[test]
fn no_tests_all_tests_sync() {
    let tmp_dir = tempdir().unwrap();
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            kind: ArtifactKind::Library,
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
            kind: ArtifactKind::Library,
            tests: vec![],
        }],
    };
    list_all_tests_sync(
        Root::new(tmp_dir.path()),
        fake_tests,
        false.into(),
        vec!["all".into()],
        vec![],
        "foo\n",
        "foo (library)\n",
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                kind: ArtifactKind::Library,
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
                kind: ArtifactKind::Binary,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                kind: ArtifactKind::Library,
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
            bar
            foo
        "},
        indoc! {"
            bar (binary)
            foo (library)
        "},
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it2".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                kind: ArtifactKind::Binary,
                tests: vec![FakeTestCase {
                    name: "testy".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bin".into(),
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                kind: ArtifactKind::Library,
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
            vec![
                "name.equals(test_it)".into(),
                "name.equals(test_it2)".into()
            ],
            vec!["package.equals(bin) || bin".into()]
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it2".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                kind: ArtifactKind::Binary,
                tests: vec![FakeTestCase {
                    name: "testy".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bin".into(),
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                kind: ArtifactKind::Library,
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
        vec![
            "name.equals(test_it)".into(),
            "name.equals(test_it2)".into(),
        ],
        vec!["package.equals(bin) || bin".into()],
        indoc! {"
            bar
            baz
            foo
        "},
        indoc! {"
            bar (library)
            foo (library)
        "},
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                kind: ArtifactKind::Library,
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
            vec!["package.equals(foo)".into()],
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
                kind: ArtifactKind::Library,
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                kind: ArtifactKind::Library,
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
            vec!["package.equals(foo) && name.equals(test_it)".into()],
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                kind: ArtifactKind::Library,
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                kind: ArtifactKind::Library,
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
    let workspace_dir = RootBuf::<WorkspaceDir>::new(tmp_dir.path().join("workspace"));

    let term = InMemoryTerm::new(50, 50);
    run_app(
        Root::new(tmp_dir.path()),
        term.clone(),
        fake_tests,
        &workspace_dir,
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
                *b"\n\
                running 1 test\n\
                this is some output from the test\n\
                this is too\n\
                test test_it ... FAILED\n\
                \n\
                failures:\n\
                \n\
                failures:\n\
                    tests::i_be_failing\n\
                    \n\
                test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 155 filtered out; \
                finished in 0.01s\n\
                \n\
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    outcome: failed_outcome.clone(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                kind: ArtifactKind::Library,
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
    let workspace_dir = RootBuf::<WorkspaceDir>::new(tmp_dir.path().join("workspace"));

    let term = InMemoryTerm::new(50, 50);
    let term_clone = term.clone();
    let contents = run_app(
        Root::new(tmp_dir.path()),
        term_clone,
        fake_tests,
        &workspace_dir,
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::WaitingForArtifacts,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                kind: ArtifactKind::Library,
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Pending,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                kind: ArtifactKind::Library,
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Running,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                kind: ArtifactKind::Library,
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Complete,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                kind: ArtifactKind::Library,
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Complete,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                kind: ArtifactKind::Library,
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
                kind: ArtifactKind::Library,
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                kind: ArtifactKind::Library,
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
        tmp_dir.join::<StateDir>("workspace/target/maelstrom/state"),
    );
    let listing = test_listing_store.load().unwrap();
    let mut expected_listing = fake_tests.listing();
    assert_eq!(listing, expected_listing);

    // remove bar
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            kind: ArtifactKind::Library,
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
            kind: ArtifactKind::Library,
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
        tmp_dir.join::<StateDir>("workspace/target/maelstrom/state"),
    );
    let listing = test_listing_store.load().unwrap();
    let mut expected_listing = fake_tests.listing();
    assert_eq!(listing, expected_listing);

    // remove the test
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            kind: ArtifactKind::Library,
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
            kind: ArtifactKind::Library,
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
        .read_dir((**tmp_dir).join("workspace/target"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(entries, vec!["maelstrom"]);
}
