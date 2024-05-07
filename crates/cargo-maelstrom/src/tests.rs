use crate::{
    cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions},
    config::Quiet,
    main_app_new,
    progress::{ProgressDriver, ProgressIndicator},
    test_listing::{
        load_test_listing, test_listing_file, ArtifactCases, ArtifactKey, ArtifactKind, Package,
        TestListing,
    },
    EnqueueResult, ListAction, LoggingOutput, MainAppDeps, MainAppState, TargetDir, Wait,
    WorkspaceDir,
};
use anyhow::Result;
use cargo_metadata::{Artifact as CargoArtifact, Package as CargoPackage};
use indicatif::InMemoryTerm;
use maelstrom_base::{
    stats::{JobState, JobStateCounts},
    ArtifactType, ClientJobId, JobCompleted, JobEffects, JobOutcome, JobOutcomeResult,
    JobOutputResult, JobSpec, JobStatus, Sha256Digest,
};
use maelstrom_client::{
    spec::{ImageConfig, Layer},
    IntrospectResponse, StateDir,
};
use maelstrom_test::digest;
use maelstrom_util::{
    fs::Fs,
    log::test_logger,
    root::{Root, RootBuf},
};
use std::collections::HashSet;
use std::{
    cell::RefCell,
    path::Path,
    rc::Rc,
    sync::atomic::{AtomicU32, Ordering},
};
use tempfile::tempdir;

#[derive(Clone)]
struct FakeTestCase {
    name: String,
    ignored: bool,
    desired_state: JobState,
    outcome: JobOutcome,
}

impl Default for FakeTestCase {
    fn default() -> Self {
        Self {
            name: "".into(),
            ignored: false,
            desired_state: JobState::Complete,
            outcome: JobOutcome::Completed(JobCompleted {
                status: JobStatus::Exited(0),
                effects: JobEffects {
                    stdout: JobOutputResult::None,
                    stderr: JobOutputResult::Inline(Box::new(*b"this output should be ignored")),
                    duration: std::time::Duration::from_secs(1),
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

    fn listing(&self) -> TestListing {
        TestListing {
            version: Default::default(),
            packages: self
                .test_binaries
                .iter()
                .map(|b| {
                    (
                        b.name.clone(),
                        Package {
                            artifacts: [(
                                ArtifactKey {
                                    name: b.name.clone(),
                                    kind: ArtifactKind::Library,
                                },
                                ArtifactCases {
                                    cases: b.tests.iter().map(|t| t.name.clone()).collect(),
                                },
                            )]
                            .into_iter()
                            .collect(),
                        },
                    )
                })
                .collect(),
        }
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
                            "lib"
                          ],
                          "crate_types": [
                            "lib"
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
            .map(|p| p.split("@").next().unwrap())
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
                        "kind": ["lib"],
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
        let binary = self.find_binary(&binary_name);
        binary.tests.iter().map(|t| t.name.to_owned()).collect()
    }

    fn ignored_cases(&self, binary: &Path) -> Vec<String> {
        let binary_name = binary.file_name().unwrap().to_str().unwrap();
        let binary = self.find_binary(&binary_name);
        binary
            .tests
            .iter()
            .filter_map(|t| t.ignored.then(|| t.name.to_owned()))
            .collect()
    }

    fn find_outcome(&self, spec: JobSpec) -> Option<JobOutcome> {
        let binary_name = spec.program.file_name().unwrap();
        let binary = self.find_binary(&binary_name);
        let case_name = spec
            .arguments
            .iter()
            .find(|a| !a.starts_with("--"))
            .unwrap();
        let case = binary.tests.iter().find(|c| &c.name == case_name).unwrap();
        (case.desired_state == JobState::Complete).then(|| case.outcome.clone())
    }

    fn find_binary(&self, binary_name: &str) -> &FakeTestBinary {
        self.test_binaries
            .iter()
            .find(|b| b.name == binary_name)
            .expect(&format!("binary {binary_name} not found"))
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
    fn drive<'dep>(&mut self, _client: &'dep impl MainAppDeps, ind: impl ProgressIndicator)
    where
        'dep: 'scope,
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
    tests: FakeTests,
    bin_path: RootBuf<BinDir>,
    target_dir: RootBuf<TargetDir>,
    next_job_id: AtomicU32,
}

impl TestMainAppDeps {
    fn new(tests: FakeTests, bin_path: RootBuf<BinDir>, target_dir: RootBuf<TargetDir>) -> Self {
        Self {
            tests,
            bin_path,
            target_dir,
            next_job_id: AtomicU32::new(1),
        }
    }
}

impl MainAppDeps for TestMainAppDeps {
    fn add_layer(&self, _layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        Ok((digest!(42), ArtifactType::Manifest))
    }

    fn introspect(&self) -> Result<IntrospectResponse> {
        todo!()
    }

    fn get_container_image(&self, _name: &str, _tag: &str) -> Result<ImageConfig> {
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

    type CargoWaitHandle = WaitForNothing;
    type CargoTestArtifactStream = std::vec::IntoIter<Result<CargoArtifact>>;

    fn run_cargo_test(
        &self,
        _color: bool,
        _feature_selection_options: &FeatureSelectionOptions,
        _compilation_options: &CompilationOptions,
        _manifest_options: &ManifestOptions,
        packages: Vec<String>,
    ) -> Result<(Self::CargoWaitHandle, Self::CargoTestArtifactStream)> {
        let fs = Fs::new();
        fs.create_dir_all(&self.target_dir).unwrap();
        fs.write((**self.target_dir).join("cargo_test_run"), "")
            .unwrap();

        Ok((
            WaitForNothing,
            self.tests.artifacts(&self.bin_path, &packages).into_iter(),
        ))
    }

    fn get_cases_from_binary(&self, binary: &Path, filter: &Option<String>) -> Result<Vec<String>> {
        match filter.as_ref().map(|s| s.as_str()) {
            Some("--ignored") => Ok(self.tests.ignored_cases(binary)),
            None => Ok(self.tests.cases(binary)),
            o => panic!("unsupported filter {o:?}"),
        }
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
        test_listing_file(target_directory.join::<StateDir>("maelstrom/state")),
        &target_directory,
        FeatureSelectionOptions::default(),
        CompilationOptions::default(),
        ManifestOptions::default(),
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

    let listing = run_or_list_all_tests_sync(
        tmp_dir,
        fake_tests.clone(),
        quiet.clone(),
        include_filter.clone(),
        exclude_filter.clone(),
        Some(ListAction::ListBinaries),
    );
    assert_eq!(listing, expected_binaries);

    let listing = run_or_list_all_tests_sync(
        tmp_dir,
        fake_tests.clone(),
        quiet.clone(),
        include_filter.clone(),
        exclude_filter.clone(),
        Some(ListAction::ListPackages),
    );
    assert_eq!(listing, expected_packages);
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
        "foo",
        "foo (library)",
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
        "\
        bar\n\
        foo\
        ",
        "\
        bar (library)\n\
        foo (library)\
        ",
        "\
        bar test_it\n\
        foo test_it\
        ",
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
            vec![
                "name.equals(test_it)".into(),
                "name.equals(test_it2)".into()
            ],
            vec!["package.equals(bin)".into()]
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
        vec![
            "name.equals(test_it)".into(),
            "name.equals(test_it2)".into(),
        ],
        vec!["package.equals(bin)".into()],
        "\
        bar\n\
        baz\n\
        foo\
        ",
        "\
        bar (library)\n\
        baz (library)\n\
        foo (library)\
        ",
        "\
        bar test_it2\n\
        foo test_it\
        ",
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
            stdout: JobOutputResult::None,
            stderr: JobOutputResult::Inline(Box::new(*b"error output")),
            duration: std::time::Duration::from_secs(1),
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
        stderr: error output\n\
        foo test_it..........................FAIL   1.000s\n\
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
    let fs = Fs::new();
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

    let state_dir = tmp_dir.join::<StateDir>("workspace/target/maelstrom/state");
    let test_listing_file = test_listing_file(&state_dir);
    let listing: TestListing = load_test_listing(&fs, &test_listing_file).unwrap().unwrap();
    assert_eq!(listing, fake_tests.listing());

    // remove bar
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

    // new listing should match
    let listing: TestListing = load_test_listing(&fs, &test_listing_file).unwrap().unwrap();
    assert_eq!(listing, fake_tests.listing());
}

#[test]
fn expected_count_updates_cases() {
    let fs = Fs::new();
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
        &tmp_dir,
        fake_tests.clone(),
        false.into(),
        vec!["all".into()],
        vec![],
    );

    let state_dir = tmp_dir.join::<StateDir>("workspace/target/maelstrom/state");
    let test_listing_file = test_listing_file(&state_dir);
    let listing: TestListing = load_test_listing(&fs, &test_listing_file).unwrap().unwrap();
    assert_eq!(listing, fake_tests.listing());

    // remove the test
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            tests: vec![],
        }],
    };

    run_all_tests_sync(
        &tmp_dir,
        fake_tests.clone(),
        false.into(),
        vec!["all".into()],
        vec![],
    );

    // new listing should match
    let listing: TestListing = load_test_listing(&fs, &test_listing_file).unwrap().unwrap();
    assert_eq!(listing, fake_tests.listing());
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
        &tmp_dir,
        fake_tests.clone(),
        false.into(),
        vec!["none".into()],
        vec![],
    );

    let fs = Fs::new();
    let state_dir = tmp_dir.join::<StateDir>("workspace/target/maelstrom/state");
    let test_listing_file_name = test_listing_file(&state_dir)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let mut entries: Vec<_> = fs
        .read_dir(state_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    entries.sort();
    assert_eq!(entries, vec![test_listing_file_name]);
}
