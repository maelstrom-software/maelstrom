mod fake_test_framework;

use crate::{
    config::{Repeat, StopAfter},
    introspect_driver::IntrospectDriver,
    test_listing::TestListingStore,
    ui::{self, Ui as _},
    BuildDir, ClientTrait, EnqueueResult, ListAction, MainApp, MainAppCombinedDeps, MainAppDeps,
    NotCollected,
};
use anyhow::Result;
use fake_test_framework::{
    BinDir, FakeTestBinary, FakeTestCase, FakeTests, TestCollector, TestOptions,
};
use indicatif::InMemoryTerm;
use indoc::indoc;
use maelstrom_base::{
    stats::JobState, ClientJobId, JobBrokerStatus, JobCompleted, JobEffects, JobOutcome,
    JobOutputResult, JobTerminationStatus, JobWorkerStatus,
};
use maelstrom_client::{
    spec::JobSpec, IntrospectResponse, JobRunningStatus, JobStatus, ProjectDir, StateDir,
};
use maelstrom_util::{
    fs::Fs,
    log::test_logger,
    root::{Root, RootBuf},
    template::TemplateVars,
};
use pretty_assertions::assert_eq;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use tempfile::tempdir;

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

struct TestClient {
    next_job_id: AtomicU32,
    tests: FakeTests,
}

impl ClientTrait for TestClient {
    fn introspect(&self) -> Result<IntrospectResponse> {
        todo!()
    }

    fn add_job(
        &self,
        spec: JobSpec,
        mut handler: impl FnMut(Result<JobStatus>) + Send + Sync + 'static,
    ) -> Result<()> {
        let client_job_id = ClientJobId::from_u32(self.next_job_id.fetch_add(1, Ordering::AcqRel));

        let case = self.tests.find_case_for_spec(spec);

        match case.desired_state {
            JobState::WaitingForArtifacts => {
                handler(Ok(JobStatus::Running(JobRunningStatus::AtBroker(
                    JobBrokerStatus::WaitingForLayers,
                ))));
            }
            JobState::Pending => {
                handler(Ok(JobStatus::Running(JobRunningStatus::AtBroker(
                    JobBrokerStatus::WaitingForWorker,
                ))));
            }
            JobState::Running => {
                handler(Ok(JobStatus::Running(JobRunningStatus::AtBroker(
                    JobBrokerStatus::AtWorker(1.into(), JobWorkerStatus::Executing),
                ))));
            }
            JobState::Complete => {
                handler(Ok(JobStatus::Completed {
                    client_job_id,
                    result: Ok(case.outcome.clone()),
                }));
            }
        }

        if case.desired_state != JobState::Complete {
            let outcome = case.outcome.clone();
            case.cb.set(move || {
                handler(Ok(JobStatus::Completed {
                    client_job_id,
                    result: Ok(outcome),
                }));
            });
        }

        Ok(())
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
    fn update(&self, resp: IntrospectResponse) {
        (self.update_func.borrow_mut().as_mut().unwrap())(resp)
    }
}

#[derive(Debug)]
struct TestArgs {
    fake_tests: FakeTests,
    quiet: bool,
    include: Vec<String>,
    exclude: Vec<String>,
    repeat: Repeat,
    stop_after: Option<StopAfter>,
    list: Option<ListAction>,
    stdout_tty: bool,
    finish: bool,
}

impl Default for TestArgs {
    fn default() -> Self {
        Self {
            fake_tests: FakeTests::default(),
            quiet: false,
            include: vec!["all".into()],
            exclude: vec![],
            repeat: Repeat::default(),
            stop_after: None,
            list: None,
            stdout_tty: false,
            finish: true,
        }
    }
}

fn run_app(
    bin_dir: &Root<BinDir>,
    term: InMemoryTerm,
    project_dir: &Root<ProjectDir>,
    test_args: TestArgs,
) -> String {
    let fs = Fs::new();
    let log = test_logger();
    slog::info!(log, "doing test"; "test_args" => ?test_args);

    fs.create_dir_all(bin_dir).unwrap();
    test_args.fake_tests.create_binaries(&fs, bin_dir);

    let target_directory = project_dir.join::<BuildDir>("target");
    let deps = TestMainAppDeps::new(
        test_args.fake_tests.clone(),
        bin_dir.to_owned(),
        target_directory.clone(),
    );

    let is_list = test_args.list.is_some();
    let state = MainAppCombinedDeps::new(
        deps,
        test_args.include,
        test_args.exclude,
        test_args.list,
        test_args.repeat,
        test_args.stop_after,
        false, // stderr_color
        project_dir,
        target_directory.join::<StateDir>("maelstrom/state"),
        TestOptions,
        log.clone(),
    )
    .unwrap();
    let (ui_send, ui_recv) = std::sync::mpsc::channel();
    let ui_sender = ui::UiSender::new(ui_send);
    let introspect_driver = TestIntrospectDriver::default();
    let mut app = MainApp::new(&state, ui_sender, introspect_driver.clone(), None).unwrap();

    let mut pending = vec![];
    loop {
        let res = app.enqueue_one().unwrap();
        let (package_name, case) = match res {
            EnqueueResult::NotEnqueued(NotCollected::Done) => break,
            EnqueueResult::NotEnqueued(NotCollected::Ignored | NotCollected::Listed) => continue,
            EnqueueResult::Enqueued { package_name, case } => (package_name, case),
        };
        let test = test_args.fake_tests.find_case(&package_name, &case);
        if test.desired_state != JobState::Complete {
            pending.push(test.clone());
        }

        introspect_driver.update(IntrospectResponse::default());
    }

    app.done_queuing().unwrap();

    if test_args.finish {
        for p in pending {
            p.maybe_complete();
        }

        app.wait_for_tests().unwrap();
        app.finish().unwrap();
    } else {
        for p in pending {
            p.discard_cb();
        }

        drop(app);
    }

    drop(introspect_driver);
    drop(state);

    if test_args.quiet {
        let mut ui = ui::QuietUi::new(is_list, test_args.stdout_tty, term.clone()).unwrap();
        ui.run(ui_recv).unwrap();

        slog::info!(log, "test complete");
        term.contents()
    } else {
        let mut ui = ui::SimpleUi::new(is_list, test_args.stdout_tty, term.clone());
        ui.run(ui_recv).unwrap();

        slog::info!(log, "test complete");
        term.contents()
    }
}

fn run_all_tests_sync(tmp_dir: &Root<TmpDir>, test_args: TestArgs) -> String {
    let bin_dir = tmp_dir.join::<BinDir>("bin");
    let project_dir = tmp_dir.join::<ProjectDir>("project");

    let term = InMemoryTerm::new(50, 50);
    run_app(&bin_dir, term.clone(), &project_dir, test_args)
}

fn list_all_tests_sync(tmp_dir: &Root<TmpDir>, test_args: TestArgs, expected_tests: &str) {
    let listing = run_all_tests_sync(
        tmp_dir,
        TestArgs {
            list: Some(ListAction::ListTests),
            ..test_args
        },
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
            TestArgs {
                fake_tests,
                ..Default::default()
            }
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
        TestArgs {
            fake_tests,
            ..Default::default()
        },
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
            TestArgs {
                fake_tests,
                ..Default::default()
            }
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
        TestArgs {
            fake_tests,
            ..Default::default()
        },
        indoc! {"
            bar test_it
            foo test_it\
        "},
    );
}

#[test]
fn two_tests_all_tests_sync_listing_with_repeat() {
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
        TestArgs {
            fake_tests,
            repeat: Repeat::try_from(2).unwrap(),
            ..Default::default()
        },
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
            TestArgs {
                fake_tests,
                include: vec!["name = \"test_it\"".into(), "name = \"test_it2\"".into()],
                exclude: vec!["package = \"bin\"".into()],
                ..Default::default()
            }
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
        TestArgs {
            fake_tests,
            include: vec!["name = \"test_it\"".into(), "name = \"test_it2\"".into()],
            exclude: vec!["package = \"bin\"".into()],
            ..Default::default()
        },
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
            TestArgs {
                fake_tests,
                include: vec!["package = \"foo\"".into()],
                ..Default::default()
            }
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
            TestArgs {
                fake_tests,
                include: vec!["and = [{ package = \"foo\" }, { name = \"test_it\" }]".into()],
                ..Default::default()
            }
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
            TestArgs {
                fake_tests,
                ..Default::default()
            }
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
fn ignored_test_sync_with_repeat() {
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
            TestArgs {
                fake_tests,
                repeat: Repeat::try_from(2).unwrap(),
                ..Default::default()
            }
        ),
        "\
        bar test_it............................OK   1.000s\n\
        bar test_it............................OK   1.000s\n\
        baz test_it............................OK   1.000s\n\
        baz test_it............................OK   1.000s\n\
        foo test_it.......................IGNORED\n\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         4\n\
        Failed Tests    :         0\n\
        Ignored Tests   :         1\n\
        \x20\x20\x20\x20foo test_it: ignored\
        "
    );
}

#[test]
fn ignored_test_via_config_sync() {
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
                    ..Default::default()
                }],
            },
        ],
    };
    let project_dir = tmp_dir.path().join("project");
    Fs.create_dir_all(&project_dir).unwrap();
    let config_path = project_dir.join(TestMainAppDeps::MAELSTROM_TEST_TOML);
    Fs.write(
        config_path,
        indoc! {"
            [[directives]]
            filter = \"package = \\\"foo\\\"\"
            ignore = true
        "},
    )
    .unwrap();
    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            TestArgs {
                fake_tests,
                ..Default::default()
            }
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
            TestArgs {
                fake_tests,
                quiet: true,
                ..Default::default()
            }
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
        &project_dir,
        TestArgs {
            fake_tests,
            ..Default::default()
        },
    );

    term.contents()
}

fn failed_outcome() -> JobOutcome {
    JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Exited(1),
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
    })
}

fn failed_outcome_no_output() -> JobOutcome {
    JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Exited(1),
        effects: JobEffects {
            stdout: JobOutputResult::Inline(Box::new(*b"")),
            stderr: JobOutputResult::Inline(Box::new(*b"")),
            duration: Duration::from_secs(1),
        },
    })
}

#[test]
fn failed_tests() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    outcome: failed_outcome(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    outcome: failed_outcome(),
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

fn run_in_progress_test(test_args: TestArgs, expected_output: &str) {
    let tmp_dir = tempdir().unwrap();
    let project_dir = RootBuf::<ProjectDir>::new(tmp_dir.path().join("project"));

    let term = InMemoryTerm::new(50, 50);
    let term_clone = term.clone();
    let contents = run_app(
        Root::new(tmp_dir.path()),
        term_clone,
        &project_dir,
        TestArgs {
            stdout_tty: true,
            finish: false,
            ..test_args
        },
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
        TestArgs {
            fake_tests,
            ..Default::default()
        },
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
        TestArgs {
            fake_tests,
            ..Default::default()
        },
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
        TestArgs {
            fake_tests,
            ..Default::default()
        },
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
        TestArgs {
            fake_tests,
            ..Default::default()
        },
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
        TestArgs {
            fake_tests,
            quiet: true,
            ..Default::default()
        },
        "#####################------------------- 1/2 tests",
    );
}

fn run_loop_test(test_args: TestArgs, expected_output: &str) {
    let tmp_dir = tempdir().unwrap();
    let project_dir = RootBuf::<ProjectDir>::new(tmp_dir.path().join("project"));

    let term = InMemoryTerm::new(50, 50);
    let term_clone = term.clone();
    let contents = run_app(
        Root::new(tmp_dir.path()),
        term_clone,
        &project_dir,
        TestArgs {
            stdout_tty: true,
            finish: false,
            ..test_args
        },
    );
    assert_eq!(contents, expected_output);
}

#[test]
fn loop_two_times() {
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
                    desired_state: JobState::Complete,
                    ..Default::default()
                }],
            },
        ],
    };
    run_loop_test(
        TestArgs {
            fake_tests,
            repeat: 2.try_into().unwrap(),
            ..Default::default()
        },
        "\
        foo test_it............................OK   1.000s\n\
        foo test_it............................OK   1.000s\n\
        bar test_it............................OK   1.000s\n\
        bar test_it............................OK   1.000s\n\
        ######################## 4/4 waiting for artifacts\n\
        ######################## 4/4 pending\n\
        ######################## 4/4 running\n\
        ######################## 4/4 complete\
        ",
    );
}

#[test]
fn loop_three_times() {
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
                    desired_state: JobState::Complete,
                    ..Default::default()
                }],
            },
        ],
    };
    run_loop_test(
        TestArgs {
            fake_tests,
            repeat: 3.try_into().unwrap(),
            ..Default::default()
        },
        "\
        foo test_it............................OK   1.000s\n\
        foo test_it............................OK   1.000s\n\
        foo test_it............................OK   1.000s\n\
        bar test_it............................OK   1.000s\n\
        bar test_it............................OK   1.000s\n\
        bar test_it............................OK   1.000s\n\
        ######################## 6/6 waiting for artifacts\n\
        ######################## 6/6 pending\n\
        ######################## 6/6 running\n\
        ######################## 6/6 complete\
        ",
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
        TestArgs {
            fake_tests: fake_tests.clone(),
            ..Default::default()
        },
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
        TestArgs {
            fake_tests: fake_tests.clone(),
            ..Default::default()
        },
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
        TestArgs {
            fake_tests: fake_tests.clone(),
            ..Default::default()
        },
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
        TestArgs {
            fake_tests: fake_tests.clone(),
            ..Default::default()
        },
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
        TestArgs {
            fake_tests,
            include: vec!["none".into()],
            ..Default::default()
        },
    );

    let entries: Vec<_> = Fs::new()
        .read_dir((**tmp_dir).join("project/target"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(entries, vec!["maelstrom"]);
}

fn stop_after_fake_tests() -> FakeTests {
    FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "bin".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it2".into(),
                    outcome: failed_outcome_no_output(),
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                tests: vec![FakeTestCase {
                    name: "testy".into(),
                    outcome: failed_outcome_no_output(),
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
    }
}

#[test]
fn stop_after_not_reached() {
    let tmp_dir = tempdir().unwrap();
    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            TestArgs {
                fake_tests: stop_after_fake_tests(),
                stop_after: Some(3.try_into().unwrap()),
                ..Default::default()
            }
        ),
        indoc! {"
        bin test_it............................OK   1.000s
        bar test_it2.........................FAIL   1.000s
        baz testy............................FAIL   1.000s
        foo test_it............................OK   1.000s

        ================== Test Summary ==================
        Successful Tests:         2
        Failed Tests    :         2
            bar test_it2: failure
            baz testy   : failure\
        "}
    );
}

#[test]
fn stop_after_1_unknown_not_run() {
    let tmp_dir = tempdir().unwrap();
    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            TestArgs {
                fake_tests: stop_after_fake_tests(),
                stop_after: Some(1.try_into().unwrap()),
                ..Default::default()
            }
        ),
        indoc! {"
        bin test_it............................OK   1.000s
        bar test_it2.........................FAIL   1.000s

        ================== Test Summary ==================
        Successful Tests:         1
        Failed Tests    :         1
            bar test_it2: failure
        Tests Not Run   :   unknown\
        "}
    );
}

#[test]
fn stop_after_1_at_least_one_not_run() {
    let tmp_dir = tempdir().unwrap();

    let mut fake_tests = stop_after_fake_tests();
    fake_tests.test_binaries[0].tests[0].desired_state = JobState::Running;
    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            TestArgs {
                fake_tests,
                stop_after: Some(1.try_into().unwrap()),
                ..Default::default()
            }
        ),
        indoc! {"
        bar test_it2.........................FAIL   1.000s

        ================== Test Summary ==================
        Successful Tests:         0
        Failed Tests    :         1
            bar test_it2: failure
        Tests Not Run   :        >1\
        "}
    );
}

#[test]
fn stop_after_1_no_tests_not_run() {
    let tmp_dir = tempdir().unwrap();

    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "bin".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ..Default::default()
                }],
            },
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
                    desired_state: JobState::Running,
                    complete_at_end: true,
                    outcome: failed_outcome_no_output(),
                    ..Default::default()
                }],
            },
        ],
    };

    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            TestArgs {
                fake_tests,
                stop_after: Some(1.try_into().unwrap()),
                ..Default::default()
            }
        ),
        indoc! {"
        bin test_it............................OK   1.000s
        bar test_it2...........................OK   1.000s
        baz testy............................FAIL   1.000s

        ================== Test Summary ==================
        Successful Tests:         2
        Failed Tests    :         1
            baz testy: failure\
        "}
    );
}

#[test]
fn stop_after_1_exact_test_not_run_known() {
    let tmp_dir = tempdir().unwrap();

    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "bin".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    desired_state: JobState::Running,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec![FakeTestCase {
                    name: "test_it2".into(),
                    desired_state: JobState::Running,
                    ..Default::default()
                }],
            },
            FakeTestBinary {
                name: "baz".into(),
                tests: vec![FakeTestCase {
                    name: "testy".into(),
                    desired_state: JobState::Running,
                    complete_at_end: true,
                    outcome: failed_outcome_no_output(),
                    ..Default::default()
                }],
            },
        ],
    };

    assert_eq!(
        run_all_tests_sync(
            Root::new(tmp_dir.path()),
            TestArgs {
                fake_tests,
                stop_after: Some(1.try_into().unwrap()),
                ..Default::default()
            }
        ),
        indoc! {"
        baz testy............................FAIL   1.000s

        ================== Test Summary ==================
        Successful Tests:         0
        Failed Tests    :         1
            baz testy: failure
        Tests Not Run   :         2\
        "}
    );
}

#[test]
fn stop_after_1_with_estimate() {
    let tmp_dir = tempdir().unwrap();
    let tmp_dir = Root::new(tmp_dir.path());
    let mut fake_tests = stop_after_fake_tests();
    for test in &mut fake_tests.test_binaries {
        test.tests[0].expected_estimated_duration = Some(Duration::from_secs(1));
    }

    let test_listing_store = TestListingStore::new(
        Fs::new(),
        tmp_dir.join::<StateDir>("project/target/maelstrom/state"),
    );
    let mut listing = test_listing_store.load().unwrap();
    fake_tests.update_listing(&mut listing);
    test_listing_store.save(listing).unwrap();

    assert_eq!(
        run_all_tests_sync(
            tmp_dir,
            TestArgs {
                fake_tests,
                stop_after: Some(1.try_into().unwrap()),
                ..Default::default()
            }
        ),
        indoc! {"
        bin test_it............................OK   1.000s
        bar test_it2.........................FAIL   1.000s

        ================== Test Summary ==================
        Successful Tests:         1
        Failed Tests    :         1
            bar test_it2: failure
        Tests Not Run   :        ~2\
        "}
    );
}
