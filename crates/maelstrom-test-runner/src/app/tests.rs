use super::{Deps, MainApp, MainAppMessage::*, MainAppMessageM, TestingOptions};
use crate::config::{Repeat, StopAfter};
use crate::deps::SimpleFilter;
use crate::fake_test_framework::{
    FakePackageId, FakeTestArtifact, FakeTestFilter, FakeTestPackage, TestCollector, TestOptions,
};
use crate::metadata::AllMetadata;
use crate::test_db::{OnDiskTestDb, TestDb};
use crate::ui::{
    UiJobEnqueued, UiJobId as JobId, UiJobResult, UiJobStatus, UiJobSummary, UiJobUpdate, UiMessage,
};
use crate::{NoCaseMetadata, NotRunEstimate, StringArtifactKey, WaitStatus};
use anyhow::anyhow;
use itertools::Itertools as _;
use maelstrom_base::{
    nonempty, ClientJobId, JobBrokerStatus, JobCompleted, JobDevice, JobEffects, JobError,
    JobMount, JobNetwork, JobOutcome, JobOutputResult, JobRootOverlay, JobTerminationStatus,
    JobWorkerStatus, NonEmpty,
};
use maelstrom_client::{
    spec::{ContainerRef, ContainerSpec, JobSpec, LayerSpec},
    JobRunningStatus, JobStatus,
};
use maelstrom_simex::SimulationExplorer;
use maelstrom_util::process::ExitCode;
use notify::event::{Event, EventAttributes, EventKind, ModifyKind};
use std::cell::RefCell;
use std::collections::HashSet;
use std::str::FromStr as _;
use std::time::Duration;
use TestMessage::*;

#[derive(Debug, PartialEq, Eq)]
enum TestMessage {
    AddJob {
        job_id: JobId,
        spec: JobSpec,
    },
    ListTests {
        artifact: FakeTestArtifact,
    },
    GetPackages,
    StartCollection {
        color: bool,
        options: TestOptions,
        packages: Vec<FakeTestPackage>,
    },
    SendUiMsg {
        msg: UiMessage,
    },
    StartRestart,
    StartShutdown,
}

#[derive(Default)]
struct TestState {
    messages: Vec<TestMessage>,
}

#[derive(Default)]
struct TestDeps(RefCell<TestState>);

impl Deps for TestDeps {
    type TestCollector = TestCollector;

    fn start_collection(
        &self,
        color: bool,
        options: &TestOptions,
        packages: Vec<&FakeTestPackage>,
    ) {
        let mut self_ = self.0.borrow_mut();
        self_.messages.push(TestMessage::StartCollection {
            color,
            options: options.clone(),
            packages: packages.into_iter().cloned().collect(),
        });
    }

    fn get_packages(&self) {
        let mut self_ = self.0.borrow_mut();
        self_.messages.push(TestMessage::GetPackages);
    }

    fn add_job(&self, job_id: JobId, spec: JobSpec) {
        let mut self_ = self.0.borrow_mut();
        self_.messages.push(TestMessage::AddJob { job_id, spec });
    }

    fn list_tests(&self, artifact: FakeTestArtifact) {
        let mut self_ = self.0.borrow_mut();
        self_.messages.push(TestMessage::ListTests { artifact });
    }

    fn start_shutdown(&self) {
        let mut self_ = self.0.borrow_mut();
        self_.messages.push(TestMessage::StartShutdown);
    }

    fn send_ui_msg(&self, msg: UiMessage) {
        let mut self_ = self.0.borrow_mut();
        self_.messages.push(TestMessage::SendUiMsg { msg });
    }

    fn start_restart(&self) {
        let mut self_ = self.0.borrow_mut();
        self_.messages.push(TestMessage::StartRestart);
    }
}

struct Fixture<'deps> {
    app: MainApp<'deps, TestDeps>,
    deps: &'deps TestDeps,
}

impl<'deps> Fixture<'deps> {
    fn new(
        deps: &'deps TestDeps,
        options: &'deps TestingOptions<FakeTestFilter, TestOptions>,
        test_db: TestDb<StringArtifactKey, NoCaseMetadata>,
    ) -> Self {
        Self {
            app: MainApp::new(deps, options, test_db),
            deps,
        }
    }

    fn expect_messages_in_any_order(&mut self, expected: Vec<TestMessage>) {
        let borrowed_messages = &mut self.deps.0.borrow_mut().messages;
        let messages: Vec<_> = borrowed_messages.iter().collect();
        for perm in expected.iter().permutations(expected.len()) {
            if perm == messages {
                borrowed_messages.clear();
                return;
            }
        }
        panic!(
            "Expected messages didn't match actual messages in any order.\n{}",
            colored_diff::PrettyDifference {
                expected: &format!("{:#?}", expected),
                actual: &format!("{:#?}", messages)
            }
        );
    }

    fn receive_message(&mut self, call: MainAppMessageM<TestDeps>) {
        self.app.receive_message(call);
    }

    fn assert_return_value_error(self, str_expr: &str) {
        let ret = self.app.main_return_value().map(|_| ());
        assert_eq!(format!("{ret:?}"), str_expr);
    }

    fn assert_return_value(
        self,
        expected_exit_code: ExitCode,
        expected_test_db: TestDb<StringArtifactKey, NoCaseMetadata>,
    ) {
        let (exit_code, test_db) = self.app.main_return_value().unwrap();
        assert_eq!(expected_exit_code, exit_code);

        let expected_test_db_on_disk: OnDiskTestDb<_, _> = expected_test_db.into();
        let test_db_on_disk: OnDiskTestDb<_, _> = test_db.into();
        assert_eq!(expected_test_db_on_disk, test_db_on_disk);
    }
}

#[derive(Debug, Clone)]
struct TestDbEntry {
    package_name: String,
    artifact_name: Option<String>,
    case_name: Option<String>,
    entry_data: Option<(crate::test_db::CaseOutcome, NonEmpty<Duration>)>,
}

impl TestDbEntry {
    fn new(package_name: &str, artifact_name: &str, case_name: &str) -> Self {
        Self {
            package_name: package_name.into(),
            artifact_name: Some(artifact_name.into()),
            case_name: Some(case_name.into()),
            entry_data: None,
        }
    }

    fn failure(
        package_name: &str,
        artifact_name: &str,
        case_name: &str,
        durations: NonEmpty<Duration>,
    ) -> Self {
        Self {
            entry_data: Some((crate::test_db::CaseOutcome::Failure, durations)),
            ..Self::new(package_name, artifact_name, case_name)
        }
    }

    fn success(
        package_name: &str,
        artifact_name: &str,
        case_name: &str,
        durations: NonEmpty<Duration>,
    ) -> Self {
        Self {
            entry_data: Some((crate::test_db::CaseOutcome::Success, durations)),
            ..Self::new(package_name, artifact_name, case_name)
        }
    }

    fn empty_artifact(package_name: &str, artifact_name: &str) -> Self {
        Self {
            package_name: package_name.into(),
            artifact_name: Some(artifact_name.into()),
            case_name: None,
            entry_data: None,
        }
    }
}

fn test_db_from_entries<IterT>(entries: IterT) -> TestDb<StringArtifactKey, NoCaseMetadata>
where
    IterT: IntoIterator<Item = TestDbEntry>,
    IterT::IntoIter: Clone,
{
    let entries_iter = entries.into_iter();
    let packages: HashSet<_> = entries_iter.clone().map(|e| e.package_name).collect();
    TestDb::from_iter(packages.into_iter().map(|pkg| {
        (
            pkg.clone(),
            crate::test_db::Package::from_iter(
                entries_iter
                    .clone()
                    .filter(|e| e.package_name == pkg && e.artifact_name.is_some())
                    .map(|e| {
                        let artifact = e.artifact_name.as_ref().unwrap().as_str();
                        let cases: Vec<_> = entries_iter
                            .clone()
                            .filter(|e| {
                                e.package_name == pkg
                                    && e.artifact_name.as_ref().is_some_and(|a| a == artifact)
                                        & e.case_name.is_some()
                            })
                            .map(|e| (e.case_name, e.entry_data))
                            .collect();
                        (
                            StringArtifactKey::from(artifact),
                            crate::test_db::Artifact::from_iter(cases.into_iter().map(
                                |(case, entry_data)| {
                                    (
                                        case.unwrap(),
                                        crate::test_db::CaseData {
                                            metadata: NoCaseMetadata,
                                            when_read: entry_data,
                                            this_run: None,
                                        },
                                    )
                                },
                            )),
                        )
                    }),
            ),
        )
    }))
}

const DEFAULT_METADATA_STR: &str = r#"
    [[directives]]
    layers = [
        { stubs = [ "/{proc,sys,tmp}/", "/dev/{full,null,random,urandom,zero}" ] },
    ]
    mounts = [
        { type = "tmp", mount_point = "/tmp" },
        { type = "proc", mount_point = "/proc" },
        { type = "sys", mount_point = "/sys" },
        { type = "devices", devices = ["full", "null", "random", "urandom", "zero"] },
    ]
"#;

fn default_metadata() -> AllMetadata<FakeTestFilter> {
    AllMetadata::from_str(DEFAULT_METADATA_STR).unwrap()
}

fn default_testing_options() -> TestingOptions<FakeTestFilter, TestOptions> {
    TestingOptions {
        test_metadata: default_metadata(),
        filter: SimpleFilter::All.into(),
        collector_options: TestOptions,
        timeout_override: None,
        stdout_color: false,
        repeat: Repeat::try_from(1).unwrap(),
        stop_after: None,
        watch: false,
        listing: false,
        watch_exclude_paths: vec![],
    }
}

macro_rules! script_test {
    (
        $test_name:ident,
        $(@ $arg_key:ident = $arg_value:expr,)*
        expected_test_db_out = [$($db_entry_out:expr),*],
        $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?
    ) => {
        script_test!(
            $test_name,
            $(@ $arg_key = $arg_value,)*
            test_db_in = [],
            expected_exit_code = ExitCode::SUCCESS,
            expected_test_db_out = [$($db_entry_out:expr),*],
            $($in_msg => { $($out_msg,)* };)+
        );
    };
    (
        $test_name:ident,
        $(@ $arg_key:ident = $arg_value:expr,)*
        test_db_in = [$($db_entry_in:expr),*],
        expected_exit_code = $exit_code:expr,
        expected_test_db_out = [$($db_entry_out:expr),*],
        $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?
    ) => {
        #[test]
        fn $test_name() {
            let deps = TestDeps::default();
            let options = TestingOptions {
                $($arg_key: $arg_value,)*
                ..default_testing_options()
            };
            let test_db = test_db_from_entries([$($db_entry_in),*]);
            let mut fixture = Fixture::new(&deps, &options, test_db);
            $(
                fixture.receive_message($in_msg);
                fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
            )+

            let test_db = test_db_from_entries([$($db_entry_out),*]);
            fixture.assert_return_value($exit_code, test_db);
        }
    }
}

macro_rules! script_test_with_error_simex {
    (
        $test_name:ident,
        $(@ $arg_key:ident = $arg_value:expr,)*
        expected_test_db_out = [$($db_entry_out:expr),*],
        $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?
    ) => {
        script_test_with_error_simex!(
            $test_name,
            $(@ $arg_key = $arg_value,)*
            test_db_in = [],
            expected_exit_code = ExitCode::SUCCESS,
            expected_test_db_out = [$($db_entry_out),*],
            $($in_msg => { $($out_msg,)* };)+
        );
    };
    (
        $test_name:ident,
        $(@ $arg_key:ident = $arg_value:expr,)*
        test_db_in = [$($db_entry_in:expr),*],
        expected_exit_code = $exit_code:expr,
        expected_test_db_out = [$($db_entry_out:expr),*],
        $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?
    ) => {
        script_test!(
            $test_name,
            $(@ $arg_key = $arg_value,)*
            test_db_in = [$($db_entry_in),*],
            expected_exit_code = $exit_code,
            expected_test_db_out = [$($db_entry_out),*],
            $($in_msg => { $($out_msg,)* };)+
        );

        paste::paste! {
            #[test]
            fn [< $test_name _error_simex >] () {
                let mut simex = SimulationExplorer::default();
                while let Some(mut simulation) = simex.next_simulation() {
                    let deps = TestDeps::default();
                    let options = TestingOptions {
                        $($arg_key: $arg_value,)*
                        ..default_testing_options()
                    };
                    let test_db = test_db_from_entries([$($db_entry_in),*]);
                    let mut fixture = Fixture::new(&deps, &options, test_db);
                    $(
                        if simulation.choose_bool() {
                            fixture.receive_message(FatalError { error: anyhow!("simex error") });
                            fixture.assert_return_value_error("Err(simex error)");
                            continue;
                        }
                        fixture.receive_message($in_msg);
                        fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
                    )+

                    fixture.receive_message(FatalError { error: anyhow!("simex error") });
                    fixture.assert_return_value_error("Err(simex error)");
                }
            }
        }
    }
}

fn fake_pkg<'a>(name: &str, artifacts: impl IntoIterator<Item = &'a str>) -> FakeTestPackage {
    FakeTestPackage {
        name: name.into(),
        artifacts: artifacts.into_iter().map(|i| i.into()).collect(),
        id: FakePackageId(name.into()),
    }
}

fn fake_artifact<'a>(name: &str, pkg: &str) -> FakeTestArtifact {
    FakeTestArtifact {
        name: name.into(),
        // this test file doesn't make use of this field
        tests: vec![],
        // this test file doesn't make use of this field
        ignored_tests: vec![],
        path: format!("/{name}_bin").into(),
        package: FakePackageId(pkg.into()),
    }
}

fn test_spec(bin: &str, name: &str) -> JobSpec {
    JobSpec {
        container: default_container(),
        program: format!("/{bin}_bin").into(),
        arguments: vec![name.into()],
        timeout: None,
        estimated_duration: None,
        allocate_tty: None,
        priority: 1,
    }
}

fn default_container() -> ContainerRef {
    ContainerRef::Inline(ContainerSpec {
        image: None,
        layers: vec![LayerSpec::Stubs {
            stubs: vec![
                "/{proc,sys,tmp}/".into(),
                "/dev/{full,null,random,urandom,zero}".into(),
            ],
        }],
        root_overlay: JobRootOverlay::None,
        environment: vec![],
        working_directory: None,
        mounts: vec![
            JobMount::Tmp {
                mount_point: "/tmp".into(),
            },
            JobMount::Proc {
                mount_point: "/proc".into(),
            },
            JobMount::Sys {
                mount_point: "/sys".into(),
            },
            JobMount::Devices {
                devices: JobDevice::Full
                    | JobDevice::Null
                    | JobDevice::Random
                    | JobDevice::Urandom
                    | JobDevice::Zero,
            },
        ],
        network: JobNetwork::Disabled,
        user: None,
        group: None,
    })
}

macro_rules! test_output_test_inner {
    (
        $macro_name:ident,
        $name:ident,
        $job_outcome:expr,
        $job_result:expr,
        $ui_job_summary:expr,
        $exit_code:expr,
        $test_db_entry:expr
    ) => {
        $macro_name! {
            $name,
            test_db_in = [],
            expected_exit_code = $exit_code,
            expected_test_db_out = [$test_db_entry],
            Start => {
                SendUiMsg {
                    msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
                },
                GetPackages
            };
            Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
                StartCollection {
                    color: false,
                    options: TestOptions,
                    packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
                }
            };
            ArtifactBuilt {
                artifact: fake_artifact("foo_test", "foo_pkg"),
            } => {
                ListTests {
                    artifact: fake_artifact("foo_test", "foo_pkg"),
                }
            };
            TestsListed {
                artifact: fake_artifact("foo_test", "foo_pkg"),
                listing: vec![("test_a".into(), NoCaseMetadata)],
                ignored_listing: vec![]
            } => {
                AddJob {
                    job_id: JobId::from(1),
                    spec: test_spec("foo_test", "test_a"),
                },
                SendUiMsg {
                    msg: UiMessage::JobEnqueued(UiJobEnqueued {
                        job_id: JobId::from(1),
                        name: "foo_pkg test_a".into()
                    })
                },
                SendUiMsg {
                    msg: UiMessage::UpdatePendingJobsCount(1)
                }
            };
            CollectionFinished { wait_status: wait_success() } => {
                SendUiMsg {
                    msg: UiMessage::DoneQueuingJobs,
                }
            };
            JobUpdate { job_id: JobId::from(1), result: $job_outcome.map(|result| {
                JobStatus::Completed {
                    client_job_id: ClientJobId::from(1),
                    result,
                }
            })} => {
                SendUiMsg {
                    msg: UiMessage::JobFinished($job_result)
                },
                SendUiMsg {
                    msg: UiMessage::AllJobsFinished($ui_job_summary)
                },
                StartShutdown
            };
        }
    };
}

macro_rules! test_output_test {
    (
        $name:ident,
        $job_outcome:expr,
        $job_result:expr,
        $ui_job_summary:expr,
        $exit_code:expr,
        $test_db_entry:expr
    ) => {
        test_output_test_inner!(
            script_test,
            $name,
            $job_outcome,
            $job_result,
            $ui_job_summary,
            $exit_code,
            $test_db_entry
        );
    };
}

macro_rules! test_output_test_with_error_simex {
    (
        $name:ident,
        $job_outcome:expr,
        $job_result:expr,
        $ui_job_summary:expr,
        $exit_code:expr,
        $test_db_entry:expr
    ) => {
        test_output_test_inner!(
            script_test_with_error_simex,
            $name,
            $job_outcome,
            $job_result,
            $ui_job_summary,
            $exit_code,
            $test_db_entry
        );
    };
}

macro_rules! test_db_test {
    (
        $name:ident,
        test_db_in = [$($db_entry_in:expr),*],
        expected_test_db_out = [$($db_entry_out:expr),*],
        $test_a_job_spec:expr,
        $test_b_job_spec:expr
    ) => {
        script_test! {
            $name,
            test_db_in = [$($db_entry_in),*],
            expected_exit_code = ExitCode::SUCCESS,
            expected_test_db_out = [$($db_entry_out),*],
            Start => {
                SendUiMsg {
                    msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
                },
                GetPackages
            };
            Packages {
                packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
            } => {
                StartCollection {
                    color: false,
                    options: TestOptions,
                    packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
                },
                SendUiMsg {
                    msg: UiMessage::UpdatePendingJobsCount(2)
                },
            };
            ArtifactBuilt {
                artifact: fake_artifact("foo_test", "foo_pkg"),
            } => {
                ListTests {
                    artifact: fake_artifact("foo_test", "foo_pkg"),
                }
            };
            TestsListed {
                artifact: fake_artifact("foo_test", "foo_pkg"),
                listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
                ignored_listing: vec![]
            } => {
                AddJob {
                    job_id: JobId::from(1),
                    spec: $test_a_job_spec,
                },
                SendUiMsg {
                    msg: UiMessage::JobEnqueued(UiJobEnqueued {
                        job_id: JobId::from(1),
                        name: "foo_pkg test_a".into()
                    })
                },
                AddJob {
                    job_id: JobId::from(2),
                    spec: $test_b_job_spec,
                },
                SendUiMsg {
                    msg: UiMessage::JobEnqueued(UiJobEnqueued {
                        job_id: JobId::from(2),
                        name: "foo_pkg test_b".into()
                    })
                },
            };
            CollectionFinished { wait_status: wait_success() } => {
                SendUiMsg {
                    msg: UiMessage::DoneQueuingJobs,
                }
            };
            JobUpdate {
                job_id: JobId::from(1),
                result: job_status_complete(0),
            } => {
                SendUiMsg {
                    msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Ok)
                }
            };
            JobUpdate {
                job_id: JobId::from(2),
                result: job_status_complete(0),
            } => {
                SendUiMsg {
                    msg: ui_job_result("foo_pkg test_b", 2, UiJobStatus::Ok)
                },
                SendUiMsg {
                    msg: UiMessage::AllJobsFinished(UiJobSummary {
                        succeeded: 2,
                        failed: vec![],
                        ignored: vec![],
                        not_run: None,
                    })
                },
                StartShutdown
            };
        }
    }
}

macro_rules! watch_simex_test_inner {
    (
        $test_name:ident,
        expected_test_db_out = [$($db_entry_out:expr),*],
        watch_events = $watch_events:expr,
        triggers_restart = $triggers_restart:expr,
        watch_exclude_paths = $watch_exclude_paths:expr,
        $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?
    ) => {
        #[test]
        fn $test_name() {
            let triggers_restart = $triggers_restart;
            let watch_exclude_paths = $watch_exclude_paths;

            let mut simex = SimulationExplorer::default();
            while let Some(mut simulation) = simex.next_simulation() {
                let deps = TestDeps::default();
                let options = TestingOptions {
                    watch: true,
                    watch_exclude_paths: watch_exclude_paths.clone(),
                    ..default_testing_options()
                };
                let test_db = test_db_from_entries([]);
                let mut fixture = Fixture::new(&deps, &options, test_db);
                let script_length = [$(stringify!($in_msg)),+].len();

                let mut index = 0;
                let mut watch_event_received = false;
                $(
                    if simulation.choose_bool() {
                        fixture.receive_message($watch_events);
                        fixture.expect_messages_in_any_order(vec![]);
                        if index != 0 {
                            // if the watch event comes before "start" it is ignored
                            watch_event_received = true;
                        }
                    }

                    fixture.receive_message($in_msg);
                    let mut messages = vec![$($out_msg,)*];

                    // if this is the last message and we received a watch event, it may trigger a
                    // restart
                    if index == script_length - 1 {
                        if watch_event_received && triggers_restart {
                            messages.push(StartRestart);
                        } else {
                            messages.push(SendUiMsg {
                                msg: UiMessage::UpdateEnqueueStatus(
                                    "waiting for changes...".into()
                                )
                            });
                        }
                    }

                    fixture.expect_messages_in_any_order(messages);

                    index += 1;
                )+

                assert!(index > 0);

                // If choose for the watch event to come at the end
                if simulation.choose_bool() {
                    fixture.receive_message($watch_events);
                    if !watch_event_received && triggers_restart {
                        fixture.expect_messages_in_any_order(vec![StartRestart]);
                    }
                }

                let test_db = test_db_from_entries([$($db_entry_out),*]);
                fixture.assert_return_value(ExitCode::SUCCESS, test_db);
            }
        }
    }
}

macro_rules! watch_simex_test {
    (
        $name:ident,
        triggers_restart = $triggers_restart:expr,
        watch_exclude_paths = $watch_exclude_paths:expr,
        $watch_events:expr
    ) => {
        watch_simex_test_inner! {
            $name,
            expected_test_db_out = [
                TestDbEntry::success("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
            ],
            watch_events = $watch_events,
            triggers_restart = $triggers_restart,
            watch_exclude_paths = $watch_exclude_paths,
            Start => {
                SendUiMsg {
                    msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
                },
                GetPackages
            };
            Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
                StartCollection {
                    color: false,
                    options: TestOptions,
                    packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
                }
            };
            ArtifactBuilt {
                artifact: fake_artifact("foo_test", "foo_pkg"),
            } => {
                ListTests {
                    artifact: fake_artifact("foo_test", "foo_pkg"),
                }
            };
            TestsListed {
                artifact: fake_artifact("foo_test", "foo_pkg"),
                listing: vec![("test_a".into(), NoCaseMetadata)],
                ignored_listing: vec![]
            } => {
                AddJob {
                    job_id: JobId::from(1),
                    spec: test_spec("foo_test", "test_a"),
                },
                SendUiMsg {
                    msg: UiMessage::JobEnqueued(UiJobEnqueued {
                        job_id: JobId::from(1),
                        name: "foo_pkg test_a".into()
                    })
                },
                SendUiMsg {
                    msg: UiMessage::UpdatePendingJobsCount(1)
                },
            };
            CollectionFinished { wait_status: wait_success() } => {
                SendUiMsg {
                    msg: UiMessage::DoneQueuingJobs,
                }
            };
            JobUpdate {
                job_id: JobId::from(1),
                result: job_status_complete(0),
            } => {
                SendUiMsg {
                    msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Ok),
                },
                SendUiMsg {
                    msg: UiMessage::AllJobsFinished(UiJobSummary {
                        succeeded: 1,
                        failed: vec![],
                        ignored: vec![],
                        not_run: None,
                    })
                }
            };
        }
    }
}

fn job_status_complete(exit_code: u8) -> anyhow::Result<JobStatus> {
    Ok(JobStatus::Completed {
        client_job_id: ClientJobId::from(1),
        result: Ok(JobOutcome::Completed(JobCompleted {
            status: JobTerminationStatus::Exited(exit_code),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
                duration: Duration::from_secs(1),
            },
        })),
    })
}

fn job_status_running() -> anyhow::Result<JobStatus> {
    Ok(JobStatus::Running(job_running_status_executing()))
}

fn job_running_status_executing() -> JobRunningStatus {
    JobRunningStatus::AtBroker(JobBrokerStatus::AtWorker(
        1.into(),
        JobWorkerStatus::Executing,
    ))
}

fn ui_job_result(name: &str, job_id: u32, status: UiJobStatus) -> UiMessage {
    UiMessage::JobFinished(UiJobResult {
        name: name.into(),
        job_id: JobId::from(job_id),
        duration: (!matches!(status, UiJobStatus::Ignored)).then_some(Duration::from_secs(1)),
        status,
        stdout: vec![],
        stderr: vec![],
    })
}

fn wait_success() -> WaitStatus {
    WaitStatus {
        exit_code: ExitCode::SUCCESS,
        output: "".into(),
    }
}

fn wait_failure() -> WaitStatus {
    WaitStatus {
        exit_code: ExitCode::FAILURE,
        output: "build error".into(),
    }
}

//  _            _
// | |_ ___  ___| |_ ___
// | __/ _ \/ __| __/ __|
// | ||  __/\__ \ |_\__ \
//  \__\___||___/\__|___/

//            _ _           _   _
//   ___ ___ | | | ___  ___| |_(_) ___  _ __
//  / __/ _ \| | |/ _ \/ __| __| |/ _ \| '_ \
// | (_| (_) | | |  __/ (__| |_| | (_) | | | |
//  \___\___/|_|_|\___|\___|\__|_|\___/|_| |_|

script_test_with_error_simex! {
    no_packages,
    expected_test_db_out = [],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![] } => {
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    }
}

script_test_with_error_simex! {
    no_artifacts,
    expected_test_db_out = [],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", [])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", [])]
        }
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test! {
    stdout_color_passed_to_start_collection,
    @ stdout_color = true,
    expected_test_db_out = [],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", [])] } => {
        StartCollection {
            color: true,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", [])]
        }
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    no_tests_listed,
    expected_test_db_out = [
        TestDbEntry::empty_artifact("foo_pkg", "foo_test")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![],
        ignored_listing: vec![]
    } => {};
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    no_tests_listed_after_collection_done,
    expected_test_db_out = [
        TestDbEntry::empty_artifact("foo_pkg", "foo_test")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    CollectionFinished { wait_status: wait_success() } => {};
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![],
        ignored_listing: vec![]
    } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    collection_error,
    test_db_in = [],
    expected_exit_code = ExitCode::FAILURE,
    expected_test_db_out = [],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test", "bar_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test", "bar_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    CollectionFinished { wait_status: wait_failure() } => {
        SendUiMsg {
            msg: UiMessage::CollectionOutput("build error".into())
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec![],
                ignored: vec![],
                not_run: Some(NotRunEstimate::About(0)),
            })
        },
        StartShutdown
    };
    ArtifactBuilt {
        artifact: fake_artifact("bar_test", "foo_pkg"),
    } => {};
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![],
        ignored_listing: vec![]
    } => {};
}

script_test_with_error_simex! {
    collection_warning,
    expected_test_db_out = [
        TestDbEntry::empty_artifact("foo_pkg", "foo_test")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    CollectionFinished { wait_status: WaitStatus {
        output: "build warning".into(),
        ..wait_success()
    } } => {
        SendUiMsg {
            msg: UiMessage::CollectionOutput("build warning".into())
        },
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![],
        ignored_listing: vec![]
    } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

//  _            _                 _               _
// | |_ ___  ___| |_    ___  _   _| |_ _ __  _   _| |_
// | __/ _ \/ __| __|  / _ \| | | | __| '_ \| | | | __|
// | ||  __/\__ \ |_  | (_) | |_| | |_| |_) | |_| | |_
//  \__\___||___/\__|  \___/ \__,_|\__| .__/ \__,_|\__|
//                                    |_|

test_output_test_with_error_simex! {
    single_test_success,
    Ok(Ok(JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Exited(0),
        effects: JobEffects {
            stdout: JobOutputResult::None,
            stderr: JobOutputResult::None,
            duration: Duration::from_secs(1)
        }
    }))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: Some(Duration::from_secs(1)),
        status: UiJobStatus::Ok,
        stdout: vec![],
        stderr: vec![],
    },
    UiJobSummary {
        succeeded: 1,
        failed: vec![],
        ignored: vec![],
        not_run: None
    },
    ExitCode::SUCCESS,
    TestDbEntry::success("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
}

test_output_test! {
    single_test_non_zero_status_code,
    Ok(Ok(JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Exited(1),
        effects: JobEffects {
            stdout: JobOutputResult::None,
            stderr: JobOutputResult::None,
            duration: Duration::from_secs(1)
        }
    }))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: Some(Duration::from_secs(1)),
        status: UiJobStatus::Failure(None),
        stdout: vec![],
        stderr: vec![],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec!["foo_pkg test_a".into()],
        ignored: vec![],
        not_run: None
    },
    ExitCode::from(1),
    TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
}

test_output_test! {
    single_test_signaled,
    Ok(Ok(JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Signaled(9),
        effects: JobEffects {
            stdout: JobOutputResult::None,
            stderr: JobOutputResult::Inline(b"signal yo".as_slice().into()),
            duration: Duration::from_secs(1)
        }
    }))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: Some(Duration::from_secs(1)),
        status: UiJobStatus::Failure(Some("killed by signal 9".into())),
        stdout: vec![],
        stderr: vec!["signal yo".into()],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec!["foo_pkg test_a".into()],
        ignored: vec![],
        not_run: None
    },
    ExitCode::FAILURE,
    TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
}

test_output_test! {
    single_test_timed_out,
    Ok(Ok(JobOutcome::TimedOut(
        JobEffects {
            stdout: JobOutputResult::None,
            stderr: JobOutputResult::None,
            duration: Duration::from_secs(1)
        }
    ))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: Some(Duration::from_secs(1)),
        status: UiJobStatus::TimedOut,
        stdout: vec![],
        stderr: vec![],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec!["foo_pkg test_a".into()],
        ignored: vec![],
        not_run: None
    },
    ExitCode::FAILURE,
    TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
}

test_output_test! {
    single_test_execution_error,
    Ok(Err(JobError::Execution("test error".into()))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: None,
        status: UiJobStatus::Error("execution error: test error".into()),
        stdout: vec![],
        stderr: vec![],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec!["foo_pkg test_a".into()],
        ignored: vec![],
        not_run: None
    },
    ExitCode::FAILURE,
    TestDbEntry::new("foo_pkg", "foo_test", "test_a")
}

test_output_test! {
    single_test_system_error,
    Ok(Err(JobError::System("test error".into()))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: None,
        status: UiJobStatus::Error("system error: test error".into()),
        stdout: vec![],
        stderr: vec![],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec!["foo_pkg test_a".into()],
        ignored: vec![],
        not_run: None
    },
    ExitCode::FAILURE,
    TestDbEntry::new("foo_pkg", "foo_test", "test_a")
}

test_output_test! {
    single_test_remote_error,
    Err(anyhow!("test error")),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: None,
        status: UiJobStatus::Error("remote error: test error".into()),
        stdout: vec![],
        stderr: vec![],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec!["foo_pkg test_a".into()],
        ignored: vec![],
        not_run: None
    },
    ExitCode::FAILURE,
    TestDbEntry::new("foo_pkg", "foo_test", "test_a")
}

test_output_test! {
    single_test_stdout_stderr_preserved_on_failure,
    Ok(Ok(JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Exited(1),
        effects: JobEffects {
            stdout: JobOutputResult::Inline(b"hello\nstdout".as_slice().into()),
            stderr: JobOutputResult::Inline(b"hello\nstderr".as_slice().into()),
            duration: Duration::from_secs(1)
        }
    }))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: Some(Duration::from_secs(1)),
        status: UiJobStatus::Failure(None),
        stdout: vec!["hello".into(), "stdout".into()],
        stderr: vec!["hello".into(), "stderr".into()],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec!["foo_pkg test_a".into()],
        ignored: vec![],
        not_run: None
    },
    ExitCode::from(1),
    TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
}

test_output_test! {
    single_test_stdout_stderr_preserved_on_timeout,
    Ok(Ok(JobOutcome::TimedOut(
        JobEffects {
            stdout: JobOutputResult::Inline(b"hello\nstdout".as_slice().into()),
            stderr: JobOutputResult::Inline(b"hello\nstderr".as_slice().into()),
            duration: Duration::from_secs(1)
        }
    ))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: Some(Duration::from_secs(1)),
        status: UiJobStatus::TimedOut,
        stdout: vec!["hello".into(), "stdout".into()],
        stderr: vec!["hello".into(), "stderr".into()],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec!["foo_pkg test_a".into()],
        ignored: vec![],
        not_run: None
    },
    ExitCode::FAILURE,
    TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
}

test_output_test! {
    single_test_stdout_stderr_discarded_on_success,
    Ok(Ok(JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Exited(0),
        effects: JobEffects {
            stdout: JobOutputResult::Inline(b"hello\nstdout".as_slice().into()),
            stderr: JobOutputResult::Inline(b"hello\nstderr".as_slice().into()),
            duration: Duration::from_secs(1)
        }
    }))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: Some(Duration::from_secs(1)),
        status: UiJobStatus::Ok,
        stdout: vec![],
        stderr: vec![],
    },
    UiJobSummary {
        succeeded: 1,
        failed: vec![],
        ignored: vec![],
        not_run: None
    },
    ExitCode::SUCCESS,
    TestDbEntry::success("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
}

test_output_test! {
    single_test_stdout_stderr_truncated,
    Ok(Ok(JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Exited(1),
        effects: JobEffects {
            stdout: JobOutputResult::Truncated {
                first: b"hello\nstdout".as_slice().into(), truncated: 12
            },
            stderr: JobOutputResult::Truncated {
                first: b"hello\nstderr".as_slice().into(), truncated: 12
            },
            duration: Duration::from_secs(1)
        }
    }))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: Some(Duration::from_secs(1)),
        status: UiJobStatus::Failure(None),
        stdout: vec![
            "hello".into(), "stdout".into(), "job 1: stdout truncated, 12 bytes lost".into()
        ],
        stderr: vec![
            "hello".into(), "stderr".into(), "job 1: stderr truncated, 12 bytes lost".into()
        ],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec!["foo_pkg test_a".into()],
        ignored: vec![],
        not_run: None
    },
    ExitCode::from(1),
    TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
}

test_output_test! {
    single_test_ignored_from_output,
    Ok(Ok(JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Exited(0),
        effects: JobEffects {
            stdout: JobOutputResult::Inline(b"fixture: ignoring test test_a".as_slice().into()),
            stderr: JobOutputResult::None,
            duration: Duration::from_secs(1)
        }
    }))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: None,
        status: UiJobStatus::Ignored,
        stdout: vec![],
        stderr: vec![],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec![],
        ignored: vec!["foo_pkg test_a".into()],
        not_run: None
    },
    ExitCode::SUCCESS,
    TestDbEntry::new("foo_pkg", "foo_test", "test_a")
}

test_output_test! {
    single_test_ignored_from_truncated_output,
    Ok(Ok(JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Exited(0),
        effects: JobEffects {
            stdout: JobOutputResult::Truncated {
                first: b"fixture: ignoring test test_a".as_slice().into(),
                truncated: 12
            },
            stderr: JobOutputResult::None,
            duration: Duration::from_secs(1)
        }
    }))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: None,
        status: UiJobStatus::Ignored,
        stdout: vec![],
        stderr: vec![],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec![],
        ignored: vec!["foo_pkg test_a".into()],
        not_run: None
    },
    ExitCode::SUCCESS,
    TestDbEntry::new("foo_pkg", "foo_test", "test_a")
}

test_output_test! {
    single_test_fixture_output_removed,
    Ok(Ok(JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Exited(1),
        effects: JobEffects {
            stdout: JobOutputResult::Inline(
                b"fixture: a\ntest stdout\nfixture: b\ntest_a FAILED\n".as_slice().into()
            ),
            stderr: JobOutputResult::None,
            duration: Duration::from_secs(1)
        }
    }))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: Some(Duration::from_secs(1)),
        status: UiJobStatus::Failure(None),
        stdout: vec!["test stdout".into()],
        stderr: vec![],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec!["foo_pkg test_a".into()],
        ignored: vec![],
        not_run: None
    },
    ExitCode::from(1),
    TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
}

test_output_test! {
    single_test_fixture_output_removed_truncated,
    Ok(Ok(JobOutcome::Completed(JobCompleted {
        status: JobTerminationStatus::Exited(1),
        effects: JobEffects {
            stdout: JobOutputResult::Truncated {
                first: b"fixture: a\ntest stdout\nfixture: b\ntest_a FAILED\n".as_slice().into(),
                truncated: 12
            },
            stderr: JobOutputResult::None,
            duration: Duration::from_secs(1)
        }
    }))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: Some(Duration::from_secs(1)),
        status: UiJobStatus::Failure(None),
        stdout: vec!["test stdout".into(), "job 1: stdout truncated, 12 bytes lost".into()],
        stderr: vec![],
    },
    UiJobSummary {
        succeeded: 0,
        failed: vec!["foo_pkg test_a".into()],
        ignored: vec![],
        not_run: None
    },
    ExitCode::from(1),
    TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
}

//                  _ _   _       _        _            _
//  _ __ ___  _   _| | |_(_)_ __ | | ___  | |_ ___  ___| |_ ___
// | '_ ` _ \| | | | | __| | '_ \| |/ _ \ | __/ _ \/ __| __/ __|
// | | | | | | |_| | | |_| | |_) | |  __/ | ||  __/\__ \ |_\__ \
// |_| |_| |_|\__,_|_|\__|_| .__/|_|\___|  \__\___||___/\__|___/
//                         |_|

script_test_with_error_simex! {
    one_failure_one_success_exit_code,
    test_db_in = [],
    expected_exit_code = ExitCode::from(1),
    expected_test_db_out = [
        TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::success("foo_pkg", "foo_test", "test_b", nonempty![Duration::from_secs(1)])
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("foo_test", "test_b"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_b".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        }
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(1),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Failure(None)),
        },
    };
    JobUpdate {
        job_id: JobId::from(2),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_b", 2, UiJobStatus::Ok),
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 1,
                failed: vec!["foo_pkg test_a".into()],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    one_failure_one_success_exit_code_listing_after_collection,
    test_db_in = [],
    expected_exit_code = ExitCode::from(1),
    expected_test_db_out = [
        TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::success("foo_pkg", "foo_test", "test_b", nonempty![Duration::from_secs(1)])
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    CollectionFinished { wait_status: wait_success() } => {};
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("foo_test", "test_b"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_b".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        },
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(1),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Failure(None)),
        },
    };
    JobUpdate {
        job_id: JobId::from(2),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_b", 2, UiJobStatus::Ok),
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 1,
                failed: vec!["foo_pkg test_a".into()],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    ignored_tests,
    expected_test_db_out = [
        TestDbEntry::success("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec!["test_b".into()]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_b", 2, UiJobStatus::Ignored)
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        }
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Ok)
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 1,
                failed: vec![],
                ignored: vec!["foo_pkg test_b".into()],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    ignored_tests_via_directive,
    @ test_metadata = AllMetadata::from_str(
        &format!("{DEFAULT_METADATA_STR}{}",
            r#"
                [[directives]]
                filter = "name = \"test_b\""
                ignore = true
            "#
        )
    ).unwrap(),
    expected_test_db_out = [
        TestDbEntry::success("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_b", 2, UiJobStatus::Ignored)
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        }
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Ok)
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 1,
                failed: vec![],
                ignored: vec!["foo_pkg test_b".into()],
                not_run: None,
            })
        },
        StartShutdown
    };
}

//   __ _ _ _            _
//  / _(_) | |_ ___ _ __(_)_ __   __ _
// | |_| | | __/ _ \ '__| | '_ \ / _` |
// |  _| | | ||  __/ |  | | | | | (_| |
// |_| |_|_|\__\___|_|  |_|_| |_|\__, |
//                               |___/

script_test_with_error_simex! {
    filtering_cases,
    @ filter = SimpleFilter::Name("test_a".into()).into(),
    expected_test_db_out = [
        TestDbEntry::success("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Ok)
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 1,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    filtering_packages,
    @ filter = SimpleFilter::Package("bar_pkg".into()).into(),
    expected_test_db_out = [
        TestDbEntry::success("bar_pkg", "bar_test", "test_a", nonempty![Duration::from_secs(1)])
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages {
        packages: vec![fake_pkg("foo_pkg", ["foo_test"]), fake_pkg("bar_pkg", ["bar_test"])]
    } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("bar_pkg", ["bar_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("bar_test", "bar_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("bar_test", "bar_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("bar_test", "bar_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("bar_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "bar_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("bar_pkg test_a", 1, UiJobStatus::Ok)
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 1,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    filtering_package_and_test,
    @ filter = SimpleFilter::And(vec![
        SimpleFilter::Package("bar_pkg".into()).into(),
        SimpleFilter::Name("test_a".into()).into(),
    ]).into(),
    expected_test_db_out = [
        TestDbEntry::success("bar_pkg", "bar_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::new("bar_pkg", "bar_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages {
        packages: vec![fake_pkg("foo_pkg", ["foo_test"]), fake_pkg("bar_pkg", ["bar_test"])]
    } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("bar_pkg", ["bar_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("bar_test", "bar_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("bar_test", "bar_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("bar_test", "bar_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("bar_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "bar_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("bar_pkg test_a", 1, UiJobStatus::Ok)
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 1,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

//  _            _          _ _
// | |_ ___  ___| |_     __| | |__
// | __/ _ \/ __| __|   / _` | '_ \
// | ||  __/\__ \ |_   | (_| | |_) |
//  \__\___||___/\__|___\__,_|_.__/
//                 |_____|

script_test_with_error_simex! {
    expected_test_count,
    test_db_in = [
        TestDbEntry::success("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::success("foo_pkg", "foo_test", "test_b", nonempty![Duration::from_secs(1)])
    ],
    expected_exit_code = ExitCode::SUCCESS,
    expected_test_db_out = [
        TestDbEntry::success(
            "foo_pkg",
            "foo_test",
            "test_a",
            nonempty![Duration::from_secs(1), Duration::from_secs(1)]
        ),
        TestDbEntry::success(
            "foo_pkg",
            "foo_test",
            "test_b",
            nonempty![Duration::from_secs(1), Duration::from_secs(1)]
        )
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages {
        packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
    } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        },
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: JobSpec {
                estimated_duration: Some(Duration::from_secs(1)),
                priority: 0,
                ..test_spec("foo_test", "test_a")
            }
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        AddJob {
            job_id: JobId::from(2),
            spec: JobSpec {
                estimated_duration: Some(Duration::from_secs(1)),
                priority: 0,
                ..test_spec("foo_test", "test_b")
            }
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_b".into()
            })
        },
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Ok)
        }
    };
    JobUpdate {
        job_id: JobId::from(2),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_b", 2, UiJobStatus::Ok)
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 2,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    getting_packages_removes_old_test_db_packages,
    @ filter = SimpleFilter::Package("non_existent_pkg".into()).into(),
    test_db_in = [
        TestDbEntry::success("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::success("bar_pkg", "bar_test", "test_a", nonempty![Duration::from_secs(1)])
    ],
    expected_exit_code = ExitCode::SUCCESS,
    expected_test_db_out = [
        TestDbEntry::success("bar_pkg", "bar_test", "test_a", nonempty![Duration::from_secs(1)])
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages {
        packages: vec![fake_pkg("bar_pkg", ["bar_test"])]
    } => {
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

test_db_test! {
    previous_success_with_estimated_duration,
    test_db_in = [
        TestDbEntry::success("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(2)]),
        TestDbEntry::success("foo_pkg", "foo_test", "test_b", nonempty![Duration::from_secs(3)])
    ],
    expected_test_db_out = [
        TestDbEntry::success(
            "foo_pkg",
            "foo_test",
            "test_a",
            nonempty![Duration::from_secs(2), Duration::from_secs(1)]
        ),
        TestDbEntry::success(
            "foo_pkg",
            "foo_test",
            "test_b",
            nonempty![Duration::from_secs(3), Duration::from_secs(1)]
        )
    ],
    JobSpec {
        estimated_duration: Some(Duration::from_secs(2)),
        priority: 0,
        ..test_spec("foo_test", "test_a")
    },
    JobSpec {
        estimated_duration: Some(Duration::from_secs(3)),
        priority: 0,
        ..test_spec("foo_test", "test_b")
    }
}

test_db_test! {
    previous_failure_with_estimated_duration,
    test_db_in = [
        TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(2)]),
        TestDbEntry::success("foo_pkg", "foo_test", "test_b", nonempty![Duration::from_secs(3)])
    ],
    expected_test_db_out = [
        TestDbEntry::success(
            "foo_pkg",
            "foo_test",
            "test_a",
            nonempty![Duration::from_secs(2), Duration::from_secs(1)]
        ),
        TestDbEntry::success(
            "foo_pkg",
            "foo_test",
            "test_b",
            nonempty![Duration::from_secs(3), Duration::from_secs(1)]
        )
    ],
    JobSpec {
        estimated_duration: Some(Duration::from_secs(2)),
        priority: 1,
        ..test_spec("foo_test", "test_a")
    },
    JobSpec {
        estimated_duration: Some(Duration::from_secs(3)),
        priority: 0,
        ..test_spec("foo_test", "test_b")
    }
}

//                         _               _            _
//  _ __ _   _ _ __  _ __ (_)_ __   __ _  | |_ ___  ___| |_ ___
// | '__| | | | '_ \| '_ \| | '_ \ / _` | | __/ _ \/ __| __/ __|
// | |  | |_| | | | | | | | | | | | (_| | | ||  __/\__ \ |_\__ \
// |_|   \__,_|_| |_|_| |_|_|_| |_|\__, |  \__\___||___/\__|___/
//                                 |___/

script_test_with_error_simex! {
    running_tests,
    expected_test_db_out = [
        TestDbEntry::success("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::success("foo_pkg", "foo_test", "test_b", nonempty![Duration::from_secs(1)])
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("foo_test", "test_b"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_b".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        },
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_running()
    } => {
        SendUiMsg {
            msg: UiMessage::JobUpdated(UiJobUpdate {
                job_id: JobId::from(1),
                status: job_running_status_executing(),
            })
        },
    };
    JobUpdate {
        job_id: JobId::from(2),
        result: job_status_running()
    } => {
        SendUiMsg {
            msg: UiMessage::JobUpdated(UiJobUpdate {
                job_id: JobId::from(2),
                status: job_running_status_executing(),
            })
        },
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Ok)
        },
    };
    JobUpdate {
        job_id: JobId::from(2),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_b", 2, UiJobStatus::Ok)
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 2,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

//                            _
//  _ __ ___ _ __   ___  __ _| |_
// | '__/ _ \ '_ \ / _ \/ _` | __|
// | | |  __/ |_) |  __/ (_| | |_
// |_|  \___| .__/ \___|\__,_|\__|
//          |_|

script_test_with_error_simex! {
    repeat_2_success,
    @ repeat = Repeat::try_from(2).unwrap(),
    expected_test_db_out = [
        TestDbEntry::success(
            "foo_pkg",
            "foo_test",
            "test_a",
            nonempty![Duration::from_secs(1), Duration::from_secs(1)]
        ),
        TestDbEntry::success(
            "foo_pkg",
            "foo_test",
            "test_b",
            nonempty![Duration::from_secs(1), Duration::from_secs(1)]
        )
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },

        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        },

        AddJob {
            job_id: JobId::from(3),
            spec: test_spec("foo_test", "test_b"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(3),
                name: "foo_pkg test_b".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(3)
        },

        AddJob {
            job_id: JobId::from(4),
            spec: test_spec("foo_test", "test_b"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(4),
                name: "foo_pkg test_b".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(4)
        }
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Ok),
        },
    };
    JobUpdate {
        job_id: JobId::from(2),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 2, UiJobStatus::Ok),
        },
    };
    JobUpdate {
        job_id: JobId::from(3),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_b", 3, UiJobStatus::Ok),
        },
    };
    JobUpdate {
        job_id: JobId::from(4),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_b", 4, UiJobStatus::Ok),
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 4,
                failed: vec![],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    repeat_2_single_failure,
    @ repeat = Repeat::try_from(2).unwrap(),
    test_db_in = [],
    expected_exit_code = ExitCode::from(1),
    expected_test_db_out = [
        TestDbEntry::failure(
            "foo_pkg",
            "foo_test",
            "test_a",
            nonempty![Duration::from_secs(1), Duration::from_secs(1)]
        )
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },

        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        },
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Ok),
        },
    };
    JobUpdate {
        job_id: JobId::from(2),
        result: job_status_complete(1),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 2, UiJobStatus::Failure(None)),
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 1,
                failed: vec!["foo_pkg test_a".into()],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    ignored_tests_not_repeated,
    @ repeat = Repeat::try_from(2).unwrap(),
    expected_test_db_out = [
        TestDbEntry::success(
            "foo_pkg",
            "foo_test",
            "test_a",
            nonempty![Duration::from_secs(1), Duration::from_secs(1)]
        ),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec!["test_b".into()]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        },
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_b", 3, UiJobStatus::Ignored)
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(3)
        }
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Ok)
        },
    };
    JobUpdate {
        job_id: JobId::from(2),
        result: job_status_complete(0),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 2, UiJobStatus::Ok)
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 2,
                failed: vec![],
                ignored: vec!["foo_pkg test_b".into()],
                not_run: None,
            })
        },
        StartShutdown
    };
}

//      _                       __ _
//  ___| |_ ___  _ __     __ _ / _| |_ ___ _ __
// / __| __/ _ \| '_ \   / _` | |_| __/ _ \ '__|
// \__ \ || (_) | |_) | | (_| |  _| ||  __/ |
// |___/\__\___/| .__/___\__,_|_|  \__\___|_|
//              |_| |_____|

script_test_with_error_simex! {
    stop_after_1_after_collection,
    @ stop_after = Some(StopAfter::try_from(1).unwrap()),
    test_db_in = [],
    expected_exit_code = ExitCode::from(1),
    expected_test_db_out = [
        TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("foo_test", "test_b"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_b".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        },
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(1),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Failure(None))
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec!["foo_pkg test_a".into()],
                ignored: vec![],
                not_run: Some(NotRunEstimate::Exactly(1)),
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    stop_after_1_one_failure,
    @ stop_after = Some(StopAfter::try_from(1).unwrap()),
    test_db_in = [],
    expected_exit_code = ExitCode::from(1),
    expected_test_db_out = [
        TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(1),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Failure(None))
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec!["foo_pkg test_a".into()],
                ignored: vec![],
                not_run: None,
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    stop_after_1_before_listing_complete,
    @ stop_after = Some(StopAfter::try_from(1).unwrap()),
    test_db_in = [],
    expected_exit_code = ExitCode::from(1),
    expected_test_db_out = [
        TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("bar_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("bar_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("foo_test", "test_b"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_b".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        },
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(1),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Failure(None))
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec!["foo_pkg test_a".into()],
                ignored: vec![],
                not_run: Some(NotRunEstimate::GreaterThan(1)),
            })
        },
        StartShutdown
    };
    TestsListed {
        artifact: fake_artifact("bar_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {}
}

script_test_with_error_simex! {
    stop_after_1_before_listing_complete_with_expected_count,
    @ stop_after = Some(StopAfter::try_from(1).unwrap()),
    test_db_in = [
        TestDbEntry::new("foo_pkg", "foo_test", "test_a"),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b"),
        TestDbEntry::new("foo_pkg", "bar_test", "test_a"),
        TestDbEntry::new("foo_pkg", "bar_test", "test_b")
    ],
    expected_exit_code = ExitCode::from(1),
    expected_test_db_out = [
        TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b"),
        TestDbEntry::new("foo_pkg", "bar_test", "test_a"),
        TestDbEntry::new("foo_pkg", "bar_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test", "bar_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test", "bar_test"])]
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(4)
        },
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("bar_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("bar_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("foo_test", "test_b"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_b".into()
            })
        },
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(1),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Failure(None))
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec!["foo_pkg test_a".into()],
                ignored: vec![],
                not_run: Some(NotRunEstimate::About(3)),
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    stop_after_1_before_listing_complete_no_pending_jobs,
    @ stop_after = Some(StopAfter::try_from(1).unwrap()),
    test_db_in = [],
    expected_exit_code = ExitCode::from(1),
    expected_test_db_out = [
        TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)])
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("bar_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("bar_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(1),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Failure(None))
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec!["foo_pkg test_a".into()],
                ignored: vec![],
                not_run: Some(NotRunEstimate::Unknown),
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    stop_after_2_repeat,
    @ stop_after = Some(StopAfter::try_from(2).unwrap()),
    @ repeat = Repeat::try_from(3).unwrap(),
    test_db_in = [],
    expected_exit_code = ExitCode::from(1),
    expected_test_db_out = [
        TestDbEntry::failure(
            "foo_pkg",
            "foo_test",
            "test_a",
            nonempty![Duration::from_secs(1), Duration::from_secs(1)]
        )
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        },
        AddJob {
            job_id: JobId::from(3),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(3),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(3)
        },
    };
    JobUpdate {
        job_id: JobId::from(3),
        result: job_status_complete(1),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 3, UiJobStatus::Failure(None))
        },
    };
    JobUpdate {
        job_id: JobId::from(2),
        result: job_status_complete(1),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 2, UiJobStatus::Failure(None))
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec!["foo_pkg test_a".into(), "foo_pkg test_a".into()],
                ignored: vec![],
                not_run: Some(NotRunEstimate::GreaterThan(1)),
            })
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    stop_after_1_update_comes_after_shutdown,
    @ stop_after = Some(StopAfter::try_from(1).unwrap()),
    test_db_in = [],
    expected_exit_code = ExitCode::from(1),
    expected_test_db_out = [
        TestDbEntry::failure("foo_pkg", "foo_test", "test_a", nonempty![Duration::from_secs(1)]),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: test_spec("foo_test", "test_a"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(1),
                name: "foo_pkg test_a".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("foo_test", "test_b"),
        },
        SendUiMsg {
            msg: UiMessage::JobEnqueued(UiJobEnqueued {
                job_id: JobId::from(2),
                name: "foo_pkg test_b".into()
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        },
    };
    CollectionFinished { wait_status: wait_success() } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    JobUpdate {
        job_id: JobId::from(1),
        result: job_status_complete(1),
    } => {
        SendUiMsg {
            msg: ui_job_result("foo_pkg test_a", 1, UiJobStatus::Failure(None))
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec!["foo_pkg test_a".into()],
                ignored: vec![],
                not_run: Some(NotRunEstimate::Exactly(1)),
            })
        },
        StartShutdown
    };
    JobUpdate {
        job_id: JobId::from(2),
        result: job_status_complete(1),
    } => {};
}

//  _ _     _   _
// | (_)___| |_(_)_ __   __ _
// | | / __| __| | '_ \ / _` |
// | | \__ \ |_| | | | | (_| |
// |_|_|___/\__|_|_| |_|\__, |
//                      |___/

script_test_with_error_simex! {
    listing,
    @ listing = true,
    expected_test_db_out = [
        TestDbEntry::new("foo_pkg", "foo_test", "test_a"),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    CollectionFinished { wait_status: wait_success() } => {};
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        SendUiMsg {
            msg: UiMessage::List("foo_pkg test_a".into())
        },
        SendUiMsg {
            msg: UiMessage::List("foo_pkg test_b".into())
        },
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    listing_ignored_tests,
    @ listing = true,
    expected_test_db_out = [
        TestDbEntry::new("foo_pkg", "foo_test", "test_a"),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    CollectionFinished { wait_status: wait_success() } => {};
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec!["test_a".into(), "test_b".into()]
    } => {
        SendUiMsg {
            msg: UiMessage::List("foo_pkg test_a".into())
        },
        SendUiMsg {
            msg: UiMessage::List("foo_pkg test_b".into())
        },
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    listing_no_tests,
    @ listing = true,
    expected_test_db_out = [
        TestDbEntry::empty_artifact("foo_pkg", "foo_test")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    CollectionFinished { wait_status: wait_success() } => {};
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![],
        ignored_listing: vec![]
    } => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    listing_and_filtering_cases,
    @ filter = SimpleFilter::Name("test_a".into()).into(),
    @ listing = true,
    expected_test_db_out = [
        TestDbEntry::new("foo_pkg", "foo_test", "test_a"),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    CollectionFinished { wait_status: wait_success() } => {};
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        SendUiMsg {
            msg: UiMessage::List("foo_pkg test_a".into())
        },
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    listing_and_repeating,
    @ listing = true,
    @ repeat = Repeat::try_from(2).unwrap(),
    expected_test_db_out = [
        TestDbEntry::new("foo_pkg", "foo_test", "test_a"),
        TestDbEntry::new("foo_pkg", "foo_test", "test_b")
    ],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    CollectionFinished { wait_status: wait_success() } => {};
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![("test_a".into(), NoCaseMetadata), ("test_b".into(), NoCaseMetadata)],
        ignored_listing: vec![]
    } => {
        SendUiMsg {
            msg: UiMessage::List("foo_pkg test_a".into())
        },
        SendUiMsg {
            msg: UiMessage::List("foo_pkg test_b".into())
        },
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        },
        StartShutdown
    };
}

//                _       _
// __      ____ _| |_ ___| |__
// \ \ /\ / / _` | __/ __| '_ \
//  \ V  V / (_| | || (__| | | |
//   \_/\_/ \__,_|\__\___|_| |_|

watch_simex_test! {
    watch_single_file_modify,
    triggers_restart = true,
    watch_exclude_paths = vec![],
    WatchEvents { events: vec![Event {
        kind: EventKind::Modify(ModifyKind::Any),
        paths: vec!["src/foo.rs".into()],
        attrs: EventAttributes::new()
    }] }
}

watch_simex_test! {
    watch_multiple_files_modified,
    triggers_restart = true,
    watch_exclude_paths = vec![],
    WatchEvents { events: vec![
        Event {
            kind: EventKind::Modify(ModifyKind::Any),
            paths: vec!["src/foo.rs".into()],
            attrs: EventAttributes::new()
        },
        Event {
            kind: EventKind::Modify(ModifyKind::Any),
            paths: vec!["src/bar.rs".into()],
            attrs: EventAttributes::new()
        }
    ] }
}

watch_simex_test! {
    watch_single_file_modify_ignored,
    triggers_restart = false,
    watch_exclude_paths = vec!["target".into()],
    WatchEvents { events: vec![Event {
        kind: EventKind::Modify(ModifyKind::Any),
        paths: vec!["target/foo_bin".into()],
        attrs: EventAttributes::new()
    }] }
}

watch_simex_test! {
    watch_multiple_files_modified_ignored,
    triggers_restart = false,
    watch_exclude_paths = vec!["target".into()],
    WatchEvents { events: vec![
        Event {
            kind: EventKind::Modify(ModifyKind::Any),
            paths: vec!["target/foo_bin".into()],
            attrs: EventAttributes::new()
        },
        Event {
            kind: EventKind::Modify(ModifyKind::Any),
            paths: vec!["target/bar_bin".into()],
            attrs: EventAttributes::new()
        },
    ] }
}

watch_simex_test! {
    watch_single_modify_one_path_ignored,
    triggers_restart = true,
    watch_exclude_paths = vec!["target".into()],
    WatchEvents { events: vec![Event {
        kind: EventKind::Modify(ModifyKind::Any),
        paths: vec!["src/foo.rs".into(), "target/foo_bin".into()],
        attrs: EventAttributes::new()
    }] }
}

watch_simex_test! {
    watch_multiple_file_modify_one_path_ignored,
    triggers_restart = true,
    watch_exclude_paths = vec!["target".into()],
    WatchEvents { events: vec![
        Event {
            kind: EventKind::Modify(ModifyKind::Any),
            paths: vec!["src/foo.rs".into()],
            attrs: EventAttributes::new()
        },
        Event {
            kind: EventKind::Modify(ModifyKind::Any),
            paths: vec!["target/foo_bin".into()],
            attrs: EventAttributes::new()
        }
    ] }
}

script_test! {
    watch_with_collection_error,
    @ watch = true,
    test_db_in = [],
    expected_exit_code = ExitCode::FAILURE,
    expected_test_db_out = [],
    Start => {
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus("building artifacts...".into()),
        },
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", ["foo_test", "bar_test"])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", ["foo_test", "bar_test"])]
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("foo_test", "foo_pkg"),
    } => {
        ListTests {
            artifact: fake_artifact("foo_test", "foo_pkg"),
        }
    };
    CollectionFinished { wait_status: wait_failure() } => {
        SendUiMsg {
            msg: UiMessage::CollectionOutput("build error".into())
        },
        SendUiMsg {
            msg: UiMessage::AllJobsFinished(UiJobSummary {
                succeeded: 0,
                failed: vec![],
                ignored: vec![],
                not_run: Some(NotRunEstimate::About(0)),
            })
        },
        SendUiMsg {
            msg: UiMessage::UpdateEnqueueStatus(
                "waiting for changes...".into()
            )
        }
    };
    ArtifactBuilt {
        artifact: fake_artifact("bar_test", "foo_pkg"),
    } => {};
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![],
        ignored_listing: vec![]
    } => {};
    WatchEvents { events: vec![Event {
        kind: EventKind::Modify(ModifyKind::Any),
        paths: vec!["src/foo.rs".into()],
        attrs: EventAttributes::new()
    }] } => {
        StartRestart
    }
}
