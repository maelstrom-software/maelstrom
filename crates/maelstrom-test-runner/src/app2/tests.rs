use super::{Deps, MainApp, MainAppMessage::*, MainAppMessageM, TestingOptions};
use crate::deps::SimpleFilter;
use crate::fake_test_framework::{
    FakePackageId, FakeTestArtifact, FakeTestFilter, FakeTestPackage, TestCollector, TestOptions,
};
use crate::metadata::{AllMetadata, TestMetadata};
use crate::test_db::TestDb;
use crate::ui::{UiJobId as JobId, UiJobResult, UiJobStatus, UiMessage};
use crate::{NoCaseMetadata, StringArtifactKey};
use anyhow::anyhow;
use itertools::Itertools as _;
use maelstrom_base::{
    ClientJobId, JobCompleted, JobDevice, JobEffects, JobError, JobMount, JobNetwork, JobOutcome,
    JobOutputResult, JobRootOverlay, JobTerminationStatus,
};
use maelstrom_client::{
    spec::{ContainerRef, ContainerSpec, JobSpec, LayerSpec},
    JobStatus,
};
use maelstrom_simex::SimulationExplorer;
use maelstrom_util::process::ExitCode;
use std::cell::RefCell;
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

    fn get_test_layers(
        &self,
        _artifact: &FakeTestArtifact,
        _metadata: &TestMetadata,
    ) -> Vec<LayerSpec> {
        vec![]
    }

    fn send_ui_msg(&self, msg: UiMessage) {
        let mut self_ = self.0.borrow_mut();
        self_.messages.push(TestMessage::SendUiMsg { msg });
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

    fn assert_return_value(&mut self, str_expr: &str) {
        let ret = self.app.main_return_value();
        assert_eq!(format!("{ret:?}"), str_expr);
    }
}

fn default_metadata() -> AllMetadata<FakeTestFilter> {
    AllMetadata::from_str(
        r#"
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
        "#,
    )
    .unwrap()
}

fn default_testing_options() -> TestingOptions<FakeTestFilter, TestOptions> {
    TestingOptions {
        test_metadata: default_metadata(),
        filter: SimpleFilter::All.into(),
        collector_options: TestOptions,
        timeout_override: None,
    }
}

macro_rules! script_test {
    (
        $test_name:ident,
        $(@ $arg_key:ident = $arg_value:expr,)*
        $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?
    ) => {
        script_test!(
            $test_name,
            $(@ $arg_key:ident = $arg_value,)*
            ExitCode::SUCCESS,
            $($in_msg => { $($out_msg,)* };)+
        );
    };
    (
        $test_name:ident,
        $(@ $arg_key:ident = $arg_value:expr,)*
        $exit_code:expr,
        $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?
    ) => {
        #[test]
        fn $test_name() {
            let deps = TestDeps::default();
            let options = TestingOptions {
                $($arg_key: $arg_value,)*
                ..default_testing_options()
            };
            let test_db = TestDb::default();
            let mut fixture = Fixture::new(&deps, &options, test_db);
            $(
                fixture.receive_message($in_msg);
                fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
            )+
            fixture.assert_return_value(&format!("Ok({:?})", $exit_code));
        }
    }
}

macro_rules! script_test_with_error_simex {
    (
        $test_name:ident,
        $(@ $arg_key:ident = $arg_value:expr,)*
        $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?
    ) => {
        script_test_with_error_simex!(
            $test_name,
            $(@ $arg_key = $arg_value,)*
            ExitCode::SUCCESS,
            $($in_msg => { $($out_msg,)* };)+
        );
    };
    (
        $test_name:ident,
        $(@ $arg_key:ident = $arg_value:expr,)*
        $exit_code:expr,
        $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?
    ) => {
        script_test!(
            $test_name,
            $(@ $arg_key = $arg_value,)*
            $exit_code,
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
                    let test_db = TestDb::default();
                    let mut fixture = Fixture::new(&deps, &options, test_db);
                    $(
                        if simulation.choose_bool() {
                            fixture.receive_message(FatalError { error: anyhow!("simex error") });
                            fixture.assert_return_value("Err(simex error)");
                            continue;
                        }
                        fixture.receive_message($in_msg);
                        fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
                    )+

                    fixture.receive_message(FatalError { error: anyhow!("simex error") });
                    fixture.assert_return_value("Err(simex error)");
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

fn fake_artifact(name: &str, pkg: &str) -> FakeTestArtifact {
    FakeTestArtifact {
        name: name.into(),
        tests: vec!["test_a".into()],
        ignored_tests: vec![],
        path: format!("/{name}_bin").into(),
        package: FakePackageId(pkg.into()),
    }
}

fn test_spec(name: &str) -> JobSpec {
    JobSpec {
        container: default_container(),
        program: "/foo_test_bin".into(),
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
    ($macro_name:ident, $name:ident, $job_outcome:expr, $job_result:expr, $exit_code:expr) => {
        $macro_name! {
            $name,
            $exit_code,
            Start => {
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
                    spec: test_spec("test_a"),
                },
                SendUiMsg {
                    msg: UiMessage::UpdatePendingJobsCount(1)
                }
            };
            CollectionFinished => {
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
                StartShutdown
            };
        }
    };
}

macro_rules! test_output_test {
    ($name:ident, $job_outcome:expr, $job_result:expr, $exit_code:expr) => {
        test_output_test_inner!(script_test, $name, $job_outcome, $job_result, $exit_code);
    };
}

macro_rules! test_output_test_with_error_simex {
    ($name:ident, $job_outcome:expr, $job_result:expr, $exit_code:expr) => {
        test_output_test_inner!(
            script_test_with_error_simex,
            $name,
            $job_outcome,
            $job_result,
            $exit_code
        );
    };
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
    Start => {
        GetPackages
    };
    Packages { packages: vec![] } => {
        StartShutdown
    }
}

script_test_with_error_simex! {
    no_artifacts,
    Start => {
        GetPackages
    };
    Packages { packages: vec![fake_pkg("foo_pkg", [])] } => {
        StartCollection {
            color: false,
            options: TestOptions,
            packages: vec![fake_pkg("foo_pkg", [])]
        }
    };
    CollectionFinished => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        },
        StartShutdown
    };
}

script_test_with_error_simex! {
    no_tests_listed,
    Start => {
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
    CollectionFinished => {
        SendUiMsg {
            msg: UiMessage::DoneQueuingJobs,
        }
    };
    TestsListed {
        artifact: fake_artifact("foo_test", "foo_pkg"),
        listing: vec![],
        ignored_listing: vec![]
    } => {
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
    ExitCode::SUCCESS
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
    ExitCode::from(1)
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
    ExitCode::FAILURE
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
    ExitCode::FAILURE
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
    ExitCode::FAILURE
}

test_output_test! {
    single_test_system,
    Ok(Err(JobError::System("test error".into()))),
    UiJobResult {
        name: "foo_pkg test_a".into(),
        job_id: JobId::from(1),
        duration: None,
        status: UiJobStatus::Error("system error: test error".into()),
        stdout: vec![],
        stderr: vec![],
    },
    ExitCode::FAILURE
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
    ExitCode::FAILURE
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
    ExitCode::from(1)
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
    ExitCode::FAILURE
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
    ExitCode::SUCCESS
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
    ExitCode::from(1)
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
    ExitCode::SUCCESS
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
    ExitCode::SUCCESS
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
    ExitCode::from(1)
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
    ExitCode::from(1)
}

//                  _ _   _       _        _            _
//  _ __ ___  _   _| | |_(_)_ __ | | ___  | |_ ___  ___| |_ ___
// | '_ ` _ \| | | | | __| | '_ \| |/ _ \ | __/ _ \/ __| __/ __|
// | | | | | | |_| | | |_| | |_) | |  __/ | ||  __/\__ \ |_\__ \
// |_| |_| |_|\__,_|_|\__|_| .__/|_|\___|  \__\___||___/\__|___/
//                         |_|

script_test_with_error_simex! {
    one_failure_one_success_exit_code,
    ExitCode::from(1),
    Start => {
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
            spec: test_spec("test_a"),
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
        AddJob {
            job_id: JobId::from(2),
            spec: test_spec("test_b"),
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(2)
        }
    };
    CollectionFinished => {
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
        StartShutdown
    };
}

script_test_with_error_simex! {
    ignored_tests,
    ExitCode::SUCCESS,
    Start => {
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
            spec: test_spec("test_a"),
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
    CollectionFinished => {
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
    Start => { GetPackages };
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
            spec: test_spec("test_a"),
        },
        SendUiMsg {
            msg: UiMessage::UpdatePendingJobsCount(1)
        },
    };
    CollectionFinished => {
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
        StartShutdown
    };
}
