use super::{Deps, MainApp, MainAppMessage::*, MainAppMessageM};
use crate::fake_test_framework::{
    FakePackageId, FakeTestArtifact, FakeTestFilter, FakeTestPackage, TestCollector, TestOptions,
};
use crate::metadata::{AllMetadata, TestMetadata};
use crate::test_db::TestDb;
use crate::ui::{UiJobId as JobId, UiJobResult, UiJobStatus, UiMessage};
use crate::{NoCaseMetadata, StringArtifactKey};
use itertools::Itertools as _;
use maelstrom_base::{
    ClientJobId, JobCompleted, JobDevice, JobEffects, JobMount, JobNetwork, JobOutcome,
    JobOutputResult, JobRootOverlay, JobTerminationStatus,
};
use maelstrom_client::{
    spec::{ContainerRef, ContainerSpec, JobSpec, LayerSpec},
    JobStatus,
};
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
        test_metadata: &'deps AllMetadata<FakeTestFilter>,
        test_db: TestDb<StringArtifactKey, NoCaseMetadata>,
        collector_options: &'deps TestOptions,
    ) -> Self {
        Self {
            app: MainApp::new(deps, test_metadata, test_db, None, collector_options),
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

macro_rules! script_test {
    ($test_name:ident, $($in_msg:expr => { $($out_msg:expr),* $(,)? });+ $(;)?) => {
        #[test]
        fn $test_name() {
            let deps = TestDeps::default();
            let all_metadata = default_metadata();
            let test_db = TestDb::default();
            let collector_options = TestOptions;
            let mut fixture = Fixture::new(&deps, &all_metadata, test_db, &collector_options);
            $(
                fixture.receive_message($in_msg);
                fixture.expect_messages_in_any_order(vec![$($out_msg,)*]);
            )+
        }
    };
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

script_test! {
    no_packages,
    Start => {
        GetPackages
    };
    Packages { packages: vec![] } => {
        StartShutdown
    }
}

script_test! {
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

script_test! {
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
        listing: vec![]
    } => {
        StartShutdown
    };
}

script_test! {
    single_test,
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
        listing: vec![("test_a".into(), NoCaseMetadata)]
    } => {
        AddJob {
            job_id: JobId::from(1),
            spec: JobSpec {
                container: default_container(),
                program: "/foo_test_bin".into(),
                arguments: vec![
                    "test_a".into(),
                ],
                timeout: None,
                estimated_duration: None,
                allocate_tty: None,
                priority: 1,
            },
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
    JobUpdate { job_id: JobId::from(1), result: Ok(JobStatus::Completed {
        client_job_id: ClientJobId::from(1),
        result: Ok(JobOutcome::Completed(JobCompleted {
            status: JobTerminationStatus::Exited(0),
            effects: JobEffects {
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
                duration: Duration::from_secs(1)
            }
        }))
    }) } => {
        SendUiMsg {
            msg: UiMessage::JobFinished(
                UiJobResult {
                    name: "foo_pkg test_a".into(),
                    job_id: JobId::from(1),
                    duration: None,
                    status: UiJobStatus::Ok,
                    stdout: vec![],
                    stderr: vec![],
                }
            )
        },
        StartShutdown
    };
}
