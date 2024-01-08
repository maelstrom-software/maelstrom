use anyhow::Result;
use assert_matches::assert_matches;
use cargo_metest::test_listing::{
    load_test_listing, ArtifactCases, ArtifactKey, ArtifactKind, Package, TestListing,
    LAST_TEST_LISTING_NAME,
};
use cargo_metest::{
    config::Quiet,
    main_app_new,
    progress::{ProgressDriver, ProgressIndicator},
    EnqueueResult, MainAppDeps,
};
use indicatif::InMemoryTerm;
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker, Hello},
    stats::{JobState, JobStateCounts},
    JobOutputResult, JobSpec, JobStatus, JobStringResult, JobSuccess,
};
use meticulous_client::{Client, ClientDeps, ClientDriver};
use meticulous_util::{config::BrokerAddr, fs::Fs};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cell::RefCell,
    collections::HashMap,
    io::{self, Read as _, Write as _},
    net::{Ipv6Addr, SocketAddrV6, TcpListener, TcpStream},
    os::unix::fs::PermissionsExt as _,
    path::Path,
    rc::Rc,
    sync::{Arc, Mutex},
};
use tempfile::{tempdir, TempDir};

fn put_file(fs: &Fs, path: &Path, contents: &str) {
    let mut f = fs.create_file(path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
}

fn put_script(fs: &Fs, path: &Path, contents: &str) {
    let mut f = fs.create_file(path).unwrap();
    f.write_all(
        format!(
            "#!/bin/bash
            set -e
            set -o pipefail
            {contents}
            "
        )
        .as_bytes(),
    )
    .unwrap();

    let mut perms = f.metadata().unwrap().permissions();
    perms.set_mode(0o777);
    f.set_permissions(perms).unwrap();
}

fn generate_cargo_project(tmp_dir: &TempDir, fake_tests: &FakeTests) -> String {
    let fs = Fs::new();
    let workspace_dir = tmp_dir.path().join("workspace");
    fs.create_dir(&workspace_dir).unwrap();
    let cargo_path = workspace_dir.join("cargo");
    put_script(
        &fs,
        &cargo_path,
        &format!(
            "\
            cd {workspace_dir:?}\n\
            cargo $@ | sort\n\
            "
        ),
    );
    put_file(
        &fs,
        &workspace_dir.join("Cargo.toml"),
        "\
        [workspace]\n\
        members = [ \"crates/*\"]
        ",
    );
    let crates_dir = workspace_dir.join("crates");
    fs.create_dir(&crates_dir).unwrap();
    for binary in &fake_tests.test_binaries {
        let crate_name = &binary.name;
        let project_dir = crates_dir.join(&crate_name);
        fs.create_dir(&project_dir).unwrap();
        put_file(
            &fs,
            &project_dir.join("Cargo.toml"),
            &format!(
                "\
                [package]\n\
                name = \"{crate_name}\"\n\
                version = \"0.1.0\"\n\
                [lib]\n\
                ",
            ),
        );
        let src_dir = project_dir.join("src");
        fs.create_dir(&src_dir).unwrap();
        let mut test_src = String::new();
        for test_case in &binary.tests {
            let test_name = &test_case.name;
            let ignored = if test_case.ignored { "#[ignore]" } else { "" };
            test_src += &format!(
                "\
                #[test]\n\
                {ignored}\
                fn {test_name}() {{}}\n\
                ",
            );
        }
        put_file(&fs, &src_dir.join("lib.rs"), &test_src);
    }

    cargo_path.display().to_string()
}

struct MessageStream {
    stream: TcpStream,
}

impl MessageStream {
    fn next<T: DeserializeOwned>(&mut self) -> io::Result<T> {
        let mut msg_len: [u8; 4] = [0; 4];
        self.stream.read_exact(&mut msg_len)?;
        let mut buf = vec![0; u32::from_le_bytes(msg_len) as usize];
        self.stream.read_exact(&mut buf).unwrap();
        Ok(bincode::deserialize_from(&buf[..]).unwrap())
    }
}

fn send_message(mut stream: &TcpStream, msg: &impl Serialize) {
    let buf = bincode::serialize(msg).unwrap();
    stream.write_all(&(buf.len() as u32).to_le_bytes()).unwrap();
    stream.write_all(&buf[..]).unwrap();
}

fn test_path(spec: &JobSpec) -> TestPath {
    let binary = spec
        .program
        .as_str()
        .split("/")
        .last()
        .unwrap()
        .split("-")
        .next()
        .unwrap()
        .into();
    let test_name = spec
        .arguments
        .iter()
        .filter(|a| !a.starts_with("-"))
        .next()
        .unwrap()
        .clone();
    TestPath { binary, test_name }
}

struct FakeBroker {
    #[allow(dead_code)]
    listener: TcpListener,
    state: BrokerState,
    address: BrokerAddr,
}

struct FakeBrokerConnection {
    messages: MessageStream,
    state: BrokerState,
}

impl FakeBroker {
    fn new(state: BrokerState) -> Self {
        let listener =
            TcpListener::bind(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)).unwrap();
        let address = BrokerAddr::new(listener.local_addr().unwrap());

        Self {
            listener,
            state,
            address,
        }
    }

    fn accept(&mut self) -> FakeBrokerConnection {
        let (stream, _) = self.listener.accept().unwrap();
        let mut messages = MessageStream { stream };

        let msg: Hello = messages.next().unwrap();
        assert_matches!(msg, Hello::Client);

        FakeBrokerConnection {
            messages,
            state: self.state.clone(),
        }
    }
}

impl FakeBrokerConnection {
    fn process(&mut self, count: usize) {
        for _ in 0..count {
            let msg = self.messages.next::<ClientToBroker>().unwrap();
            match msg {
                ClientToBroker::JobRequest(id, spec) => {
                    let test_path = test_path(&spec);
                    match self.state.job_responses.remove(&test_path).unwrap() {
                        JobAction::Respond(res) => send_message(
                            &self.messages.stream,
                            &BrokerToClient::JobResponse(id, res),
                        ),
                        JobAction::Ignore => (),
                    }
                }
                ClientToBroker::JobStateCountsRequest => send_message(
                    &self.messages.stream,
                    &BrokerToClient::JobStateCountsResponse(self.state.job_states.clone()),
                ),

                _ => (),
            }
        }
    }
}

#[derive(Clone)]
enum JobAction {
    Ignore,
    Respond(JobStringResult),
}

#[derive(Default, Clone)]
struct BrokerState {
    job_responses: HashMap<TestPath, JobAction>,
    job_states: JobStateCounts,
}

#[derive(Clone)]
struct FakeTestCase {
    name: String,
    ignored: bool,
    desired_state: JobState,
}

impl Default for FakeTestCase {
    fn default() -> Self {
        Self {
            name: "".into(),
            ignored: false,
            desired_state: JobState::Complete,
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
}

#[derive(Clone, Hash, PartialOrd, Ord, PartialEq, Eq)]
struct TestPath {
    binary: String,
    test_name: String,
}

impl FakeTests {
    fn all_test_paths(&self) -> impl Iterator<Item = (&FakeTestCase, TestPath)> + '_ {
        self.test_binaries
            .iter()
            .map(|b| {
                b.tests.iter().filter_map(|t| {
                    (!t.ignored).then(|| {
                        (
                            t,
                            TestPath {
                                binary: b.name.clone(),
                                test_name: t.name.clone(),
                            },
                        )
                    })
                })
            })
            .flatten()
    }

    fn get(&self, package_name: &str, case: &str) -> &FakeTestCase {
        let binary = self
            .test_binaries
            .iter()
            .find(|b| b.name == package_name)
            .unwrap();
        binary.tests.iter().find(|t| t.name == case).unwrap()
    }
}

#[derive(Default, Clone)]
struct TestClientDriver {
    deps: Arc<Mutex<Option<ClientDeps>>>,
}

impl ClientDriver for TestClientDriver {
    fn drive(&mut self, deps: ClientDeps) {
        *self.deps.lock().unwrap() = Some(deps);
    }

    fn stop(&mut self) -> Result<()> {
        Ok(())
    }
}

impl TestClientDriver {
    fn process_broker_msg(&self, count: usize) {
        let mut locked_deps = self.deps.lock().unwrap();
        let deps = locked_deps.as_mut().unwrap();

        for _ in 0..count {
            deps.socket_reader.process_one();
            deps.dispatcher.try_process_one().unwrap();
        }
    }

    fn process_client_messages(&self) {
        let mut locked_deps = self.deps.lock().unwrap();
        let deps = locked_deps.as_mut().unwrap();
        while deps.dispatcher.try_process_one().is_ok() {}
    }
}

#[derive(Default, Clone)]
struct TestProgressDriver<'scope> {
    update_func: Rc<RefCell<Option<Box<dyn FnMut(JobStateCounts) -> Result<bool> + 'scope>>>>,
}

impl<'scope> ProgressDriver<'scope> for TestProgressDriver<'scope> {
    fn drive<'dep, ProgressIndicatorT>(
        &mut self,
        _client: &'dep Mutex<Client>,
        ind: ProgressIndicatorT,
    ) where
        ProgressIndicatorT: ProgressIndicator,
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

fn run_app(
    term: InMemoryTerm,
    fake_tests: FakeTests,
    workspace_root: &Path,
    state: BrokerState,
    cargo: String,
    stdout_tty: bool,
    quiet: Quiet,
    include_filter: Vec<String>,
    exclude_filter: Vec<String>,
    list: bool,
    finish: bool,
) -> String {
    let cargo_metadata = cargo_metadata::MetadataCommand::new()
        .manifest_path(workspace_root.join("Cargo.toml"))
        .exec()
        .unwrap();

    let mut stderr = vec![];
    let mut b = FakeBroker::new(state);
    let client_driver = TestClientDriver::default();

    let deps = MainAppDeps::new(
        cargo,
        include_filter,
        exclude_filter,
        list,
        &mut stderr,
        false, // stderr_color
        &workspace_root,
        &cargo_metadata.workspace_packages(),
        b.address.clone(),
        client_driver.clone(),
    )
    .unwrap();
    let prog_driver = TestProgressDriver::default();
    let mut app =
        main_app_new(&deps, stdout_tty, quiet, term.clone(), prog_driver.clone()).unwrap();

    let mut b_conn = b.accept();

    loop {
        let res = app.enqueue_one().unwrap();
        let (package_name, case) = match res {
            EnqueueResult::Done => break,
            EnqueueResult::Ignored | EnqueueResult::Listed => continue,
            EnqueueResult::Enqueued { package_name, case } => (package_name, case),
        };
        let test = fake_tests.get(&package_name, &case);

        // process job enqueuing
        client_driver.process_client_messages();
        b_conn.process(1);
        if test.desired_state == JobState::Complete {
            client_driver.process_broker_msg(1);
        }

        let counts = deps
            .client
            .lock()
            .unwrap()
            .get_job_state_counts_async()
            .unwrap();
        client_driver.process_client_messages();

        // process job state request
        b_conn.process(1);
        client_driver.process_broker_msg(1);

        prog_driver.update(counts.try_recv().unwrap()).unwrap();
    }

    app.drain().unwrap();
    client_driver.process_client_messages();

    if finish {
        app.finish().unwrap();
    }

    term.contents()
}

fn run_or_list_all_tests_sync(
    tmp_dir: &TempDir,
    fake_tests: FakeTests,
    quiet: Quiet,
    include_filter: Vec<String>,
    exclude_filter: Vec<String>,
    list: bool,
) -> String {
    let mut state = BrokerState::default();
    for (_, test_path) in fake_tests.all_test_paths() {
        state.job_responses.insert(
            test_path,
            JobAction::Respond(Ok(JobSuccess {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::Inline(Box::new(*b"this output should be ignored")),
            })),
        );
    }

    let workspace = tmp_dir.path().join("workspace");
    if !workspace.exists() {
        generate_cargo_project(tmp_dir, &fake_tests);
    }
    let cargo = workspace.join("cargo").to_str().unwrap().into();

    let term = InMemoryTerm::new(50, 50);
    run_app(
        term.clone(),
        fake_tests,
        &workspace,
        state,
        cargo,
        false, // stdout_tty
        quiet,
        include_filter,
        exclude_filter,
        list,
        true, // finish
    )
}

fn run_all_tests_sync(
    tmp_dir: &TempDir,
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
        false, /* list */
    )
}

fn list_all_tests_sync(
    tmp_dir: &TempDir,
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
        true, /* list */
    )
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
            &tmp_dir,
            fake_tests,
            false.into(),
            vec!["all".into()],
            vec![]
        ),
        "\
        all jobs completed\n\
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
    assert_eq!(
        list_all_tests_sync(
            &tmp_dir,
            fake_tests,
            false.into(),
            vec!["all".into()],
            vec![]
        ),
        ""
    );
}

#[test]
fn two_tests_all_tests_sync() {
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
            &tmp_dir,
            fake_tests,
            false.into(),
            vec!["all".into()],
            vec![]
        ),
        "\
        bar test_it.....................................OK\n\
        foo test_it.....................................OK\n\
        all jobs completed\n\
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
        list_all_tests_sync(
            &tmp_dir,
            fake_tests,
            false.into(),
            vec!["all".into()],
            vec![]
        ),
        "\
        bar test_it\n\
        foo test_it\
        "
    );
}

#[test]
fn four_tests_filtered_sync() {
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
        ],
    };
    assert_eq!(
        run_all_tests_sync(
            &tmp_dir,
            fake_tests,
            false.into(),
            vec![
                "name.equals(test_it)".into(),
                "name.equals(test_it2)".into()
            ],
            vec!["package.equals(bin)".into()]
        ),
        "\
        bar test_it2....................................OK\n\
        foo test_it.....................................OK\n\
        all jobs completed\n\
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
                name: "foo".into(),
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
        ],
    };
    assert_eq!(
        list_all_tests_sync(
            &tmp_dir,
            fake_tests,
            false.into(),
            vec![
                "name.equals(test_it)".into(),
                "name.equals(test_it2)".into()
            ],
            vec!["package.equals(bin)".into()]
        ),
        "\
        bar test_it2\n\
        foo test_it\
        "
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
            &tmp_dir,
            fake_tests,
            false.into(),
            vec!["package.equals(foo)".into()],
            vec![]
        ),
        "\
        foo test_it.....................................OK\n\
        all jobs completed\n\
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
            &tmp_dir,
            fake_tests,
            false.into(),
            vec!["package.equals(foo) && name.equals(test_it)".into()],
            vec![]
        ),
        "\
        foo test_it.....................................OK\n\
        all jobs completed\n\
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
                name: "foo".into(),
                tests: vec![FakeTestCase {
                    name: "test_it".into(),
                    ignored: true,
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
            &tmp_dir,
            fake_tests,
            false.into(),
            vec!["all".into()],
            vec![]
        ),
        "\
        bar test_it.....................................OK\n\
        baz test_it.....................................OK\n\
        foo test_it................................IGNORED\n\
        all jobs completed\n\
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
            &tmp_dir,
            fake_tests,
            true.into(),
            vec!["all".into()],
            vec![]
        ),
        "\
        all jobs completed\n\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         2\n\
        Failed Tests    :         0\
        "
    );
}

fn run_failed_tests(fake_tests: FakeTests) -> String {
    let tmp_dir = tempdir().unwrap();

    let mut state = BrokerState::default();
    for (_, test_path) in fake_tests.all_test_paths() {
        state.job_responses.insert(
            test_path,
            JobAction::Respond(Ok(JobSuccess {
                status: JobStatus::Exited(1),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::Inline(Box::new(*b"error output")),
            })),
        );
    }

    let cargo = generate_cargo_project(&tmp_dir, &fake_tests);
    let term = InMemoryTerm::new(50, 50);
    run_app(
        term.clone(),
        fake_tests,
        &tmp_dir.path().join("workspace"),
        state,
        cargo,
        false, // stdout_tty
        Quiet::from(false),
        vec!["all".into()],
        vec![],
        false, // list
        true,  // finish
    );

    term.contents()
}

#[test]
fn failed_tests() {
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
        run_failed_tests(fake_tests),
        "\
        bar test_it...................................FAIL\n\
        stderr: error output\n\
        foo test_it...................................FAIL\n\
        stderr: error output\n\
        all jobs completed\n\
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

    let mut state = BrokerState::default();
    for (test, test_path) in fake_tests.all_test_paths() {
        if test.desired_state == JobState::Complete {
            state.job_responses.insert(
                test_path,
                JobAction::Respond(Ok(JobSuccess {
                    status: JobStatus::Exited(0),
                    stdout: JobOutputResult::None,
                    stderr: JobOutputResult::None,
                })),
            );
        } else {
            state.job_responses.insert(test_path, JobAction::Ignore);
        }
        state.job_states[test.desired_state] += 1;
    }

    let cargo = generate_cargo_project(&tmp_dir, &fake_tests);
    let term = InMemoryTerm::new(50, 50);
    let term_clone = term.clone();
    let contents = run_app(
        term_clone,
        fake_tests,
        &tmp_dir.path().join("workspace"),
        state,
        cargo,
        true, // stdout_tty
        quiet,
        vec!["all".into()],
        vec![],
        false, // list
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
        foo test_it.....................................OK\n\
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
        &tmp_dir,
        fake_tests.clone(),
        false.into(),
        vec!["all".into()],
        vec![],
    );

    let path = tmp_dir
        .path()
        .join("workspace/target")
        .join(LAST_TEST_LISTING_NAME);
    let listing: TestListing = load_test_listing(&path).unwrap().unwrap();
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
    let fs = Fs::new();
    fs.remove_dir_all(tmp_dir.path().join("workspace/crates/bar"))
        .unwrap();

    run_all_tests_sync(
        &tmp_dir,
        fake_tests.clone(),
        false.into(),
        vec!["all".into()],
        vec![],
    );

    // new listing should match
    let listing: TestListing = load_test_listing(&path).unwrap().unwrap();
    assert_eq!(listing, fake_tests.listing());
}

#[test]
fn expected_count_updates_cases() {
    let tmp_dir = tempdir().unwrap();
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

    let path = tmp_dir
        .path()
        .join("workspace/target")
        .join(LAST_TEST_LISTING_NAME);
    let listing: TestListing = load_test_listing(&path).unwrap().unwrap();
    assert_eq!(listing, fake_tests.listing());

    // remove the test
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            tests: vec![],
        }],
    };
    let fs = Fs::new();
    fs.write(tmp_dir.path().join("workspace/crates/foo/src/lib.rs"), "")
        .unwrap();

    run_all_tests_sync(
        &tmp_dir,
        fake_tests.clone(),
        false.into(),
        vec!["all".into()],
        vec![],
    );

    // new listing should match
    let listing: TestListing = load_test_listing(&path).unwrap().unwrap();
    assert_eq!(listing, fake_tests.listing());
}
