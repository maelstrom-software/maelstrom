use assert_matches::assert_matches;
use cargo_metest::{config::Quiet, MainApp};
use enum_map::enum_map;
use indicatif::InMemoryTerm;
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker, Hello},
    stats::{JobState, JobStateCounts},
    JobOutputResult, JobResult, JobSpec, JobStatus, JobSuccess,
};
use meticulous_util::{config::BrokerAddr, fs::Fs};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::io::{self, Read as _, Write as _};
use std::net::{Ipv6Addr, SocketAddrV6, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::time::Duration;
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

struct MessageStream<'a> {
    stream: &'a TcpStream,
}

impl<'a> MessageStream<'a> {
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

fn fake_broker_main(listener: TcpListener, mut state: BrokerState) {
    let (stream, _) = listener.accept().unwrap();
    let mut messages = MessageStream { stream: &stream };

    let msg: Hello = messages.next().unwrap();
    assert_matches!(msg, Hello::Client);

    while let Ok(msg) = messages.next::<ClientToBroker>() {
        match msg {
            ClientToBroker::JobRequest(id, spec) => {
                let test_path = test_path(&spec);
                match state.job_responses.remove(&test_path).unwrap() {
                    JobAction::Respond(res) => {
                        send_message(&stream, &BrokerToClient::JobResponse(id, res))
                    }
                    JobAction::Ignore => (),
                }
            }
            ClientToBroker::JobStateCountsRequest => send_message(
                &stream,
                &BrokerToClient::JobStateCountsResponse(state.job_states.clone()),
            ),

            _ => continue,
        }
    }
}

enum JobAction {
    Ignore,
    Respond(JobResult),
}

#[derive(Default)]
struct BrokerState {
    job_responses: HashMap<TestPath, JobAction>,
    job_states: JobStateCounts,
}

fn fake_broker(state: BrokerState) -> BrokerAddr {
    let broker_listener =
        TcpListener::bind(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)).unwrap();
    let broker_address = BrokerAddr::new(broker_listener.local_addr().unwrap());
    std::thread::spawn(move || fake_broker_main(broker_listener, state));
    broker_address
}

#[derive(Default)]
struct FakeTestCase {
    name: String,
    ignored: bool,
}

#[derive(Default)]
struct FakeTestBinary {
    name: String,
    tests: Vec<FakeTestCase>,
}

struct FakeTests {
    test_binaries: Vec<FakeTestBinary>,
}

#[derive(Clone, Hash, PartialOrd, Ord, PartialEq, Eq)]
struct TestPath {
    binary: String,
    test_name: String,
}

impl FakeTests {
    fn all_tests(&self) -> impl Iterator<Item = TestPath> + '_ {
        self.test_binaries
            .iter()
            .map(|b| {
                b.tests.iter().filter_map(|t| {
                    (!t.ignored).then(|| TestPath {
                        binary: b.name.clone(),
                        test_name: t.name.clone(),
                    })
                })
            })
            .flatten()
    }
}

fn run_app(
    term: InMemoryTerm,
    state: BrokerState,
    cargo: String,
    stdout_tty: bool,
    quiet: Quiet,
    package: Option<String>,
    filter: Option<String>,
) {
    let tmp_dir = tempdir().unwrap();

    let mut stderr = vec![];
    let app = MainApp::new(
        cargo,
        package,
        filter,
        &mut stderr,
        false,
        &tmp_dir,
        fake_broker(state),
    )
    .unwrap();
    std::thread::scope(|scope| app.run(stdout_tty, quiet, term.clone(), scope)).unwrap_or_else(
        |e| {
            panic!(
                "err = {e:?} stderr = {}",
                String::from_utf8_lossy(&stderr[..])
            )
        },
    );
}

fn run_all_tests_sync(
    fake_tests: FakeTests,
    quiet: Quiet,
    package: Option<String>,
    filter: Option<String>,
) -> String {
    let tmp_dir = tempdir().unwrap();

    let mut state = BrokerState::default();
    for test in fake_tests.all_tests() {
        state.job_responses.insert(
            test.clone(),
            JobAction::Respond(Ok(JobSuccess {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::Inline(Box::new(*b"this output should be ignored")),
            })),
        );
    }

    let cargo = generate_cargo_project(&tmp_dir, &fake_tests);
    let term = InMemoryTerm::new(50, 50);
    run_app(term.clone(), state, cargo, false, quiet, package, filter);

    term.contents()
}

#[test]
fn no_tests_all_tests_sync() {
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            tests: vec![],
        }],
    };
    assert_eq!(
        run_all_tests_sync(fake_tests, false.into(), None, None),
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
fn two_tests_all_tests_sync() {
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
        run_all_tests_sync(fake_tests, false.into(), None, None),
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
fn three_tests_filtered_sync() {
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
                    name: "testy".into(),
                    ..Default::default()
                }],
            },
        ],
    };
    assert_eq!(
        run_all_tests_sync(fake_tests, false.into(), None, Some("test_it".into())),
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
fn three_tests_single_package_sync() {
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
        run_all_tests_sync(fake_tests, false.into(), Some("foo".into()), None),
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
            fake_tests,
            false.into(),
            Some("foo".into()),
            Some("test_it".into())
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
        run_all_tests_sync(fake_tests, false.into(), None, None),
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
        run_all_tests_sync(fake_tests, true.into(), None, None),
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
    for test in fake_tests.all_tests() {
        state.job_responses.insert(
            test.clone(),
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
        state,
        cargo,
        false,
        Quiet::from(false),
        None,
        None,
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

fn run_in_progress_test(
    fake_tests: FakeTests,
    job_states: JobStateCounts,
    quiet: Quiet,
    expected_output: &str,
) {
    let tmp_dir = tempdir().unwrap();

    let mut state = BrokerState::default();
    for test in fake_tests
        .all_tests()
        .take(job_states[JobState::Complete] as usize)
    {
        state.job_responses.insert(
            test.clone(),
            JobAction::Respond(Ok(JobSuccess {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            })),
        );
    }
    for test in fake_tests
        .all_tests()
        .skip(job_states[JobState::Complete] as usize)
    {
        state.job_responses.insert(test.clone(), JobAction::Ignore);
    }
    state.job_states = job_states;

    let cargo = generate_cargo_project(&tmp_dir, &fake_tests);
    let term = InMemoryTerm::new(50, 50);
    let term_clone = term.clone();
    std::thread::spawn(move || run_app(term_clone, state, cargo, true, quiet, None, None));

    // wait until we get the expected contents
    while term.contents() != expected_output {
        std::thread::sleep(Duration::from_millis(550));
        println!("waiting, current terminal output:\n{}", term.contents());
    }
}

#[test]
fn waiting_for_artifacts() {
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
    let state = enum_map! {
        JobState::WaitingForArtifacts => 2,
        JobState::Pending => 0,
        JobState::Running => 0,
        JobState::Complete => 0,
    };
    run_in_progress_test(
        fake_tests,
        state,
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
    let state = enum_map! {
        JobState::WaitingForArtifacts => 0,
        JobState::Pending => 2,
        JobState::Running => 0,
        JobState::Complete => 0,
    };
    run_in_progress_test(
        fake_tests,
        state,
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
    let state = enum_map! {
        JobState::WaitingForArtifacts => 0,
        JobState::Pending => 0,
        JobState::Running => 2,
        JobState::Complete => 0,
    };
    run_in_progress_test(
        fake_tests,
        state,
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
    let state = enum_map! {
        JobState::WaitingForArtifacts => 0,
        JobState::Pending => 0,
        JobState::Running => 1,
        JobState::Complete => 1,
    };
    run_in_progress_test(
        fake_tests,
        state,
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
    let state = enum_map! {
        JobState::WaitingForArtifacts => 0,
        JobState::Pending => 0,
        JobState::Running => 1,
        JobState::Complete => 1,
    };
    run_in_progress_test(
        fake_tests,
        state,
        true.into(),
        "#####################-------------------- 1/2 jobs",
    );
}
