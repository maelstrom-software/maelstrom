use crate::MainApp;
use assert_matches::assert_matches;
use enum_map::enum_map;
use indicatif::InMemoryTerm;
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker, Hello},
    stats::{JobState, JobStateCounts},
    JobDetails, JobOutputResult, JobResult, JobStatus,
};
use meticulous_client::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read as _, Write as _};
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;
use tempfile::{tempdir, TempDir};

fn put_file(path: &Path, contents: &str) {
    let mut f = File::create(path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
}

fn put_script(path: &Path, contents: &str) {
    let mut f = File::create(path).unwrap();
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

// XXX remi: This is a total hack to get around isolation issues
const PATH: &'static str = env!("PATH");
const HOME: &'static str = env!("HOME");

fn generate_cargo_project(tmp_dir: &TempDir, fake_tests: &FakeTests) -> String {
    let workspace_dir = tmp_dir.path().join("workspace");
    std::fs::create_dir(&workspace_dir).unwrap();
    let cargo_path = workspace_dir.join("cargo");
    put_script(
        &cargo_path,
        &format!(
            "\
            cd {workspace_dir:?}\n\
            export HOME={HOME}
            export PATH={PATH}
            cargo $@ | sort\n\
            "
        ),
    );
    put_file(
        &workspace_dir.join("Cargo.toml"),
        "\
        [workspace]\n\
        members = [ \"crates/*\"]
        ",
    );
    let crates_dir = workspace_dir.join("crates");
    std::fs::create_dir(&crates_dir).unwrap();
    for binary in &fake_tests.test_binaries {
        let crate_name = &binary.name;
        let project_dir = crates_dir.join(&crate_name);
        std::fs::create_dir(&project_dir).unwrap();
        put_file(
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
        std::fs::create_dir(&src_dir).unwrap();
        let mut test_src = String::new();
        for test_name in &binary.tests {
            test_src += &format!(
                "\
                #[test]\n\
                fn {test_name}() {{}}\n\
                ",
            );
        }
        put_file(&src_dir.join("lib.rs"), &test_src);
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

fn test_path(details: &JobDetails) -> TestPath {
    let binary = details
        .program
        .split("/")
        .last()
        .unwrap()
        .split("-")
        .next()
        .unwrap()
        .into();
    let test_name = details
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
            ClientToBroker::JobRequest(id, details) => {
                let test_path = test_path(&details);
                if let Some(res) = state.job_responses.remove(&test_path) {
                    send_message(&stream, &BrokerToClient::JobResponse(id, res))
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

#[derive(Default)]
struct BrokerState {
    job_responses: HashMap<TestPath, JobResult>,
    job_states: JobStateCounts,
}

fn fake_broker(state: BrokerState) -> SocketAddr {
    let broker_listener =
        TcpListener::bind(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)).unwrap();
    let broker_address = broker_listener.local_addr().unwrap();
    std::thread::spawn(move || fake_broker_main(broker_listener, state));
    broker_address
}

struct FakeTestBinary {
    name: String,
    tests: Vec<String>,
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
                b.tests.iter().map(|t| TestPath {
                    binary: b.name.clone(),
                    test_name: t.into(),
                })
            })
            .flatten()
    }
}

fn run_app(term: InMemoryTerm, state: BrokerState, cargo: String, stdout_tty: bool, quiet: bool) {
    let broker_address = fake_broker(state);
    let client = Mutex::new(Client::new(broker_address).unwrap());

    let mut stderr = vec![];
    let app = MainApp::new(client, cargo, None, None, &mut stderr, false);
    app.run(stdout_tty, quiet, term.clone())
        .unwrap_or_else(|e| {
            panic!(
                "err = {e:?} stderr = {}",
                String::from_utf8_lossy(&stderr[..])
            )
        });
}

fn run_complete_test(fake_tests: FakeTests, quiet: bool) -> String {
    let tmp_dir = tempdir().unwrap();

    let mut state = BrokerState::default();
    for test in fake_tests.all_tests() {
        state.job_responses.insert(
            test.clone(),
            JobResult::Ran {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::Inline(Box::new(*b"this output should be ignored")),
            },
        );
    }

    let cargo = generate_cargo_project(&tmp_dir, &fake_tests);
    let term = InMemoryTerm::new(50, 50);
    run_app(term.clone(), state, cargo, false, quiet);

    term.contents()
}

#[test]
fn no_tests_complete() {
    let fake_tests = FakeTests {
        test_binaries: vec![FakeTestBinary {
            name: "foo".into(),
            tests: vec![],
        }],
    };
    assert_eq!(
        run_complete_test(fake_tests, false),
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
fn two_tests_complete() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec!["test_it".into()],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec!["test_it".into()],
            },
        ],
    };
    assert_eq!(
        run_complete_test(fake_tests, false),
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
fn two_tests_complete_quiet() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec!["test_it".into()],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec!["test_it".into()],
            },
        ],
    };
    assert_eq!(
        run_complete_test(fake_tests, true),
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
            JobResult::Ran {
                status: JobStatus::Exited(1),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::Inline(Box::new(*b"error output")),
            },
        );
    }

    let cargo = generate_cargo_project(&tmp_dir, &fake_tests);
    let term = InMemoryTerm::new(50, 50);
    run_app(term.clone(), state, cargo, false, false);

    term.contents()
}

#[test]
fn failed_tests() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec!["test_it".into()],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec!["test_it".into()],
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

fn run_in_progress_test(fake_tests: FakeTests, job_states: JobStateCounts, quiet: bool) -> String {
    let tmp_dir = tempdir().unwrap();

    let mut state = BrokerState::default();
    for test in fake_tests
        .all_tests()
        .take(job_states[JobState::Complete] as usize)
    {
        state.job_responses.insert(
            test.clone(),
            JobResult::Ran {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            },
        );
    }
    state.job_states = job_states;

    let cargo = generate_cargo_project(&tmp_dir, &fake_tests);
    let term = InMemoryTerm::new(50, 50);
    let term_clone = term.clone();
    std::thread::spawn(move || run_app(term_clone, state, cargo, true, quiet));

    // wait until contents settles
    let mut last_contents = String::new();
    loop {
        std::thread::sleep(Duration::from_millis(550));
        let new_contents = term.contents();
        if new_contents == last_contents {
            return new_contents;
        }
        last_contents = new_contents;
    }
}

#[test]
fn waiting_for_artifacts() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec!["test_it".into()],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec!["test_it".into()],
            },
        ],
    };
    let state = enum_map! {
        JobState::WaitingForArtifacts => 2,
        JobState::Pending => 0,
        JobState::Running => 0,
        JobState::Complete => 0,
    };
    assert_eq!(
        run_in_progress_test(fake_tests, state, false),
        "\
        ######################## 2/2 waiting for artifacts\n\
        ------------------------ 0/2 pending\n\
        ------------------------ 0/2 running\n\
        ------------------------ 0/2 complete\
        "
    );
}

#[test]
fn pending() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec!["test_it".into()],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec!["test_it".into()],
            },
        ],
    };
    let state = enum_map! {
        JobState::WaitingForArtifacts => 0,
        JobState::Pending => 2,
        JobState::Running => 0,
        JobState::Complete => 0,
    };
    assert_eq!(
        run_in_progress_test(fake_tests, state, false),
        "\
        ######################## 2/2 waiting for artifacts\n\
        ######################## 2/2 pending\n\
        ------------------------ 0/2 running\n\
        ------------------------ 0/2 complete\
        "
    );
}

#[test]
fn running() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec!["test_it".into()],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec!["test_it".into()],
            },
        ],
    };
    let state = enum_map! {
        JobState::WaitingForArtifacts => 0,
        JobState::Pending => 0,
        JobState::Running => 2,
        JobState::Complete => 0,
    };
    assert_eq!(
        run_in_progress_test(fake_tests, state, false),
        "\
        ######################## 2/2 waiting for artifacts\n\
        ######################## 2/2 pending\n\
        ######################## 2/2 running\n\
        ------------------------ 0/2 complete\
        "
    );
}

#[test]
fn complete() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec!["test_it".into()],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec!["test_it".into()],
            },
        ],
    };
    let state = enum_map! {
        JobState::WaitingForArtifacts => 0,
        JobState::Pending => 0,
        JobState::Running => 1,
        JobState::Complete => 1,
    };
    assert_eq!(
        run_in_progress_test(fake_tests, state, false),
        "\
        foo test_it.....................................OK\n\
        ######################## 2/2 waiting for artifacts\n\
        ######################## 2/2 pending\n\
        ######################## 2/2 running\n\
        #############----------- 1/2 complete\
        "
    );
}

#[test]
fn complete_quiet() {
    let fake_tests = FakeTests {
        test_binaries: vec![
            FakeTestBinary {
                name: "foo".into(),
                tests: vec!["test_it".into()],
            },
            FakeTestBinary {
                name: "bar".into(),
                tests: vec!["test_it".into()],
            },
        ],
    };
    let state = enum_map! {
        JobState::WaitingForArtifacts => 0,
        JobState::Pending => 0,
        JobState::Running => 1,
        JobState::Complete => 1,
    };
    assert_eq!(
        run_in_progress_test(fake_tests, state, true),
        "#####################-------------------- 1/2 jobs"
    );
}
