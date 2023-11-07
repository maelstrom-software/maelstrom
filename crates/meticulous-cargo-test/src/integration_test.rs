use crate::MainApp;
use assert_matches::assert_matches;
use enum_map::enum_map;
use indicatif::InMemoryTerm;
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker, Hello},
    stats::{JobState, JobStateCounts},
    JobOutputResult, JobResult, JobStatus,
};
use meticulous_client::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::File;
use std::io::{self, Read as _, Write as _};
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;
use tempfile::{tempdir, TempDir};

fn put_script(path: &Path, contents: &str) {
    let mut f = File::create(path).unwrap();
    f.write_all(
        format!(
            "#!/bin/bash
            set -e
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

fn generate_fake_cargo(tmp_dir: &TempDir, fake_tests: &FakeTests) -> String {
    let cargo_path = tmp_dir.path().join("cargo");

    let mut json = String::new();
    for name in &fake_tests.tests {
        let executable_path = tmp_dir.path().join(name);
        let test = serde_json::json!({
            "reason": "compiler-artifact",
            "package_id": format!("{name} 0.1.0 (path+file:///{name})"),
            "manifest_path": format!("/{name}/Cargo.toml"),
            "target": {
                "name": &name,
                "kind": ["lib"],
                "crate_types": ["lib"],
                "required_features": [],
                "src_path": format!("/{name}/src/lib.rs"),
                "edition": "2021",
                "doctest": true,
                "test": true,
                "doc": true,
            },
            "profile": {
                "opt_level": "0",
                "debuginfo": 2,
                "debug_assertions": false,
                "overflow_checks": true,
                "test": true,
            },
            "features": [],
            "filenames": [&executable_path],
            "executable": &executable_path,
            "fresh": true,
        });

        json += &serde_json::to_string(&test).unwrap();
        json += "\n";

        put_script(&executable_path, &format!("echo 'test_it: test'"));
    }
    put_script(&cargo_path, &format!("echo '{json}'"));

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

fn fake_broker_main(listener: TcpListener, mut state: BrokerState) {
    let (stream, _) = listener.accept().unwrap();
    let mut messages = MessageStream { stream: &stream };

    let msg: Hello = messages.next().unwrap();
    assert_matches!(msg, Hello::Client);

    while let Ok(msg) = messages.next::<ClientToBroker>() {
        match msg {
            ClientToBroker::JobRequest(id, _details) => {
                if let Some(res) = state.job_responses.pop() {
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
    job_responses: Vec<JobResult>,
    job_states: JobStateCounts,
}

fn fake_broker(state: BrokerState) -> SocketAddr {
    let broker_listener =
        TcpListener::bind(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)).unwrap();
    let broker_address = broker_listener.local_addr().unwrap();
    std::thread::spawn(move || fake_broker_main(broker_listener, state));
    broker_address
}

struct FakeTests {
    tests: Vec<String>,
}

fn run_complete_test(fake_tests: FakeTests, quiet: bool) -> String {
    let tmp_dir = tempdir().unwrap();

    let mut state = BrokerState::default();
    for _ in 0..100 {
        state.job_responses.push(JobResult::Ran {
            status: JobStatus::Exited(0),
            stdout: JobOutputResult::None,
            stderr: JobOutputResult::Inline(Box::new(*b"this output should be ignored")),
        })
    }
    let broker_address = fake_broker(state);
    let client = Mutex::new(Client::new(broker_address).unwrap());

    let cargo = generate_fake_cargo(&tmp_dir, &fake_tests);
    let app = MainApp::new(client, cargo, None, std::io::sink(), false);
    let term = InMemoryTerm::new(50, 50);
    app.run(false, quiet, term.clone()).unwrap();

    term.contents()
}

#[test]
fn no_tests_complete() {
    let fake_tests = FakeTests { tests: vec![] };
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
        tests: vec!["foo".into(), "bar".into()],
    };
    assert_eq!(
        run_complete_test(fake_tests, false),
        "\
        foo test_it.....................................OK\n\
        bar test_it.....................................OK\n\
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
        tests: vec!["foo".into(), "bar".into()],
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
    for _ in 0..100 {
        state.job_responses.push(JobResult::Ran {
            status: JobStatus::Exited(1),
            stdout: JobOutputResult::None,
            stderr: JobOutputResult::Inline(Box::new(*b"error output")),
        })
    }
    let broker_address = fake_broker(state);
    let client = Mutex::new(Client::new(broker_address).unwrap());

    let cargo = generate_fake_cargo(&tmp_dir, &fake_tests);
    let app = MainApp::new(client, cargo, None, std::io::sink(), false);
    let term = InMemoryTerm::new(50, 50);
    app.run(false, false, term.clone()).unwrap();

    term.contents()
}

#[test]
fn failed_tests() {
    let fake_tests = FakeTests {
        tests: vec!["foo".into(), "bar".into()],
    };
    assert_eq!(
        run_failed_tests(fake_tests),
        "\
        foo test_it...................................FAIL\n\
        stderr: error output\n\
        bar test_it...................................FAIL\n\
        stderr: error output\n\
        all jobs completed\n\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         0\n\
        Failed Tests    :         2\n\
        \x20\x20\x20\x20foo test_it: failure\n\
        \x20\x20\x20\x20bar test_it: failure\
        "
    );
}

fn run_in_progress_test(fake_tests: FakeTests, job_states: JobStateCounts, quiet: bool) -> String {
    let tmp_dir = tempdir().unwrap();

    let mut state = BrokerState::default();
    for _ in 0..job_states[JobState::Complete] {
        state.job_responses.push(JobResult::Ran {
            status: JobStatus::Exited(0),
            stdout: JobOutputResult::None,
            stderr: JobOutputResult::None,
        })
    }
    state.job_states = job_states;
    let broker_address = fake_broker(state);
    let client = Mutex::new(Client::new(broker_address).unwrap());

    let cargo = generate_fake_cargo(&tmp_dir, &fake_tests);
    let app = MainApp::new(client, cargo, None, std::io::sink(), false);
    let term = InMemoryTerm::new(50, 50);
    let term_clone = term.clone();
    std::thread::spawn(move || app.run(true, quiet, term_clone).unwrap());

    // wait until contents settles
    let mut last_contents = String::new();
    loop {
        std::thread::sleep(Duration::from_millis(100));
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
        tests: vec!["foo".into(), "bar".into()],
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
        tests: vec!["foo".into(), "bar".into()],
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
        tests: vec!["foo".into(), "bar".into()],
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
        tests: vec!["foo".into(), "bar".into()],
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
        tests: vec!["foo".into(), "bar".into()],
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
