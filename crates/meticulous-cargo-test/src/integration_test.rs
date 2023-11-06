use crate::MainApp;
use assert_matches::assert_matches;
use indicatif::InMemoryTerm;
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker, Hello},
    JobOutputResult, JobResult, JobStatus,
};
use meticulous_client::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::File;
use std::io::{self, Read as _, Write as _};
use std::net::{Ipv6Addr, SocketAddrV6, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt as _;
use std::sync::Mutex;
use tempfile::{tempdir, TempDir};

fn generate_fake_cargo(tmp_dir: &TempDir) -> String {
    let file_path = tmp_dir.path().join("cargo");
    let executable_path = tmp_dir.path().join("foo");
    let mut f = File::create(&file_path).unwrap();

    let test = serde_json::json!({
        "reason": "compiler-artifact",
        "package_id": "foo 0.1.0 (path+file:///foo)",
        "manifest_path": "/foo/Cargo.toml",
        "target": {
            "name": "foo",
            "kind": ["lib"],
            "crate_types": ["lib"],
            "required_features": [],
            "src_path": "/foo/src/lib.rs",
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

    let json = serde_json::to_string(&test).unwrap();
    f.write_all(
        format!(
            "#!/bin/bash
            set -e
            echo '{json}'
            "
        )
        .as_bytes(),
    )
    .unwrap();

    let mut perms = f.metadata().unwrap().permissions();
    perms.set_mode(0o777);
    f.set_permissions(perms).unwrap();

    let mut f = File::create(&executable_path).unwrap();
    f.write_all(
        format!(
            "#!/bin/bash
            set -e
            echo 'foo: test'
            "
        )
        .as_bytes(),
    )
    .unwrap();

    let mut perms = f.metadata().unwrap().permissions();
    perms.set_mode(0o777);
    f.set_permissions(perms).unwrap();

    file_path.display().to_string()
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

fn fake_broker(listener: TcpListener) {
    let (stream, _) = listener.accept().unwrap();
    let mut messages = MessageStream { stream: &stream };

    let msg: Hello = messages.next().unwrap();
    assert_matches!(msg, Hello::Client);

    while let Ok(msg) = messages.next::<ClientToBroker>() {
        match msg {
            ClientToBroker::JobRequest(id, _details) => send_message(
                &stream,
                &BrokerToClient::JobResponse(
                    id,
                    JobResult::Ran {
                        status: JobStatus::Exited(0),
                        stdout: JobOutputResult::None,
                        stderr: JobOutputResult::None,
                    },
                ),
            ),

            _ => continue,
        }
    }
}

#[test]
fn one_test() {
    let tmp_dir = tempdir().unwrap();

    let broker_listener =
        TcpListener::bind(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)).unwrap();
    let broker_address = broker_listener.local_addr().unwrap();
    std::thread::spawn(move || fake_broker(broker_listener));
    let client = Mutex::new(Client::new(broker_address).unwrap());
    let cargo = generate_fake_cargo(&tmp_dir);
    let app = MainApp::new(client, cargo, std::io::sink(), false);
    let term = InMemoryTerm::new(50, 50);
    app.run(false, false, term.clone()).unwrap();
    assert_eq!(
        term.contents(),
        "\
        foo foo.........................................OK\n\
        all jobs completed\n\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         1\n\
        Failed Tests    :         0\
        "
    );
}
