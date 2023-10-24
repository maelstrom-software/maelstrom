//! Code for the client binary.

use anyhow::Result;
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker, Hello},
    JobDetails,
};
use meticulous_util::net;
use regex::Regex;
use std::{
    collections::HashMap,
    io::BufReader,
    net::{SocketAddr, TcpStream},
    process::Command,
    str,
};

fn get_test_binaries() -> Result<Vec<String>> {
    let output = Command::new("cargo").arg("test").arg("--no-run").output()?;
    Ok(Regex::new(r"Executable unittests.*\((.*)\)")?
        .captures_iter(str::from_utf8(&output.stderr)?)
        .map(|capture| capture.get(1).unwrap().as_str().to_string())
        .collect())
}

fn get_cases_from_binary(binary: &str) -> Result<Vec<String>> {
    let output = Command::new(binary)
        .arg("--list")
        .arg("--format")
        .arg("terse")
        .output()?;
    Ok(Regex::new(r"\b([^ ]*): test")?
        .captures_iter(str::from_utf8(&output.stdout)?)
        .map(|capture| capture.get(1).unwrap().as_str().to_string())
        .collect())
}

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub fn main(broker_addr: SocketAddr) -> Result<()> {
    let mut pairs = vec![];
    for binary in get_test_binaries()? {
        for case in get_cases_from_binary(&binary)? {
            pairs.push((binary.clone(), case))
        }
    }
    let mut write_stream = TcpStream::connect(broker_addr)?;
    let read_stream = write_stream.try_clone()?;
    let mut read_stream = BufReader::new(read_stream);

    net::write_message_to_socket(&mut write_stream, Hello::Client)?;
    let mut map = HashMap::new();
    for (id, (binary, case)) in pairs.into_iter().enumerate() {
        let id = (id as u32).into();
        map.insert(id, case.clone());
        net::write_message_to_socket(
            &mut write_stream,
            ClientToBroker::JobRequest(
                id,
                JobDetails {
                    program: binary,
                    arguments: vec!["--exact".to_string(), case],
                    layers: vec![],
                },
            ),
        )?;
    }

    while !map.is_empty() {
        match net::read_message_from_socket(&mut read_stream)? {
            BrokerToClient::JobResponse(id, result) => {
                let case = map.remove(&id).unwrap();
                println!("{case}: {result:?}");
            }
            BrokerToClient::TransferArtifact(_) => {
                todo!();
            }
            BrokerToClient::StatisticsResponse(_) => {
                panic!("Got a StatisticsResponse even though we don't send requests");
            }
        }
    }

    Ok(())
}
