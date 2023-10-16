//! Code for the client binary.

use anyhow::Result;
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker, Hello},
    ClientJobId, JobDetails,
};
use meticulous_util::net;
use regex::Regex;
use std::collections::HashMap;
use std::{net::SocketAddr, str};
use tokio::{io::BufReader, net::TcpStream, process::Command};

async fn get_test_binaries() -> Result<Vec<String>> {
    let output = Command::new("cargo")
        .arg("test")
        .arg("--no-run")
        .output()
        .await?;
    Ok(Regex::new(r"Executable unittests.*\((.*)\)")?
        .captures_iter(str::from_utf8(&output.stderr)?)
        .map(|capture| capture.get(1).unwrap().as_str().to_string())
        .collect())
}

async fn get_cases_from_binary(binary: &str) -> Result<Vec<String>> {
    let output = Command::new(binary)
        .arg("--list")
        .arg("--format")
        .arg("terse")
        .output()
        .await?;
    Ok(Regex::new(r"\b([^ ]*): test")?
        .captures_iter(str::from_utf8(&output.stdout)?)
        .map(|capture| capture.get(1).unwrap().as_str().to_string())
        .collect())
}

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub async fn main(_name: String, broker_addr: SocketAddr) -> Result<()> {
    let mut pairs = vec![];
    for binary in get_test_binaries().await? {
        for case in get_cases_from_binary(&binary).await? {
            pairs.push((binary.clone(), case))
        }
    }
    let (read_stream, mut write_stream) = TcpStream::connect(&broker_addr).await?.into_split();
    let mut read_stream = BufReader::new(read_stream);

    net::write_message_to_async_socket(&mut write_stream, Hello::Client).await?;
    let mut map = HashMap::new();
    for (id, (binary, case)) in pairs.into_iter().enumerate() {
        let id = ClientJobId(id as u32);
        map.insert(id, case.clone());
        net::write_message_to_async_socket(
            &mut write_stream,
            ClientToBroker::JobRequest(
                id,
                JobDetails {
                    program: binary,
                    arguments: vec!["--exact".to_string(), case],
                    layers: vec![],
                },
            ),
        )
        .await?;
    }

    while !map.is_empty() {
        match net::read_message_from_async_socket(&mut read_stream).await? {
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
