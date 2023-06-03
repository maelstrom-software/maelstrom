//! Code for the client binary.

use crate::{proto, ClientExecutionId, ExecutionDetails, Result};
use std::collections::BTreeMap;

async fn get_test_binaries() -> Result<Vec<String>> {
    let output = tokio::process::Command::new("cargo")
        .arg("test")
        .arg("--no-run")
        .output()
        .await?;
    Ok(regex::Regex::new(r"Executable unittests.*\((.*)\)")?
        .captures_iter(std::str::from_utf8(&output.stderr)?)
        .map(|capture| capture.get(1).unwrap().as_str().to_string())
        .collect())
}

async fn get_cases_from_binary(binary: &str) -> Result<Vec<String>> {
    let output = tokio::process::Command::new(binary)
        .arg("--list")
        .arg("--format")
        .arg("terse")
        .output()
        .await?;
    Ok(regex::Regex::new(r"\b([^ ]*): test")?
        .captures_iter(std::str::from_utf8(&output.stdout)?)
        .map(|capture| capture.get(1).unwrap().as_str().to_string())
        .collect())
}

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub async fn main(name: String, broker_addr: std::net::SocketAddr) -> Result<()> {
    let mut pairs = vec![];
    for binary in get_test_binaries().await?.into_iter() {
        for case in get_cases_from_binary(&binary).await?.into_iter() {
            pairs.push((binary.clone(), case))
        }
    }
    let (read_stream, mut write_stream) = tokio::net::TcpStream::connect(&broker_addr)
        .await?
        .into_split();
    let mut read_stream = tokio::io::BufReader::new(read_stream);

    proto::write_message(&mut write_stream, proto::Hello::Client { name }).await?;
    let mut map = BTreeMap::new();
    for (id, (binary, case)) in pairs.into_iter().enumerate() {
        let id = ClientExecutionId(id as u32);
        map.insert(id, case.clone());
        proto::write_message(
            &mut write_stream,
            proto::ClientRequest(
                id,
                ExecutionDetails {
                    program: binary,
                    arguments: vec!["--exact".to_string(), case],
                },
            ),
        )
        .await?;
    }

    while !map.is_empty() {
        let proto::ClientResponse(id, result) = proto::read_message(&mut read_stream).await?;
        let case = map.remove(&id).unwrap();
        println!("{}: {:?}", case, result);
    }

    Ok(())
}
