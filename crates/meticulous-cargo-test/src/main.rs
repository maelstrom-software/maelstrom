use anyhow::Result;
use clap::Parser;
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker, Hello},
    JobDetails,
};
use meticulous_util::net;
use regex::Regex;
use std::{
    collections::HashMap,
    io::{self, BufReader},
    net::{SocketAddr, TcpStream, ToSocketAddrs as _},
    process::Command,
    str,
};

fn parse_socket_addr(arg: &str) -> io::Result<SocketAddr> {
    let addrs: Vec<SocketAddr> = arg.to_socket_addrs()?.collect();
    // It's not clear how we could end up with an empty iterator. We'll assume
    // that's impossible until proven wrong.
    Ok(*addrs.get(0).unwrap())
}

/// The meticulous client. This process sends work to the broker to be executed by workers.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Socket address of broker. Examples: 127.0.0.1:5000 host.example.com:2000".
    #[arg(value_parser = parse_socket_addr)]
    broker: SocketAddr,
}

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
pub fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut pairs = vec![];
    for binary in get_test_binaries()? {
        for case in get_cases_from_binary(&binary)? {
            pairs.push((binary.clone(), case))
        }
    }
    let mut write_stream = TcpStream::connect(cli.broker)?;
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

#[test]
fn test_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
