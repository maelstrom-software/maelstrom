use anyhow::Result;
use clap::Parser;
use meticulous_base::JobDetails;
use meticulous_client::Client;
use serde::Deserialize;
use serde_json::{self, Deserializer};
use std::{
    fs::File,
    io::{self, Read},
    net::{SocketAddr, ToSocketAddrs as _},
    path::{Path, PathBuf},
    process::ExitCode,
};

/// The meticulous client. This process sends jobs to the broker to be executed.
#[derive(Parser)]
#[command(version)]
struct CliOptions {
    /// Socket address of broker. Examples: 127.0.0.1:5000 host.example.com:2000"
    #[arg(short = 'b', long)]
    broker: String,

    /// File to read jobs from instead of stdin.
    #[arg(short = 'f', long)]
    file: Option<PathBuf>,
}

fn parse_socket_addr(value: String) -> Result<SocketAddr> {
    let addrs: Vec<SocketAddr> = value.to_socket_addrs()?.collect();
    // It's not clear how we could end up with an empty iterator. We'll assume that's
    // impossible until proven wrong.
    Ok(*addrs.get(0).unwrap())
}

#[derive(Deserialize, Debug)]
struct JobDescription {
    program: String,
    arguments: Option<Vec<String>>,
    layers: Option<Vec<String>>,
}

fn main() -> Result<ExitCode> {
    let cli_options = CliOptions::parse();
    let mut client = Client::new(parse_socket_addr(cli_options.broker)?)?;
    let reader: Box<dyn Read> = if let Some(file) = cli_options.file {
        Box::new(File::open(file)?)
    } else {
        Box::new(io::stdin().lock())
    };
    let jobs = Deserializer::from_reader(reader).into_iter::<JobDescription>();
    for job in jobs {
        let job = job?;
        let layers = job
            .layers
            .unwrap_or(vec![])
            .iter()
            .map(|layer| client.add_artifact(Path::new(layer).to_owned()))
            .collect::<Result<Vec<_>>>()?;
        client.add_job(JobDetails {
            program: job.program,
            arguments: job.arguments.unwrap_or(vec![]),
            layers,
        });
    }
    client.wait_for_oustanding_jobs()
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    CliOptions::command().debug_assert()
}
