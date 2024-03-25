use anyhow::{anyhow, Result};
use maelstrom_base::{
    proto::{ArtifactFetcherToBroker, BrokerToArtifactFetcher, Hello},
    Sha256Digest,
};
use maelstrom_util::{config::common::BrokerAddr, fs::Fs, net};
use slog::{debug, Logger};
use std::{
    io::{self, Read as _},
    net::TcpStream,
    path::PathBuf,
};

pub fn main(
    digest: &Sha256Digest,
    path: PathBuf,
    broker_addr: BrokerAddr,
    log: &mut Logger,
) -> Result<u64> {
    let mut stream = TcpStream::connect(broker_addr.inner())?;
    net::write_message_to_socket(&mut stream, Hello::ArtifactFetcher)?;

    let msg = ArtifactFetcherToBroker(digest.clone());
    debug!(log, "artifact fetcher sending message"; "msg" => ?msg);

    net::write_message_to_socket(&mut stream, msg)?;
    let msg = net::read_message_from_socket::<BrokerToArtifactFetcher>(&mut stream)?;
    debug!(log, "artifact fetcher received message"; "msg" => ?msg);
    let size = msg
        .0
        .map_err(|e| anyhow!("Broker error reading artifact: {e}"))?;

    let fs = Fs::new();
    let file = fs.create_file(path)?;
    let copied = io::copy(&mut stream.take(size), &mut file.into_inner())?;
    if copied != size {
        return Err(anyhow!("got unexpected EOF receiving artifact"));
    }

    Ok(size)
}
