use anyhow::{anyhow, Result};
use maelstrom_base::{
    proto::{ArtifactFetcherToBroker, BrokerToArtifactFetcher, Hello},
    Sha256Digest,
};
use maelstrom_util::{config::BrokerAddr, fs::Fs, net};
use slog::{debug, Logger};
use std::{
    io::{self, BufReader, Read as _},
    net::TcpStream,
    path::PathBuf,
};

pub fn main(
    digest: &Sha256Digest,
    path: PathBuf,
    broker_addr: BrokerAddr,
    log: &mut Logger,
) -> Result<u64> {
    let mut writer = TcpStream::connect(broker_addr.inner())?;
    let mut reader = BufReader::new(writer.try_clone()?);
    net::write_message_to_socket(&mut writer, Hello::ArtifactFetcher)?;

    let msg = ArtifactFetcherToBroker(digest.clone());
    debug!(log, "artifact fetcher sending message"; "msg" => ?msg);

    net::write_message_to_socket(&mut writer, msg)?;
    let msg = net::read_message_from_socket::<BrokerToArtifactFetcher>(&mut reader)?;
    debug!(log, "artifact fetcher received message"; "msg" => ?msg);
    let size = msg
        .0
        .map_err(|e| anyhow!("Broker error reading artifact: {e}"))?;

    let fs = Fs::new();
    let mut file = fs.create_file(path)?;
    let copied = io::copy(&mut reader.take(size), &mut file)?;
    if copied != size {
        return Err(anyhow!("got unexpected EOF receiving artifact"));
    }

    Ok(size)
}
