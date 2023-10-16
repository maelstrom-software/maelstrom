use crate::config;
use anyhow::{anyhow, Result};
use meticulous_base::{
    proto::{ArtifactFetcherToBroker, BrokerToArtifactFetcher, Hello},
    Sha256Digest,
};
use meticulous_util::{
    io::{FixedSizeReader, Sha256Reader},
    net,
};
use slog::{debug, Logger};
use std::{
    io::{BufReader, Read},
    net::TcpStream,
    path::PathBuf,
};
use tar::Archive;

fn read_to_end(mut input: impl Read) -> Result<()> {
    let mut buf = [0u8; 4096];
    while input.read(&mut buf)? > 0 {}
    Ok(())
}

pub fn main(
    digest: &Sha256Digest,
    path: PathBuf,
    broker_addr: config::BrokerAddr,
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
    let reader = FixedSizeReader::new(reader, size);
    let mut reader = Sha256Reader::new(reader);
    Archive::new(&mut reader).unpack(path)?;
    read_to_end(&mut reader)?;
    let (_, actual_digest) = reader.finalize();
    actual_digest.verify(digest)?;
    Ok(size)
}
