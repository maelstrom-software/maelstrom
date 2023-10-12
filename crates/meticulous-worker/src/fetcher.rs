use anyhow::anyhow;
use meticulous_base::{proto, Sha256Digest};
use meticulous_util::{
    error::Result,
    net::{self, FixedSizeReader, Sha256Reader},
};
use slog::debug;
use std::{net::TcpStream, path::PathBuf};

fn read_to_end(mut input: impl std::io::Read) -> std::io::Result<()> {
    let mut buf = [0u8; 4096];
    while input.read(&mut buf)? > 0 {}
    Ok(())
}

pub fn main(
    digest: &Sha256Digest,
    path: PathBuf,
    broker_addr: super::config::Broker,
    log: &mut slog::Logger,
) -> Result<u64> {
    let mut writer = TcpStream::connect(broker_addr.inner())?;
    let mut reader = std::io::BufReader::new(writer.try_clone()?);
    net::write_message_to_socket(&mut writer, proto::Hello::ArtifactFetcher)?;
    let msg = proto::ArtifactFetcherToBroker(digest.clone());
    debug!(log, "artifact fetcher sending message"; "msg" => ?msg);
    net::write_message_to_socket(&mut writer, msg)?;
    let msg = net::read_message_from_socket::<proto::BrokerToArtifactFetcher>(&mut reader)?;
    debug!(log, "artifact fetcher received message"; "msg" => ?msg);
    let size = msg
        .0
        .map_err(|e| anyhow!("Broker error reading artifact: {e}"))?;
    let reader = FixedSizeReader::new(reader, size);
    let mut reader = Sha256Reader::new(reader);
    tar::Archive::new(&mut reader).unpack(path)?;
    read_to_end(&mut reader)?;
    let (_, actual_digest) = reader.finalize();
    actual_digest.verify(digest)?;
    Ok(size)
}
