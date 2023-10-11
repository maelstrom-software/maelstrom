use anyhow::anyhow;
use meticulous_base::{proto, Sha256Digest};
use meticulous_util::{
    error::Result,
    net::{self, FixedSizeReader, Sha256Reader},
};
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
) -> Result<u64> {
    let mut writer = TcpStream::connect(broker_addr.inner())?;
    let mut reader = std::io::BufReader::new(writer.try_clone()?);
    net::write_message_to_socket(&mut writer, proto::Hello::ArtifactFetcher)?;
    net::write_message_to_socket(&mut writer, proto::ArtifactFetcherToBroker(digest.clone()))?;
    match net::read_message_from_socket::<proto::BrokerToArtifactFetcher>(&mut reader)?.0 {
        None => Err(anyhow!("Broker error reading artifact {digest}")),
        Some(size) => {
            let reader = FixedSizeReader::new(reader, size);
            let mut reader = Sha256Reader::new(reader);
            tar::Archive::new(&mut reader).unpack(path)?;
            read_to_end(&mut reader)?;
            let (_, actual_digest) = reader.finalize();
            actual_digest.verify(digest)?;
            Ok(size)
        }
    }
}
