use anyhow::{anyhow, Result};
use maelstrom_base::{
    proto::{ArtifactFetcherToBroker, BrokerToArtifactFetcher, Hello},
    Sha256Digest,
};
use maelstrom_linux as linux;
use maelstrom_util::{config::common::BrokerAddr, fs::Fs, io, net};
use slog::{debug, Logger};
use std::os::fd::AsRawFd as _;
use std::{net::TcpStream, path::PathBuf};

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
    let expected_size = msg
        .0
        .map_err(|e| anyhow!("Broker error reading artifact: {e}"))?;

    let fs = Fs::new();
    let file = fs.create_file(path)?;

    let mut writer = io::MaybeFastWriter::new(log.clone());

    let stream_fd = linux::Fd::from_raw(stream.as_raw_fd());
    let file_fd = linux::Fd::from_raw(file.as_raw_fd());

    let mut file_offset = 0;
    while file_offset < expected_size {
        let remaining = expected_size - file_offset;
        let to_read = std::cmp::min(writer.buffer_size(), remaining as usize);
        let written = writer.write_fd(stream_fd, None, to_read)?;
        if written == 0 {
            return Err(anyhow!("got unexpected EOF receiving artifact"));
        }

        writer.copy_to_fd(file_fd, Some(file_offset))?;
        file_offset += written as u64;
    }

    Ok(expected_size)
}
