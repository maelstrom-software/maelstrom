use crate::{
    dispatcher::{self, Message},
    types::{DispatcherSender, TempFile, TempFileFactory},
};
use anyhow::{anyhow, bail, Result};
use maelstrom_base::{
    proto::{ArtifactFetcherToBroker, BrokerToArtifactFetcher, Hello},
    Sha256Digest,
};
use maelstrom_linux as linux;
use maelstrom_util::{
    cache::{fs::TempFile as _, GotArtifact},
    config::common::BrokerAddr,
    fs::Fs,
    io, net,
};
use slog::{debug, o, warn, Logger};
use std::{net::TcpStream, os::fd::AsRawFd as _, sync::Arc, thread};
use std_semaphore::Semaphore;

pub struct ArtifactFetcher {
    broker_addr: BrokerAddr,
    dispatcher_sender: DispatcherSender,
    log: Logger,
    semaphore: Arc<Semaphore>,
    temp_file_factory: TempFileFactory,
}

impl ArtifactFetcher {
    pub fn new(
        max_simultaneous_fetches: usize,
        dispatcher_sender: DispatcherSender,
        broker_addr: BrokerAddr,
        log: Logger,
        temp_file_factory: TempFileFactory,
    ) -> Self {
        ArtifactFetcher {
            broker_addr,
            dispatcher_sender,
            log,
            semaphore: Arc::new(Semaphore::new(max_simultaneous_fetches.try_into().unwrap())),
            temp_file_factory,
        }
    }
}

impl dispatcher::ArtifactFetcher for ArtifactFetcher {
    fn start_artifact_fetch(&mut self, digest: Sha256Digest) {
        let mut log = self.log.new(o!(
            "digest" => digest.to_string(),
            "broker_addr" => self.broker_addr.to_string()
        ));
        debug!(log, "artifact fetcher starting");
        let broker_addr = self.broker_addr;
        let dispatcher_sender = self.dispatcher_sender.clone();
        let semaphore = self.semaphore.clone();
        let temp_file_factory = self.temp_file_factory.clone();
        thread::spawn(move || {
            let result = main(broker_addr, &digest, &mut log, semaphore, temp_file_factory);
            debug!(log, "artifact fetcher completed"; "result" => ?result);
            let _ = dispatcher_sender.send(Message::ArtifactFetchCompleted(
                digest,
                result.map(GotArtifact::file),
            ));
        });
    }
}

fn main(
    broker_addr: BrokerAddr,
    digest: &Sha256Digest,
    log: &mut Logger,
    semaphore: Arc<Semaphore>,
    temp_file_factory: TempFileFactory,
) -> Result<TempFile> {
    let _permit = semaphore.access();
    debug!(log, "artifact fetcher acquired semaphore");

    let temp_file = temp_file_factory.temp_file().inspect_err(|err| {
        warn!(log, "artifact fetcher failed to create a temporary file"; "err" => ?err);
    })?;

    let mut stream = TcpStream::connect(broker_addr.inner())?;
    net::write_message_to_socket(&mut stream, Hello::ArtifactFetcher)?;

    let msg = ArtifactFetcherToBroker(digest.clone());
    debug!(log, "artifact fetcher sending message"; "msg" => ?msg);
    net::write_message_to_socket(&mut stream, msg)?;

    let msg = net::read_message_from_socket::<BrokerToArtifactFetcher>(&mut stream)?;
    debug!(log, "artifact fetcher received message"; "msg" => ?msg);
    let size = msg
        .0
        .map_err(|e| anyhow!("broker error reading artifact: {e}"))?;

    let fs = Fs::new();
    let file = fs.create_file(temp_file.path())?;
    let mut writer = io::MaybeFastWriter::new(log.clone());
    let stream_fd = linux::Fd::from_raw(stream.as_raw_fd());
    let file_fd = linux::Fd::from_raw(file.as_raw_fd());

    let mut file_offset = 0;
    while file_offset < size {
        let remaining = size - file_offset;
        let to_read = std::cmp::min(writer.buffer_size(), remaining as usize);
        let written = writer.write_fd(stream_fd, None, to_read)?;
        if written == 0 {
            bail!("got unexpected EOF receiving artifact");
        }

        writer.copy_to_fd(file_fd, Some(file_offset))?;
        file_offset += written as u64;
    }

    Ok(temp_file)
}
