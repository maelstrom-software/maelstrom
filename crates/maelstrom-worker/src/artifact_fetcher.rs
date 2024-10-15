use crate::{
    dispatcher::{self, Message},
    types::{DispatcherSender, TempFileFactory},
};
use anyhow::{anyhow, Result};
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
use slog::{debug, o, Logger};
use std::{net::TcpStream, os::fd::AsRawFd as _, path::PathBuf, sync::Arc, thread};
use std_semaphore::Semaphore;

pub const MAX_ARTIFACT_FETCHES: u64 = 10;

pub struct ArtifactFetcher {
    broker_addr: BrokerAddr,
    dispatcher_sender: DispatcherSender,
    log: Logger,
    semaphore: Arc<Semaphore>,
    temp_file_factory: TempFileFactory,
}

impl ArtifactFetcher {
    pub fn new(
        dispatcher_sender: DispatcherSender,
        broker_addr: BrokerAddr,
        log: Logger,
        temp_file_factory: TempFileFactory,
    ) -> Self {
        ArtifactFetcher {
            broker_addr,
            dispatcher_sender,
            log,
            semaphore: Arc::new(Semaphore::new(MAX_ARTIFACT_FETCHES as isize)),
            temp_file_factory,
        }
    }
}

impl dispatcher::ArtifactFetcher for ArtifactFetcher {
    fn start_artifact_fetch(&mut self, digest: Sha256Digest) {
        let log = self.log.new(o!(
            "digest" => digest.to_string(),
            "broker_addr" => self.broker_addr.to_string()
        ));
        main(
            self.broker_addr,
            digest,
            self.dispatcher_sender.clone(),
            log,
            self.semaphore.clone(),
            self.temp_file_factory.clone(),
        );
    }
}

fn main(
    broker_addr: BrokerAddr,
    digest: Sha256Digest,
    dispatcher_sender: DispatcherSender,
    mut log: Logger,
    semaphore: Arc<Semaphore>,
    temp_file_factory: TempFileFactory,
) {
    match temp_file_factory.temp_file() {
        Err(err) => {
            debug!(log, "artifact fetcher failed to get a temporary file"; "err" => ?err);
            dispatcher_sender
                .send(Message::ArtifactFetchCompleted(digest, Err(err)))
                .ok();
        }
        Ok(temp_file) => {
            debug!(log, "artifact fetcher starting");
            thread::spawn(move || {
                let _permit = semaphore.access();
                let result =
                    main_inner(&digest, temp_file.path().to_owned(), broker_addr, &mut log);
                debug!(log, "artifact fetcher completed"; "result" => ?result);
                dispatcher_sender
                    .send(Message::ArtifactFetchCompleted(
                        digest,
                        result.map(|_| GotArtifact::File { source: temp_file }),
                    ))
                    .ok();
            });
        }
    }
}

fn main_inner(
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