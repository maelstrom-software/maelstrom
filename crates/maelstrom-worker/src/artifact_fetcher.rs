use crate::{
    dispatcher::{self, Message},
    types::{DispatcherSender, TempFile, TempFileFactory},
};
use anyhow::{anyhow, bail, Result};
use maelstrom_base::{
    proto::{ArtifactFetcherToBroker, BrokerToArtifactFetcher, Hello},
    Sha256Digest,
};
use maelstrom_linux::Fd;
use maelstrom_util::{
    cache::{fs::TempFile as _, GotArtifact},
    config::common::BrokerAddr,
    fs::Fs,
    io::MaybeFastWriter,
    net,
    sync::Pool,
};
use slog::{debug, o, warn, Logger};
use std::{cmp, net::TcpStream, num::NonZeroU32, os::fd::AsRawFd, sync::Arc, thread};

pub struct ArtifactFetcher {
    broker_addr: BrokerAddr,
    dispatcher_sender: DispatcherSender,
    log: Logger,
    pool: Arc<Pool<TcpStream>>,
    temp_file_factory: TempFileFactory,
}

impl ArtifactFetcher {
    pub fn new(
        max_simultaneous_fetches: NonZeroU32,
        dispatcher_sender: DispatcherSender,
        broker_addr: BrokerAddr,
        log: Logger,
        temp_file_factory: TempFileFactory,
    ) -> Self {
        ArtifactFetcher {
            broker_addr,
            dispatcher_sender,
            log,
            pool: Arc::new(Pool::new(max_simultaneous_fetches)),
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
        debug!(log, "artifact fetcher request enqueued");
        let broker_addr = self.broker_addr;
        let dispatcher_sender = self.dispatcher_sender.clone();
        let pool = self.pool.clone();
        let temp_file_factory = self.temp_file_factory.clone();
        thread::spawn(move || {
            let result = pool.call_with_item(|stream| {
                main(broker_addr, &digest, &log, stream, temp_file_factory)
            });
            debug!(log, "artifact fetcher request completed"; "result" => ?result);
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
    log: &Logger,
    mut stream_option: Option<TcpStream>,
    temp_file_factory: TempFileFactory,
) -> Result<(TcpStream, TempFile)> {
    if stream_option.is_some() {
        debug!(log, "artifact fetcher reusing existing connection");
    } else {
        debug!(log, "artifact fetcher creating new connection");
    }

    let temp_file = temp_file_factory.temp_file().inspect_err(|err| {
        warn!(log, "artifact fetcher failed to create a temporary file"; "err" => %err);
    })?;

    // Loop up to two times. It's possible that a re-used existing connection isn't really active.
    // The broker could have silently shut it down, or may be in the process of shutting it down.
    // For this reason, if we have a reused connection and get an error writing to it or reading
    // the first response, try again with a newly-created connection.
    let (mut stream, size) = loop {
        let (mut stream, can_retry) = match stream_option {
            Some(stream) => (stream, true),
            None => {
                debug!(log, "artifact fetcher connecting to broker");
                let mut stream = TcpStream::connect(broker_addr.inner())?;

                net::write_message_to_socket(&mut stream, Hello::ArtifactFetcher, log)?;

                (stream, false)
            }
        };

        let size_result = (|| {
            net::write_message_to_socket(
                &mut stream,
                ArtifactFetcherToBroker(digest.clone()),
                log,
            )?;

            net::read_message_from_socket::<BrokerToArtifactFetcher>(&mut stream, log)?
                .0
                .map_err(|e| anyhow!("broker error reading artifact: {e}"))
        })();

        match size_result {
            Ok(size) => {
                break (stream, size);
            }
            Err(err) if !can_retry => {
                return Err(err);
            }
            Err(err) => {
                debug!(
                    log,
                    "artifact fetcher failed to use preexisting connection; retrying with new connection";
                    "err" => ?err);
                stream_option = None;
            }
        }
    };

    let fs = Fs::new();
    let mut file = fs.create_file(temp_file.path())?;
    let copied = copy_using_splice(&mut stream, &mut file, size, log)?;
    if copied < size {
        debug!(log, "artifact fetcher got premature EOF copying file");
        bail!("premature EOF reading artifact");
    }

    Ok((stream, temp_file))
}

fn copy_using_splice(
    reader: &mut impl AsRawFd,
    writer: &mut impl AsRawFd,
    to_copy: u64,
    log: &Logger,
) -> Result<u64> {
    let mut buffer = MaybeFastWriter::new(log);
    let read_fd = Fd::from_raw(reader.as_raw_fd());
    let write_fd = Fd::from_raw(writer.as_raw_fd());

    let mut copied = 0;
    while copied < to_copy {
        let to_read = cmp::min(buffer.buffer_size(), (to_copy - copied) as usize);
        let read = buffer.write_fd(read_fd, None, to_read)?;
        if read == 0 {
            break;
        }
        buffer.copy_to_fd(write_fd, None)?;
        copied += read as u64;
    }

    Ok(copied)
}
