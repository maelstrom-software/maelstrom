use crate::{
    dispatcher::{self, Message},
    types::{DispatcherSender, TempFile, TempFileFactory},
};
use anyhow::{anyhow, bail, Result};
use maelstrom_base::{
    proto::{ArtifactFetcherToBroker, BrokerToArtifactFetcher, Hello},
    Sha256Digest,
};
use maelstrom_util::{
    cache::{fs::TempFile as _, GotArtifact},
    config::common::BrokerAddr,
    fs::Fs,
    io,
    net::{self, AsRawFdExt as _},
    sync::Pool,
};
use slog::{debug, o, warn, Logger};
use std::{net::TcpStream, num::NonZeroU32, sync::Arc, thread};

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
    stream_option: Option<TcpStream>,
    temp_file_factory: TempFileFactory,
) -> Result<(TcpStream, TempFile)> {
    if stream_option.is_some() {
        debug!(log, "artifact fetcher reusing existing connection");
    } else {
        debug!(log, "artifact fetcher creating new connection");
    }

    let temp_file = temp_file_factory.temp_file().inspect_err(|err| {
        warn!(log, "artifact fetcher failed to create a temporary file"; "error" => %err);
    })?;

    // Loop up to two times. It's possible that a re-used existing connection isn't really active.
    // The broker could have silently shut it down, or may be in the process of shutting it down.
    // For this reason, if we have a reused connection and get an error writing to it or reading
    // the first response, try again with a newly-created connection.
    let mut stream = match stream_option {
        Some(stream) => stream,
        None => {
            debug!(log, "artifact fetcher connecting to broker");
            let mut stream = TcpStream::connect(broker_addr.inner())?.set_socket_options()?;
            net::write_message_to_socket(&mut stream, Hello::ArtifactFetcher, log)?;
            stream
        }
    };

    net::write_message_to_socket(&mut stream, ArtifactFetcherToBroker(digest.clone()), log)?;

    let BrokerToArtifactFetcher(result) = net::read_message_from_socket(&mut stream, log)?;
    let size = result.map_err(|e| anyhow!("broker error reading artifact: {e}"))?;

    let fs = Fs::new();
    let mut file = fs.create_file(temp_file.path())?;
    let copied = io::copy_using_splice(&mut stream, &mut file, size, log)?;
    if copied < size {
        debug!(log, "artifact fetcher got premature EOF copying file");
        bail!("premature EOF reading artifact");
    }

    Ok((stream, temp_file))
}
