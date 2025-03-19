use crate::scheduler_task;
use anyhow::{anyhow, Result};
use maelstrom_base::{
    proto::{ArtifactFetcherToBroker, BrokerToArtifactFetcher},
    Sha256Digest,
};
use maelstrom_util::{
    fs::{File, Fs},
    net,
};
use slog::{debug, Logger};
use std::{
    io::{self, Read as _},
    net::TcpStream,
    sync::mpsc,
};

fn get_file<'fs, TempFileT>(
    fs: &'fs Fs,
    digest: &Sha256Digest,
    scheduler_task_sender: &scheduler_task::Sender<TempFileT>,
) -> Result<(File<'fs>, u64)>
where
    TempFileT: Send + Sync + 'static,
{
    let (channel_sender, channel_receiver) = mpsc::channel();
    scheduler_task_sender.send(scheduler_task::Message::GetArtifactForWorker(
        digest.clone(),
        channel_sender,
    ))?;

    let (path, size) = channel_receiver
        .recv()?
        .ok_or_else(|| anyhow!("couldn't get reference count on artifact"))?;
    let f = fs.open_file(path)?;
    Ok((f, size))
}

fn send_artifact<TempFileT>(
    scheduler_task_sender: &scheduler_task::Sender<TempFileT>,
    file: File,
    mut socket: &mut impl io::Write,
    size: u64,
    digest: Sha256Digest,
) -> Result<()>
where
    TempFileT: Send + Sync + 'static,
{
    let copied = io::copy(&mut file.take(size), &mut socket)?;
    scheduler_task_sender.send(scheduler_task::Message::DecrementRefcount(digest))?;
    if copied == size {
        Ok(())
    } else {
        Err(anyhow!("unexpected EOF"))
    }
}

fn handle_one_message<TempFileT>(
    msg: ArtifactFetcherToBroker,
    mut socket: &mut impl io::Write,
    scheduler_task_sender: &scheduler_task::Sender<TempFileT>,
    log: &mut Logger,
) -> Result<()>
where
    TempFileT: Send + Sync + 'static,
{
    let ArtifactFetcherToBroker(digest) = msg;
    let fs = Fs::new();
    let result = get_file(&fs, &digest, scheduler_task_sender);
    let msg = BrokerToArtifactFetcher(
        result
            .as_ref()
            .map(|(_, size)| *size)
            .map_err(|e| e.to_string()),
    );
    net::write_message_to_socket(&mut socket, &msg, log)?;

    let (f, size) = result?;
    send_artifact(scheduler_task_sender, f, &mut socket, size, digest)?;

    Ok(())
}

fn connection_loop<TempFileT>(
    mut socket: TcpStream,
    scheduler_task_sender: &scheduler_task::Sender<TempFileT>,
    log: &mut Logger,
) -> Result<()>
where
    TempFileT: Send + Sync + 'static,
{
    loop {
        let msg = net::read_message_from_socket(&mut socket, log)?;
        handle_one_message(msg, &mut socket, scheduler_task_sender, log)?;
    }
}

pub fn connection_main<TempFileT>(
    socket: TcpStream,
    scheduler_task_sender: scheduler_task::Sender<TempFileT>,
    mut log: Logger,
) -> Result<()>
where
    TempFileT: Send + Sync + 'static,
{
    debug!(log, "artifact fetcher connected");
    let err = connection_loop(socket, &scheduler_task_sender, &mut log).unwrap_err();
    debug!(log, "artifact fetcher disconnected"; "error" => %err);
    Err(err)
}
