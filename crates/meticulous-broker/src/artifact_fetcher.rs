use super::scheduler_task::{SchedulerMessage, SchedulerSender};
use anyhow::anyhow;
use meticulous_base::{proto, Sha256Digest};
use meticulous_util::{error::Result, net};
use slog::{debug, Logger};
use std::net::TcpStream;

fn get_file(
    digest: &Sha256Digest,
    scheduler_sender: &SchedulerSender,
) -> Result<(std::fs::File, u64)> {
    let (channel_sender, channel_receiver) = std::sync::mpsc::channel();
    scheduler_sender.send(SchedulerMessage::GetArtifactForWorker(
        digest.clone(),
        channel_sender,
    ))?;

    match channel_receiver.recv()? {
        Some((path, size)) => {
            let f = std::fs::File::open(path)?;
            Ok((f, size))
        }
        None => Err(anyhow!("Cache doesn't contain artifact {digest}")),
    }
}

fn handle_one_message(
    msg: proto::ArtifactFetcherToBroker,
    mut socket: &mut TcpStream,
    scheduler_sender: &SchedulerSender,
    log: &mut Logger,
) -> Result<()> {
    debug!(log, "received artifact fetcher message"; "msg" => ?msg);
    let proto::ArtifactFetcherToBroker(digest) = msg;
    let result = get_file(&digest, scheduler_sender);
    let msg = proto::BrokerToArtifactFetcher(
        result
            .as_ref()
            .map(|(_, size)| *size)
            .map_err(|e| e.to_string()),
    );
    debug!(log, "sending artifact fetcher message"; "msg" => ?msg);
    net::write_message_to_socket(&mut socket, msg)?;
    let (mut f, size) = result?;
    let copied = std::io::copy(&mut f, &mut socket)?;
    assert_eq!(copied, size);
    scheduler_sender.send(SchedulerMessage::DecrementRefcount(digest))?;
    Ok(())
}

fn connection_loop(
    mut socket: TcpStream,
    scheduler_sender: &SchedulerSender,
    log: &mut Logger,
) -> Result<()> {
    loop {
        let msg = net::read_message_from_socket(&mut socket)?;
        handle_one_message(msg, &mut socket, scheduler_sender, log)?;
    }
}

pub fn connection_main(
    socket: TcpStream,
    scheduler_sender: SchedulerSender,
    mut log: Logger,
) -> Result<()> {
    debug!(log, "connection upgraded to artifact fetcher connection");
    let err = connection_loop(socket, &scheduler_sender, &mut log).unwrap_err();
    debug!(log, "artifact fetcher connection ended"; "err" => %err);
    Err(err)
}
