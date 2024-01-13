use crate::scheduler_task::{SchedulerMessage, SchedulerSender};
use anyhow::Result;
use maelstrom_base::proto::{ArtifactPusherToBroker, BrokerToArtifactPusher};
use maelstrom_util::{
    io::{FixedSizeReader, Sha256Reader},
    net,
};
use slog::{debug, Logger};
use std::{
    io,
    net::TcpStream,
    path::{Path, PathBuf},
};

fn handle_one_message(
    msg: ArtifactPusherToBroker,
    socket: &mut TcpStream,
    scheduler_sender: &SchedulerSender,
    cache_tmp_path: &Path,
) -> Result<()> {
    let ArtifactPusherToBroker(digest, size) = msg;
    let mut tmp = tempfile::Builder::new()
        .prefix(&digest.to_string())
        .suffix(".tar")
        .tempfile_in(cache_tmp_path)?;
    let fixed_size_reader = FixedSizeReader::new(socket, size);
    let mut sha_reader = Sha256Reader::new(fixed_size_reader);
    let copied = io::copy(&mut sha_reader, &mut tmp)?;
    assert_eq!(copied, size);
    let (_, actual_digest) = sha_reader.finalize();
    actual_digest.verify(&digest)?;
    let (_, path) = tmp.keep()?;
    scheduler_sender.send(SchedulerMessage::GotArtifact(digest.clone(), path, size))?;
    Ok(())
}

fn connection_loop(
    mut socket: TcpStream,
    scheduler_sender: &SchedulerSender,
    cache_tmp_path: &Path,
    log: &mut Logger,
) -> Result<()> {
    loop {
        let msg = net::read_message_from_socket(&mut socket)?;
        debug!(log, "received artifact pusher message"; "msg" => ?msg);
        let result = handle_one_message(msg, &mut socket, scheduler_sender, cache_tmp_path);
        let msg = BrokerToArtifactPusher(result.as_ref().map(|_| ()).map_err(|e| e.to_string()));
        debug!(log, "sending artifact pusher message"; "msg" => ?msg);
        net::write_message_to_socket(&mut socket, msg)?;
        result?;
    }
}

pub fn connection_main(
    socket: TcpStream,
    scheduler_sender: SchedulerSender,
    cache_tmp_path: PathBuf,
    mut log: Logger,
) -> Result<()> {
    debug!(log, "artifact pusher connected");
    let err = connection_loop(socket, &scheduler_sender, &cache_tmp_path, &mut log).unwrap_err();
    debug!(log, "artifact pusher disconnected"; "err" => %err);
    Err(err)
}
