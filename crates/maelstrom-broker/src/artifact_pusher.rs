use crate::scheduler_task::{SchedulerMessage, SchedulerSender};
use anyhow::Result;
use maelstrom_base::proto::{ArtifactPusherToBroker, BrokerToArtifactPusher};
use maelstrom_util::{
    cache::{
        fs::{std::Fs, TempFile as _},
        TempFileFactory,
    },
    io::{FixedSizeReader, Sha256Stream},
    net,
};
use slog::{debug, Logger};
use std::{fs::File, io, net::TcpStream};

fn handle_one_message(
    msg: ArtifactPusherToBroker,
    socket: &mut TcpStream,
    scheduler_sender: &SchedulerSender,
    temp_file_factory: &TempFileFactory<Fs>,
) -> Result<()> {
    let ArtifactPusherToBroker(digest, size) = msg;
    let temp_file = temp_file_factory.temp_file()?;
    let mut temp_file_inner = File::create(temp_file.path())?;
    let fixed_size_reader = FixedSizeReader::new(socket, size);
    let mut sha_reader = Sha256Stream::new(fixed_size_reader);
    let copied = io::copy(&mut sha_reader, &mut temp_file_inner)?;
    assert_eq!(copied, size);
    let (_, actual_digest) = sha_reader.finalize();
    actual_digest.verify(&digest)?;
    scheduler_sender.send(SchedulerMessage::GotArtifact(digest, temp_file))?;
    Ok(())
}

fn connection_loop(
    mut socket: TcpStream,
    scheduler_sender: &SchedulerSender,
    temp_file_factory: &TempFileFactory<Fs>,
    log: &mut Logger,
) -> Result<()> {
    loop {
        let msg = net::read_message_from_socket(&mut socket)?;
        debug!(log, "received artifact pusher message"; "msg" => ?msg);
        let result = handle_one_message(msg, &mut socket, scheduler_sender, temp_file_factory);
        let msg = BrokerToArtifactPusher(result.as_ref().map(|_| ()).map_err(|e| e.to_string()));
        debug!(log, "sending artifact pusher message"; "msg" => ?msg);
        net::write_message_to_socket(&mut socket, msg)?;
        result?;
    }
}

pub fn connection_main(
    socket: TcpStream,
    scheduler_sender: SchedulerSender,
    temp_file_factory: TempFileFactory<Fs>,
    mut log: Logger,
) -> Result<()> {
    debug!(log, "artifact pusher connected");
    let err = connection_loop(socket, &scheduler_sender, &temp_file_factory, &mut log).unwrap_err();
    debug!(log, "artifact pusher disconnected"; "err" => %err);
    Err(err)
}
