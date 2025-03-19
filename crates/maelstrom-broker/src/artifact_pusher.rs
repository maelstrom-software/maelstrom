use crate::{cache::TempFileFactory, scheduler_task};
use anyhow::{Error, Result};
use maelstrom_base::proto::{ArtifactPusherToBroker, BrokerToArtifactPusher};
use maelstrom_util::{
    cache::fs::TempFile as _,
    io::{FixedSizeReader, Sha256Stream},
    net,
};
use slog::{debug, Logger};
use std::{fs::File, io, net::TcpStream};

fn handle_one_message<TempFileFactoryT: TempFileFactory>(
    msg: ArtifactPusherToBroker,
    socket: &mut TcpStream,
    scheduler_task_sender: &scheduler_task::Sender<TempFileFactoryT::TempFile>,
    temp_file_factory: &TempFileFactoryT,
) -> Result<()>
where
    TempFileFactoryT::TempFile: Send + Sync + 'static,
{
    let ArtifactPusherToBroker(digest, size) = msg;
    let temp_file = temp_file_factory.temp_file()?;
    let mut temp_file_inner = File::create(temp_file.path())?;
    let fixed_size_reader = FixedSizeReader::new(socket, size);
    let mut sha_reader = Sha256Stream::new(fixed_size_reader);
    let copied = io::copy(&mut sha_reader, &mut temp_file_inner)?;
    assert_eq!(copied, size);
    let (_, actual_digest) = sha_reader.finalize();
    actual_digest.verify(&digest)?;
    scheduler_task_sender.send(scheduler_task::Message::GotArtifact(digest, temp_file))?;
    Ok(())
}

fn connection_loop<TempFileFactoryT: TempFileFactory>(
    mut socket: TcpStream,
    scheduler_task_sender: &scheduler_task::Sender<TempFileFactoryT::TempFile>,
    temp_file_factory: &TempFileFactoryT,
    log: &Logger,
) -> Result<()>
where
    TempFileFactoryT::TempFile: Send + Sync + 'static,
{
    loop {
        let msg = net::read_message_from_socket(&mut socket, log)?;
        let result = handle_one_message(msg, &mut socket, scheduler_task_sender, temp_file_factory);
        let msg = BrokerToArtifactPusher(result.as_ref().map(drop).map_err(Error::to_string));
        net::write_message_to_socket(&mut socket, &msg, log)?;
        result?;
    }
}

pub fn connection_main<TempFileFactoryT: TempFileFactory>(
    socket: TcpStream,
    scheduler_task_sender: scheduler_task::Sender<TempFileFactoryT::TempFile>,
    temp_file_factory: TempFileFactoryT,
    log: Logger,
) -> Result<()>
where
    TempFileFactoryT::TempFile: Send + Sync + 'static,
{
    debug!(log, "artifact pusher connected");
    let err =
        connection_loop(socket, &scheduler_task_sender, &temp_file_factory, &log).unwrap_err();
    debug!(log, "artifact pusher disconnected"; "error" => %err);
    Err(err)
}
