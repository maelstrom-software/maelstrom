use super::{
    scheduler_task::{SchedulerMessage, SchedulerSender},
    IdVendor,
};
use anyhow::anyhow;
use meticulous_base::{proto, ClientId, Sha256Digest, WorkerId};
use meticulous_util::{
    error::Result,
    net::{self, FixedSizeReader, Sha256Reader},
};
use slog::{debug, info, o, warn, Logger};
use std::{
    net::TcpStream,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

/// Main loop for a client or worker socket-like object (socket or websocket). There should be one
/// of these for each connected client or worker socket. This function will run until the client or
/// worker is closed. There is no error return since this function will always eventually run into
/// an error, and we don't really care what that error is.
/// Returns a person with the name given them
///
/// # Type Arguments
///
/// * `IdT` - The type of the ID used to identify this client or worker
///
/// * `FromSchedulerMessageT` - The type of the messages sent from the scheduler to this client or
/// worker
///
/// # Arguments
///
/// * `scheduler_sender` - The sender for sending messages to the sender. A "connected" message
/// will be sent using this sender which will include a newly-created FromSchedulerMessageT sender
/// that the scheduler can use to send messages back to this task. After that messages read from
/// `reader` will be sent to the scheduler over this sender. Finally, when it's time to
/// disconnected, a "disconnected" message will be sent using this sender
///
/// * `id` - The id for this client or worker. This will be used in the "connected" message and the
/// "disconnected" message
///
/// * `connected_msg_builder` - A closure used to build the "connected" scheduler message. It takes
/// the `id` and the newly-created FromSchedulerMessageT sender used to communicate with this task.
/// This will be sent immediately to the scheduler on `scheduler_sender`
///
/// * `disconnected_msg_builder` - A closure used to build the "disconnected" scheduler message. It
/// takes the `id`. This will be sent on `scheduler_sender` right before this function returns to
/// tell the scheduler that this client/worker has disconnected
///
/// * `socket_reader_main` - An async closure that is called on a new task to read all of the
/// messages from the socket-like object and write them to the supplied scheduler sender. The
/// scheduler sender will be a clone of `scheduler_sender`
///
/// * `socket_writer_main` - An async closure that is called on a new task to read all of the
/// messages from the supplied scheduler receiver and write them to the socket-like object.
/// The scheduler receiver will be a newly-created FromSchedulerMessageT receiver
///
pub async fn connection_main<IdT, FromSchedulerMessageT, ReaderFutureT, WriterFutureT>(
    scheduler_sender: SchedulerSender,
    id: IdT,
    connected_msg_builder: impl FnOnce(IdT, UnboundedSender<FromSchedulerMessageT>) -> SchedulerMessage,
    disconnected_msg_builder: impl FnOnce(IdT) -> SchedulerMessage,
    socket_reader_main: impl FnOnce(SchedulerSender) -> ReaderFutureT,
    socket_writer_main: impl FnOnce(UnboundedReceiver<FromSchedulerMessageT>) -> WriterFutureT,
) where
    IdT: Copy + Send + 'static,
    FromSchedulerMessageT: serde::Serialize + Send + 'static,
    ReaderFutureT: std::future::Future<Output = ()> + Send + 'static,
    WriterFutureT: std::future::Future<Output = ()> + Send + 'static,
{
    let (socket_sender, socket_receiver) = mpsc::unbounded_channel();

    // Tell the scheduler that a client/worker has connected. This messages needs to be sent before
    // any messages from this client/worker.
    if scheduler_sender
        .send(connected_msg_builder(id, socket_sender))
        .is_err()
    {
        // If we couldn't send a message to the scheduler, it means the scheduler has quit and we
        // should too.
        return;
    }

    // Spawn two tasks to read and write from the socket. Plumb the message queues appropriately.
    let mut join_set = tokio::task::JoinSet::new();
    join_set.spawn(socket_reader_main(scheduler_sender.clone()));
    join_set.spawn(socket_writer_main(socket_receiver));

    // Wait for one task to complete and then cancel the other one and wait for it.
    join_set.join_next().await;
    join_set.shutdown().await;

    // Tell the scheduler we're done. We do this after waiting for all tasks to complete, since we
    // need to ensure that all messages sent by the socket_reader arrive before this one.
    scheduler_sender.send(disconnected_msg_builder(id)).ok();
}

fn artifact_fetcher_get_file(
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

fn artifact_fetcher_handle_one_message(
    msg: proto::ArtifactFetcherToBroker,
    mut socket: &mut TcpStream,
    scheduler_sender: &SchedulerSender,
    log: &mut Logger,
) -> Result<()> {
    debug!(log, "received artifact fetcher message"; "msg" => ?msg);
    let proto::ArtifactFetcherToBroker(digest) = msg;
    let result = artifact_fetcher_get_file(&digest, scheduler_sender);
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

fn artifact_fetcher_connection_loop(
    mut socket: TcpStream,
    scheduler_sender: &SchedulerSender,
    log: &mut Logger,
) -> Result<()> {
    loop {
        let msg = net::read_message_from_socket(&mut socket)?;
        artifact_fetcher_handle_one_message(msg, &mut socket, scheduler_sender, log)?;
    }
}

fn artifact_fetcher_connection_main(
    socket: TcpStream,
    scheduler_sender: SchedulerSender,
    mut log: Logger,
) -> Result<()> {
    debug!(log, "connection upgraded to artifact fetcher connection");
    let err = artifact_fetcher_connection_loop(socket, &scheduler_sender, &mut log).unwrap_err();
    debug!(log, "artifact fetcher connection ended"; "err" => %err);
    Err(err)
}

fn artifact_pusher_handle_one_message(
    msg: proto::ArtifactPusherToBroker,
    socket: &mut TcpStream,
    scheduler_sender: &SchedulerSender,
    cache_tmp_path: &Path,
) -> Result<()> {
    let proto::ArtifactPusherToBroker(digest, size) = msg;
    let mut tmp = tempfile::Builder::new()
        .prefix(&digest.to_string())
        .suffix(".tar")
        .tempfile_in(cache_tmp_path)?;
    let fixed_size_reader = FixedSizeReader::new(socket, size);
    let mut sha_reader = Sha256Reader::new(fixed_size_reader);
    let copied = std::io::copy(&mut sha_reader, &mut tmp)?;
    assert_eq!(copied, size);
    let (_, actual_digest) = sha_reader.finalize();
    actual_digest.verify(&digest)?;
    let (_, path) = tmp.keep()?;
    scheduler_sender.send(SchedulerMessage::GotArtifact(digest.clone(), path, size))?;
    Ok(())
}

fn artifact_pusher_connection_loop(
    mut socket: TcpStream,
    scheduler_sender: &SchedulerSender,
    cache_tmp_path: &Path,
    log: &mut Logger,
) -> Result<()> {
    loop {
        let msg = net::read_message_from_socket(&mut socket)?;
        debug!(log, "received artifact pusher message"; "msg" => ?msg);
        let result =
            artifact_pusher_handle_one_message(msg, &mut socket, scheduler_sender, cache_tmp_path);
        let msg =
            proto::BrokerToArtifactPusher(result.as_ref().map(|_| ()).map_err(|e| e.to_string()));
        debug!(log, "sending artifact pusher message"; "msg" => ?msg);
        net::write_message_to_socket(&mut socket, msg)?;
        result?;
    }
}

fn artifact_pusher_connection_main(
    socket: TcpStream,
    scheduler_sender: SchedulerSender,
    cache_tmp_path: PathBuf,
    mut log: Logger,
) -> Result<()> {
    debug!(log, "connection upgraded to artifact pusher connection");
    let err = artifact_pusher_connection_loop(socket, &scheduler_sender, &cache_tmp_path, &mut log)
        .unwrap_err();
    debug!(log, "artifact pusher connection ended"; "err" => %err);
    Err(err)
}

/// Main loop for the listener. This should be run on a task of its own. There should be at least
/// one of these in a broker process. It will only return when it encounters an error. Until then,
/// it listens on a socket and spawns new tasks for each client or worker that connects.
pub async fn listener_main(
    listener: tokio::net::TcpListener,
    scheduler_sender: SchedulerSender,
    id_vendor: Arc<IdVendor>,
    cache_tmp_path: PathBuf,
    log: Logger,
) {
    while let Ok((mut socket, peer_addr)) = listener.accept().await {
        let scheduler_sender = scheduler_sender.clone();
        let id_vendor = id_vendor.clone();
        let cache_tmp_path = cache_tmp_path.clone();

        let log = log.new(o!("peer_addr" => peer_addr));
        debug!(log, "received connect");

        tokio::task::spawn(async move {
            match net::read_message_from_async_socket(&mut socket).await {
                Ok(proto::Hello::Client) => {
                    let (read_stream, write_stream) = socket.into_split();
                    let read_stream = tokio::io::BufReader::new(read_stream);
                    let id: ClientId = id_vendor.vend();
                    let log = log.new(o!("cid" => id.to_string()));
                    debug!(log, "connection upgraded to client connection");
                    let log_clone = log.clone();
                    let log_clone2 = log.clone();
                    connection_main(
                        scheduler_sender,
                        id,
                        SchedulerMessage::ClientConnected,
                        SchedulerMessage::ClientDisconnected,
                        |scheduler_sender| {
                            net::async_socket_reader(read_stream, scheduler_sender, move |msg| {
                                debug!(log_clone, "received client message"; "msg" => ?msg);
                                SchedulerMessage::FromClient(id, msg)
                            })
                        },
                        |scheduler_receiver| {
                            net::async_socket_writer(scheduler_receiver, write_stream, move |msg| {
                                debug!(log_clone2, "sending client message"; "msg" => ?msg);
                            })
                        },
                    )
                    .await;
                    debug!(log, "received client disconnect");
                }
                Ok(proto::Hello::Worker { slots }) => {
                    let (read_stream, write_stream) = socket.into_split();
                    let read_stream = tokio::io::BufReader::new(read_stream);
                    let id: WorkerId = id_vendor.vend();
                    let log = log.new(o!("wid" => id.to_string()));
                    info!(log, "connection upgraded to worker connection"; "slots" => slots);
                    let log_clone = log.clone();
                    let log_clone2 = log.clone();
                    connection_main(
                        scheduler_sender,
                        id,
                        |id, sender| SchedulerMessage::WorkerConnected(id, slots as usize, sender),
                        SchedulerMessage::WorkerDisconnected,
                        |scheduler_sender| {
                            net::async_socket_reader(read_stream, scheduler_sender, move |msg| {
                                debug!(log_clone, "received worker message"; "msg" => ?msg);
                                SchedulerMessage::FromWorker(id, msg)
                            })
                        },
                        |scheduler_receiver| {
                            net::async_socket_writer(scheduler_receiver, write_stream, move |msg| {
                                debug!(log_clone2, "sending worker message"; "msg" => ?msg);
                            })
                        },
                    )
                    .await;
                    info!(log, "received worker disconnect");
                }
                Ok(proto::Hello::ArtifactFetcher) => {
                    let log = log.clone();
                    let socket = socket.into_std().unwrap();
                    socket.set_nonblocking(false).unwrap();
                    std::thread::spawn(move || -> Result<()> {
                        artifact_fetcher_connection_main(socket, scheduler_sender, log)
                    });
                }
                Ok(proto::Hello::ArtifactPusher) => {
                    let log = log.clone();
                    let socket = socket.into_std().unwrap();
                    socket.set_nonblocking(false).unwrap();
                    std::thread::spawn(move || -> Result<()> {
                        artifact_pusher_connection_main(
                            socket,
                            scheduler_sender,
                            cache_tmp_path,
                            log,
                        )
                    });
                }
                Err(err) => {
                    warn!(log, "error reading hello message"; "err" => %err);
                }
            }
        });
    }
}
