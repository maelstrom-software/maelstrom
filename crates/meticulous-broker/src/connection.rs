use super::{
    artifact_fetcher, artifact_pusher,
    scheduler_task::{SchedulerMessage, SchedulerSender},
    IdVendor,
};
use meticulous_base::{proto, ClientId, WorkerId};
use meticulous_util::{error::Result, net};
use proto::Hello;
use serde::Serialize;
use slog::{debug, error, info, o, warn, Logger};
use std::{future::Future, path::PathBuf, sync::Arc, thread};
use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::{self, JoinSet},
};

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
    FromSchedulerMessageT: Serialize + Send + 'static,
    ReaderFutureT: Future<Output = ()> + Send + 'static,
    WriterFutureT: Future<Output = ()> + Send + 'static,
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
    let mut join_set = JoinSet::new();
    join_set.spawn(socket_reader_main(scheduler_sender.clone()));
    join_set.spawn(socket_writer_main(socket_receiver));

    // Wait for one task to complete and then cancel the other one and wait for it.
    join_set.join_next().await;
    join_set.shutdown().await;

    // Tell the scheduler we're done. We do this after waiting for all tasks to complete, since we
    // need to ensure that all messages sent by the socket_reader arrive before this one.
    scheduler_sender.send(disconnected_msg_builder(id)).ok();
}

async fn unassigned_connection_main(
    mut socket: TcpStream,
    scheduler_sender: SchedulerSender,
    id_vendor: Arc<IdVendor>,
    cache_tmp_path: PathBuf,
    log: Logger,
) {
    match net::read_message_from_async_socket(&mut socket).await {
        Ok(Hello::Client) => {
            let (read_stream, write_stream) = socket.into_split();
            let read_stream = BufReader::new(read_stream);
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
        Ok(Hello::Worker { slots }) => {
            let (read_stream, write_stream) = socket.into_split();
            let read_stream = BufReader::new(read_stream);
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
        Ok(Hello::ArtifactFetcher) => {
            let log = log.clone();
            let socket = socket.into_std().unwrap();
            socket.set_nonblocking(false).unwrap();
            thread::spawn(move || -> Result<()> {
                artifact_fetcher::connection_main(socket, scheduler_sender, log)
            });
        }
        Ok(Hello::ArtifactPusher) => {
            let log = log.clone();
            let socket = socket.into_std().unwrap();
            socket.set_nonblocking(false).unwrap();
            thread::spawn(move || -> Result<()> {
                artifact_pusher::connection_main(socket, scheduler_sender, cache_tmp_path, log)
            });
        }
        Err(err) => {
            warn!(log, "error reading hello message"; "err" => %err);
        }
    }
}

/// Main loop for the listener. This should be run on a task of its own. There should be at least
/// one of these in a broker process. It will only return when it encounters an error. Until then,
/// it listens on a socket and spawns new tasks for each client or worker that connects.
pub async fn listener_main(
    listener: TcpListener,
    scheduler_sender: SchedulerSender,
    id_vendor: Arc<IdVendor>,
    cache_tmp_path: PathBuf,
    log: Logger,
) {
    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                let log = log.new(o!("peer_addr" => peer_addr));
                debug!(log, "received connection");
                task::spawn(unassigned_connection_main(
                    socket,
                    scheduler_sender.clone(),
                    id_vendor.clone(),
                    cache_tmp_path.clone(),
                    log,
                ));
            }
            Err(err) => {
                error!(log, "error accepting connection"; "err" => err);
                return;
            }
        }
    }
}
