use crate::{
    artifact_fetcher, artifact_pusher,
    cache::TempFileFactory,
    scheduler_task::{SchedulerMessage, SchedulerSender},
    IdVendor,
};
use anyhow::Result;
use maelstrom_base::{
    proto::{ClientToBroker, Hello},
    ClientId, MonitorId, WorkerId,
};
use maelstrom_util::net::{self, AsRawFdExt};
use serde::Serialize;
use slog::{debug, error, info, o, warn, Logger};
use std::{future::Future, sync::Arc, thread};
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
///   worker
///
/// # Arguments
///
/// * `scheduler_sender` - The sender for sending messages to the sender. A "connected" message
///   will be sent using this sender which will include a newly-created FromSchedulerMessageT
///   sender that the scheduler can use to send messages back to this task. After that messages
///   read from `reader` will be sent to the scheduler over this sender. Finally, when it's time to
///   disconnected, a "disconnected" message will be sent using this sender
///
/// * `id` - The id for this client or worker. This will be used in the "connected" message and the
///   "disconnected" message
///
/// * `connected_msg_builder` - A closure used to build the "connected" scheduler message. It takes
///   the `id` and the newly-created FromSchedulerMessageT sender used to communicate with this
///   task. This will be sent immediately to the scheduler on `scheduler_sender`
///
/// * `disconnected_msg_builder` - A closure used to build the "disconnected" scheduler message. It
///   takes the `id`. This will be sent on `scheduler_sender` right before this function returns to
///   tell the scheduler that this client/worker has disconnected
///
/// * `socket_reader_main` - An async closure that is called on a new task to read all of the
///   messages from the socket-like object and write them to the supplied scheduler sender. The
///   scheduler sender will be a clone of `scheduler_sender`
///
/// * `socket_writer_main` - An async closure that is called on a new task to read all of the
///   messages from the supplied scheduler receiver and write them to the socket-like object. The
///   scheduler receiver will be a newly-created FromSchedulerMessageT receiver
///
pub async fn connection_main<IdT, FromSchedulerMessageT, ReaderFutureT, WriterFutureT, TempFileT>(
    scheduler_sender: SchedulerSender<TempFileT>,
    id: IdT,
    connected_msg_builder: impl FnOnce(
        IdT,
        UnboundedSender<FromSchedulerMessageT>,
    ) -> SchedulerMessage<TempFileT>,
    disconnected_msg_builder: impl FnOnce(IdT) -> SchedulerMessage<TempFileT>,
    socket_reader_main: impl FnOnce(SchedulerSender<TempFileT>) -> ReaderFutureT,
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

async fn unassigned_connection_main<TempFileFactoryT>(
    socket: TcpStream,
    scheduler_sender: SchedulerSender<TempFileFactoryT::TempFile>,
    id_vendor: Arc<IdVendor>,
    temp_file_factory: TempFileFactoryT,
    log: Logger,
) where
    TempFileFactoryT: TempFileFactory + Send + 'static,
    TempFileFactoryT::TempFile: Send + Sync + 'static,
{
    let mut socket = match socket.set_socket_options() {
        Ok(socket) => socket,
        Err(err) => {
            warn!(log, "error setting socket options"; "error" => %err);
            return;
        }
    };
    match net::read_message_from_async_socket(&mut socket, &log).await {
        Ok(Hello::Client) => {
            let (read_stream, write_stream) = socket.into_split();
            let read_stream = BufReader::new(read_stream);
            let id: ClientId = id_vendor.vend();
            let log = log.new(o!("cid" => id.to_string()));
            let log_clone = log.clone();
            let log_clone2 = log.clone();
            debug!(log, "client connected");
            connection_main(
                scheduler_sender,
                id,
                SchedulerMessage::ClientConnected,
                SchedulerMessage::ClientDisconnected,
                |scheduler_sender| async move {
                    let _ = net::async_socket_reader(
                        read_stream,
                        scheduler_sender,
                        |msg| {
                            assert!(!matches!(&msg, ClientToBroker::JobRequest(_, spec) if spec.must_be_run_locally()));
                            SchedulerMessage::FromClient(id, msg)
                        },
                        &log_clone
                    )
                    .await;
                },
                |scheduler_receiver| async move {
                    let _ = net::async_socket_writer(scheduler_receiver, write_stream, &log_clone2).await;
                },
            )
            .await;
            debug!(log, "client disconnected");
        }
        Ok(Hello::Worker { slots }) => {
            let (read_stream, write_stream) = socket.into_split();
            let read_stream = BufReader::new(read_stream);
            let id: WorkerId = id_vendor.vend();
            let log = log.new(o!("wid" => id.to_string(), "slots" => slots));
            info!(log, "worker connected");
            let log_clone = log.clone();
            let log_clone2 = log.clone();
            connection_main(
                scheduler_sender,
                id,
                |id, sender| SchedulerMessage::WorkerConnected(id, slots as usize, sender),
                SchedulerMessage::WorkerDisconnected,
                |scheduler_sender| async move {
                    let _ = net::async_socket_reader(
                        read_stream,
                        scheduler_sender,
                        |msg| SchedulerMessage::FromWorker(id, msg),
                        &log_clone,
                    )
                    .await;
                },
                |scheduler_receiver| async move {
                    let _ = net::async_socket_writer(scheduler_receiver, write_stream, &log_clone2)
                        .await;
                },
            )
            .await;
            info!(log, "worker disconnected");
        }
        Ok(Hello::Monitor) => {
            let (read_stream, write_stream) = socket.into_split();
            let read_stream = BufReader::new(read_stream);
            let id: MonitorId = id_vendor.vend();
            let log = log.new(o!("mid" => id.to_string()));
            let log_clone = log.clone();
            let log_clone2 = log.clone();
            debug!(log, "monitor connected");
            connection_main(
                scheduler_sender,
                id,
                SchedulerMessage::MonitorConnected,
                SchedulerMessage::MonitorDisconnected,
                |scheduler_sender| async move {
                    let _ = net::async_socket_reader(
                        read_stream,
                        scheduler_sender,
                        |msg| SchedulerMessage::FromMonitor(id, msg),
                        &log_clone,
                    )
                    .await;
                },
                |scheduler_receiver| async move {
                    let _ = net::async_socket_writer(scheduler_receiver, write_stream, &log_clone2)
                        .await;
                },
            )
            .await;
            debug!(log, "monitor disconnected");
        }
        Ok(Hello::ArtifactFetcher) => {
            let log = log.new(o!("afid" => id_vendor.vend::<u32>().to_string()));
            let socket = socket.into_std().unwrap();
            socket.set_nonblocking(false).unwrap();
            thread::spawn(move || -> Result<()> {
                artifact_fetcher::connection_main(socket, scheduler_sender, log)
            });
        }
        Ok(Hello::ArtifactPusher) => {
            let log = log.new(o!("apid" => id_vendor.vend::<u32>().to_string()));
            let socket = socket.into_std().unwrap();
            socket.set_nonblocking(false).unwrap();
            thread::spawn(move || -> Result<()> {
                artifact_pusher::connection_main(socket, scheduler_sender, temp_file_factory, log)
            });
        }
        Err(err) => {
            warn!(log, "error reading hello message"; "error" => %err);
        }
    }
}

/// Main loop for the listener. This should be run on a task of its own. There should be at least
/// one of these in a broker process. It will only return when it encounters an error. Until then,
/// it listens on a socket and spawns new tasks for each client or worker that connects.
pub async fn listener_main<TempFileFactoryT>(
    listener: TcpListener,
    scheduler_sender: SchedulerSender<TempFileFactoryT::TempFile>,
    id_vendor: Arc<IdVendor>,
    temp_file_factory: TempFileFactoryT,
    log: Logger,
) where
    TempFileFactoryT: TempFileFactory + Send + 'static,
    TempFileFactoryT::TempFile: Send + Sync + 'static,
{
    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                let log = log.new(o!("peer_addr" => peer_addr));
                debug!(log, "new connection");
                task::spawn(unassigned_connection_main(
                    socket,
                    scheduler_sender.clone(),
                    id_vendor.clone(),
                    temp_file_factory.clone(),
                    log,
                ));
            }
            Err(err) => {
                error!(log, "error accepting connection"; "error" => %err);
                return;
            }
        }
    }
}
