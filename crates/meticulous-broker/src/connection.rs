use super::{
    scheduler_task::{scheduler::Message, SchedulerMessage, SchedulerSender},
    IdVendor,
};
use meticulous_base::proto;
use meticulous_util::{error::Error, net};
use std::{net::Shutdown, sync::Arc};
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

/// Main loop for the listener. This should be run on a task of its own. There should be at least
/// one of these in a broker process. It will only return when it encounters an error. Until then,
/// it listens on a socket and spawns new tasks for each client or worker that connects.
pub async fn listener_main(
    listener: tokio::net::TcpListener,
    scheduler_sender: SchedulerSender,
    id_vendor: Arc<IdVendor>,
) {
    while let Ok((mut socket, peer_addr)) = listener.accept().await {
        let scheduler_sender = scheduler_sender.clone();
        let id_vendor = id_vendor.clone();

        tokio::task::spawn(async move {
            let hello = net::read_message_from_async_socket(&mut socket).await?;
            println!("{hello:?} from {peer_addr} connected");
            match hello {
                proto::Hello::Client => {
                    let (read_stream, write_stream) = socket.into_split();
                    let read_stream = tokio::io::BufReader::new(read_stream);
                    let id = id_vendor.vend();
                    connection_main(
                        scheduler_sender,
                        id,
                        SchedulerMessage::ClientConnected,
                        SchedulerMessage::ClientDisconnected,
                        |scheduler_sender| {
                            net::async_socket_reader(read_stream, scheduler_sender, move |req| {
                                SchedulerMessage::FromClient(id, req)
                            })
                        },
                        |scheduler_receiver| {
                            net::async_socket_writer(scheduler_receiver, write_stream)
                        },
                    )
                    .await
                }
                proto::Hello::Worker { slots } => {
                    let (read_stream, write_stream) = socket.into_split();
                    let read_stream = tokio::io::BufReader::new(read_stream);
                    let id = id_vendor.vend();
                    connection_main(
                        scheduler_sender,
                        id,
                        |id, sender| SchedulerMessage::WorkerConnected(id, slots as usize, sender),
                        SchedulerMessage::WorkerDisconnected,
                        |scheduler_sender| {
                            net::async_socket_reader(read_stream, scheduler_sender, move |req| {
                                SchedulerMessage::FromWorker(id, req)
                            })
                        },
                        |scheduler_receiver| {
                            net::async_socket_writer(scheduler_receiver, write_stream)
                        },
                    )
                    .await
                }
                proto::Hello::WorkerArtifact { ref digest } => {
                    use std::io::Write;
                    let mut socket = socket.into_std().unwrap();
                    socket.set_nonblocking(false).unwrap();
                    socket.shutdown(Shutdown::Read).unwrap();
                    let (channel_sender, channel_receiver) = std::sync::mpsc::channel();
                    scheduler_sender
                        .send(Message::GetArtifactForWorker(
                            digest.clone(),
                            channel_sender,
                        ))
                        .unwrap();
                    if let Some(path) = channel_receiver.recv().unwrap() {
                        let mut f = std::fs::File::open(path).unwrap();
                        socket.write_all(&[1u8]).unwrap();
                        std::io::copy(&mut f, &mut socket)?;
                        scheduler_sender
                            .send(Message::DecrementRefcount(digest.clone()))
                            .unwrap();
                    }
                }
                proto::Hello::ClientArtifact { ref digest } => {
                    use std::io::Write;
                    let mut socket = socket.into_std().unwrap();
                    socket.set_nonblocking(false).unwrap();
                    // XXX get the cache's preferred directory and use tempfile_in().
                    let mut tmp = tempfile::Builder::new()
                        .prefix(&digest.to_string())
                        .suffix(".tar.gz")
                        .tempfile()?;
                    // XXX Validate the file.
                    let size = std::io::copy(&mut socket, &mut tmp)?;
                    let (_, path) = tmp.keep()?;
                    scheduler_sender.send(Message::GotArtifact(digest.clone(), path, size))?;
                    socket.write_all(&[1u8]).unwrap();
                }
            }
            println!("{hello:?} from {peer_addr} disconnected");
            Ok::<(), Error>(())
        });
    }
}
