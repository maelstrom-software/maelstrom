use super::SchedulerMessage;
use meticulous_base::proto;
use meticulous_util::{
    error::{Error, Result},
    net,
};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

pub struct IdVendor {
    id: AtomicU32,
}

impl Default for IdVendor {
    fn default() -> Self {
        IdVendor {
            id: AtomicU32::new(0),
        }
    }
}

impl IdVendor {
    pub fn vend<T: From<u32>>(&self) -> T {
        self.id.fetch_add(1, Ordering::SeqCst).into()
    }
}

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
// XXX: Unit test this function.
pub async fn socket_main<IdT, FromSchedulerMessageT, ReaderFutureT, WriterFutureT>(
    scheduler_sender: UnboundedSender<SchedulerMessage>,
    id: IdT,
    connected_msg_builder: impl FnOnce(IdT, UnboundedSender<FromSchedulerMessageT>) -> SchedulerMessage,
    disconnected_msg_builder: impl FnOnce(IdT) -> SchedulerMessage,
    socket_reader_main: impl FnOnce(UnboundedSender<SchedulerMessage>) -> ReaderFutureT,
    socket_writer_main: impl FnOnce(UnboundedReceiver<FromSchedulerMessageT>) -> WriterFutureT,
) where
    IdT: Copy + Send + 'static,
    FromSchedulerMessageT: serde::Serialize + Send + 'static,
    ReaderFutureT: std::future::Future<Output = Result<()>> + Send + 'static,
    WriterFutureT: std::future::Future<Output = Result<()>> + Send + 'static,
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
// XXX: Unit test this function.
pub async fn listener_main(
    listener: tokio::net::TcpListener,
    scheduler_sender: UnboundedSender<SchedulerMessage>,
    id_vendor: Arc<IdVendor>,
) -> Result<()> {
    loop {
        let (mut socket, peer_addr) = listener.accept().await?;
        let scheduler_sender = scheduler_sender.clone();
        let id_vendor = id_vendor.clone();

        tokio::task::spawn(async move {
            let hello = net::read_message_from_socket(&mut socket).await?;
            println!("{hello:?} from {peer_addr} connected");
            match hello {
                proto::Hello::Client => {
                    let (read_stream, write_stream) = socket.into_split();
                    let read_stream = tokio::io::BufReader::new(read_stream);
                    let id = id_vendor.vend();
                    socket_main(
                        scheduler_sender,
                        id,
                        SchedulerMessage::ClientConnected,
                        SchedulerMessage::ClientDisconnected,
                        |scheduler_sender| {
                            net::socket_reader(read_stream, scheduler_sender, move |req| {
                                SchedulerMessage::FromClient(id, req)
                            })
                        },
                        |scheduler_receiver| net::socket_writer(scheduler_receiver, write_stream),
                    )
                    .await
                }
                proto::Hello::Worker { slots } => {
                    let (read_stream, write_stream) = socket.into_split();
                    let read_stream = tokio::io::BufReader::new(read_stream);
                    let id = id_vendor.vend();
                    socket_main(
                        scheduler_sender,
                        id,
                        |id, sender| SchedulerMessage::WorkerConnected(id, slots as usize, sender),
                        SchedulerMessage::WorkerDisconnected,
                        |scheduler_sender| {
                            net::socket_reader(read_stream, scheduler_sender, move |req| {
                                SchedulerMessage::FromWorker(id, req)
                            })
                        },
                        |scheduler_receiver| net::socket_writer(scheduler_receiver, write_stream),
                    )
                    .await
                }
                proto::Hello::ClientArtifact { .. } | proto::Hello::WorkerArtifact { .. } => {
                    todo!("artifact handling not yet implemented")
                }
            }
            println!("{hello:?} from {peer_addr} disconnected");
            Ok::<(), Error>(())
        });
    }
}
