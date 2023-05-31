//! Code for the broker binary.

mod scheduler;

use crate::{channel_reader, proto, ClientId, Error, Result, WorkerId};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

/// The production implementation of [scheduler::SchedulerDeps]. This implementation just hands the
/// message to the provided sender. No state is required, so we just implemented it for [()].
impl scheduler::SchedulerDeps for () {
    type ClientSender = UnboundedSender<proto::ClientResponse>;
    type WorkerSender = UnboundedSender<proto::WorkerRequest>;

    fn send_response_to_client(
        &mut self,
        sender: &mut Self::ClientSender,
        response: proto::ClientResponse,
    ) {
        sender.send(response).ok();
    }

    fn send_request_to_worker(
        &mut self,
        sender: &mut Self::WorkerSender,
        request: proto::WorkerRequest,
    ) {
        sender.send(request).ok();
    }
}

/// The production scheduler message type. Some [scheduler::Message] arms contain a
/// [scheduler::SchedulerDeps], so it's defined as a generic type. But in this module, we only use
/// one implementation of [scheduler::SchedulerDeps].
type SchedulerMessage = scheduler::Message<()>;

/// Main loop for the scheduler. This should be run on a task of its own. There should be exactly
/// one of these in a broker process. It will return when all senders associated with the
/// receiver are closed, which will happen when the listener and all outstanding worker and client
/// socket tasks terminate.
///
/// This function ignores any errors it encounters sending a message to an [UnboundedSender]. The
/// rationale is that this indicates that the socket connection has closed, and there are no more
/// worker tasks to handle that connection. This means that a disconnected message is on its way to
/// notify the scheduler. It is best to just ignore the error in that case. Besides, the
/// [scheduler::SchedulerDeps] interface doesn't give us a way to return an error, for precisely
/// this reason.
async fn scheduler_main(receiver: UnboundedReceiver<SchedulerMessage>) {
    let mut scheduler = scheduler::Scheduler::default();
    channel_reader::run(receiver, |msg| scheduler.receive_message(&mut (), msg)).await;
}

/// Main loop for a client or worker socket. There should be one of these for each connected client
/// or worker socket. This function will run until the client is closed. There is no error return
/// since this function will always eventually run into an error.
// XXX: Unit test this function.
async fn socket_main<I, S, R>(
    read_stream: impl tokio::io::AsyncRead + Send + Unpin + 'static,
    write_stream: impl tokio::io::AsyncWrite + Send + Unpin + 'static,
    scheduler_sender: UnboundedSender<SchedulerMessage>,
    id: I,
    connected_msg: impl FnOnce(I, UnboundedSender<S>) -> SchedulerMessage,
    transform_msg: impl Fn(I, R) -> SchedulerMessage + Send + 'static,
    disconnected_msg: impl FnOnce(I) -> SchedulerMessage,
) where
    I: Copy + Send + 'static,
    S: serde::Serialize + Send + 'static,
    R: serde::de::DeserializeOwned + 'static,
{
    let (socket_sender, socket_receiver) = tokio::sync::mpsc::unbounded_channel();

    // First, tell the scheduler that a client/worker has connected. This messages needs to be
    // sent before any messages from this client/worker.
    if scheduler_sender
        .send(connected_msg(id, socket_sender))
        .is_err()
    {
        // If we couldn't send a message to the scheduler, it means the scheduler has quit and we
        // should too.
        return;
    }

    // Next, spawn two tasks to read and write from the socket. Plumb the message queues
    // appropriately.
    let mut join_set = tokio::task::JoinSet::new();
    join_set.spawn(proto::socket_reader(
        read_stream,
        scheduler_sender.clone(),
        move |req| transform_msg(id, req),
    ));
    join_set.spawn(proto::socket_writer(socket_receiver, write_stream));

    // Wait for one task to complete and then cancel the other one and wait for it.
    join_set.join_next().await;
    join_set.shutdown().await;

    // Tell the scheduler we're done. We do this after waiting for all tasks to complete, since we
    // need to ensure that all messages sent by the socket_reader arrive before this one.
    scheduler_sender.send(disconnected_msg(id)).ok();
}

/// Main loop for the listener. This should be run on a task of its own. There should be at least
/// one of these in a broker process. It will only return when it encounters an error. Until then,
/// it listens on a socket and spawns new tasks for each client or worker that connects.
// XXX: Unit test this function.
async fn listener_main(
    port: Option<u16>,
    scheduler_sender: UnboundedSender<SchedulerMessage>,
) -> Result<()> {
    let sockaddr =
        std::net::SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, port.unwrap_or(0), 0, 0);
    let listener = tokio::net::TcpListener::bind(sockaddr).await?;

    println!("listening on: {}", listener.local_addr()?);

    for id in 0.. {
        let (socket, peer_addr) = listener.accept().await?;
        let (read_stream, write_stream) = socket.into_split();
        let mut read_stream = tokio::io::BufReader::new(read_stream);

        let scheduler_sender_clone = scheduler_sender.clone();

        tokio::task::spawn(async move {
            let hello = proto::read_message(&mut read_stream).await?;
            println!("{hello:?} from {peer_addr} connected, assigned id: {id}");
            match hello {
                proto::Hello::Client { .. } => {
                    socket_main(
                        read_stream,
                        write_stream,
                        scheduler_sender_clone,
                        ClientId(id),
                        SchedulerMessage::ClientConnected,
                        SchedulerMessage::FromClient,
                        SchedulerMessage::ClientDisconnected,
                    )
                    .await
                }
                proto::Hello::Worker { slots, .. } => {
                    socket_main(
                        read_stream,
                        write_stream,
                        scheduler_sender_clone,
                        WorkerId(id),
                        |id, sender| SchedulerMessage::WorkerConnected(id, slots as usize, sender),
                        SchedulerMessage::FromWorker,
                        SchedulerMessage::WorkerDisconnected,
                    )
                    .await
                }
            }
            println!("{hello:?} from {peer_addr}, id {id}, disconnected");
            Ok::<(), Error>(())
        });
    }
    unreachable!();
}

/// "Main loop" for a signal handler. This function will block until it receives the indicated
/// signal, then it will return an error.
async fn signal_handler(kind: tokio::signal::unix::SignalKind) -> Result<()> {
    tokio::signal::unix::signal(kind)?.recv().await;
    Err(Error::msg(format!("received signal {:?}", kind)))
}

/// The main function for the broker. This should be called on a task of its own. It will return
/// if there is an error establishing the listener socket, when a signal is received, or when the
/// listener socket returns an error at accept time.
pub async fn main(port: Option<u16>) -> Result<()> {
    let (scheduler_sender, scheduler_receiver) = tokio::sync::mpsc::unbounded_channel();

    let mut join_set = tokio::task::JoinSet::new();
    join_set.spawn(listener_main(port, scheduler_sender));
    join_set.spawn(async move {
        scheduler_main(scheduler_receiver).await;
        Ok(())
    });
    join_set.spawn(signal_handler(tokio::signal::unix::SignalKind::interrupt()));
    join_set.spawn(signal_handler(tokio::signal::unix::SignalKind::terminate()));

    // Loop until we get the first error return from a task, then return that.
    loop {
        join_set
            .join_next()
            .await
            .expect("at least one task should return an error")
            .expect("no task should panic or be canceled")?;
    }
}
