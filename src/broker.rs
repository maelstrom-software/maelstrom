mod heap;
mod scheduler;

use crate::{
    proto::{self, ClientHello, ClientResponse, Hello, WorkerHello, WorkerRequest},
    ClientId, Error, Result, WorkerId,
};

type WorkerSocketSender = tokio::sync::mpsc::UnboundedSender<WorkerRequest>;
type ClientSocketSender = tokio::sync::mpsc::UnboundedSender<ClientResponse>;
type SchedulerReceiver = tokio::sync::mpsc::UnboundedReceiver<scheduler::Message<()>>;
type SchedulerSender = tokio::sync::mpsc::UnboundedSender<scheduler::Message<()>>;

/// Main loop for the scheduler. This should be run on a task of its own. There should be exactly
/// one of these in a broker process. It will return when all senders associated with the
/// receiver are closed, which will happen when the listener and all outstanding worker and client
/// socket tasks terminate.
///
/// This function ignores any errors it encounters sending a message to a [ClientSocketSender] or
/// [WorkerSocketSender]. The rationale is that this indicates that the socket connection has
/// closed, and there are no more worker tasks to handle that connection. This means that a
/// disconnected message is on its way to notify the scheduler. It is best to just ignore the error
/// in that case. Besides, the [SchedulerDeps] interface doesn't give us a way to return an error,
/// for precisely this reason.
async fn scheduler_main(mut receiver: SchedulerReceiver) {
    impl scheduler::SchedulerDeps for () {
        type ClientSender = ClientSocketSender;
        type WorkerSender = WorkerSocketSender;

        fn send_response_to_client(
            &mut self,
            sender: &mut ClientSocketSender,
            response: ClientResponse,
        ) {
            sender.send(response).ok();
        }

        fn send_request_to_worker(
            &mut self,
            sender: &mut WorkerSocketSender,
            request: WorkerRequest,
        ) {
            sender.send(request).ok();
        }
    }

    let mut scheduler = scheduler::Scheduler::default();
    while let Some(msg) = receiver.recv().await {
        scheduler.receive_message(&mut (), msg);
    }
}

/// Main loop for a client socket. There should be one of these for each connected client socket.
/// This function will run until the client socket is closed. There is no error return since this
/// function will always eventually run into an error. This function will always send a
/// disconnected message to the scheduler, assuming it managed to send a connected message.
async fn client_socket_main(
    read_stream: impl tokio::io::AsyncRead + Send + Unpin + 'static,
    write_stream: impl tokio::io::AsyncWrite + Send + Unpin + 'static,
    scheduler_sender: SchedulerSender,
    id: u32,
    ClientHello { name }: ClientHello,
) {
    println!("client {name} connected and assigned id {id}");

    let id = ClientId(id);
    let (client_socket_sender, client_socket_receiver) = tokio::sync::mpsc::unbounded_channel();
    if scheduler_sender
        .send(scheduler::Message::ClientConnected(
            id,
            client_socket_sender,
        ))
        .is_ok()
    {
        // If we couldn't send a message to the scheduler, it means the scheduler has quit and we
        // should to. Otherwise, start one task for reading and one for writing.
        let mut join_set = tokio::task::JoinSet::new();
        join_set.spawn(proto::socket_reader(
            read_stream,
            scheduler_sender.clone(),
            move |req| scheduler::Message::FromClient(id, req),
        ));
        join_set.spawn(proto::socket_writer(client_socket_receiver, write_stream));

        // Wait for one task to complete and then cancel the other one and wait for it.
        join_set.join_next().await;
        join_set.shutdown().await;

        // Tell the scheduler we're done.
        scheduler_sender
            .send(scheduler::Message::ClientDisconnected(id))
            .ok();
    }

    println!("client {name}:{id:?} disconnected");
}

async fn worker_socket_main(
    read_stream: impl tokio::io::AsyncRead + Send + Unpin + 'static,
    write_stream: impl tokio::io::AsyncWrite + Send + Unpin + 'static,
    scheduler_sender: SchedulerSender,
    id: u32,
    WorkerHello { name, slots }: WorkerHello,
) {
    println!("worker {name} with {slots} slots connected and assigned id {id}");

    let id = WorkerId(id);
    let (worker_socket_sender, worker_socket_receiver) = tokio::sync::mpsc::unbounded_channel();
    if scheduler_sender
        .send(scheduler::Message::WorkerConnected(
            id,
            slots as usize,
            worker_socket_sender,
        ))
        .is_ok()
    {
        let mut join_set = tokio::task::JoinSet::new();
        join_set.spawn(proto::socket_reader(
            read_stream,
            scheduler_sender.clone(),
            move |resp| scheduler::Message::FromWorker(id, resp),
        ));
        join_set.spawn(proto::socket_writer(worker_socket_receiver, write_stream));

        join_set.join_next().await;
        join_set.shutdown().await;
        scheduler_sender
            .send(scheduler::Message::WorkerDisconnected(id))
            .ok();
    }

    println!("worker {name}:{id:?} disconnected");
}

/// Main loop for the listener. This should be run on a task of its own. There should be at least
/// one of these in a broker process. It will only return when it encounters an error. Until then,
/// it listens on a socket and spawns new tasks for each client or worker that connects.
async fn listener_main(port: Option<u16>, scheduler_sender: SchedulerSender) -> Result<()> {
    let sockaddr =
        std::net::SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, port.unwrap_or(0), 0, 0);
    let listener = tokio::net::TcpListener::bind(sockaddr).await?;

    println!("listening on: {}", listener.local_addr()?);

    for id in 0.. {
        let (socket, _) = listener.accept().await?;
        let (read_stream, write_stream) = socket.into_split();
        let mut read_stream = tokio::io::BufReader::new(read_stream);

        let scheduler_sender_clone = scheduler_sender.clone();

        tokio::task::spawn(async move {
            match proto::read_message(&mut read_stream).await? {
                Hello::Client(hello) => {
                    client_socket_main(read_stream, write_stream, scheduler_sender_clone, id, hello)
                        .await
                }
                Hello::Worker(hello) => {
                    worker_socket_main(read_stream, write_stream, scheduler_sender_clone, id, hello)
                        .await
                }
            };
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

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
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
