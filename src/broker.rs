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

    fn send_request_to_worker(&mut self, sender: &mut WorkerSocketSender, request: WorkerRequest) {
        sender.send(request).ok();
    }
}

pub async fn scheduler_main(mut receiver: SchedulerReceiver) -> Result<()> {
    let mut scheduler = scheduler::Scheduler::default();
    while let Some(msg) = receiver.recv().await {
        scheduler.receive_message(&mut (), msg);
    }
    Ok(())
}

async fn socket_main(
    socket: tokio::net::TcpStream,
    scheduler_sender: SchedulerSender,
    id: u32,
) -> Result<()> {
    let (read_stream, write_stream) = socket.into_split();
    let mut read_stream = tokio::io::BufReader::new(read_stream);

    let hello: Hello = proto::read_message(&mut read_stream).await?;
    match hello {
        Hello::Client(ClientHello { name }) => {
            println!("client {name} connected and assigned id {id}");
            let id = ClientId(id);
            let (client_socket_sender, client_socket_receiver) =
                tokio::sync::mpsc::unbounded_channel();
            scheduler_sender.send(scheduler::Message::ClientConnected(
                id,
                client_socket_sender,
            ))?;
            let scheduler_sender_clone = scheduler_sender.clone();

            let mut join_set = tokio::task::JoinSet::new();
            join_set.spawn(async move {
                proto::socket_reader(read_stream, scheduler_sender_clone, |req| {
                    scheduler::Message::FromClient(id, req)
                })
                .await
            });
            join_set.spawn(async move {
                proto::socket_writer(client_socket_receiver, write_stream).await
            });

            join_set.join_next().await.unwrap()??;
            join_set.shutdown().await;
            scheduler_sender.send(scheduler::Message::ClientDisconnected(id))?;
        }
        Hello::Worker(WorkerHello { name, slots }) => {
            println!("worker {name} with {slots} slots connected and assigned id {id}");
            let id = WorkerId(id);
            let (worker_socket_sender, worker_socket_receiver) =
                tokio::sync::mpsc::unbounded_channel();
            scheduler_sender.send(scheduler::Message::WorkerConnected(
                id,
                slots as usize,
                worker_socket_sender,
            ))?;
            let scheduler_sender_clone = scheduler_sender.clone();

            let mut join_set = tokio::task::JoinSet::new();
            join_set.spawn(async move {
                proto::socket_reader(read_stream, scheduler_sender_clone, |resp| {
                    scheduler::Message::FromWorker(id, resp)
                })
                .await
            });
            join_set.spawn(async move {
                proto::socket_writer(worker_socket_receiver, write_stream).await
            });

            join_set.join_next().await.unwrap()??;
            join_set.shutdown().await;
            scheduler_sender.send(scheduler::Message::WorkerDisconnected(id))?;
        }
    }
    Ok(())
}

async fn listener_main(port: Option<u16>, scheduler_sender: SchedulerSender) -> Result<()> {
    let sockaddr =
        std::net::SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, port.unwrap_or(0), 0, 0);
    let listener = tokio::net::TcpListener::bind(sockaddr).await?;
    println!("listening on: {}", listener.local_addr()?);

    let mut id = 0;

    loop {
        let (socket, peer_address) = listener.accept().await?;
        println!("got connection from {peer_address}");
        let scheduler_sender_clone = scheduler_sender.clone();
        tokio::task::spawn(async move { socket_main(socket, scheduler_sender_clone, id).await });
        id += 1;
    }
}

async fn signal_handler(kind: tokio::signal::unix::SignalKind) -> Result<()> {
    tokio::signal::unix::signal(kind)?.recv().await;
    Err(Error::msg(format!("received signal {:?}", kind)))
}

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub async fn main(port: Option<u16>) -> Result<()> {
    let (scheduler_sender, scheduler_receiver) = tokio::sync::mpsc::unbounded_channel();

    let mut join_set = tokio::task::JoinSet::new();
    join_set.spawn(async move { listener_main(port, scheduler_sender).await });
    join_set.spawn(async move { scheduler_main(scheduler_receiver).await });
    join_set.spawn(async { signal_handler(tokio::signal::unix::SignalKind::interrupt()).await });
    join_set.spawn(async { signal_handler(tokio::signal::unix::SignalKind::terminate()).await });

    loop {
        join_set
            .join_next()
            .await
            .expect("at least one task should return an error")??;
    }
}
