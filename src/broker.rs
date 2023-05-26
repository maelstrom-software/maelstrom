mod heap;
mod scheduler;

use crate::{
    proto::{self, ClientHello, Hello, WorkerHello},
    ClientId, Error, Result, WorkerId,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

mod scheduler_task {
    use super::scheduler;
    use crate::{
        proto::{ClientRequest, ClientResponse, WorkerRequest, WorkerResponse},
        ClientId, Result, WorkerId,
    };
    use std::collections::HashMap;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

    #[derive(Debug)]
    pub enum Message {
        ClientConnected(ClientId, UnboundedSender<ClientResponse>),
        ClientDisconnected(ClientId),
        FromClient(ClientId, ClientRequest),
        WorkerConnected(WorkerId, usize, UnboundedSender<WorkerRequest>),
        WorkerDisconnected(WorkerId),
        FromWorker(WorkerId, WorkerResponse),
    }

    #[derive(Default)]
    struct SchedulerAdapter {
        clients: HashMap<ClientId, UnboundedSender<ClientResponse>>,
        workers: HashMap<WorkerId, UnboundedSender<WorkerRequest>>,
    }

    impl scheduler::SchedulerDeps for SchedulerAdapter {
        fn send_response_to_client(&mut self, id: ClientId, response: ClientResponse) {
            self.clients.get_mut(&id).unwrap().send(response).ok();
        }

        fn send_request_to_worker(&mut self, id: WorkerId, request: WorkerRequest) {
            self.workers.get_mut(&id).unwrap().send(request).ok();
        }
    }

    pub async fn main(mut receiver: UnboundedReceiver<Message>) -> Result<()> {
        let mut adapter = SchedulerAdapter::default();
        let mut scheduler = scheduler::Scheduler::default();
        while let Some(msg) = receiver.recv().await {
            let msg = match msg {
                Message::ClientConnected(id, sender) => {
                    assert!(
                        adapter.clients.insert(id, sender).is_none(),
                        "duplicate client id {id:?}"
                    );
                    scheduler::Message::ClientConnected(id)
                }
                Message::ClientDisconnected(id) => scheduler::Message::ClientDisconnected(id),
                Message::FromClient(id, request) => scheduler::Message::FromClient(id, request),
                Message::WorkerConnected(id, slots, sender) => {
                    assert!(
                        adapter.workers.insert(id, sender).is_none(),
                        "duplicate worker id {id:?}"
                    );
                    scheduler::Message::WorkerConnected(id, slots)
                }
                Message::WorkerDisconnected(id) => scheduler::Message::WorkerDisconnected(id),
                Message::FromWorker(id, response) => scheduler::Message::FromWorker(id, response),
            };
            scheduler.receive_message(&mut adapter, msg);
        }
        Ok(())
    }
}

async fn socket_main(
    socket: tokio::net::TcpStream,
    scheduler_sender: UnboundedSender<scheduler_task::Message>,
    id: u32,
) -> Result<()> {
    let (read_stream, write_stream) = socket.into_split();
    let mut read_stream = tokio::io::BufReader::new(read_stream);

    let hello: Hello = proto::read_message(&mut read_stream).await?;
    match hello {
        Hello::Client(ClientHello { name }) => {
            println!("client {name} connected and assigned id {id}");
            let id = ClientId(id);
            let (client_socket_sender, client_socket_receiver) = unbounded_channel();
            scheduler_sender.send(scheduler_task::Message::ClientConnected(
                id,
                client_socket_sender,
            ))?;
            let scheduler_sender_clone = scheduler_sender.clone();

            let mut join_set = tokio::task::JoinSet::new();
            join_set.spawn(async move {
                proto::socket_reader(read_stream, scheduler_sender_clone, |req| {
                    scheduler_task::Message::FromClient(id, req)
                })
                .await
            });
            join_set.spawn(async move {
                proto::socket_writer(client_socket_receiver, write_stream).await
            });

            join_set.join_next().await.unwrap()??;
            join_set.shutdown().await;
            scheduler_sender.send(scheduler_task::Message::ClientDisconnected(id))?;
        }
        Hello::Worker(WorkerHello { name, slots }) => {
            println!("worker {name} with {slots} slots connected and assigned id {id}");
            let id = WorkerId(id);
            let (worker_socket_sender, worker_socket_receiver) = unbounded_channel();
            scheduler_sender.send(scheduler_task::Message::WorkerConnected(
                id,
                slots as usize,
                worker_socket_sender,
            ))?;
            let scheduler_sender_clone = scheduler_sender.clone();

            let mut join_set = tokio::task::JoinSet::new();
            join_set.spawn(async move {
                proto::socket_reader(read_stream, scheduler_sender_clone, |resp| {
                    scheduler_task::Message::FromWorker(id, resp)
                })
                .await
            });
            join_set.spawn(async move {
                proto::socket_writer(worker_socket_receiver, write_stream).await
            });

            join_set.join_next().await.unwrap()??;
            join_set.shutdown().await;
            scheduler_sender.send(scheduler_task::Message::WorkerDisconnected(id))?;
        }
    }
    Ok(())
}

async fn listener_main(
    port: Option<u16>,
    scheduler_sender: UnboundedSender<scheduler_task::Message>,
) -> Result<()> {
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
    let (scheduler_sender, scheduler_receiver) = unbounded_channel();

    let mut join_set = tokio::task::JoinSet::new();
    join_set.spawn(async move { listener_main(port, scheduler_sender).await });
    join_set.spawn(async move { scheduler_task::main(scheduler_receiver).await });
    join_set.spawn(async { signal_handler(tokio::signal::unix::SignalKind::interrupt()).await });
    join_set.spawn(async { signal_handler(tokio::signal::unix::SignalKind::terminate()).await });

    loop {
        join_set
            .join_next()
            .await
            .expect("at least one task should return an error")??;
    }
}
