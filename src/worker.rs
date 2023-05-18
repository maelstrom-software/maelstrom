use crate::{
    channel_reader,
    proto::{read_message, write_message},
    task::spawn,
    Error, ExecutionDetails, ExecutionId, ExecutionResult, Result, WorkerHello, WorkerRequest,
    WorkerResponse,
};

pub mod dispatcher;
pub mod executor;

#[derive(Debug)]
pub enum ToDispatcher {
    FromBroker(WorkerRequest),
    FromExecutor(ExecutionId, ExecutionResult),
}

impl From<WorkerRequest> for ToDispatcher {
    fn from(request: WorkerRequest) -> ToDispatcher {
        ToDispatcher::FromBroker(request)
    }
}

/// Loop reading messages from a socket and writing them to an mpsc channel.
/// If this function encounters an error reading from the socket, it will return that error. On the
/// other hand, if it encounters an error writing to the sender -- which indicates that there is no
/// longer a receiver for the channel -- it will return Ok(()).
async fn socket_reader<T, U>(
    mut socket: (impl tokio::io::AsyncRead + Unpin),
    channel: tokio::sync::mpsc::UnboundedSender<U>,
) -> Result<()>
where
    T: serde::de::DeserializeOwned,
    U: From<T>,
{
    loop {
        let msg = read_message::<T>(&mut socket).await?;
        if channel.send(msg.into()).is_err() {
            return Ok(());
        }
    }
}

/// Loop reading messages from an mpsc channel and writing them to a socket. This will
/// return Ok(()) when all producers have closed their mpsc channel senders and there are no more
/// messages to read.
async fn socket_writer<T>(
    mut channel: tokio::sync::mpsc::UnboundedReceiver<T>,
    mut socket: (impl tokio::io::AsyncWrite + Unpin),
) -> Result<()>
where
    T: serde::Serialize,
{
    while let Some(msg) = channel.recv().await {
        write_message(&mut socket, msg).await?;
    }
    Ok(())
}

struct DispatcherAdapter {
    dispatcher_sender: tokio::sync::mpsc::UnboundedSender<ToDispatcher>,
    broker_socket_sender: tokio::sync::mpsc::UnboundedSender<WorkerResponse>,
}

impl dispatcher::DispatcherDeps for DispatcherAdapter {
    type ExecutionHandle = executor::Handle;

    fn start_execution(
        &mut self,
        id: ExecutionId,
        details: ExecutionDetails,
    ) -> Self::ExecutionHandle {
        let sender = self.dispatcher_sender.clone();
        executor::start(details, move |result| {
            sender.send(ToDispatcher::FromExecutor(id, result)).ok();
        })
    }

    fn send_response_to_broker(&mut self, message: WorkerResponse) {
        self.broker_socket_sender.send(message).ok();
    }
}

async fn dispatcher_main(
    slots: u32,
    dispatcher_receiver: tokio::sync::mpsc::UnboundedReceiver<ToDispatcher>,
    dispatcher_sender: tokio::sync::mpsc::UnboundedSender<ToDispatcher>,
    broker_socket_sender: tokio::sync::mpsc::UnboundedSender<WorkerResponse>,
) -> Result<()> {
    let adapter = DispatcherAdapter {
        dispatcher_sender,
        broker_socket_sender,
    };
    let mut dispatcher = dispatcher::Dispatcher::new(adapter, slots);
    channel_reader::run(dispatcher_receiver, move |msg| {
        dispatcher.receive_message(msg)
    })
    .await;
    Ok(())
}

async fn signal_handler(kind: tokio::signal::unix::SignalKind) -> Result<()> {
    tokio::signal::unix::signal(kind)?.recv().await;
    Err(Error::msg(format!("received signal {:?}", kind)))
}

/// The main function for the worker. This should be called on a task of its own. It will return
/// when a signal is received or when one of the worker tasks completes because of an error.
pub async fn main(name: String, slots: u32, broker_addr: std::net::SocketAddr) -> Result<()> {
    let (read_stream, mut write_stream) = tokio::net::TcpStream::connect(&broker_addr)
        .await?
        .into_split();
    let read_stream = tokio::io::BufReader::new(read_stream);

    write_message(&mut write_stream, WorkerHello { name, slots }).await?;

    let (dispatcher_sender, dispatcher_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (writer_sender, writer_receiver) = tokio::sync::mpsc::unbounded_channel();

    let dispatcher_sender_clone = dispatcher_sender.clone();

    tokio::select! {
        res = spawn(async move {
            socket_reader::<WorkerRequest, ToDispatcher>(read_stream, dispatcher_sender_clone).await
        }) => res?,
        res = spawn(async move {
            socket_writer(writer_receiver, write_stream).await
        }) => res?,
        res = spawn(async move {
            dispatcher_main(slots, dispatcher_receiver, dispatcher_sender, writer_sender).await
        }) => res?,
        res = spawn(async {
            signal_handler(tokio::signal::unix::SignalKind::interrupt()).await
        }) => res?,
        res = spawn(async {
            signal_handler(tokio::signal::unix::SignalKind::terminate()).await
        }) => res?,
    }
}
