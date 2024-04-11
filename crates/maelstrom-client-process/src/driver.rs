use crate::{
    artifact_upload::{ArtifactPusher, ArtifactUploadTracker},
    dispatcher::{self, Dispatcher},
    local_broker::{self, LocalBroker},
    DispatcherAdapter, LocalBrokerAdapter,
};
use anyhow::{anyhow, Context as _, Result};
use async_trait::async_trait;
use maelstrom_base::proto::{ClientToBroker, Hello};
use maelstrom_util::{config::common::BrokerAddr, net, sync};
use std::ops::ControlFlow;
use tokio::{
    net::{
        tcp::{self, OwnedWriteHalf},
        TcpStream,
    },
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    task::{self, JoinHandle},
};

pub fn new_driver() -> Box<dyn ClientDriver + Send + Sync> {
    Box::<MultiThreadedClientDriver>::default()
}

pub struct SocketReader {
    stream: tcp::OwnedReadHalf,
    channel: UnboundedSender<local_broker::Message>,
}

impl SocketReader {
    fn new(stream: tcp::OwnedReadHalf, channel: UnboundedSender<local_broker::Message>) -> Self {
        Self { stream, channel }
    }

    pub async fn process_one(&mut self) -> bool {
        let Ok(message) = net::read_message_from_async_socket(&mut self.stream).await else {
            return false;
        };
        self.channel
            .send(local_broker::Message::Broker(message))
            .is_ok()
    }
}

pub struct ClientDeps {
    dispatcher: Dispatcher<DispatcherAdapter>,
    local_broker: LocalBroker<LocalBrokerAdapter>,
    artifact_pusher: ArtifactPusher,
    socket_reader: SocketReader,
    pub dispatcher_sender: UnboundedSender<dispatcher::Message<DispatcherAdapter>>,
    dispatcher_receiver: UnboundedReceiver<dispatcher::Message<DispatcherAdapter>>,
    local_broker_receiver: UnboundedReceiver<local_broker::Message>,
    broker_socket_writer: OwnedWriteHalf,
    broker_receiver: UnboundedReceiver<ClientToBroker>,
}

impl ClientDeps {
    pub async fn new(
        broker_addr: BrokerAddr,
        upload_tracker: ArtifactUploadTracker,
    ) -> Result<Self> {
        let mut broker_socket = TcpStream::connect(broker_addr.inner())
            .await
            .with_context(|| format!("failed to connect to {broker_addr}"))?;
        net::write_message_to_async_socket(&mut broker_socket, Hello::Client).await?;

        let (artifact_pusher_sender, artifact_pusher_receiver) = mpsc::unbounded_channel();
        let (broker_socket_reader, broker_socket_writer) = broker_socket.into_split();

        let (dispatcher_sender, dispatcher_receiver) = mpsc::unbounded_channel();
        let (broker_sender, broker_receiver) = mpsc::unbounded_channel();
        let (local_broker_sender, local_broker_receiver) = mpsc::unbounded_channel();

        let dispatcher_adapter = DispatcherAdapter {
            local_broker_sender: local_broker_sender.clone(),
        };
        let dispatcher = Dispatcher::new(dispatcher_adapter);
        let local_broker_adapter = LocalBrokerAdapter {
            dispatcher_sender: dispatcher_sender.clone(),
            broker_sender,
            artifact_pusher_sender,
        };
        let local_broker = LocalBroker::new(local_broker_adapter);
        let socket_reader = SocketReader::new(broker_socket_reader, local_broker_sender);
        let artifact_pusher =
            ArtifactPusher::new(broker_addr, artifact_pusher_receiver, upload_tracker);

        Ok(Self {
            artifact_pusher,
            socket_reader,
            dispatcher,
            local_broker,
            local_broker_receiver,
            dispatcher_sender,
            dispatcher_receiver,
            broker_socket_writer,
            broker_receiver,
        })
    }
}

#[async_trait]
pub trait ClientDriver {
    async fn drive(&self, deps: ClientDeps);
    async fn stop(&self) -> Result<()>;
}

#[derive(Default)]
struct MultiThreadedClientDriver {
    handle: Mutex<Option<JoinHandle<Result<()>>>>,
}

#[async_trait]
impl ClientDriver for MultiThreadedClientDriver {
    async fn drive(&self, mut deps: ClientDeps) {
        let mut locked_handle = self.handle.lock().await;
        assert!(locked_handle.is_none());
        *locked_handle = Some(task::spawn(async move {
            let dispatcher_handle = task::spawn(async move {
                let mut cf = ControlFlow::Continue(());
                while cf.is_continue() {
                    let message = deps
                        .dispatcher_receiver
                        .recv()
                        .await
                        .ok_or_else(|| anyhow!("dispatcher hangup"))?;
                    cf = deps.dispatcher.receive_message(message);
                }
                Ok(())
            });
            let pusher_handle = task::spawn(async move {
                while deps.artifact_pusher.process_one().await {}
                deps.artifact_pusher.wait().await
            });
            let reader_handle =
                task::spawn(async move { while deps.socket_reader.process_one().await {} });
            let local_broker_handle = task::spawn(sync::channel_reader(
                deps.local_broker_receiver,
                move |msg| deps.local_broker.receive_message(msg),
            ));
            let socket_writer_handle = task::spawn(net::async_socket_writer(
                deps.broker_receiver,
                deps.broker_socket_writer,
                |_| {},
            ));

            let res = dispatcher_handle.await;
            reader_handle.abort();
            reader_handle.await.ok();
            pusher_handle.await.ok();
            socket_writer_handle.await.ok();
            local_broker_handle.await.ok();
            res?
        }));
    }

    async fn stop(&self) -> Result<()> {
        self.handle.lock().await.take().unwrap().await?
    }
}
