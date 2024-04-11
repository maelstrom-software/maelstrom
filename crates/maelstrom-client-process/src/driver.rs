use crate::artifact_upload::{ArtifactPusher, ArtifactUploadTracker};
use crate::dispatcher::{self, Dispatcher};
use crate::test::client_driver::SingleThreadedClientDriver;
use anyhow::{anyhow, Context as _, Result};
use async_trait::async_trait;
use maelstrom_base::{
    proto::{ClientToBroker, Hello},
    stats::JobStateCounts,
    ClientJobId, JobOutcomeResult, Sha256Digest,
};
use maelstrom_client_base::{ClientDriverMode, ClientMessageKind};
use maelstrom_util::{config::common::BrokerAddr, net};
use std::{ops::ControlFlow, path::PathBuf};
use tokio::{
    net::{tcp, TcpStream},
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot::Sender as OneShotSender,
        Mutex,
    },
    task::{self, JoinHandle},
};

pub struct ArtifactPushRequest {
    pub path: PathBuf,
    pub digest: Sha256Digest,
}

pub struct DispatcherAdapter {
    stream: tcp::OwnedWriteHalf,
    artifact_pusher: Sender<ArtifactPushRequest>,
}

impl DispatcherAdapter {
    pub fn new(stream: tcp::OwnedWriteHalf, artifact_pusher: Sender<ArtifactPushRequest>) -> Self {
        Self {
            stream,
            artifact_pusher,
        }
    }
}

impl dispatcher::Deps for DispatcherAdapter {
    type JobHandle = OneShotSender<(ClientJobId, JobOutcomeResult)>;

    fn job_done(&self, handle: Self::JobHandle, cjid: ClientJobId, result: JobOutcomeResult) {
        handle.send((cjid, result)).ok();
    }

    type JobStateCountsHandle = OneShotSender<JobStateCounts>;

    fn job_state_counts(&self, handle: Self::JobStateCountsHandle, counts: JobStateCounts) {
        handle.send(counts).ok();
    }

    async fn send_message_to_broker(&mut self, message: ClientToBroker) -> Result<()> {
        net::write_message_to_async_socket(&mut self.stream, message).await
    }

    async fn send_artifact_to_broker(&mut self, digest: Sha256Digest, path: PathBuf) -> Result<()> {
        Ok(self
            .artifact_pusher
            .send(ArtifactPushRequest { path, digest })
            .await?)
    }
}

pub fn new_driver(mode: ClientDriverMode) -> Box<dyn ClientDriver + Send + Sync> {
    match mode {
        ClientDriverMode::MultiThreaded => Box::<MultiThreadedClientDriver>::default(),
        ClientDriverMode::SingleThreaded => Box::<SingleThreadedClientDriver>::default(),
    }
}

pub struct SocketReader {
    stream: tcp::OwnedReadHalf,
    channel: Sender<dispatcher::Message<DispatcherAdapter>>,
}

impl SocketReader {
    fn new(
        stream: tcp::OwnedReadHalf,
        channel: Sender<dispatcher::Message<DispatcherAdapter>>,
    ) -> Self {
        Self { stream, channel }
    }

    pub async fn process_one(&mut self) -> bool {
        let Ok(msg) = net::read_message_from_async_socket(&mut self.stream).await else {
            return false;
        };
        self.channel
            .send(dispatcher::Message::Broker(msg))
            .await
            .is_ok()
    }
}

pub struct ClientDeps {
    pub dispatcher: Dispatcher<DispatcherAdapter>,
    pub dispatcher_receiver: Receiver<dispatcher::Message<DispatcherAdapter>>,
    pub dispatcher_sender: Sender<dispatcher::Message<DispatcherAdapter>>,
    pub artifact_pusher: ArtifactPusher,
    pub socket_reader: SocketReader,
}

impl ClientDeps {
    pub async fn new(
        broker_addr: BrokerAddr,
        upload_tracker: ArtifactUploadTracker,
    ) -> Result<Self> {
        let mut stream = TcpStream::connect(broker_addr.inner())
            .await
            .with_context(|| format!("failed to connect to {broker_addr}"))?;
        net::write_message_to_async_socket(&mut stream, Hello::Client).await?;

        let (dispatcher_sender, dispatcher_receiver) = mpsc::channel(1000);
        let (artifact_send, artifact_recv) = mpsc::channel(1000);
        let (read_half, write_half) = stream.into_split();
        Ok(Self {
            dispatcher: Dispatcher::new(DispatcherAdapter::new(write_half, artifact_send)),
            artifact_pusher: ArtifactPusher::new(broker_addr, artifact_recv, upload_tracker),
            socket_reader: SocketReader::new(read_half, dispatcher_sender.clone()),
            dispatcher_sender,
            dispatcher_receiver,
        })
    }
}

#[async_trait]
pub trait ClientDriver {
    async fn drive(&self, deps: ClientDeps);
    async fn stop(&self) -> Result<()>;

    async fn process_broker_msg_single_threaded(&self, _count: usize) {
        unimplemented!()
    }

    async fn process_client_messages_single_threaded(&self, _wanted: ClientMessageKind) {
        unimplemented!()
    }

    async fn process_artifact_single_threaded(&self) {
        unimplemented!()
    }
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
                    let msg = deps
                        .dispatcher_receiver
                        .recv()
                        .await
                        .ok_or_else(|| anyhow!("dispatcher hangup"))?;
                    cf = deps.dispatcher.receive_message(msg).await?;
                }
                Ok(())
            });
            let pusher_handle = task::spawn(async move {
                while deps.artifact_pusher.process_one().await {}
                deps.artifact_pusher.wait().await
            });
            let reader_handle =
                task::spawn(async move { while deps.socket_reader.process_one().await {} });

            let res = dispatcher_handle.await;
            reader_handle.abort();
            reader_handle.await.ok();
            pusher_handle.await.ok();
            res?
        }));
    }

    async fn stop(&self) -> Result<()> {
        self.handle.lock().await.take().unwrap().await?
    }
}
