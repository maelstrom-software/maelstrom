use crate::artifact_upload::{ArtifactPusher, ArtifactUploadTracker};
use crate::dispatcher::{Dispatcher, DispatcherMessage};
use crate::test::client_driver::SingleThreadedClientDriver;
use anyhow::{Context as _, Result};
use maelstrom_base::proto::Hello;
use maelstrom_client_base::{ClientDriverMode, ClientMessageKind};
use maelstrom_util::{config::BrokerAddr, net};
use std::{
    net::TcpStream,
    sync::mpsc::{self, SyncSender},
    thread::{self, JoinHandle},
};

pub fn new_driver(mode: ClientDriverMode) -> Box<dyn ClientDriver + Send + Sync> {
    match mode {
        ClientDriverMode::MultiThreaded => Box::<MultiThreadedClientDriver>::default(),
        ClientDriverMode::SingleThreaded => Box::<SingleThreadedClientDriver>::default(),
    }
}

pub struct SocketReader {
    stream: TcpStream,
    channel: SyncSender<DispatcherMessage>,
}

impl SocketReader {
    fn new(stream: TcpStream, channel: SyncSender<DispatcherMessage>) -> Self {
        Self { stream, channel }
    }

    pub fn process_one(&mut self) -> bool {
        let Ok(msg) = net::read_message_from_socket(&mut self.stream) else {
            return false;
        };
        self.channel
            .send(DispatcherMessage::BrokerToClient(msg))
            .is_ok()
    }
}

pub struct ClientDeps {
    pub dispatcher: Dispatcher,
    pub artifact_pusher: ArtifactPusher,
    pub socket_reader: SocketReader,
    pub dispatcher_sender: SyncSender<DispatcherMessage>,
}

impl ClientDeps {
    pub fn new(broker_addr: BrokerAddr, upload_tracker: ArtifactUploadTracker) -> Result<Self> {
        let mut stream = TcpStream::connect(broker_addr.inner())
            .with_context(|| format!("failed to connect to {broker_addr}"))?;
        net::write_message_to_socket(&mut stream, Hello::Client)?;

        let (dispatcher_sender, dispatcher_receiver) = mpsc::sync_channel(1000);
        let (artifact_send, artifact_recv) = mpsc::sync_channel(1000);
        let stream_clone = stream.try_clone()?;
        Ok(Self {
            dispatcher: Dispatcher::new(dispatcher_receiver, stream_clone, artifact_send),
            artifact_pusher: ArtifactPusher::new(broker_addr, artifact_recv, upload_tracker),
            socket_reader: SocketReader::new(stream, dispatcher_sender.clone()),
            dispatcher_sender,
        })
    }
}

pub trait ClientDriver {
    fn drive(&mut self, deps: ClientDeps);
    fn stop(&mut self) -> Result<()>;

    fn process_broker_msg_single_threaded(&self, _count: usize) {
        unimplemented!()
    }

    fn process_client_messages_single_threaded(&self) -> Option<ClientMessageKind> {
        unimplemented!()
    }

    fn process_artifact_single_threaded(&self) {
        unimplemented!()
    }
}

#[derive(Default)]
struct MultiThreadedClientDriver {
    handle: Option<JoinHandle<Result<()>>>,
}

impl ClientDriver for MultiThreadedClientDriver {
    fn drive(&mut self, mut deps: ClientDeps) {
        assert!(self.handle.is_none());
        self.handle = Some(thread::spawn(move || {
            thread::scope(|scope| {
                let dispatcher_handle = scope.spawn(move || {
                    while deps.dispatcher.process_one()? {}
                    deps.dispatcher.stream.shutdown(std::net::Shutdown::Both)?;
                    Ok(())
                });
                scope.spawn(move || while deps.artifact_pusher.process_one(scope) {});
                scope.spawn(move || while deps.socket_reader.process_one() {});
                dispatcher_handle.join().unwrap()
            })
        }));
    }

    fn stop(&mut self) -> Result<()> {
        self.handle.take().unwrap().join().unwrap()
    }
}
