use anyhow::{anyhow, Result};
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    SinkExt as _, StreamExt as _,
};
use gloo_net::websocket::{futures::WebSocket, Message};
use gloo_utils::errors::JsError;
use maelstrom_base::proto::{self, BrokerToMonitor, MonitorToBroker};
use std::cell::RefCell;
use wasm_bindgen_futures::spawn_local;

pub trait MonitorConnection {
    fn send(&self, message: MonitorToBroker) -> Result<()>;
    fn try_recv(&self) -> Result<Option<BrokerToMonitor>>;
}

pub struct RpcConnection {
    send: RefCell<Sender<Message>>,
    recv: RefCell<Receiver<Message>>,
}

impl RpcConnection {
    pub fn new(uri: &str) -> Result<Self, JsError> {
        let socket = WebSocket::open(uri)?;

        let (mut write, mut read) = socket.split();

        let (mut task_send, recv) = mpsc::channel(1000);
        spawn_local(async move {
            while let Some(Ok(msg)) = read.next().await {
                if task_send.send(msg).await.is_err() {
                    break;
                }
            }
        });

        let (send, mut task_recv) = mpsc::channel(1000);
        spawn_local(async move {
            while let Some(message) = task_recv.next().await {
                if write.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(Self {
            send: RefCell::new(send),
            recv: RefCell::new(recv),
        })
    }
}

impl MonitorConnection for RpcConnection {
    fn send(&self, message: MonitorToBroker) -> Result<()> {
        self.send
            .borrow_mut()
            .try_send(Message::Bytes(proto::serialize(&message).unwrap()))?;
        Ok(())
    }

    fn try_recv(&self) -> Result<Option<BrokerToMonitor>> {
        match self.recv.borrow_mut().try_next() {
            Ok(Some(Message::Bytes(b))) => Ok(Some(proto::deserialize(&b)?)),
            Ok(Some(Message::Text(_))) => Err(anyhow!("Unexpected Message::Text")),
            Ok(None) => Err(anyhow!("websocket closed")),
            Err(_) => Ok(None),
        }
    }
}
