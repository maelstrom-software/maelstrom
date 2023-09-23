#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
mod wasm {
    use anyhow::Result;
    use futures::{SinkExt as _, StreamExt as _};
    use gloo_net::websocket::{futures::WebSocket, Message};
    use gloo_utils::errors::JsError;
    use meticulous_base::{
        proto::{BrokerToClient, ClientToBroker},
        BrokerStatistics,
    };
    use std::{cell::RefCell, time::Duration};
    use wasm_bindgen_futures::spawn_local;

    pub trait ClientConnection {
        fn send(&self, message: ClientToBroker) -> Result<()>;
        fn try_recv(&self) -> Result<Option<BrokerToClient>>;
    }

    pub struct UiHandler<RpcConnectionT> {
        rpc: RpcConnectionT,
        stats: Option<BrokerStatistics>,
    }

    impl<RpcConnectionT> UiHandler<RpcConnectionT> {
        pub fn new(rpc: RpcConnectionT, _cc: &eframe::CreationContext<'_>) -> Self {
            Self { rpc, stats: None }
        }
    }

    impl<RpcConnectionT: ClientConnection> eframe::App for UiHandler<RpcConnectionT> {
        fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
            egui::CentralPanel::default().show(ctx, |ui| {
                ui.heading("Meticulous UI");
                if let Some(stats) = &self.stats {
                    ui.label(&format!("number of clients: {}", stats.num_clients));
                    ui.label(&format!("number of workers: {}", stats.num_workers));
                    ui.label(&format!("number of requests: {}", stats.num_requests));
                } else {
                    ui.label("loading..");
                }

                self.rpc.send(ClientToBroker::StatisticsRequest).unwrap();

                if let Some(msg) = self.rpc.try_recv().unwrap() {
                    match msg {
                        BrokerToClient::StatisticsResponse(stats) => self.stats = Some(stats),
                        _ => unimplemented!(),
                    }
                }

                ctx.request_repaint_after(Duration::from_millis(500));
            });
        }
    }

    fn window() -> web_sys::Window {
        web_sys::window().expect("no global `window` exists")
    }

    fn host() -> String {
        window().location().host().unwrap()
    }

    struct RpcConnection {
        send: RefCell<futures::channel::mpsc::Sender<Message>>,
        recv: RefCell<futures::channel::mpsc::Receiver<Message>>,
    }

    impl RpcConnection {
        fn new(uri: &str) -> Result<Self, JsError> {
            let socket = WebSocket::open(uri)?;

            let (mut write, mut read) = socket.split();

            let (mut task_send, recv) = futures::channel::mpsc::channel(1000);
            spawn_local(async move {
                while let Some(Ok(msg)) = read.next().await {
                    if task_send.send(msg).await.is_err() {
                        break;
                    }
                }
            });

            let (send, mut task_recv) = futures::channel::mpsc::channel(1000);
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

    impl ClientConnection for RpcConnection {
        fn send(&self, message: ClientToBroker) -> anyhow::Result<()> {
            self.send
                .borrow_mut()
                .try_send(Message::Bytes(bincode::serialize(&message).unwrap()))?;
            Ok(())
        }

        fn try_recv(&self) -> anyhow::Result<Option<BrokerToClient>> {
            match self.recv.borrow_mut().try_next() {
                Ok(Some(Message::Bytes(b))) => Ok(Some(bincode::deserialize(&b)?)),
                Ok(Some(Message::Text(_))) => Err(anyhow::Error::msg("Unexpected Message::Text")),
                Ok(None) => Err(anyhow::Error::msg("websocket closed")),
                Err(_) => Ok(None),
            }
        }
    }

    pub async fn start() -> Result<(), JsError> {
        console_error_panic_hook::set_once();
        wasm_logger::init(wasm_logger::Config::default());

        let uri = format!("ws://{}", host());
        let rpc = RpcConnection::new(&uri)?;

        let runner = eframe::WebRunner::new();
        let web_options = eframe::WebOptions::default();
        runner
            .start(
                "canvas",
                web_options,
                Box::new(|cc| Box::new(UiHandler::new(rpc, cc))),
            )
            .await
            .map_err(|e| JsError::try_from(e).unwrap())?;

        Ok(())
    }
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
pub async fn start() -> Result<(), wasm_bindgen::JsValue> {
    match wasm::start().await {
        Ok(()) => Ok(()),
        Err(e) => panic!("error: {e:?}"),
    }
}
