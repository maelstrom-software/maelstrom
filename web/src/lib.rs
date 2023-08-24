#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
mod wasm {
    use futures::{SinkExt as _, StreamExt as _};
    use gloo_net::websocket::{futures::WebSocket, Message};
    use gloo_utils::errors::JsError;
    use meticulous_ui::UiHandler;
    use std::cell::RefCell;
    use wasm_bindgen_futures::spawn_local;

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
                    if let Err(_) = task_send.send(msg).await {
                        break;
                    }
                }
            });

            let (send, mut task_recv) = futures::channel::mpsc::channel(1000);
            spawn_local(async move {
                while let Some(message) = task_recv.next().await {
                    if let Err(_) = write.send(message).await {
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

    impl meticulous_ui::ClientRpcConnection for RpcConnection {
        fn send(&self, message: meticulous_ui::Request) -> meticulous_ui::Result<()> {
            self.send
                .borrow_mut()
                .try_send(Message::Bytes(bincode::serialize(&message).unwrap()))?;
            Ok(())
        }

        fn try_recv(&self) -> meticulous_ui::Result<Option<meticulous_ui::Response>> {
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
