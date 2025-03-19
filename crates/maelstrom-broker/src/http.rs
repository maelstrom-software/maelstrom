//! The task main and all associated code to implement the HTTP server for the broker.
//!
//! The HTTP server is responsible for doing two things.
//!
//! First, it serves up the actual website. This is prebuilt, including all of the Wasm, and put in
//! a tar file. The tar file is then embedded in this module as compile time.
//!
//! Second, it handles WebSockets. These are treated just like monitor connections.
use crate::{connection, scheduler_task, IdVendor};
use anyhow::{Error, Result};
use bytes::Bytes;
use futures::{
    sink::SinkExt,
    stream::{SplitSink, SplitStream, StreamExt},
};
use http_body_util::{
    //    combinators::BoxBody,
    //    BodyExt as _,
    //    Empty,
    Full,
    //    StreamBody
};
use hyper::{
    body::Incoming, server::conn::http1, service::Service, upgrade::Upgraded, Request, Response,
};
use hyper_tungstenite::{tungstenite, HyperWebsocket, WebSocketStream};
use hyper_util::rt::tokio::TokioIo;
use maelstrom_base::{
    proto::{self, BrokerToMonitor, MonitorToBroker},
    MonitorId,
};
use maelstrom_web::WASM_TAR;
use slog::{debug, error, o, Logger};
use std::{
    collections::HashMap,
    future::Future,
    path::Path,
    pin::Pin,
    sync::Arc,
    //    task::{Context, Poll},
};
use tar::Archive;
use tokio::{net::TcpListener, sync::mpsc::UnboundedReceiver};
use tungstenite::Message;

pub struct TarHandler {
    map: HashMap<String, &'static [u8]>,
}

impl TarHandler {
    pub fn from_embedded() -> Self {
        let bytes = WASM_TAR;
        let mut map = HashMap::new();
        let mut ar = Archive::new(bytes);
        for entry in ar.entries().unwrap() {
            let entry = entry.unwrap();
            let header = entry.header();
            let path = format!("./{}", header.path().unwrap().to_str().unwrap());
            let start = entry.raw_file_position() as usize;
            let end = start + header.size().unwrap() as usize;
            map.insert(path, &bytes[start..end]);
        }
        Self { map }
    }

    fn get_file(&self, path: &str, log: Logger) -> Response<Full<Bytes>> {
        fn mime_for_path(path: &str) -> &'static str {
            if let Some(ext) = Path::new(path).extension() {
                match &ext.to_str().unwrap().to_lowercase()[..] {
                    "wasm" => return "application/wasm",
                    "js" => return "text/javascript",
                    "html" => return "text/html",
                    _ => (),
                }
            }
            "application/octet-stream"
        }

        let mut path = format!(".{}", path);

        if path == "./" {
            path = "./index.html".into();
        }

        self.map
            .get(&path)
            .map(|&b| {
                debug!(log, "received http get request"; "path" => %path, "resp" => 200);
                Response::builder()
                    .status(200)
                    .header("Content-Type", mime_for_path(&path))
                    .body(Full::from(b))
                    .unwrap()
            })
            .unwrap_or_else(|| {
                debug!(log, "received http get request"; "path" => %path, "resp" => 404);
                Response::builder()
                    .status(404)
                    .body(Full::from(&b""[..]))
                    .unwrap()
            })
    }
}

/// Looping reading from `scheduler_task_receiver` and writing to `socket`. If an error is encountered,
/// return immediately.
async fn websocket_writer(
    mut scheduler_task_receiver: UnboundedReceiver<BrokerToMonitor>,
    mut socket: SplitSink<WebSocketStream<TokioIo<Upgraded>>, Message>,
) {
    while let Some(msg) = scheduler_task_receiver.recv().await {
        if socket
            .send(Message::binary(proto::serialize(&msg).unwrap()))
            .await
            .is_err()
        {
            break;
        }
    }
}

/// Looping reading from `socket` and writing to `scheduler_task_sender`. If an error is encountered,
/// return immediately.
async fn websocket_reader<TempFileT>(
    mut socket: SplitStream<WebSocketStream<TokioIo<Upgraded>>>,
    scheduler_task_sender: scheduler_task::Sender<TempFileT>,
    mid: MonitorId,
) {
    while let Some(Ok(Message::Binary(msg))) = socket.next().await {
        let Ok(msg) = proto::deserialize(&msg) else {
            break;
        };
        let msg = match msg {
            MonitorToBroker::StatisticsRequest => {
                scheduler_task::Message::StatisticsRequestFromMonitor(mid)
            }
            MonitorToBroker::StopRequest => scheduler_task::Message::StopRequestFromMonitor,
        };
        if scheduler_task_sender.send(msg).is_err() {
            break;
        }
    }
}

/// Task main loop for handing a websocket. This just calls into [connection::connection_main].
async fn websocket_main<TempFileT>(
    websocket: HyperWebsocket,
    scheduler_task_sender: scheduler_task::Sender<TempFileT>,
    id_vendor: Arc<IdVendor>,
    log: Logger,
) where
    TempFileT: Send + 'static,
{
    let Ok(websocket) = websocket.await else {
        return;
    };
    let (write_stream, read_stream) = websocket.split();
    let id: MonitorId = id_vendor.vend();
    let log = log.new(o!("mid" => id.to_string(), "websocket" => true));
    debug!(
        log,
        "http connection upgraded to websocket monitor connection"
    );
    connection::connection_main(
        scheduler_task_sender,
        id,
        scheduler_task::Message::MonitorConnected,
        scheduler_task::Message::MonitorDisconnected,
        |scheduler_task_sender| websocket_reader(read_stream, scheduler_task_sender, id),
        |scheduler_task_receiver| websocket_writer(scheduler_task_receiver, write_stream),
    )
    .await;
    debug!(log, "received websocket monitor disconnect")
}

struct Handler<TempFileT> {
    tar_handler: Arc<TarHandler>,
    scheduler_task_sender: scheduler_task::Sender<TempFileT>,
    id_vendor: Arc<IdVendor>,
    log: Logger,
}

impl<TempFileT> Service<Request<Incoming>> for Handler<TempFileT>
where
    TempFileT: Send + Sync + 'static,
{
    type Response = Response<Full<Bytes>>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn call(&self, mut request: Request<Incoming>) -> Self::Future {
        let resp = (|| {
            if hyper_tungstenite::is_upgrade_request(&request) {
                let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)?;
                tokio::spawn(websocket_main(
                    websocket,
                    self.scheduler_task_sender.clone(),
                    self.id_vendor.clone(),
                    self.log.clone(),
                ));
                Ok(response)
            } else {
                Ok(self
                    .tar_handler
                    .get_file(&request.uri().to_string(), self.log.clone()))
            }
        })();

        Box::pin(async { resp })
    }
}

pub async fn listener_main<TempFileT>(
    listener: TcpListener,
    scheduler_task_sender: scheduler_task::Sender<TempFileT>,
    id_vendor: Arc<IdVendor>,
    log: Logger,
) where
    TempFileT: Send + Sync + 'static,
{
    let http = http1::Builder::new();

    let tar_handler = Arc::new(TarHandler::from_embedded());

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let log = log.new(o!("peer_addr" => peer_addr));
                debug!(log, "received http connection");
                let connection = http
                    .serve_connection(
                        TokioIo::new(stream),
                        Handler {
                            tar_handler: tar_handler.clone(),
                            scheduler_task_sender: scheduler_task_sender.clone(),
                            id_vendor: id_vendor.clone(),
                            log,
                        },
                    )
                    .with_upgrades();
                tokio::spawn(async move { connection.await.ok() });
            }
            Err(err) => {
                error!(log, "error accepting http connection"; "error" => %err);
                return;
            }
        }
    }
}
