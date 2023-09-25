use crate::{proto, Result};
use futures::stream::{SplitSink, SplitStream};
use futures::{sink::SinkExt, stream::StreamExt};
use hyper::service::Service;
use hyper::upgrade::Upgraded;
use hyper::{Body, Request, Response};
use hyper_tungstenite::WebSocketStream;
use hyper_tungstenite::{tungstenite, HyperWebsocket};
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tungstenite::Message;

const WASM_TAR: &[u8] = include_bytes!("../../../target/web.tar");

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

#[derive(Clone)]
pub struct TarHandler {
    map: HashMap<String, &'static [u8]>,
}

impl TarHandler {
    pub fn from_memory(bytes: &'static [u8]) -> Self {
        let mut map = HashMap::new();
        let mut ar = tar::Archive::new(bytes);
        for entry in ar.entries().unwrap() {
            let entry = entry.unwrap();
            let header = entry.header();

            let path = header.path().unwrap().to_str().unwrap().into();
            let start = entry.raw_file_position() as usize;
            let end = start + header.size().unwrap() as usize;
            map.insert(path, &bytes[start..end]);
        }
        Self { map }
    }

    fn get_file(&self, path: &str) -> Response<Body> {
        let mut path = format!(".{}", path);

        if path == "./" {
            path = "./index.html".into();
        }

        self.map
            .get(&path[..])
            .map(|&b| {
                Response::builder()
                    .status(200)
                    .header("Content-Type", mime_for_path(&path))
                    .body(Body::from(b))
                    .unwrap()
            })
            .unwrap_or(
                Response::builder()
                    .status(404)
                    .body(Body::from(&b""[..]))
                    .unwrap(),
            )
    }
}

#[derive(Clone)]
struct Handler {
    tar_handler: TarHandler,
    broker_addr: SocketAddr,
}

impl Service<Request<Body>> for Handler {
    type Response = Response<Body>;
    type Error = crate::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        let resp = (|| {
            if hyper_tungstenite::is_upgrade_request(&request) {
                let broker_addr = self.broker_addr;
                let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)?;
                tokio::spawn(async move { serve_websocket(websocket, broker_addr).await.ok() });
                Ok(response)
            } else {
                Ok(self.tar_handler.get_file(&request.uri().to_string()))
            }
        })();

        Box::pin(async { resp })
    }
}

async fn serve_broker_to_client(
    broker_read: &mut ReadHalf<TcpStream>,
    ws_write: &mut SplitSink<WebSocketStream<Upgraded>, Message>,
) -> Result<()> {
    while let proto::BrokerToClient::UiResponse(resp) = proto::read_message(broker_read).await? {
        ws_write
            .send(Message::binary(bincode::serialize(&resp).unwrap()))
            .await?
    }
    Ok(())
}

async fn serve_client_to_broker(
    broker_write: &mut WriteHalf<TcpStream>,
    ws_read: &mut SplitStream<WebSocketStream<Upgraded>>,
) -> Result<()> {
    while let Some(Ok(Message::Binary(msg))) = ws_read.next().await {
        let msg = bincode::deserialize(&msg)?;
        proto::write_message(broker_write, proto::ClientToBroker::UiRequest(msg)).await?;
    }

    Ok(())
}

async fn serve_websocket(websocket: HyperWebsocket, broker_addr: SocketAddr) -> Result<()> {
    let websocket = websocket.await?;
    let broker_socket = TcpStream::connect(broker_addr).await?;

    let (mut broker_read, mut broker_write) = tokio::io::split(broker_socket);
    let (mut ws_write, mut ws_read) = websocket.split();

    proto::write_message(&mut broker_write, proto::Hello::Client).await?;

    tokio::select! {
        res = serve_broker_to_client(&mut broker_read, &mut ws_write) => res?,
        res = serve_client_to_broker(&mut broker_write, &mut ws_read) => res?,
    }

    Ok(())
}

pub async fn main(broker_addr: SocketAddr, listener: tokio::net::TcpListener) -> Result<()> {
    let mut http = hyper::server::conn::Http::new();
    http.http1_only(true);
    http.http1_keep_alive(true);

    let tar_handler = TarHandler::from_memory(WASM_TAR);

    loop {
        let (stream, _) = listener.accept().await?;
        let tar_handler = tar_handler.clone();
        let connection = http
            .serve_connection(
                stream,
                Handler {
                    tar_handler: tar_handler.clone(),
                    broker_addr,
                },
            )
            .with_upgrades();
        tokio::spawn(async move { connection.await.ok() });
    }
}
