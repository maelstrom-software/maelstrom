use crate::Result;
use futures::{sink::SinkExt, stream::StreamExt};
use hyper::{Body, Request, Response};
use hyper_tungstenite::{tungstenite, HyperWebsocket};
use std::net::SocketAddrV6;
use tungstenite::Message;

async fn handle_request(mut request: Request<Body>) -> Result<Response<Body>> {
    if hyper_tungstenite::is_upgrade_request(&request) {
        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)?;
        tokio::spawn(async move { serve_websocket(websocket).await.ok() });
        Ok(response)
    } else {
        // XXX here we need to serve up the client application
        Ok(Response::new(Body::from("Hello HTTP!")))
    }
}

async fn serve_websocket(websocket: HyperWebsocket) -> Result<()> {
    let mut websocket = websocket.await?;
    while let Some(message) = websocket.next().await {
        if let Message::Binary(msg) = message? {
            // XXX here we need to handle the client application RPCs
            websocket.send(Message::binary(msg)).await?;
        }
    }

    Ok(())
}

pub async fn run_server(port: Option<u16>) -> Result<()> {
    let address = SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, port.unwrap_or(0), 0, 0);
    let listener = tokio::net::TcpListener::bind(&address).await?;
    println!("web UI listing on {:?}", listener.local_addr()?);

    let mut http = hyper::server::conn::Http::new();
    http.http1_only(true);
    http.http1_keep_alive(true);

    loop {
        let (stream, _) = listener.accept().await?;
        let connection = http
            .serve_connection(stream, hyper::service::service_fn(handle_request))
            .with_upgrades();
        tokio::spawn(async move { connection.await.ok() });
    }
}
