mod heap;
pub mod scheduler; // XXX make private

use crate::{proto, Error, Result};

async fn listener_main(port: Option<u16>) -> Result<()> {
    let sockaddr =
        std::net::SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, port.unwrap_or(0), 0, 0);
    let listener = tokio::net::TcpListener::bind(sockaddr).await?;
    println!("listening on: {}", listener.local_addr()?);

    loop {
        let (mut socket, _) = listener.accept().await?;
        let hello: proto::Hello = proto::read_message(&mut socket).await?;
        println!("got: {hello:?}");
        std::mem::forget(socket);
    }
}

async fn signal_handler(kind: tokio::signal::unix::SignalKind) -> Result<()> {
    tokio::signal::unix::signal(kind)?.recv().await;
    Err(Error::msg(format!("received signal {:?}", kind)))
}

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub async fn main(port: Option<u16>) -> Result<()> {
    let mut join_set = tokio::task::JoinSet::new();
    join_set.spawn(async move { listener_main(port).await });
    join_set.spawn(async { signal_handler(tokio::signal::unix::SignalKind::interrupt()).await });
    join_set.spawn(async { signal_handler(tokio::signal::unix::SignalKind::terminate()).await });

    loop {
        join_set
            .join_next()
            .await
            .expect("at least one task should return an error")??;
    }
}
