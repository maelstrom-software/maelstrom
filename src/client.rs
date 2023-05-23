use crate::{proto, Result};

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub async fn main(name: String, broker_addr: std::net::SocketAddr) -> Result<()> {
    let (read_stream, mut write_stream) = tokio::net::TcpStream::connect(&broker_addr)
        .await?
        .into_split();
    let _read_stream = tokio::io::BufReader::new(read_stream);

    proto::write_message(
        &mut write_stream,
        proto::Hello::Client(proto::ClientHello { name }),
    )
    .await?;

    Ok(())
}
