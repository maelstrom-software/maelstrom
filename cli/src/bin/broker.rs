use clap::{value_parser, Parser};
use std::net::SocketAddrV6;

/// The meticulous worker. This process executes subprocesses as directed by the broker.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// The port the broker listens for connections from workers and clients on
    #[arg(
        long,
        value_parser = value_parser!(u16).range(1..)
    )]
    broker_port: Option<u16>,

    /// The port the HTTP UI is served up on
    #[arg(
        long,
        value_parser = value_parser!(u16).range(1..)
    )]
    http_port: Option<u16>,
}

fn main() -> meticulous::Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let sockaddr = std::net::SocketAddrV6::new(
            std::net::Ipv6Addr::UNSPECIFIED,
            cli.broker_port.unwrap_or(0),
            0,
            0,
        );
        let broker_listener = tokio::net::TcpListener::bind(sockaddr).await?;
        let broker_addr = broker_listener.local_addr()?;
        println!("broker listening on: {broker_addr}");

        let sock_addr = SocketAddrV6::new(
            std::net::Ipv6Addr::UNSPECIFIED,
            cli.http_port.unwrap_or(0),
            0,
            0,
        );
        let http_listener = tokio::net::TcpListener::bind(&sock_addr).await?;
        println!("web UI listing on {:?}", http_listener.local_addr()?);

        tokio::spawn(
            async move { meticulous::broker::http::main(broker_addr, http_listener).await },
        );
        meticulous::broker::main(broker_listener).await
    })?;
    Ok(())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
