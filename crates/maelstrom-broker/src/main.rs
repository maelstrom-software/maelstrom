use anyhow::{Context, Result};
use maelstrom_broker::config::Config;
use slog::{info, Logger};
use std::{
    net::{Ipv6Addr, SocketAddrV6},
    process,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn broker_main(config: Config, log: Logger) -> Result<()> {
    let sock_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, *config.port.inner(), 0, 0);
    let listener = TcpListener::bind(sock_addr)
        .await
        .context("binding listener socket")?;

    let sock_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, *config.http_port.inner(), 0, 0);
    let http_listener = TcpListener::bind(sock_addr)
        .await
        .context("binding http listener socket")?;

    let listener_addr = listener
        .local_addr()
        .context("retrieving listener local address")?;
    let http_listener_addr = http_listener
        .local_addr()
        .context("retrieving listener local address")?;
    info!(log, "started";
                    "config" => ?config,
                    "addr" => listener_addr,
                    "http_addr" => http_listener_addr,
                    "pid" => process::id());

    maelstrom_broker::main(
        listener,
        http_listener,
        config.cache_root,
        config.cache_size,
        log.clone(),
    )
    .await;
    info!(log, "exiting");
    Ok(())
}

fn main() -> Result<()> {
    let config = Config::new("maelstrom/broker", "MAELSTROM_BROKER")?;
    maelstrom_util::log::run_with_logger(config.log_level, |log| broker_main(config, log))
}
