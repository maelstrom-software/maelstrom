use anyhow::Result;
use std::os::linux::net::SocketAddrExt as _;
use std::os::unix::net::{SocketAddr, UnixListener};

pub fn main() -> Result<()> {
    maelstrom_util::log::run_with_logger(maelstrom_util::config::common::LogLevel::Debug, |log| {
        let name = format!("maelstrom-client-{}", std::process::id());
        let listener = UnixListener::bind_addr(&SocketAddr::from_abstract_name(name.as_bytes())?)?;
        slog::info!(log, "listening on unix-abstract:{name}");

        let (sock, addr) = listener.accept()?;
        slog::info!(log, "got connection"; "address" => ?addr);

        maelstrom_client_process::client_process_main(sock, Some(log))
    })
}
