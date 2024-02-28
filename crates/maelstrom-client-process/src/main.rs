use anyhow::Result;
use slog::Drain as _;
use std::os::linux::net::SocketAddrExt as _;
use std::os::unix::net::{SocketAddr, UnixListener};

pub fn main() -> Result<()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, slog::o!());

    let name = format!("maelstrom-client-{}", std::process::id());
    let listener = UnixListener::bind_addr(&SocketAddr::from_abstract_name(name.as_bytes())?)?;
    slog::info!(log, "listening on unix-abstract:{name}");

    let (sock, addr) = listener.accept()?;
    slog::info!(log, "got connection"; "address" => ?addr);

    maelstrom_client_process::run_process_client(sock, Some(log))
}
