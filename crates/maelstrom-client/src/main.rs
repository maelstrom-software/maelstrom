use anyhow::Result;
use maelstrom_util::log::LoggerFactory;
use std::os::linux::net::SocketAddrExt as _;
use std::os::unix::net::{SocketAddr, UnixListener};

pub fn main() -> Result<()> {
    maelstrom_client_process::clone_into_pid_and_user_namespace()?;

    maelstrom_util::log::run_with_logger(maelstrom_util::config::common::LogLevel::Debug, |log| {
        let name = format!("maelstrom-client-{}", std::process::id());
        let listener = UnixListener::bind_addr(&SocketAddr::from_abstract_name(name.as_bytes())?)?;
        slog::info!(log, "listening on unix-abstract:{name}");

        println!("{name}");

        let (sock, addr) = listener.accept()?;
        slog::info!(log, "got connection"; "address" => ?addr);

        let res = maelstrom_client_process::main_after_clone(
            sock,
            LoggerFactory::FromLogger(log.clone()),
        );
        slog::info!(log, "shutting down"; "res" => ?res);
        res
    })
}
