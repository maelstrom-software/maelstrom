use clap::{builder::NonEmptyStringValueParser, value_parser, Parser};
use std::net::SocketAddr;

fn parse_socket_addr(arg: &str) -> std::io::Result<SocketAddr> {
    use std::net::ToSocketAddrs as _;
    let addrs: Vec<SocketAddr> = arg.to_socket_addrs()?.collect();
    // It's not clear how we could end up with an empty iterator. We'll assume
    // that's impossible until proven wrong.
    Ok(*addrs.get(0).unwrap())
}

/// The meticulous worker. This process executes subprocesses as directed by the broker.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Socket address of broker. Examples: 127.0.0.1:5000 host.example.com:2000".
    #[arg(value_parser = parse_socket_addr)]
    broker: SocketAddr,

    /// Name of the worker provided to the broker. The broker will reject workers with duplicate
    /// names.
    #[arg(
        short,
        long,
        default_value_t = gethostname::gethostname().into_string().unwrap(),
        value_parser = NonEmptyStringValueParser::new()
    )]
    name: String,

    /// The number of execution slots available. Most program executions will take one job slot.
    #[arg(
        short,
        long,
        default_value_t = num_cpus::get().try_into().unwrap(),
        value_parser = value_parser!(u32).range(1..1000)
    )]
    slots: u32,
}

fn main() -> meticulous::Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        meticulous::worker::main(cli.name, cli.slots as usize, cli.broker).await
    })?;
    Ok(())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
