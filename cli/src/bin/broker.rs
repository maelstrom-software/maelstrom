use clap::{value_parser, Parser};

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
        tokio::spawn(async move { meticulous::broker::http::run_server(cli.http_port).await });
        meticulous::broker::main(cli.broker_port).await
    })?;
    Ok(())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
