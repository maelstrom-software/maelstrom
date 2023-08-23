use clap::{value_parser, Parser};

/// The meticulous worker. This process executes subprocesses as directed by the broker.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// The number of execution slots available. Most program executions will take one job slot.
    #[arg(
        short,
        long,
        value_parser = value_parser!(u16).range(1..)
    )]
    port: Option<u16>,
}

fn main() -> meticulous::Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async { meticulous::broker::main(cli.port).await })?;
    Ok(())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
