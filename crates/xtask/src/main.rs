mod publish;

use anyhow::Result;
use clap::{Parser, Subcommand};

/// Perform a number of different tasks for the Maelstrom project related to building, testing,
/// publishing, etc..
#[derive(Debug, Parser)]
#[clap(bin_name = "cargo-xtask", version)]
struct CliArgs {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Publish(publish::CliArgs),
}

fn main() -> Result<()> {
    match CliArgs::parse().command {
        Command::Publish(options) => publish::main(options),
    }
}
