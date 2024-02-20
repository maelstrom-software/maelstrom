mod changelog;
mod publish;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
enum Command {
    Changelog(changelog::CliArgs),
    Publish(publish::CliArgs),
}

/// Perform a number of different tasks for the Maelstrom project related to building, testing,
/// publishing, etc..
#[derive(Debug, Parser)]
#[clap(bin_name = "cargo-xtask", version)]
#[command(styles = maelstrom_util::clap::styles())]
struct CliArgs {
    #[clap(subcommand)]
    command: Command,
}

fn main() -> Result<()> {
    match CliArgs::parse().command {
        Command::Changelog(options) => changelog::main(options),
        Command::Publish(options) => publish::main(options),
    }
}
