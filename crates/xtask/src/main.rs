mod changelog;
mod distribute;
mod publish;
mod book;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
enum Command {
    Book(book::CliArgs),
    Changelog(changelog::CliArgs),
    Publish(publish::CliArgs),
    Distribute(distribute::CliArgs),
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
        Command::Book(options) => book::main(options),
        Command::Changelog(options) => changelog::main(options),
        Command::Publish(options) => publish::main(options),
        Command::Distribute(options) => distribute::main(options),
    }
}
