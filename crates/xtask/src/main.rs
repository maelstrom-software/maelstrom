mod book;
mod changelog;
mod clone_benchmark;
mod distribute;
mod publish;
mod update_stats_spreadsheet;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
enum Command {
    Book(book::CliArgs),
    Changelog(changelog::CliArgs),
    Publish(publish::CliArgs),
    Distribute(distribute::CliArgs),
    UpdateStatsSpreadsheet(update_stats_spreadsheet::CliArgs),
    CloneBenchmark(clone_benchmark::CliArgs),
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
        Command::UpdateStatsSpreadsheet(options) => update_stats_spreadsheet::main(options),
        Command::CloneBenchmark(options) => clone_benchmark::main(options),
    }
}
