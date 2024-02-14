mod close;
mod extract_release_notes;
mod open;

use anyhow::Result;
use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
enum Command {
    Close(close::CliArgs),
    ExtractReleaseNotes(extract_release_notes::CliArgs),
    Open(open::CliArgs),
}

/// Utilities for working with the CHANGELOG.md file.
#[derive(Debug, Parser)]
pub struct CliArgs {
    /// Location of changelog.
    #[arg(long, short, default_value = "CHANGELOG.md")]
    file: Utf8PathBuf,

    #[clap(subcommand)]
    command: Command,
}

pub fn main(args: CliArgs) -> Result<()> {
    match args.command {
        Command::Close(options) => close::main(args.file, options),
        Command::ExtractReleaseNotes(options) => extract_release_notes::main(args.file, options),
        Command::Open(options) => open::main(args.file, options),
    }
}
