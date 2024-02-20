mod close;
mod extract_release_notes;
mod open;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
    #[arg(
        long,
        short,
        value_name = "PATH",
        default_value = PathBuf::from("CHANGELOG.md").into_os_string()
    )]
    file: PathBuf,

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
