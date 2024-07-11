mod close;
mod open;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Subcommand)]
enum Command {
    Close(close::CliArgs),
    Open(open::CliArgs),
}

/// Utilities for working with the book directories.
#[derive(Debug, Parser)]
pub struct CliArgs {
    /// Location of the directory containing book versions.
    #[arg(
        long,
        short,
        value_name = "PATH",
        default_value = PathBuf::from("doc/book/").into_os_string()
    )]
    path: PathBuf,

    #[clap(subcommand)]
    command: Command,
}

pub fn main(args: CliArgs) -> Result<()> {
    match args.command {
        Command::Close(options) => close::main(args.path, options),
        Command::Open(options) => open::main(args.path, options),
    }
}
