use anyhow::Result;
use camino::Utf8PathBuf;
use clap::Parser;

/// Get changelog ready for changes after a release.
#[derive(Debug, Parser)]
pub struct CliArgs;

pub fn main(changelog: Utf8PathBuf, _args: CliArgs) -> Result<()> {
    println!("opening {changelog}");
    Ok(())
}
