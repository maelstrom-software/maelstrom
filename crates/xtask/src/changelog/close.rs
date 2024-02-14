use anyhow::Result;
use camino::Utf8PathBuf;
use clap::Parser;

/// Close changelog to prepare for release.
#[derive(Debug, Parser)]
pub struct CliArgs {
    /// Release version.
    version: String,
}

pub fn main(changelog: Utf8PathBuf, args: CliArgs) -> Result<()> {
    println!("closing {changelog} for {}", args.version);
    Ok(())
}
