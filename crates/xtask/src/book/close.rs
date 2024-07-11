use anyhow::{bail, Result};
use clap::Parser;
use std::{
    path::PathBuf,
    process::Command,
    os::unix::fs as unix_fs,
    fs,
};

/// Close book to prepare for release.
#[derive(Debug, Parser)]
pub struct CliArgs {
    /// Version to release.
    version: String,
}

pub fn main(books: PathBuf, args: CliArgs) -> Result<()> {
    let mut head_path = books.clone();
    head_path.push("head");
    let mut version_path = books.clone();
    version_path.push(&args.version);
    let mut latest_path = books.clone();
    latest_path.push("latest");

    let status = Command::new("git").arg("mv").arg(&head_path).arg(&version_path).status()?;
    if !status.success() {
	bail!("git mv failed");
    }

    fs::remove_file(&latest_path)?;
    unix_fs::symlink(&args.version, &latest_path)?;

    let status = Command::new("git").arg("add").arg(&latest_path).status()?;
    if !status.success() {
	bail!("git mv failed");
    }

    Ok(())
}
