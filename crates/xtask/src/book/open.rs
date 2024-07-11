use anyhow::{bail, Result};
use clap::Parser;
use std::{path::PathBuf, process::Command};

/// Get book ready for changes after a release.
#[derive(Debug, Parser)]
pub struct CliArgs {}

pub fn main(books: PathBuf, _args: CliArgs) -> Result<()> {
    let mut head_path = books.clone();
    head_path.push("head");
    let mut latest_path = books.clone();
    latest_path.push("latest");

    let status = Command::new("cp")
        .arg("-rpH")
        .arg(&latest_path)
        .arg(&head_path)
        .status()?;
    if !status.success() {
        bail!("cp failed");
    }

    let status = Command::new("git").arg("add").arg(&head_path).status()?;
    if !status.success() {
        bail!("git add failed");
    }

    Ok(())
}
