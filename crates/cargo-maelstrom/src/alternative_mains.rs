use crate::metadata::{DEFAULT_TEST_METADATA, MAELSTROM_TEST_TOML};
use crate::pattern;
use anyhow::Result;
use cargo_metadata::Package;
use maelstrom_base::Utf8Path;
use maelstrom_util::{fs::Fs, process::ExitCode};
use std::io::Write;

/// Write out a default config file to `<workspace-root>/<MAELSTROM_TEST_TOML>` if nothing exists
/// there already.
pub fn init(workspace_root: &Utf8Path) -> Result<ExitCode> {
    let path = workspace_root.join(MAELSTROM_TEST_TOML);
    if !Fs.exists(&path) {
        Fs.write(&path, DEFAULT_TEST_METADATA)?;
        println!("Wrote default config to {path}.");
        Ok(ExitCode::SUCCESS)
    } else {
        println!("Config already exists at {path}. Doing nothing.");
        Ok(ExitCode::FAILURE)
    }
}

pub fn list_packages(
    workspace_packages: &[&Package],
    include: &[String],
    exclude: &[String],
    out: &mut impl Write,
) -> Result<ExitCode> {
    let filter = pattern::compile_filter(include, exclude)?;
    for package in workspace_packages {
        if crate::filter_package(package, &filter) {
            writeln!(out, "{}", &package.name)?;
        }
    }
    Ok(ExitCode::SUCCESS)
}
