use crate::pattern;
use anyhow::Result;
use cargo_metadata::Package;
use maelstrom_util::process::ExitCode;
use std::io::Write;

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
