use super::MAELSTROM_TEST_TOML;
use crate::{pattern, CargoArtifactKey};
use anyhow::Result;
use cargo_metadata::Package as CargoPackage;
use maelstrom_base::Utf8Path;
use maelstrom_test_runner::metadata::DEFAULT_TEST_METADATA;
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

/// Returns `true` if the given `CargoPackage` matches the given pattern
pub fn filter_package(package: &CargoPackage, p: &pattern::Pattern) -> bool {
    let c = pattern::Context {
        package: package.name.clone(),
        artifact: None,
        case: None,
    };
    pattern::interpret_pattern(p, &c).unwrap_or(true)
}

pub fn list_packages(
    workspace_packages: &[&CargoPackage],
    include: &[String],
    exclude: &[String],
    out: &mut impl Write,
) -> Result<ExitCode> {
    let filter = pattern::compile_filter(include, exclude)?;
    for package in workspace_packages {
        if filter_package(package, &filter) {
            writeln!(out, "{}", &package.name)?;
        }
    }
    Ok(ExitCode::SUCCESS)
}

pub fn list_binaries(
    workspace_packages: &[&CargoPackage],
    include: &[String],
    exclude: &[String],
    out: &mut impl Write,
) -> Result<ExitCode> {
    let filter = pattern::compile_filter(include, exclude)?;
    for package in workspace_packages {
        for target in &package.targets {
            let artifact_key = CargoArtifactKey::from(target);
            let c = pattern::Context {
                package: package.name.clone(),
                artifact: Some(pattern::Artifact {
                    name: artifact_key.name,
                    kind: artifact_key.kind,
                }),
                case: None,
            };
            if pattern::interpret_pattern(&filter, &c).unwrap_or(true) && target.test {
                let target_kind = pattern::ArtifactKind::from_target(target);
                let mut binary_name = String::new();
                if target.name != package.name {
                    binary_name += " ";
                    binary_name += &target.name;
                }
                writeln!(out, "{}{} ({})", &package.name, binary_name, target_kind)?;
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

pub fn client_bg_proc() -> Result<ExitCode> {
    maelstrom_client::bg_proc_main()?;
    Ok(ExitCode::from(0))
}
