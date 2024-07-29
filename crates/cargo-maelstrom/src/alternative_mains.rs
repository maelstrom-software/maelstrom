use crate::{pattern, CargoArtifactKey};
use anyhow::Result;
use cargo_metadata::Package as CargoPackage;
use maelstrom_util::process::ExitCode;
use std::io::Write;

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
