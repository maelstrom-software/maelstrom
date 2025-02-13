use crate::{pattern, CacheDir, GoPackage, GoTestCollector, ProjectDir};
use anyhow::Result;
use maelstrom_test_runner::{
    ui::{UiMessage, UiSender},
    CollectTests as _, TestPackage as _,
};
use maelstrom_util::{process::ExitCode, root::Root};

/// Returns `true` if the given `GoPackage` matches the given pattern
fn filter_package(package: &GoPackage, p: &pattern::Pattern) -> bool {
    let c = pattern::Context {
        package_import_path: package.0.import_path.clone(),
        package_path: package.0.root_relative_path().display().to_string(),
        package_name: package.0.name.clone(),
        case: None,
    };
    pattern::interpret_pattern(p, &c).unwrap_or(true)
}

pub fn list_packages(
    ui: UiSender,
    project_dir: &Root<ProjectDir>,
    cache_dir: &Root<CacheDir>,
    include: &[String],
    exclude: &[String],
) -> Result<ExitCode> {
    ui.send(UiMessage::UpdateEnqueueStatus("listing packages...".into()));

    let collector = GoTestCollector::new(cache_dir, Default::default(), project_dir);
    let packages = collector.get_packages(&ui)?;
    let filter = pattern::compile_filter(include, exclude)?;
    for package in packages {
        if filter_package(&package, &filter) {
            ui.send(UiMessage::List(package.name().into()));
        }
    }
    Ok(ExitCode::SUCCESS)
}
