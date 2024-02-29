use anyhow::{bail, Result};
use cargo_metadata::{camino::Utf8Path, semver::Version, Metadata, MetadataCommand};
use clap::{ArgAction::Count, Parser};
use reqwest::blocking::Client;
use std::{collections::HashMap, process::Command};

#[derive(Debug, Eq, PartialEq)]
struct PackageInfo<'a> {
    to_publish: bool,
    path: &'a Utf8Path,
    version: &'a Version,
    dependencies: Vec<&'a str>, // Only intra-workspace dependencies are included.
    license: Option<&'a str>,
    description: Option<&'a str>,
    homepage: Option<&'a str>,
    documentation: Option<&'a str>,
    repository: Option<&'a str>,
    readme: Option<&'a Utf8Path>,
}

type Packages<'a> = HashMap<&'a str, PackageInfo<'a>>;

/// Return a map from all of the packages in the workspace to some relevant information about them.
/// Only intra-workspace dependencies are included.
fn extract_workspace_packages(metadata: &Metadata) -> Packages {
    let packages = Packages::from_iter(metadata.workspace_packages().iter().map(|pkg| {
        (
            pkg.name.as_str(),
            PackageInfo {
                to_publish: !pkg.publish.as_ref().is_some_and(Vec::is_empty),
                path: pkg.manifest_path.parent().unwrap(),
                version: &pkg.version,
                dependencies: pkg.dependencies.iter().map(|d| d.name.as_str()).collect(),
                license: pkg.license.as_deref(),
                description: pkg.description.as_deref(),
                homepage: pkg.homepage.as_deref(),
                documentation: pkg.documentation.as_deref(),
                repository: pkg.repository.as_deref(),
                readme: pkg.readme.as_deref(),
            },
        )
    }));
    Packages::from_iter(
        packages
            .iter()
            .map(|(&pkg, info @ PackageInfo { dependencies, .. })| {
                (
                    pkg,
                    PackageInfo {
                        dependencies: dependencies
                            .iter()
                            .copied()
                            .filter(|dep| packages.contains_key(dep))
                            .collect(),
                        ..*info
                    },
                )
            }),
    )
}

/// Return a vector of packages that aren't marked as `publish = false`. Return an error if any
/// publishable package has a depdendency that is unpublishable.
fn get_publishable_packages<'a>(packages: &Packages<'a>) -> Result<Vec<&'a str>> {
    packages
        .iter()
        .filter(|(_, info)| info.to_publish)
        .map(|(package, info)| {
            for dependency in &info.dependencies {
                // Some dependencies may be to packages outside of the workspace.
                if !packages.get(dependency).unwrap().to_publish {
                    bail!(
                        "cannot publish {package} because its dependency \
                            {dependency} isn't publishable",
                    );
                }
            }
            if info.license.is_none() {
                bail!("cannot publish {package} because it doesn't have a license field")
            }
            if info.description.is_none() {
                bail!("cannot publish {package} because it doesn't have a description field")
            }
            if info.homepage.is_none() {
                bail!("cannot publish {package} because it doesn't have a homepage field")
            }
            if info.documentation.is_none() {
                bail!("cannot publish {package} because it doesn't have a documentation field")
            }
            if info.repository.is_none() {
                bail!("cannot publish {package} because it doesn't have a repository field")
            }
            if info.readme.is_none() {
                bail!("cannot publish {package} because it doesn't have a README")
            }
            Ok(*package)
        })
        .collect()
}

#[derive(Debug, Eq, PartialEq)]
enum TopologicalSortResult<T> {
    Ordering(Vec<T>),
    Cycle(Vec<T>),
}

fn topological_sort<T, I, F>(items: I, get_dependencies: F) -> TopologicalSortResult<T>
where
    I: IntoIterator<Item = T>,
    F: Fn(&T) -> Vec<T>,
    T: Eq + Clone,
{
    struct StackEntry<T> {
        item: T,
        remaining_dependencies: Vec<T>,
        path: Vec<T>,
    }

    let mut stack = Vec::from_iter(items.into_iter().map(|item| {
        let remaining_dependencies = get_dependencies(&item);
        let path = vec![item.clone()];
        StackEntry {
            item,
            remaining_dependencies,
            path,
        }
    }));

    let mut emitted = Vec::new();

    while let Some(mut entry) = stack.pop() {
        if emitted.contains(&entry.item) {
            continue;
        }
        match entry.remaining_dependencies.pop() {
            None => {
                emitted.push(entry.item);
            }
            Some(dependency) => {
                let dependency_path = [entry.path.as_slice(), &[dependency.clone()]].concat();

                // Make sure we don't have a cycle.
                let dependency_first_pos_in_path = dependency_path
                    .iter()
                    .position(|item| item == &dependency)
                    .unwrap();
                if dependency_first_pos_in_path < dependency_path.len() - 1 {
                    return TopologicalSortResult::Cycle(
                        dependency_path
                            .into_iter()
                            .skip(dependency_first_pos_in_path)
                            .collect(),
                    );
                }

                // Re-push this node before pushing the dependency so that we maintain DFS.
                stack.push(entry);
                let dependency_dependencies = get_dependencies(&dependency);
                stack.push(StackEntry {
                    item: dependency,
                    remaining_dependencies: dependency_dependencies,
                    path: dependency_path,
                });
            }
        }
    }

    TopologicalSortResult::Ordering(emitted)
}

/// Return true if version `version` of package `name` is published.
fn package_published(name: &str, version: &Version) -> Result<bool> {
    Ok(Client::builder()
        .user_agent("maelstrom xtask")
        .build()?
        .get(format!("https://crates.io/api/v1/crates/{name}/{version}"))
        .send()?
        .json::<serde_json::Value>()?
        .get("version")
        .is_some())
}

/// Publish all of the packages in the workspace, except those marked with publish = false.
///
/// If there is a dependency cycle, or if a publish package depends on an unpublished package, the
/// program will exit with an error.
#[derive(Debug, Parser)]
pub struct CliArgs {
    /// Don't try to publish, just check for dependency cycles and unpublished dependencies.
    #[arg(long)]
    lint: bool,

    /// Don't verify the contents of packages by building them.
    #[arg(long)]
    no_verify: bool,

    /// Allow package with dirty working directories to be published.
    #[arg(long)]
    allow_dirty: bool,

    /// Do not print cargo log messages (-qq don't print current package).
    #[arg(long, short, action = Count)]
    quiet: u8,

    /// Use verbose output (-vv very verbose/build.rs output).
    #[arg(long, short, action = Count)]
    verbose: u8,
}

pub fn main(args: CliArgs) -> Result<()> {
    let metadata = MetadataCommand::new().no_deps().exec()?;
    let packages = extract_workspace_packages(&metadata);
    let to_publish = get_publishable_packages(&packages)?;
    let ordering = match topological_sort(to_publish, |package| {
        packages.get(package).unwrap().dependencies.clone()
    }) {
        TopologicalSortResult::Ordering(ordering) => ordering,
        TopologicalSortResult::Cycle(cycle) => {
            bail!("found dependency cycle: {}", cycle.join(" -> "));
        }
    };

    if args.lint {
        return Ok(());
    }

    let mut cargo_args = vec!["publish"];
    args.no_verify.then(|| cargo_args.push("--no-verify"));
    args.allow_dirty.then(|| cargo_args.push("--allow-dirty"));
    (args.quiet > 0).then(|| cargo_args.push("--quiet"));
    cargo_args.extend(vec!["--verbose"; args.verbose.into()]);

    for package in ordering {
        let info = packages.get(package).unwrap();
        if !info.version.pre.is_empty() {
            bail!("cannot publish pre-release version {}", info.version);
        }
        if package_published(package, info.version)? {
            (args.quiet < 2).then(|| println!("Package {package} already published."));
            continue;
        }
        match args.quiet {
            0 => println!("Publishing {package}:"),
            1 => println!("Publishing {package}."),
            _ => {}
        };
        if !Command::new("cargo")
            .args(cargo_args.clone())
            .current_dir(info.path)
            .status()?
            .success()
        {
            bail!("cargo publish exited with non-zero status");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use maelstrom_simex::{Simulation, SimulationExplorer};
    use maelstrom_util::ext::BoolExt as _;
    use std::collections::HashSet;

    fn topological_sort_test<F>(max_item_count: usize, mut f: F)
    where
        F: for<'a, 'b> FnMut(usize, usize, &'a mut Simulation<'b>) -> bool,
    {
        SimulationExplorer::default().for_each(|mut simulation| {
            let item_count = simulation.choose_integer(1, max_item_count);
            let items = 0..item_count;
            let order = Vec::from_iter(
                items
                    .clone()
                    .flat_map(|item| items.clone().map(move |dependency| (item, dependency)))
                    .map(|(item, dependency)| f(item, dependency, &mut simulation)),
            );
            let depends_on = |item: usize, dependency: usize| order[item * item_count + dependency];

            let result = super::topological_sort(items.clone(), |item| {
                items
                    .clone()
                    .filter(|dependency| depends_on(*item, *dependency))
                    .collect()
            });

            match result {
                TopologicalSortResult::Ordering(ordering) => {
                    // To check that our ordering does not conflict with our order, visit each
                    // item in order and make sure all of its dependencies have already been
                    // visited.
                    let mut visited = HashSet::<usize>::new();
                    for item in ordering {
                        for dependency in items.clone() {
                            assert!(!depends_on(item, dependency) || visited.contains(&dependency));
                        }
                        visited.insert(item).assert_is_true();
                    }
                    assert_eq!(HashSet::from_iter(items), visited);
                }
                TopologicalSortResult::Cycle(cycle) => {
                    // First, check that this cycle is syntactically correct.
                    assert!(cycle.len() > 1);
                    assert_eq!(cycle.first(), cycle.last());
                    assert_eq!(
                        HashSet::<&usize>::from_iter(cycle.iter()).len(),
                        cycle.len() - 1
                    );

                    // Second, check that every edge in the cycle is actually in our order.
                    for window in cycle.windows(2) {
                        assert!(depends_on(window[0], window[1]));
                    }
                }
            }
        });
    }

    #[test]
    fn topological_sort() {
        topological_sort_test(4, |_: usize, _: usize, simulation: &mut Simulation| {
            simulation.choose_bool()
        });
    }

    #[test]
    fn topological_sort_no_cycles() {
        topological_sort_test(6, |item: usize, dependency, simulation: &mut Simulation| {
            item < dependency && simulation.choose_bool()
        });
    }

    #[test]
    fn extract_workspace_packages() {
        let metadata = r###"{
          "packages": [
            {
              "name": "in-workspace-1",
              "version": "0.6.0-dev",
              "id": "in-workspace-1 0.6.0-dev (path+file:///home/user/workspace/crates/in-workspace-1)",
              "dependencies": [
                {
                  "name": "anyhow",
                  "source": "registry+https://github.com/rust-lang/crates.io-index",
                  "req": "^1.0.71",
                  "kind": null,
                  "optional": false,
                  "uses_default_features": true,
                  "features": []
                },
                {
                  "name": "in-workspace-2",
                  "source": null,
                  "req": "^0.6.0-dev",
                  "kind": null,
                  "optional": false,
                  "uses_default_features": true,
                  "features": [],
                  "path": "/home/user/workspace/crates/in-workspace-2"
                }
              ],
              "targets": [
                {
                  "kind": [],
                  "crate_types": [],
                  "name": "",
                  "src_path": ""
                }
              ],
              "features": {},
              "manifest_path": "/home/user/workspace/crates/in-workspace-1/Cargo.toml",
              "publish": null
            },
            {
              "name": "in-workspace-2",
              "version": "0.6.0-dev",
              "id": "in-workspace-2 0.6.0-dev (path+file:///home/user/workspace/crates/in-workspace-2)",
              "dependencies": [
                {
                  "name": "anyhow",
                  "source": "registry+https://github.com/rust-lang/crates.io-index",
                  "req": "^1.0.71",
                  "kind": null,
                  "optional": false,
                  "uses_default_features": true,
                  "features": []
                }
              ],
              "targets": [
                {
                  "kind": [],
                  "crate_types": [],
                  "name": "",
                  "src_path": ""
                }
              ],
              "features": {},
              "manifest_path": "/home/user/workspace/crates/in-workspace-2/Cargo.toml",
              "publish": null
            },
            {
              "name": "in-workspace-3",
              "version": "0.6.0-dev",
              "id": "in-workspace-3 0.6.0-dev (path+file:///home/user/workspace/crates/in-workspace-3)",
              "dependencies": [
                {
                  "name": "anyhow",
                  "source": "registry+https://github.com/rust-lang/crates.io-index",
                  "req": "^1.0.71",
                  "kind": null,
                  "optional": false,
                  "uses_default_features": true,
                  "features": []
                },
                {
                  "name": "in-workspace-1",
                  "source": null,
                  "req": "^0.6.0-dev",
                  "kind": null,
                  "optional": false,
                  "uses_default_features": true,
                  "features": [],
                  "path": "/home/user/workspace/crates/in-workspace-1"
                }
              ],
              "targets": [
                {
                  "kind": [],
                  "crate_types": [],
                  "name": "",
                  "src_path": ""
                }
              ],
              "features": {},
              "manifest_path": "/home/user/workspace/crates/in-workspace-3/Cargo.toml",
              "publish": []
            }
          ],
          "workspace_members": [
            "in-workspace-1 0.6.0-dev (path+file:///home/user/workspace/crates/in-workspace-1)",
            "in-workspace-2 0.6.0-dev (path+file:///home/user/workspace/crates/in-workspace-2)",
            "in-workspace-3 0.6.0-dev (path+file:///home/user/workspace/crates/in-workspace-3)"
          ],
          "target_directory": "/home/user/workspace/target",
          "version": 1,
          "workspace_root": "/home/user/workspace"
        }"###;
        let metadata: Metadata = serde_json::from_str(metadata).unwrap();
        let packages = super::extract_workspace_packages(&metadata);
        assert_eq!(
            packages,
            HashMap::from_iter([
                (
                    "in-workspace-1",
                    PackageInfo {
                        to_publish: true,
                        path: "/home/user/workspace/crates/in-workspace-1".into(),
                        version: &Version::parse("0.6.0-dev").unwrap(),
                        dependencies: vec!["in-workspace-2"],
                        license: None,
                        description: None,
                        homepage: None,
                        documentation: None,
                        repository: None,
                        readme: None,
                    }
                ),
                (
                    "in-workspace-2",
                    PackageInfo {
                        to_publish: true,
                        path: "/home/user/workspace/crates/in-workspace-2".into(),
                        version: &Version::parse("0.6.0-dev").unwrap(),
                        dependencies: vec![],
                        license: None,
                        description: None,
                        homepage: None,
                        documentation: None,
                        repository: None,
                        readme: None,
                    }
                ),
                (
                    "in-workspace-3",
                    PackageInfo {
                        to_publish: false,
                        path: "/home/user/workspace/crates/in-workspace-3".into(),
                        version: &Version::parse("0.6.0-dev").unwrap(),
                        dependencies: vec!["in-workspace-1"],
                        license: None,
                        description: None,
                        homepage: None,
                        documentation: None,
                        repository: None,
                        readme: None,
                    }
                )
            ])
        );
    }

    #[test]
    fn get_publishable_packages_ok() {
        let version = &Version::new(1, 2, 3);
        let packages = Packages::from_iter([
            (
                "foo",
                PackageInfo {
                    to_publish: true,
                    path: "/foo".into(),
                    version,
                    dependencies: vec!["bar"],
                    license: "foo-license".into(),
                    description: "foo-description".into(),
                    homepage: "foo-homepage".into(),
                    documentation: "foo-documentation".into(),
                    repository: "foo-respository".into(),
                    readme: Utf8Path::new("foo-readme").into(),
                },
            ),
            (
                "bar",
                PackageInfo {
                    to_publish: true,
                    path: "/bar".into(),
                    version,
                    dependencies: vec![],
                    license: "bar-license".into(),
                    description: "bar-description".into(),
                    homepage: "bar-homepage".into(),
                    documentation: "bar-documentation".into(),
                    repository: "bar-respository".into(),
                    readme: Utf8Path::new("bar-readme").into(),
                },
            ),
            (
                "baz",
                PackageInfo {
                    to_publish: false,
                    path: "/baz".into(),
                    version,
                    dependencies: vec!["bar"],
                    license: None,
                    description: None,
                    homepage: None,
                    documentation: None,
                    repository: None,
                    readme: None,
                },
            ),
        ]);
        assert_eq!(
            HashSet::<&str>::from_iter(get_publishable_packages(&packages).unwrap()),
            HashSet::from_iter(["foo", "bar"])
        );
    }

    #[test]
    fn get_publishable_packages_bad_dependency() {
        let version = &Version::new(1, 2, 3);
        let packages = Packages::from_iter([
            (
                "foo",
                PackageInfo {
                    to_publish: true,
                    path: "/foo".into(),
                    version,
                    dependencies: vec!["bar"],
                    license: "foo-license".into(),
                    description: "foo-description".into(),
                    homepage: "foo-homepage".into(),
                    documentation: "foo-documentation".into(),
                    repository: "foo-respository".into(),
                    readme: Utf8Path::new("foo-readme").into(),
                },
            ),
            (
                "bar",
                PackageInfo {
                    to_publish: false,
                    path: "/bar".into(),
                    version,
                    dependencies: vec![],
                    license: None,
                    description: None,
                    homepage: None,
                    documentation: None,
                    repository: None,
                    readme: None,
                },
            ),
        ]);
        let error = get_publishable_packages(&packages).unwrap_err();
        assert_eq!(
            format!("{error}"),
            "cannot publish foo because its dependency bar isn't publishable"
        );
    }

    #[test]
    fn get_publishable_packages_missing_license() {
        let version = &Version::new(1, 2, 3);
        let packages = Packages::from_iter([(
            "foo",
            PackageInfo {
                to_publish: true,
                path: "/foo".into(),
                version,
                dependencies: vec![],
                license: None,
                description: "foo-description".into(),
                homepage: "foo-homepage".into(),
                documentation: "foo-documentation".into(),
                repository: "foo-respository".into(),
                readme: Utf8Path::new("foo-readme").into(),
            },
        )]);
        let error = get_publishable_packages(&packages).unwrap_err();
        assert_eq!(
            format!("{error}"),
            "cannot publish foo because it doesn't have a license field"
        );
    }

    #[test]
    fn get_publishable_packages_missing_description() {
        let version = &Version::new(1, 2, 3);
        let packages = Packages::from_iter([(
            "foo",
            PackageInfo {
                to_publish: true,
                path: "/foo".into(),
                version,
                dependencies: vec![],
                license: "foo-license".into(),
                description: None,
                homepage: "foo-homepage".into(),
                documentation: "foo-documentation".into(),
                repository: "foo-respository".into(),
                readme: Utf8Path::new("foo-readme").into(),
            },
        )]);
        let error = get_publishable_packages(&packages).unwrap_err();
        assert_eq!(
            format!("{error}"),
            "cannot publish foo because it doesn't have a description field"
        );
    }

    #[test]
    fn get_publishable_packages_missing_homepage() {
        let version = &Version::new(1, 2, 3);
        let packages = Packages::from_iter([(
            "foo",
            PackageInfo {
                to_publish: true,
                path: "/foo".into(),
                version,
                dependencies: vec![],
                license: "foo-license".into(),
                description: "foo-description".into(),
                homepage: None,
                documentation: "foo-documentation".into(),
                repository: "foo-respository".into(),
                readme: Utf8Path::new("foo-readme").into(),
            },
        )]);
        let error = get_publishable_packages(&packages).unwrap_err();
        assert_eq!(
            format!("{error}"),
            "cannot publish foo because it doesn't have a homepage field"
        );
    }

    #[test]
    fn get_publishable_packages_missing_documentation() {
        let version = &Version::new(1, 2, 3);
        let packages = Packages::from_iter([(
            "foo",
            PackageInfo {
                to_publish: true,
                path: "/foo".into(),
                version,
                dependencies: vec![],
                license: "foo-license".into(),
                description: "foo-description".into(),
                homepage: "foo-homepage".into(),
                documentation: None,
                repository: "foo-respository".into(),
                readme: Utf8Path::new("foo-readme").into(),
            },
        )]);
        let error = get_publishable_packages(&packages).unwrap_err();
        assert_eq!(
            format!("{error}"),
            "cannot publish foo because it doesn't have a documentation field"
        );
    }

    #[test]
    fn get_publishable_packages_missing_repository() {
        let version = &Version::new(1, 2, 3);
        let packages = Packages::from_iter([(
            "foo",
            PackageInfo {
                to_publish: true,
                path: "/foo".into(),
                version,
                dependencies: vec![],
                license: "foo-license".into(),
                description: "foo-description".into(),
                homepage: "foo-homepage".into(),
                documentation: "foo-documentation".into(),
                repository: None,
                readme: Utf8Path::new("foo-readme").into(),
            },
        )]);
        let error = get_publishable_packages(&packages).unwrap_err();
        assert_eq!(
            format!("{error}"),
            "cannot publish foo because it doesn't have a repository field"
        );
    }

    #[test]
    fn get_publishable_packages_missing_readme() {
        let version = &Version::new(1, 2, 3);
        let packages = Packages::from_iter([(
            "foo",
            PackageInfo {
                to_publish: true,
                path: "/foo".into(),
                version,
                dependencies: vec![],
                license: "foo-license".into(),
                description: "foo-description".into(),
                homepage: "foo-homepage".into(),
                documentation: "foo-documentation".into(),
                repository: "foo-respository".into(),
                readme: None,
            },
        )]);
        let error = get_publishable_packages(&packages).unwrap_err();
        assert_eq!(
            format!("{error}"),
            "cannot publish foo because it doesn't have a README"
        );
    }
}
