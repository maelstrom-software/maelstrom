use anyhow::Result;
use meticulous_base::Sha256Digest;
use meticulous_client::Client;
use meticulous_util::fs::Fs;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

fn create_artifact_for_binary(binary_path: &Path) -> Result<PathBuf> {
    let fs = Fs::new();
    let binary = fs.open_file(binary_path)?;

    let mut tar_path = PathBuf::from(binary_path);
    assert!(tar_path.set_extension("tar"));

    if fs.exists(&tar_path) {
        let binary_mtime = binary.metadata()?.modified()?;
        let tar_mtime = fs.metadata(&tar_path)?.modified()?;
        if binary_mtime < tar_mtime {
            return Ok(tar_path);
        }
    }

    let tar_file = fs.create_file(&tar_path)?;
    let mut a = tar::Builder::new(tar_file);

    let binary_path_in_tar = Path::new("./").join(binary_path.file_name().unwrap());
    a.append_file(binary_path_in_tar, &mut binary.into_inner())?;
    a.finish()?;

    Ok(tar_path)
}

fn create_artifact_for_binary_deps(binary_path: &Path) -> Result<PathBuf> {
    let fs = Fs::new();

    let mut tar_path = PathBuf::from(binary_path);
    assert!(tar_path.set_extension("deps.tar"));

    if fs.exists(&tar_path) {
        let binary_mtime = fs.metadata(binary_path)?.modified()?;
        let tar_mtime = fs.metadata(&tar_path)?.modified()?;

        if binary_mtime < tar_mtime {
            return Ok(tar_path);
        }
    }

    let dep_tree = lddtree::DependencyAnalyzer::new("/".into());
    let deps = dep_tree.analyze(binary_path)?;

    let mut paths = BTreeSet::new();
    if let Some(p) = deps.interpreter {
        if let Some(lib) = deps.libraries.get(&p) {
            paths.insert(lib.path.clone());
        }
    }

    fn walk_deps(
        deps: &[String],
        libraries: &HashMap<String, lddtree::Library>,
        paths: &mut BTreeSet<PathBuf>,
    ) {
        for dep in deps {
            if let Some(lib) = libraries.get(dep) {
                paths.insert(lib.path.clone());
            }
            if let Some(lib) = libraries.get(dep) {
                walk_deps(&lib.needed, libraries, paths);
            }
        }
    }
    walk_deps(&deps.needed, &deps.libraries, &mut paths);

    fn remove_root(path: &Path) -> PathBuf {
        path.components().skip(1).collect()
    }

    let tar_file = fs.create_file(&tar_path)?;
    let mut a = tar::Builder::new(tar_file);

    for path in paths {
        a.append_path_with_name(&path, &remove_root(&path))?;
    }

    a.finish()?;

    Ok(tar_path)
}

pub fn add_generated_artifacts(
    client: &Mutex<Client>,
    binary_path: &Path,
) -> Result<(Sha256Digest, Sha256Digest)> {
    let binary_artifact = client
        .lock()
        .unwrap()
        .add_artifact(&create_artifact_for_binary(binary_path)?)?;
    let deps_artifact = client
        .lock()
        .unwrap()
        .add_artifact(&create_artifact_for_binary_deps(binary_path)?)?;
    Ok((binary_artifact, deps_artifact))
}
