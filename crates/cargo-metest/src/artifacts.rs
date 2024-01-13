use crate::progress::ProgressIndicator;
use anyhow::Result;
use indicatif::ProgressBar;
use maelstrom_util::fs::Fs;
use meticulous_base::Sha256Digest;
use meticulous_client::Client;
use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::Mutex,
};
use tar::Header;

fn create_artifact_for_binary(binary_path: &Path, prog: Option<ProgressBar>) -> Result<PathBuf> {
    let prog = prog.unwrap_or_else(ProgressBar::hidden);

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
    let mut header = Header::new_gnu();
    let meta = binary.metadata()?;
    prog.set_length(meta.len());
    header.set_metadata(&meta.into_inner());
    a.append_data(&mut header, binary_path_in_tar, prog.wrap_read(binary))?;
    a.finish()?;

    Ok(tar_path)
}

fn create_artifact_for_binary_deps(
    binary_path: &Path,
    prog: Option<ProgressBar>,
) -> Result<PathBuf> {
    let prog = prog.unwrap_or_else(ProgressBar::hidden);
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

    let files = paths
        .iter()
        .map(|p| fs.open_file(p))
        .collect::<Result<Vec<_>>>()?;

    let metas = files
        .iter()
        .map(|f| f.metadata())
        .collect::<Result<Vec<_>>>()?;

    let total_size = metas.iter().map(|m| m.len()).sum();
    prog.set_length(total_size);

    for ((path, file), meta) in paths
        .into_iter()
        .zip(files.into_iter())
        .zip(metas.into_iter())
    {
        let mut header = Header::new_gnu();
        header.set_metadata(&meta.into_inner());
        a.append_data(&mut header, &remove_root(&path), prog.wrap_read(file))?;
    }

    a.finish()?;

    Ok(tar_path)
}

pub struct GeneratedArtifacts {
    pub binary: Sha256Digest,
    pub deps: Sha256Digest,
}

pub fn add_generated_artifacts(
    client: &Mutex<Client>,
    binary_path: &Path,
    ind: &impl ProgressIndicator,
) -> Result<GeneratedArtifacts> {
    let prog = ind.new_side_progress("tar");
    let binary_artifact = client
        .lock()
        .unwrap()
        .add_artifact(&create_artifact_for_binary(binary_path, prog)?)?;
    let prog = ind.new_side_progress("tar");
    let deps_artifact = client
        .lock()
        .unwrap()
        .add_artifact(&create_artifact_for_binary_deps(binary_path, prog)?)?;
    Ok(GeneratedArtifacts {
        binary: binary_artifact,
        deps: deps_artifact,
    })
}
