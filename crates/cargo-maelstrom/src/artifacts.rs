use anyhow::Result;
use maelstrom_base::Sha256Digest;
use maelstrom_client::Client;
use maelstrom_util::{fs::Fs, manifest::ManifestBuilder};
use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::Mutex,
};

fn create_artifact_for_binary(
    binary_path: &Path,
    data_upload: impl FnMut(&Path) -> Result<Sha256Digest>,
) -> Result<PathBuf> {
    let fs = Fs::new();
    let binary = fs.open_file(binary_path)?;

    let mut manifest_path = PathBuf::from(binary_path);
    assert!(manifest_path.set_extension("manifest"));

    if fs.exists(&manifest_path) {
        let binary_mtime = binary.metadata()?.modified()?;
        let manifest_mtime = fs.metadata(&manifest_path)?.modified()?;
        if binary_mtime < manifest_mtime {
            return Ok(manifest_path);
        }
    }

    let mut temp_manifest_path = manifest_path.clone();
    temp_manifest_path.set_extension("manifest.temp");

    let manifest_file = fs.create_file(&temp_manifest_path)?;
    let mut manifest =
        ManifestBuilder::new(manifest_file, true /* follow_symlinks */, data_upload)?;

    let binary_path_in_manifest = Path::new("./").join(binary_path.file_name().unwrap());
    manifest.add_file(binary_path, binary_path_in_manifest)?;

    fs.rename(&temp_manifest_path, &manifest_path)?;
    Ok(manifest_path)
}

fn create_artifact_for_binary_deps(
    binary_path: &Path,
    data_upload: impl FnMut(&Path) -> Result<Sha256Digest>,
) -> Result<PathBuf> {
    let fs = Fs::new();

    let mut manifest_path = PathBuf::from(binary_path);
    assert!(manifest_path.set_extension("deps.manifest"));

    if fs.exists(&manifest_path) {
        let binary_mtime = fs.metadata(binary_path)?.modified()?;
        let manifest_mtime = fs.metadata(&manifest_path)?.modified()?;

        if binary_mtime < manifest_mtime {
            return Ok(manifest_path);
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

    let mut temp_manifest_path = manifest_path.clone();
    temp_manifest_path.set_extension("manifest.temp");
    let manifest_file = fs.create_file(&temp_manifest_path)?;
    let mut manifest =
        ManifestBuilder::new(manifest_file, true /* follow_symlinks */, data_upload)?;

    for path in paths {
        manifest.add_file(&path, &remove_root(&path))?;
    }

    fs.rename(&temp_manifest_path, &manifest_path)?;
    Ok(manifest_path)
}

pub struct GeneratedArtifacts {
    pub binary: Sha256Digest,
    pub deps: Sha256Digest,
}

pub fn add_generated_artifacts(
    client: &Mutex<Client>,
    binary_path: &Path,
) -> Result<GeneratedArtifacts> {
    let upload = |p: &Path| client.lock().unwrap().add_artifact(p);

    let binary_artifact = upload(&create_artifact_for_binary(binary_path, upload)?)?;
    let deps_artifact = upload(&create_artifact_for_binary_deps(binary_path, upload)?)?;
    Ok(GeneratedArtifacts {
        binary: binary_artifact,
        deps: deps_artifact,
    })
}
