use anyhow::{anyhow, Context as _, Result};
use byteorder::{BigEndian, ReadBytesExt as _, WriteBytesExt as _};
use maelstrom_base::{
    manifest::{ManifestEntryData, ManifestReader},
    Sha256Digest,
};
use maelstrom_client::Client;
use maelstrom_util::{fs::Fs, manifest::ManifestBuilder};
use std::collections::HashSet;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt as _;
use std::time::SystemTime;
use std::{
    collections::{BTreeSet, HashMap},
    io,
    path::{Path, PathBuf},
    sync::Mutex,
};

fn manifest_input_path_from_manifest_path(path: &Path) -> PathBuf {
    let mut path = path.to_owned();
    path.set_extension(format!(
        "{}.input",
        path.extension().unwrap().to_str().unwrap()
    ));
    path
}

fn temp_path(path: &Path) -> PathBuf {
    let mut path = path.to_owned();
    path.set_extension(format!(
        "{}.tmp",
        path.extension().unwrap().to_str().unwrap()
    ));
    path
}

fn upload_data_from_manifest_input(
    fs: &Fs,
    path: &Path,
    data_upload: &mut impl FnMut(&Path) -> Result<Sha256Digest>,
) -> Result<HashSet<Sha256Digest>> {
    let mut digests = HashSet::new();
    for path in decode_paths(fs.open_file(path)?).with_context(|| path.display().to_string())? {
        digests.insert(data_upload(&path)?);
    }
    Ok(digests)
}

fn verify_manifest_digests(
    fs: &Fs,
    path: &Path,
    uploaded_digests: HashSet<Sha256Digest>,
) -> Result<bool> {
    let manifest = ManifestReader::new(fs.open_file(path)?)?;
    for entry in manifest {
        if let ManifestEntryData::File(Some(digest)) = entry?.data {
            if !uploaded_digests.contains(&digest) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn check_for_cached_manifest(
    fs: &Fs,
    binary_mtime: SystemTime,
    path: &Path,
    data_upload: &mut impl FnMut(&Path) -> Result<Sha256Digest>,
) -> Result<bool> {
    let input_path = manifest_input_path_from_manifest_path(path);
    if fs.exists(path) && fs.exists(&input_path) {
        let manifest_mtime = fs.metadata(path)?.modified()?;
        if binary_mtime < manifest_mtime {
            let uploaded_digests = upload_data_from_manifest_input(fs, &input_path, data_upload)?;
            if verify_manifest_digests(fs, path, uploaded_digests)
                .with_context(|| path.display().to_string())?
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn encode_paths(paths: &[PathBuf], mut out: impl io::Write) -> Result<()> {
    out.write_u64::<BigEndian>(paths.len() as u64)?;
    for path in paths {
        let s = path.as_os_str();
        out.write_u64::<BigEndian>(s.len() as u64)?;
        out.write_all(s.as_encoded_bytes())?;
    }
    Ok(())
}

fn decode_paths(mut input: impl io::Read) -> Result<Vec<PathBuf>> {
    let mut paths = vec![];
    let num_paths = input.read_u64::<BigEndian>()?;
    for _ in 0..num_paths {
        let path_len = input.read_u64::<BigEndian>()?;
        let mut buffer = vec![0; path_len as usize];
        input.read_exact(&mut buffer)?;
        paths.push(OsString::from_vec(buffer).into());
    }

    let extra = std::io::copy(&mut input, &mut std::io::sink())?;
    if extra > 0 {
        return Err(anyhow!("unknown trailing data"));
    }

    Ok(paths)
}

fn build_manifest(
    fs: &Fs,
    manifest_path: &Path,
    paths: Vec<PathBuf>,
    strip_prefix: impl AsRef<Path>,
    data_upload: impl FnMut(&Path) -> Result<Sha256Digest>,
) -> Result<()> {
    let temp_manifest_path = temp_path(&manifest_path);
    let manifest_file = fs.create_file(&temp_manifest_path)?;
    let mut manifest =
        ManifestBuilder::new(manifest_file, true /* follow_symlinks */, data_upload)?;

    let manifest_input_path = manifest_input_path_from_manifest_path(manifest_path);
    let temp_manifest_input_path = temp_path(&manifest_input_path);

    for path in &paths {
        manifest.add_file(path, path.strip_prefix(strip_prefix.as_ref()).unwrap())?;
    }

    encode_paths(&paths, fs.create_file(&temp_manifest_input_path)?)?;

    fs.rename(temp_manifest_input_path, manifest_input_path)?;
    fs.rename(temp_manifest_path, manifest_path)?;
    Ok(())
}

fn create_artifact_for_binary(
    binary_path: &Path,
    mut data_upload: impl FnMut(&Path) -> Result<Sha256Digest>,
) -> Result<PathBuf> {
    let fs = Fs::new();

    let mut manifest_path = PathBuf::from(binary_path);
    assert!(manifest_path.set_extension("manifest"));

    let binary_mtime = fs.metadata(binary_path)?.modified()?;
    if check_for_cached_manifest(&fs, binary_mtime, &manifest_path, &mut data_upload)? {
        return Ok(manifest_path);
    }

    build_manifest(
        &fs,
        &manifest_path,
        vec![binary_path.to_path_buf()],
        binary_path.parent().unwrap(),
        data_upload,
    )?;
    Ok(manifest_path)
}

fn create_artifact_for_binary_deps(
    binary_path: &Path,
    mut data_upload: impl FnMut(&Path) -> Result<Sha256Digest>,
) -> Result<PathBuf> {
    let fs = Fs::new();

    let mut manifest_path = PathBuf::from(binary_path);
    assert!(manifest_path.set_extension("deps.manifest"));

    let binary_mtime = fs.metadata(binary_path)?.modified()?;
    if check_for_cached_manifest(&fs, binary_mtime, &manifest_path, &mut data_upload)? {
        return Ok(manifest_path);
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

    build_manifest(
        &fs,
        &manifest_path,
        paths.into_iter().collect(),
        "/",
        data_upload,
    )?;
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
