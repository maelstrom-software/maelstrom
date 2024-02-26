use anyhow::{bail, Result};
use byteorder::{BigEndian, ReadBytesExt as _, WriteBytesExt as _};
use maelstrom_base::Sha256Digest;
use maelstrom_client::Client;
use maelstrom_util::{fs::Fs, manifest::ManifestBuilder};
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt as _;
use std::{
    collections::{BTreeSet, HashMap},
    io,
    path::{Path, PathBuf},
};

fn so_listing_path_from_binary_path(path: &Path) -> PathBuf {
    let mut path = path.to_owned();
    path.set_extension("so_listing");
    path
}

fn check_for_cached_so_listing(fs: &Fs, binary_path: &Path) -> Result<Option<Vec<PathBuf>>> {
    let listing_path = so_listing_path_from_binary_path(binary_path);
    if fs.exists(&listing_path) {
        let listing_mtime = fs.metadata(&listing_path)?.modified()?;
        let binary_mtime = fs.metadata(binary_path)?.modified()?;
        if binary_mtime < listing_mtime {
            return Ok(Some(decode_paths(fs.open_file(listing_path)?)?));
        }
    }
    Ok(None)
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
        bail!("unknown trailing data")
    }

    Ok(paths)
}

fn build_manifest(
    fs: &Fs,
    manifest_path: &Path,
    paths: Vec<PathBuf>,
    strip_prefix: impl AsRef<Path>,
    data_upload: impl FnMut(&Path) -> Result<Sha256Digest>,
    log: slog::Logger,
) -> Result<()> {
    let manifest_file = fs.create_file(manifest_path)?;
    let mut manifest =
        ManifestBuilder::new(manifest_file, true /* follow_symlinks */, data_upload)?;

    for path in &paths {
        slog::debug!(log, "build manifest: builder.add_file"; "path" => ?path);
        manifest.add_file(path, path.strip_prefix(strip_prefix.as_ref()).unwrap())?;
    }

    Ok(())
}

fn create_artifact_for_binary(
    binary_path: &Path,
    data_upload: impl FnMut(&Path) -> Result<Sha256Digest>,
    log: slog::Logger,
) -> Result<PathBuf> {
    let fs = Fs::new();

    let mut manifest_path = PathBuf::from(binary_path);
    assert!(manifest_path.set_extension("manifest"));

    slog::debug!(log, "building manifest for binary"; "path" => ?manifest_path);
    build_manifest(
        &fs,
        &manifest_path,
        vec![binary_path.to_path_buf()],
        binary_path.parent().unwrap(),
        data_upload,
        log,
    )?;
    Ok(manifest_path)
}

fn read_shared_libraries(fs: &Fs, path: &Path, log: slog::Logger) -> Result<Vec<PathBuf>> {
    if let Some(paths) = check_for_cached_so_listing(fs, path)? {
        slog::debug!(log, "found cached shared libraries"; "path" => ?path);
        return Ok(paths);
    }

    slog::debug!(log, "reading shared libraries"; "path" => ?path);

    let dep_tree = lddtree::DependencyAnalyzer::new("/".into());
    let deps = dep_tree.analyze(path)?;

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

    Ok(paths.into_iter().collect())
}

fn create_artifact_for_binary_deps(
    binary_path: &Path,
    data_upload: impl FnMut(&Path) -> Result<Sha256Digest>,
    log: slog::Logger,
) -> Result<PathBuf> {
    let fs = Fs::new();

    let mut manifest_path = PathBuf::from(binary_path);
    assert!(manifest_path.set_extension("deps.manifest"));

    let paths = read_shared_libraries(&fs, binary_path, log.clone())?;
    encode_paths(
        &paths,
        fs.create_file(so_listing_path_from_binary_path(binary_path))?,
    )?;

    slog::debug!(log, "building manifest for binary deps"; "path" => ?manifest_path);
    build_manifest(&fs, &manifest_path, paths, "/", data_upload, log)?;
    Ok(manifest_path)
}

pub struct GeneratedArtifacts {
    pub binary: Sha256Digest,
    pub deps: Sha256Digest,
}

pub fn add_generated_artifacts(
    client: &Client,
    binary_path: &Path,
    log: slog::Logger,
) -> Result<GeneratedArtifacts> {
    let upload = |p: &Path| client.add_artifact(p);

    let binary_artifact = upload(&create_artifact_for_binary(
        binary_path,
        upload,
        log.clone(),
    )?)?;
    let deps_artifact = upload(&create_artifact_for_binary_deps(binary_path, upload, log)?)?;
    Ok(GeneratedArtifacts {
        binary: binary_artifact,
        deps: deps_artifact,
    })
}
