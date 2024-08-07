use crate::ClientTrait;
use anyhow::Result;
use maelstrom_base::Sha256Digest;
use maelstrom_client::spec::{Layer, PrefixOptions};
use std::path::Path;

fn path_layer_for_binary(binary_path: &Path, log: slog::Logger) -> Result<Layer> {
    slog::debug!(log, "adding layer for binary"; "binary" => ?binary_path);
    Ok(Layer::Paths {
        paths: vec![binary_path.to_path_buf().try_into()?],
        prefix_options: PrefixOptions {
            strip_prefix: Some(binary_path.parent().unwrap().to_path_buf().try_into()?),
            ..Default::default()
        },
    })
}

fn so_layer_for_binary(binary_path: &Path, log: slog::Logger) -> Result<Layer> {
    slog::debug!(log, "adding layer for binary deps"; "binary" => ?binary_path);
    Ok(Layer::SharedLibraryDependencies {
        binary_paths: vec![binary_path.to_owned().try_into()?],
        prefix_options: PrefixOptions {
            follow_symlinks: true,
            ..Default::default()
        },
    })
}

#[derive(Clone)]
pub struct GeneratedArtifacts {
    pub binary: Sha256Digest,
    pub deps: Sha256Digest,
}

pub fn add_generated_artifacts(
    client: &impl ClientTrait,
    binary_path: &Path,
    log: slog::Logger,
) -> Result<GeneratedArtifacts> {
    let (binary_artifact, _) =
        client.add_layer(path_layer_for_binary(binary_path, log.clone())?)?;
    let (deps_artifact, _) = client.add_layer(so_layer_for_binary(binary_path, log)?)?;
    Ok(GeneratedArtifacts {
        binary: binary_artifact,
        deps: deps_artifact,
    })
}
