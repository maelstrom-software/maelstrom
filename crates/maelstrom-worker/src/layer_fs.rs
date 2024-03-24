use anyhow::Result;
use futures::StreamExt as _;
use maelstrom_base::{manifest::UnixTimestamp, ArtifactType, Sha256Digest};
use maelstrom_fuse::{BottomLayerBuilder, LayerFs, UpperLayerBuilder};
use maelstrom_util::async_fs::Fs;
use std::path::{Path, PathBuf};

async fn dir_size(fs: &Fs, path: &Path) -> Result<u64> {
    let mut total = 0;
    let mut entries = fs.read_dir(path).await?;
    while let Some(e) = entries.next().await {
        let e = e?;
        total += e.metadata().await?.len();
    }
    Ok(total)
}

pub async fn build_bottom_layer(
    log: slog::Logger,
    layer_path: PathBuf,
    cache_path: PathBuf,
    artifact_digest: Sha256Digest,
    artifact_type: ArtifactType,
    artifact_path: PathBuf,
) -> Result<u64> {
    let fs = Fs::new();
    fs.create_dir_all(&layer_path).await?;
    let mut builder =
        BottomLayerBuilder::new(log, &fs, &layer_path, &cache_path, UnixTimestamp::EPOCH).await?;
    let artifact_file = fs.open_file(artifact_path).await?;
    match artifact_type {
        ArtifactType::Tar => builder.add_from_tar(artifact_digest, artifact_file).await?,
        ArtifactType::Manifest => builder.add_from_manifest(artifact_file).await?,
    }
    builder.finish();

    dir_size(&fs, &layer_path).await
}

pub async fn build_upper_layer(
    log: slog::Logger,
    layer_path: PathBuf,
    cache_path: PathBuf,
    lower_layer_path: PathBuf,
    upper_layer_path: PathBuf,
) -> Result<u64> {
    let fs = Fs::new();
    fs.create_dir_all(&layer_path).await?;
    let lower = LayerFs::from_path(log.clone(), &lower_layer_path, &cache_path)?;
    let upper = LayerFs::from_path(log.clone(), &upper_layer_path, &cache_path)?;
    let mut builder = UpperLayerBuilder::new(log, &layer_path, &cache_path, &lower).await?;
    builder.fill_from_bottom_layer(&upper).await?;
    builder.fill_from_bottom_layer(&upper).await?;
    builder.finish();

    dir_size(&fs, &layer_path).await
}
