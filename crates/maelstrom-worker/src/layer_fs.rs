use anyhow::Result;
use futures::StreamExt as _;
use maelstrom_base::{manifest::UnixTimestamp, ArtifactType, Sha256Digest};
use maelstrom_layer_fs::{BlobDir, BottomLayerBuilder, LayerFs, UpperLayerBuilder};
use maelstrom_util::{async_fs::Fs, root::Root};
use std::path::{Path, PathBuf};
use tokio::io::BufReader;

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
    blob_dir: &Root<BlobDir>,
    artifact_digest: Sha256Digest,
    artifact_type: ArtifactType,
    artifact_path: PathBuf,
) -> Result<u64> {
    let fs = Fs::new();
    fs.create_dir_all(&layer_path).await?;
    let mut builder =
        BottomLayerBuilder::new(log, &fs, &layer_path, blob_dir, UnixTimestamp::EPOCH).await?;
    let artifact_file = BufReader::new(fs.open_file(artifact_path).await?);
    match artifact_type {
        ArtifactType::Tar => builder.add_from_tar(artifact_digest, artifact_file).await?,
        ArtifactType::Manifest => builder.add_from_manifest(artifact_file).await?,
    }
    builder.finish().await?;

    dir_size(&fs, &layer_path).await
}

pub async fn build_upper_layer(
    log: slog::Logger,
    layer_path: PathBuf,
    blob_dir: &Root<BlobDir>,
    lower_layer_path: PathBuf,
    upper_layer_path: PathBuf,
) -> Result<u64> {
    let fs = Fs::new();
    fs.create_dir_all(&layer_path).await?;
    let lower = LayerFs::from_path(&lower_layer_path, blob_dir)?;
    let upper = LayerFs::from_path(&upper_layer_path, blob_dir)?;
    let mut builder = UpperLayerBuilder::new(log, &layer_path, blob_dir, &lower).await?;
    builder.fill_from_bottom_layer(&upper).await?;
    builder.fill_from_bottom_layer(&upper).await?;
    builder.finish().await?;

    dir_size(&fs, &layer_path).await
}
