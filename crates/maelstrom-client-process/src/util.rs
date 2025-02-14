use anyhow::Result;
use maelstrom_base::Sha256Digest;
use maelstrom_util::{async_fs::Fs, io::Sha256Stream};
use std::{path::Path, time::SystemTime};
use tokio::io;

pub async fn calculate_digest(path: &Path) -> Result<(SystemTime, Sha256Digest)> {
    let fs = Fs::new();
    let mut f = fs.open_file(path).await?;
    let mut hasher = Sha256Stream::new(io::sink());
    io::copy(&mut f, &mut hasher).await?;
    let mtime = f.metadata().await?.modified()?;

    Ok((mtime, hasher.finalize().1))
}
