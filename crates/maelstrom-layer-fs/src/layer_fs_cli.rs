use anyhow::{anyhow, Result};
use maelstrom_layer_fs::{DirectoryDataReader, FileId, FileMetadataReader, FileType, LayerFs};
use maelstrom_util::root::Root;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args();
    args.next();
    let path = args
        .next()
        .ok_or_else(|| anyhow!("expected path to fs layer"))?;
    let offset: u32 = args
        .next()
        .ok_or_else(|| anyhow!("expected file offset"))?
        .parse()?;

    let layer_fs = LayerFs::from_path(path.as_ref(), Root::new("/dev/null".as_ref()))?;
    let layer_id = layer_fs.layer_id().await?;

    let mut reader = FileMetadataReader::new(&layer_fs, layer_id).await?;
    let file_id = FileId::new(layer_id, offset.try_into()?);
    let (type_, attrs) = reader.get_attr(file_id).await?;
    println!("{type_:#?}\n{attrs:#?}");

    if type_ == FileType::Directory {
        let mut reader = DirectoryDataReader::new(&layer_fs, file_id).await?;
        while let Some(entry) = reader.next_entry().await? {
            println!("{entry:#?}");
        }
    }

    Ok(())
}
