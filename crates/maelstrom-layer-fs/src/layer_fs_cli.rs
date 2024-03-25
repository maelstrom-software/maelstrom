use anyhow::{anyhow, Result};
use maelstrom_layer_fs::{DirectoryDataReader, FileId, FileMetadataReader, FileType, LayerFs};
use slog::{o, Drain, LevelFilter, Logger};
use slog_async::Async;
use slog_term::{FullFormat, TermDecorator};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let drain = LevelFilter::new(drain, slog::Level::Debug).fuse();
    let log = Logger::root(drain, o!());

    let mut args = env::args();
    args.next();
    let path = args.next().ok_or(anyhow!("expected path to fs layer"))?;
    let offset: u32 = args
        .next()
        .ok_or(anyhow!("expected file offset"))?
        .parse()?;

    let layer_fs = LayerFs::from_path(log, path.as_ref(), "/dev/null".as_ref())?;
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
