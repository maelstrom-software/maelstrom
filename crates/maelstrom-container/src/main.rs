use anyhow::{bail, Result};
use clap::Parser;
use maelstrom_container::{ImageDownloader, ImageName};
use std::path::PathBuf;

#[derive(Parser)]
#[command(styles=maelstrom_util::clap::styles())]
struct CliOptions {
    image_name: String,
    layer_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = CliOptions::parse();
    let image_name: ImageName = opt.image_name.parse()?;

    let ImageName::Docker(ref_) = &image_name else {
        bail!("local image path not supported yet");
    };

    let ind = indicatif::ProgressBar::new(0);
    let downloader = ImageDownloader::new(reqwest::Client::new());
    let image = downloader.download_image(ref_, &opt.layer_dir, ind).await?;
    println!("{image:#?}");

    Ok(())
}
