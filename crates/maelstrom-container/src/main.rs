use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use maelstrom_container::{ImageDownloader, ImageName};
use std::path::PathBuf;

#[derive(Subcommand)]
enum CliCommands {
    Download {
        image_name: String,
        layer_dir: PathBuf,
    },
    Inspect {
        image_name: String,
    },
}

#[derive(Parser)]
#[command(styles=maelstrom_util::clap::styles())]
struct CliOptions {
    #[command(subcommand)]
    command: CliCommands,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = CliOptions::parse();
    match opt.command {
        CliCommands::Download {
            image_name,
            layer_dir,
        } => {
            let image_name: ImageName = image_name.parse()?;

            let ImageName::Docker(ref_) = &image_name else {
                bail!("local image path not supported yet");
            };

            let ind = indicatif::ProgressBar::new(0);
            let downloader = ImageDownloader::new(reqwest::Client::new());
            let image = downloader.download_image(ref_, &layer_dir, ind).await?;
            println!("{image:#?}");
        }
        CliCommands::Inspect { image_name } => {
            let image_name: ImageName = image_name.parse()?;

            let ImageName::Docker(ref_) = &image_name else {
                bail!("local image path not supported yet");
            };

            let downloader = ImageDownloader::new(reqwest::Client::new());
            let resp = downloader.inspect(ref_).await?;
            println!("{resp:#?}");
        }
    }

    Ok(())
}
