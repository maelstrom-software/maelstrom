use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use maelstrom_container::{DockerReference, ImageDownloader, ImageName};
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

async fn resolve_name(image_name: &str) -> Result<DockerReference> {
    let image_name: ImageName = image_name.parse()?;

    let ImageName::Docker(mut ref_) = image_name else {
        bail!("local image path not supported yet");
    };

    let mut downloader = ImageDownloader::new(reqwest::Client::new());
    let digest = downloader.resolve_tag(&ref_).await?;
    println!("digest = {digest:#?}");
    ref_.tag = None;
    ref_.digest = Some(digest);
    Ok(ref_)
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = CliOptions::parse();
    match opt.command {
        CliCommands::Download {
            image_name,
            layer_dir,
        } => {
            let ref_ = resolve_name(&image_name).await?;

            let ind = indicatif::ProgressBar::new(0);
            let downloader = ImageDownloader::new(reqwest::Client::new());
            let image = downloader.download_image(&ref_, &layer_dir, ind).await?;
            println!("{image:#?}");
        }
        CliCommands::Inspect { image_name } => {
            let ref_ = resolve_name(&image_name).await?;

            let downloader = ImageDownloader::new(reqwest::Client::new());
            let resp = downloader.inspect(&ref_).await?;
            println!("{resp:#?}");
        }
    }

    Ok(())
}
