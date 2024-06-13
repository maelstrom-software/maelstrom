use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use maelstrom_container::{
    local_registry::LocalRegistry, DockerReference, ImageDownloader, ImageName,
};
use maelstrom_util::config::common::LogLevel;
use std::path::PathBuf;

#[derive(Subcommand)]
enum CliCommands {
    Download {
        image_name: String,
        layer_dir: PathBuf,
        accept_invalid_certs: bool,
    },
    Inspect {
        image_name: String,
        accept_invalid_certs: bool,
    },
    Registry {
        source_path: PathBuf,
    },
}

#[derive(Parser)]
#[command(styles=maelstrom_util::clap::styles())]
struct CliOptions {
    #[command(subcommand)]
    command: CliCommands,
}

async fn resolve_name(image_name: &str, accept_invalid_certs: bool) -> Result<DockerReference> {
    let image_name: ImageName = image_name.parse()?;

    let ImageName::Docker(mut ref_) = image_name else {
        bail!("local image path not supported yet");
    };

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(accept_invalid_certs)
        .build()
        .unwrap();
    let mut downloader = ImageDownloader::new(client);
    let digest = downloader.resolve_tag(&ref_).await?;
    println!("digest = {digest:#?}");
    ref_.tag = None;
    ref_.digest = Some(digest);
    Ok(ref_)
}

#[tokio::main]
async fn run(opt: CliOptions, log: slog::Logger) -> Result<()> {
    match opt.command {
        CliCommands::Download {
            image_name,
            layer_dir,
            accept_invalid_certs,
        } => {
            let ref_ = resolve_name(&image_name, accept_invalid_certs).await?;

            let ind = indicatif::ProgressBar::new(0);
            let client = reqwest::Client::builder()
                .danger_accept_invalid_certs(accept_invalid_certs)
                .build()
                .unwrap();
            let downloader = ImageDownloader::new(client);
            let image = downloader.download_image(&ref_, &layer_dir, ind).await?;
            println!("{image:#?}");
        }
        CliCommands::Inspect {
            image_name,
            accept_invalid_certs,
        } => {
            let ref_ = resolve_name(&image_name, accept_invalid_certs).await?;

            let client = reqwest::Client::builder()
                .danger_accept_invalid_certs(accept_invalid_certs)
                .build()
                .unwrap();
            let downloader = ImageDownloader::new(client);
            let resp = downloader.inspect(&ref_).await?;
            println!("{resp:#?}");
        }
        CliCommands::Registry { source_path } => {
            let registry = LocalRegistry::new(source_path, log).await?;
            println!("listening on {}", registry.address()?);
            registry.run_until_error().await?;
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let opt = CliOptions::parse();
    maelstrom_util::log::run_with_logger(LogLevel::Debug, |log| run(opt, log))
}
