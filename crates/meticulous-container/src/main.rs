use anyhow::Result;
use clap::Parser;
use meticulous_container::download_image;
use std::path::PathBuf;

#[derive(Parser)]
struct CliOptions {
    package_name: String,
    version: String,
    layer_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = CliOptions::parse();

    let image = download_image(&opt.package_name, &opt.version, &opt.layer_dir).await?;
    println!("{image:#?}");

    Ok(())
}
