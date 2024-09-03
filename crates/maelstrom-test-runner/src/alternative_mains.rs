use anyhow::Result;
use maelstrom_base::Utf8Path;
use maelstrom_util::{fs::Fs, process::ExitCode};

pub fn client_bg_proc() -> Result<ExitCode> {
    maelstrom_client::bg_proc_main()?;
    Ok(ExitCode::from(0))
}

/// Write out a default config file to `<project-dir>/<TEST_TOML>` if nothing exists there already.
pub fn init(
    project_dir: &Utf8Path,
    test_metadata_file_name: &str,
    test_metdata_default_contents: &str,
) -> Result<ExitCode> {
    let path = project_dir.join(test_metadata_file_name);
    if !Fs.exists(&path) {
        Fs.write(&path, test_metdata_default_contents)?;
        println!("Wrote default config to {path}.");
        Ok(ExitCode::SUCCESS)
    } else {
        println!("Config already exists at {path}. Doing nothing.");
        Ok(ExitCode::FAILURE)
    }
}
