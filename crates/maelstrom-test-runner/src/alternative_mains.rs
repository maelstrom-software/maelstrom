use crate::metadata::DEFAULT_TEST_METADATA;
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
    test_toml_name: &str,
    test_toml_specific_contents: &str,
) -> Result<ExitCode> {
    let path = project_dir.join(test_toml_name);
    if !Fs.exists(&path) {
        Fs.write(
            &path,
            [DEFAULT_TEST_METADATA, test_toml_specific_contents].join(""),
        )?;
        println!("Wrote default config to {path}.");
        Ok(ExitCode::SUCCESS)
    } else {
        println!("Config already exists at {path}. Doing nothing.");
        Ok(ExitCode::FAILURE)
    }
}
