use anyhow::Result;
use maelstrom_client::ProjectDir;
use maelstrom_util::{fs::Fs, process::ExitCode, root::Root};

/// Write out a default config file to `<project-dir>/<TEST_TOML>` if nothing exists there already.
pub fn main(
    project_dir: &Root<ProjectDir>,
    test_metadata_file_name: &str,
    default_test_metadata_file_contents: &str,
) -> Result<ExitCode> {
    let path = project_dir
        .join::<()>(test_metadata_file_name)
        .into_path_buf();
    if !Fs.exists(&path) {
        Fs.write(&path, default_test_metadata_file_contents)?;
        println!("Wrote default config to {}.", path.to_string_lossy());
        Ok(ExitCode::SUCCESS)
    } else {
        println!(
            "Config already exists at {}. Doing nothing.",
            path.to_string_lossy()
        );
        Ok(ExitCode::FAILURE)
    }
}
