use anyhow::Result;
use maelstrom_client::ProjectDir;
use maelstrom_util::{fs::Fs, process::ExitCode, root::Root};
use crate::TestRunner;

/// Write out a default config file to `<project-dir>/<TEST_TOML>` if nothing exists there already.
pub fn main<TestRunnerT: TestRunner>(
    project_dir: &Root<ProjectDir>,
) -> Result<ExitCode> {
    let path = project_dir
        .join::<()>(TestRunnerT::TEST_METADATA_FILE_NAME)
        .into_path_buf();
    if !Fs.exists(&path) {
        Fs.write(&path, TestRunnerT::DEFAULT_TEST_METADATA_FILE_CONTENTS)?;
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
