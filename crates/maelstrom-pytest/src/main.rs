use anyhow::Result;
use maelstrom_base::Utf8PathBuf;
use maelstrom_pytest::cli::ExtraCommandLineOptions;
use maelstrom_test_runner::TestRunner;
use maelstrom_util::process::ExitCode;
use std::env;

struct MaelstromPytestTestRunner;

impl TestRunner for MaelstromPytestTestRunner {
    fn get_base_directory_prefix(&self) -> &'static str {
        "maelstrom/maelstrom-pytest"
    }

    fn get_environment_variable_prefix(&self) -> &'static str {
        "MAELSTROM_PYTEST"
    }

    fn get_test_metadata_file_name(&self) -> &str {
        maelstrom_pytest::TEST_METADATA_FILE_NAME
    }

    fn get_test_metadata_default_contents(&self) -> &str {
        maelstrom_pytest::DEFAULT_TEST_METADATA_CONTENTS
    }
}

pub fn main() -> Result<ExitCode> {
    maelstrom_test_runner::main(
        clap::command!(),
        env::args(),
        MaelstromPytestTestRunner,
        |extra_options: &ExtraCommandLineOptions| extra_options.list,
        |_| -> Result<Utf8PathBuf> { Ok(".".into()) },
        maelstrom_pytest::main,
    )
}
