use anyhow::Result;
use maelstrom_base::Utf8PathBuf;
use maelstrom_pytest::cli::ExtraCommandLineOptions;
use maelstrom_util::process::ExitCode;
use std::env;

pub fn main() -> Result<ExitCode> {
    maelstrom_test_runner::main(
        clap::command!(),
        "maelstrom/maelstrom-pytest",
        "MAELSTROM_PYTEST",
        env::args(),
        |extra_options: &ExtraCommandLineOptions| extra_options.list,
        |_| -> Result<Utf8PathBuf> { Ok(".".into()) },
        maelstrom_pytest::TEST_METADATA_FILE_NAME,
        maelstrom_pytest::ADDED_DEFAULT_TEST_METADATA,
        maelstrom_pytest::main,
    )
}
