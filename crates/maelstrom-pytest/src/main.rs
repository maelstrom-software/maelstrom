use anyhow::Result;
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
        maelstrom_pytest::main,
    )
}
