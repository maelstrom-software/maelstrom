use anyhow::Result;
use maelstrom_go_test::cli::ExtraCommandLineOptions;
use maelstrom_util::process::ExitCode;
use std::env;

pub fn main() -> Result<ExitCode> {
    maelstrom_test_runner::main(
        clap::command!(),
        env::args(),
        maelstrom_go_test::TestRunner,
        |extra_options: &ExtraCommandLineOptions| extra_options.list.any(),
        |_| Ok(".".into()),
        maelstrom_go_test::main,
    )
}
