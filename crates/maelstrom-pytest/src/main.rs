use anyhow::Result;
use maelstrom_util::process::ExitCode;
use std::env;

pub fn main() -> Result<ExitCode> {
    maelstrom_test_runner::main(
        clap::command!(),
        env::args(),
        maelstrom_pytest::TestRunner,
        maelstrom_pytest::main,
    )
}
