use anyhow::Result;
use clap::command;
use maelstrom_pytest::TestRunner;
use maelstrom_util::process::ExitCode;
use std::env;

pub fn main() -> Result<ExitCode> {
    maelstrom_test_runner::main(command!(), env::args(), TestRunner)
}
