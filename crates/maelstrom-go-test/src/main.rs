use anyhow::Result;
use clap::command;
use maelstrom_go_test::TestRunner;
use maelstrom_util::process::ExitCode;
use std::env;

pub fn main() -> Result<ExitCode> {
    maelstrom_test_runner::main::<TestRunner>(command!(), env::args())
}
