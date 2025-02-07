use anyhow::Result;
use cargo_maelstrom::cli::ExtraCommandLineOptions;
use maelstrom_test_runner::TestRunner;
use maelstrom_util::process::ExitCode;
use std::env;

struct CargoMaelstromTestRunner;

impl TestRunner for CargoMaelstromTestRunner {
    fn get_base_directory_prefix(&self) -> &'static str {
        "maelstrom/cargo-maelstrom"
    }

    fn get_environment_variable_prefix(&self) -> &'static str {
        "CARGO_MAELSTROM"
    }
}

pub fn main() -> Result<ExitCode> {
    let mut args = Vec::from_iter(env::args());
    if args.len() > 1 && args[0].ends_with(format!("cargo-{}", args[1]).as_str()) {
        args.remove(1);
    }

    maelstrom_test_runner::main(
        clap::command!(),
        args,
        CargoMaelstromTestRunner,
        |extra_options: &ExtraCommandLineOptions| extra_options.list.any(),
        cargo_maelstrom::get_project_dir,
        cargo_maelstrom::TEST_METADATA_FILE_NAME,
        cargo_maelstrom::DEFAULT_TEST_METADATA_CONTENTS,
        cargo_maelstrom::main,
    )
}
