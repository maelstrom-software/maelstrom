use anyhow::Result;
use maelstrom_go_test::cli::ExtraCommandLineOptions;
use maelstrom_test_runner::TestRunner;
use maelstrom_util::process::ExitCode;
use std::env;

struct MaelstromGoTestTestRunner;

impl TestRunner for MaelstromGoTestTestRunner {
    fn get_base_directory_prefix(&self) -> &'static str {
        "maelstrom/maelstrom-go-test"
    }

    fn get_environment_variable_prefix(&self) -> &'static str {
        "MAELSTROM_GO_TEST"
    }
}

pub fn main() -> Result<ExitCode> {
    maelstrom_test_runner::main(
        clap::command!(),
        env::args(),
        MaelstromGoTestTestRunner,
        |extra_options: &ExtraCommandLineOptions| extra_options.list.any(),
        |_| Ok(".".into()),
        maelstrom_go_test::TEST_METADATA_FILE_NAME,
        maelstrom_go_test::DEFAULT_TEST_METADATA_CONTENTS,
        maelstrom_go_test::main,
    )
}
