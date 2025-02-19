use clap::{command, Args};
use maelstrom_test_runner::config::{
    ExtraCommandLineOptions as TestRunnerExtraCommandLineOptions, IntoParts,
};

#[derive(Args)]
#[command(next_help_heading = "Test Selection Options")]
pub struct ExtraCommandLineOptions {
    #[command(flatten)]
    pub parent: TestRunnerExtraCommandLineOptions,

    #[arg(
        long = "list",
        help = "Instead of running tests, print the tests that would have been run.",
        help_heading = "List Options"
    )]
    pub list: bool,
}

impl AsRef<TestRunnerExtraCommandLineOptions> for ExtraCommandLineOptions {
    fn as_ref(&self) -> &TestRunnerExtraCommandLineOptions {
        &self.parent
    }
}

impl IntoParts for ExtraCommandLineOptions {
    type First = TestRunnerExtraCommandLineOptions;
    type Second = bool;
    fn into_parts(self) -> (TestRunnerExtraCommandLineOptions, bool) {
        (self.parent, self.list)
    }
}
