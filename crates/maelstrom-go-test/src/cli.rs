use clap::{command, Args};
use maelstrom_test_runner::config::{
    ExtraCommandLineOptions as TestRunnerExtraCommandLineOptions, IntoParts,
};

#[derive(Args)]
#[command(next_help_heading = "Test Selection Options")]
pub struct ExtraCommandLineOptions {
    #[command(flatten)]
    pub parent: TestRunnerExtraCommandLineOptions,

    #[command(flatten)]
    pub list: ListOptions,
}

#[derive(Args, Debug, Default)]
#[group(multiple = false)]
#[command(next_help_heading = "List Options")]
pub struct ListOptions {
    #[arg(
        long = "list-tests",
        visible_alias = "list",
        help = "Instead of running tests, print the tests that would have been run. \
            May require building test binaries."
    )]
    pub tests: bool,

    #[arg(
        long = "list-packages",
        help = "Instead of running tests, print the packages of those tests that would \
            have been run."
    )]
    pub packages: bool,
}

impl AsRef<TestRunnerExtraCommandLineOptions> for ExtraCommandLineOptions {
    fn as_ref(&self) -> &TestRunnerExtraCommandLineOptions {
        &self.parent
    }
}

impl IntoParts for ExtraCommandLineOptions {
    type First = TestRunnerExtraCommandLineOptions;
    type Second = ListOptions;
    fn into_parts(self) -> (TestRunnerExtraCommandLineOptions, ListOptions) {
        (self.parent, self.list)
    }
}
