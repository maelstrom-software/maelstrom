use clap::{command, Args};

#[derive(Args)]
#[command(next_help_heading = "Test Selection Options")]
pub struct ExtraCommandLineOptions {
    #[command(flatten)]
    pub parent: maelstrom_test_runner::config::ExtraCommandLineOptions,

    #[command(flatten)]
    pub list: ListOptions,

    #[command(flatten)]
    pub test_metadata: TestMetadataOptions,
}

#[derive(Args)]
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
        long = "list-binaries",
        help = "Instead of running tests, print the test binaries of those tests that would \
            have been run."
    )]
    pub binaries: bool,

    #[arg(
        long = "list-packages",
        help = "Instead of running tests, print the packages of those tests that would \
            have been run."
    )]
    pub packages: bool,
}

#[derive(Args)]
#[command(next_help_heading = "Test Metadata Options")]
pub struct TestMetadataOptions {
    #[arg(
        long,
        help = "Write out a starter test metadata file if one does not exist, then exit."
    )]
    pub init: bool,
}

impl AsRef<maelstrom_test_runner::config::ExtraCommandLineOptions> for ExtraCommandLineOptions {
    fn as_ref(&self) -> &maelstrom_test_runner::config::ExtraCommandLineOptions {
        &self.parent
    }
}
