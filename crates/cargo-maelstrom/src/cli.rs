use clap::{command, Args};

#[derive(Args)]
#[command(next_help_heading = "Test Selection Options")]
pub struct ExtraCommandLineOptions {
    #[arg(
        long,
        short = 'i',
        value_name = "FILTER-EXPRESSION",
        default_value = "all",
        help = "Only include tests which match the given filter. Can be specified multiple times."
    )]
    pub include: Vec<String>,

    #[arg(
        long,
        short = 'x',
        value_name = "FILTER-EXPRESSION",
        help = "Only include tests which don't match the given filter. Can be specified multiple times."
    )]
    pub exclude: Vec<String>,

    #[command(flatten)]
    pub list: ListOptions,

    #[command(flatten)]
    pub test_metadata: TestMetadataOptions,

    #[arg(long, hide(true), help = "Only used for testing purposes.")]
    pub client_bg_proc: bool,
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
