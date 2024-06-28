use clap::{command, Args};

#[derive(Args)]
#[command(next_help_heading = "Test Selection Options")]
pub struct ExtraCommandLineOptions {
    #[command(flatten)]
    pub parent: maelstrom_test_runner::config::ExtraCommandLineOptions,

    #[arg(
        long = "list",
        help = "Instead of running tests, print the tests that would have been run.",
        help_heading = "List Options"
    )]
    pub list: bool,
}

impl AsRef<maelstrom_test_runner::config::ExtraCommandLineOptions> for ExtraCommandLineOptions {
    fn as_ref(&self) -> &maelstrom_test_runner::config::ExtraCommandLineOptions {
        &self.parent
    }
}
