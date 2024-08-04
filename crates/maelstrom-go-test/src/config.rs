use maelstrom_macro::Config;

#[derive(Config, Debug)]
pub struct Config {
    #[config(flatten)]
    pub parent: maelstrom_test_runner::config::Config,

    #[config(flatten, next_help_heading = "Go Testing Options")]
    pub go_test_options: GoTestOptions,
}

#[derive(Config, Clone, Debug, Default)]
pub struct GoTestOptions {
    /// Controls the value of the `-vet` flag being passed to `go test`. See `go help test` for
    /// details.
    #[config(option, default = r#""go test's default""#, value_name = "VET-OPTIONS")]
    pub vet: Option<String>,

    /// Tells long-running tests to shorten their run time. See `go help testflag` for details.
    #[config(flag)]
    pub short: bool,

    /// Shows the full file names in error messages. See `go help testflag` for details.
    #[config(flag)]
    pub fullpath: bool,

    /// Extra arguments to pass to the test binary. See `go help testflag` for what it normally
    /// accepts.
    #[config(
        var_arg,
        value_name = "EXTRA-TEST-BINARY-ARGS",
        default = r#""no args""#
    )]
    pub extra_test_binary_args: Vec<String>,
}

impl AsRef<maelstrom_test_runner::config::Config> for Config {
    fn as_ref(&self) -> &maelstrom_test_runner::config::Config {
        &self.parent
    }
}
