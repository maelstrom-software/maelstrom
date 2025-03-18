use maelstrom_macro::Config;
use maelstrom_test_runner::config::{AsParts, Config as TestRunnerConfig};

#[derive(Config, Debug)]
pub struct Config {
    #[config(flatten)]
    pub parent: TestRunnerConfig,

    #[config(flatten, next_help_heading = "Go Test Config Values")]
    pub go_test: GoTestConfig,
}

#[derive(Clone, Config, Debug, Default)]
pub struct GoTestConfig {
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

impl AsRef<TestRunnerConfig> for Config {
    fn as_ref(&self) -> &TestRunnerConfig {
        &self.parent
    }
}

impl AsParts for Config {
    type First = TestRunnerConfig;
    type Second = GoTestConfig;
    fn as_parts(&self) -> (&TestRunnerConfig, &GoTestConfig) {
        (&self.parent, &self.go_test)
    }
}
