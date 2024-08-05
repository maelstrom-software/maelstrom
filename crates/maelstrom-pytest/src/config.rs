use maelstrom_macro::Config;

#[derive(Config, Clone, Debug, Default)]
pub struct PytestConfigValues {
    /// Collect tests from the provided module instead of using pytest's default collection
    /// algorithm. This will pass the provided module to pytest along with the --pyargs flag.
    #[config(
        option,
        value_name = "MODULE",
        default = r#""pytest's default collection algorithm""#
    )]
    pub collect_from_module: Option<String>,

    /// Extra arguments to pass to pytest when running the test. See `pytest --help` to see what it
    /// accepts.
    #[config(var_arg, value_name = "EXTRA-PYTEST-ARGS", default = r#""no args""#)]
    pub extra_pytest_args: Vec<String>,
}

#[derive(Config, Debug)]
pub struct Config {
    #[config(flatten)]
    pub parent: maelstrom_test_runner::config::Config,

    #[config(flatten, next_help_heading = "Pytest Config Options")]
    pub pytest_options: PytestConfigValues,
}

impl AsRef<maelstrom_test_runner::config::Config> for Config {
    fn as_ref(&self) -> &maelstrom_test_runner::config::Config {
        &self.parent
    }
}
