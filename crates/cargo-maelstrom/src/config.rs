use crate::cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions};
use maelstrom_macro::Config;
use maelstrom_test_runner::config::{Config as TestRunnerConfig, IntoParts};

#[derive(Config, Debug)]
pub struct Config {
    #[config(flatten)]
    pub parent: TestRunnerConfig,

    #[config(flatten)]
    pub cargo: CargoConfig,
}

#[derive(Config, Debug)]
pub struct CargoConfig {
    #[config(flatten, next_help_heading = "Feature Selection Config Options")]
    pub feature_selection_options: FeatureSelectionOptions,

    #[config(flatten, next_help_heading = "Compilation Config Options")]
    pub compilation_options: CompilationOptions,

    #[config(flatten, next_help_heading = "Manifest Config Options")]
    pub manifest_options: ManifestOptions,

    /// Extra arguments to pass to the test binary. Refer to the help text of a test binary.
    #[config(
        var_arg,
        value_name = "EXTRA-TEST-BINARY-ARGS",
        default = r#""no args""#,
        next_help_heading = "Test Binary Options"
    )]
    pub extra_test_binary_args: Vec<String>,
}

impl AsRef<TestRunnerConfig> for Config {
    fn as_ref(&self) -> &TestRunnerConfig {
        &self.parent
    }
}

impl IntoParts for Config {
    type First = TestRunnerConfig;
    type Second = CargoConfig;
    fn into_parts(self) -> (TestRunnerConfig, CargoConfig) {
        (self.parent, self.cargo)
    }
}
