use crate::{
    cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions},
    CargoOptions,
};
use maelstrom_macro::Config;
use maelstrom_test_runner::config::{Config as TestRunnerConfig, IntoParts};

#[derive(Config, Debug)]
pub struct Config {
    #[config(flatten)]
    pub parent: TestRunnerConfig,

    #[config(flatten, next_help_heading = "Feature Selection Config Options")]
    pub cargo_feature_selection_options: FeatureSelectionOptions,

    #[config(flatten, next_help_heading = "Compilation Config Options")]
    pub cargo_compilation_options: CompilationOptions,

    #[config(flatten, next_help_heading = "Manifest Config Options")]
    pub cargo_manifest_options: ManifestOptions,

    /// Extra arguments to pass to the test binary. See the help text for a test binary to see what
    /// it accepts.
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
    type Second = CargoOptions;
    fn into_parts(self) -> (TestRunnerConfig, CargoOptions) {
        (
            self.parent,
            CargoOptions {
                feature_selection_options: self.cargo_feature_selection_options,
                compilation_options: self.cargo_compilation_options,
                manifest_options: self.cargo_manifest_options,
                extra_test_binary_args: self.extra_test_binary_args,
            },
        )
    }
}
