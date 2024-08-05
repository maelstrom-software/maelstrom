use crate::cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions};
use maelstrom_macro::Config;

#[derive(Config, Debug)]
pub struct Config {
    #[config(flatten)]
    pub parent: maelstrom_test_runner::config::Config,

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

impl AsRef<maelstrom_test_runner::config::Config> for Config {
    fn as_ref(&self) -> &maelstrom_test_runner::config::Config {
        &self.parent
    }
}
