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
}

impl AsRef<maelstrom_test_runner::config::Config> for Config {
    fn as_ref(&self) -> &maelstrom_test_runner::config::Config {
        &self.parent
    }
}
