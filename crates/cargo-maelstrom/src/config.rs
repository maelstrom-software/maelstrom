use crate::cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions};
use derive_more::From;
use maelstrom_macro::Config;
use maelstrom_util::config::common::{BrokerAddr, LogLevel};
use serde::Deserialize;
use std::{
    fmt::{self, Debug, Formatter},
    result,
};

#[derive(Clone, Deserialize, From)]
#[serde(transparent)]
pub struct Quiet(bool);

impl Quiet {
    pub fn into_inner(self) -> bool {
        self.0
    }
}

impl Debug for Quiet {
    fn fmt(&self, f: &mut Formatter<'_>) -> result::Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

#[derive(Config, Debug)]
pub struct Config {
    /// Socket address of broker.
    #[config(short = 'b', value_name = "SOCKADDR")]
    pub broker: BrokerAddr,

    /// Minimum log level to output.
    #[config(short = 'l', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,

    /// Don't output information about the tests being run.
    #[config(flag, short = 'q')]
    pub quiet: Quiet,

    /// Override timeout value for all tests specified (O indicates no timeout).
    #[config(
        option,
        short = 't',
        value_name = "SECONDS",
        default = r#""whatever individual tests specify""#,
        next_help_heading = "Test Override Config Options",
    )]
    pub timeout: Option<u32>,

    #[config(flatten, next_help_heading = "Feature Selection Config Options")]
    pub cargo_feature_selection_options: FeatureSelectionOptions,

    #[config(flatten, next_help_heading = "Compilation Config Options")]
    pub cargo_compilation_options: CompilationOptions,

    #[config(flatten, next_help_heading = "Manifest Config Options")]
    pub cargo_manifest_options: ManifestOptions,
}
