use crate::cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions};
use derive_more::From;
use maelstrom_macro::Config;
use maelstrom_util::config::common::{BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots};
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
    /// Socket address of broker. If not provided, all tests will be run locally.
    #[config(
        option,
        short = 'b',
        value_name = "SOCKADDR",
        default = r#""standalone mode""#
    )]
    pub broker: Option<BrokerAddr>,

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
        next_help_heading = "Test Override Config Options"
    )]
    pub timeout: Option<u32>,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative. SI and binary suffixes are supported.
    #[config(
        short = 's',
        value_name = "BYTES",
        default = "CacheSize::default()",
        next_help_heading = "Local Worker Options"
    )]
    pub cache_size: CacheSize,

    /// The maximum amount of bytes to return inline for captured stdout and stderr.
    #[config(short = 'i', value_name = "BYTES", default = "InlineLimit::default()")]
    pub inline_limit: InlineLimit,

    /// The number of job slots available.
    #[config(short = 'S', value_name = "N", default = "Slots::default()")]
    pub slots: Slots,

    #[config(flatten, next_help_heading = "Feature Selection Config Options")]
    pub cargo_feature_selection_options: FeatureSelectionOptions,

    #[config(flatten, next_help_heading = "Compilation Config Options")]
    pub cargo_compilation_options: CompilationOptions,

    #[config(flatten, next_help_heading = "Manifest Config Options")]
    pub cargo_manifest_options: ManifestOptions,
}
