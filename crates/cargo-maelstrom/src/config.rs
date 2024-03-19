use crate::cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions};
use anyhow::Result;
use derive_more::From;
use maelstrom_config::CommandBuilder;
use maelstrom_util::config::{BrokerAddr, LogLevel};
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

#[derive(Debug)]
pub struct Config {
    pub broker: BrokerAddr,
    pub log_level: LogLevel,
    pub quiet: Quiet,
    pub timeout: Option<u32>,
    pub cargo_feature_selection_options: FeatureSelectionOptions,
    pub cargo_compilation_options: CompilationOptions,
    pub cargo_manifest_options: ManifestOptions,
}

impl maelstrom_config::Config for Config {
    fn add_command_line_options(builder: CommandBuilder) -> CommandBuilder {
        builder
            .value(
                "broker",
                Some('b'),
                "SOCKADDR",
                None,
                r#"Socket address of broker. Examples: "[::]:5000", "host.example.com:2000"."#,
            )
            .value(
                "log_level",
                Some('l'),
                "LEVEL",
                Some("info".to_string()),
                "Minimum log level to output.",
            )
            .flag_value(
                "quiet",
                Some('q'),
                "Don't output information about the tests being run.",
            )
            .next_help_heading("Test Override Config Options")
            .value(
                "timeout",
                Some('t'),
                "SECONDS",
                Some("whatever individual tests specify".to_string()),
                "Override timeout value for all tests specified (O indicates no timeout).",
            )
            .next_help_heading("Feature Selection Config Options")
            .value(
                "features",
                Some('F'),
                "FEATURES",
                Some("cargo's default".to_string()),
                "Comma separated list of features to activate.",
            )
            .flag_value(
                "all_features",
                None,
                "Activate all available features.",
            )
            .flag_value(
                "no_default_features",
                None,
                "Do not activate the `default` feature.",
            )
            .next_help_heading("Compilation Config Options")
            .value(
                "profile",
                None,
                "PROFILE-NAME",
                Some("cargo's default".to_string()),
                "Build artifacts with the specified profile.",
            )
            .value(
                "target",
                None,
                "TRIPLE",
                Some("cargo's default".to_string()),
                "Build for the target triple.",
            )
            .value(
                "target_dir",
                None,
                "DIRECTORY",
                Some("cargo's default".to_string()),
                "Directory for all generated artifacts."
            )
            .next_help_heading("Manifest Config Options")
            .value(
                "manifest_path",
                None,
                "PATH",
                Some("cargo's default".to_string()),
                "Path to Cargo.toml.",
            )
            .flag_value(
                "frozen",
                None,
                "Require that Cargo.lock and cache are both up to date.",
            )
            .flag_value(
                "locked",
                None,
                "Require that Cargo.lock is up to date.",
            )
            .flag_value(
                "offline",
                None,
                "Run without cargo accessing the network.",
            )
    }

    fn from_config_bag(config: &mut maelstrom_config::ConfigBag) -> Result<Self> {
        Ok(Self {
            broker: config.get("broker")?,
            log_level: config.get_or("log_level", LogLevel::Info)?,
            quiet: config.get_flag("quiet")?.unwrap_or(Quiet::from(false)),
            timeout: config.get_option("timeout")?,
            cargo_feature_selection_options: FeatureSelectionOptions {
                features: config.get_option("features")?,
                all_features: config.get_flag("all_features")?.unwrap_or(false),
                no_default_features: config.get_flag("no_default_features")?.unwrap_or(false),
            },
            cargo_compilation_options: CompilationOptions {
                profile: config.get_option("profile")?,
                target: config.get_option("target")?,
                target_dir: config.get_option("target_dir")?,
            },
            cargo_manifest_options: ManifestOptions {
                manifest_path: config.get_option("manifest_path")?,
                frozen: config.get_flag("frozen")?.unwrap_or(false),
                locked: config.get_flag("locked")?.unwrap_or(false),
                offline: config.get_flag("offline")?.unwrap_or(false),
            },
        })
    }
}
