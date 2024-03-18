use crate::cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions};
use anyhow::{Context as _, Result};
use clap::{Arg, ArgAction, ArgMatches, Command};
use derive_more::From;
use maelstrom_util::config::{BrokerAddr, LogLevel};
use serde::Deserialize;
use std::{
    env,
    fmt::{self, Debug, Formatter},
    fs,
    path::PathBuf,
    process, result,
};
use xdg::BaseDirectories;

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

impl Config {
    pub fn add_command_line_options(command: Command) -> Command {
        command
        .styles(maelstrom_util::clap::styles())
        .after_help(
            "Configuration values can be specified in three ways: fields in a config file, \
            environment variables, or command-line options. Command-line options have the \
            highest precendence, followed by environment variables.\n\
            \n\
            The configuration value 'config_value' would be set via the '--config-value' \
            command-line option, the CARGO_MAELSTROM_CONFIG_VALUE environment variable, \
            and the 'config_value' key in a configuration file.\n\
            \n\
            All values except for 'broker' have reasonable defaults.")
        .next_help_heading("Config Options")
        .arg(
            Arg::new("config-file")
                .long("config-file")
                .short('c')
                .value_name("PATH")
                .action(ArgAction::Set)
                .help(
                    "Configuration file. Values set in the configuration file will be overridden by \
                    values set through environment variables and values set on the command line"
                )
        )
        .arg(
            Arg::new("print-config")
                .long("print-config")
                .short('P')
                .action(ArgAction::SetTrue)
                .help("Print configuration and exit"),
        )
        .arg(
            Arg::new("broker")
                .long("broker")
                .short('b')
                .value_name("SOCKADDR")
                .action(ArgAction::Set)
                .help(r#"Socket address of broker. Examples: "[::]:5000", "host.example.com:2000""#)
        )
        .arg(
            Arg::new("log-level")
                .long("log-level")
                .short('l')
                .value_name("LEVEL")
                .action(ArgAction::Set)
                .help("Minimum log level to output")
        )
        .arg(
            Arg::new("quiet")
                .long("quiet")
                .short('q')
                .action(ArgAction::SetTrue)
                .help("Don't output information about the tests being run")
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
                .short('t')
                .value_name("SECONDS")
                .action(ArgAction::Set)
                .help("Override timeout value for all tests specified (O indicates no timeout)")
        )
        .next_help_heading("Feature Selection")
        .arg(
            Arg::new("features")
                .long("features")
                .short('F')
                .value_name("FEATURES")
                .action(ArgAction::Set)
                .help("Comma separated list of features to activate")
        )
        .arg(
            Arg::new("all-features")
                .long("all-features")
                .action(ArgAction::SetTrue)
                .help("Activate all available features")
        )
        .arg(
            Arg::new("no-default-features")
                .long("no-default-features")
                .action(ArgAction::SetTrue)
                .help("Do not activate the `default` feature")
        )
        .next_help_heading("Compilation Options")
        .arg(
            Arg::new("profile")
                .long("profile")
                .value_name("PROFILE-NAME")
                .action(ArgAction::Set)
                .help("Build artifacts with the specified profile")
        )
        .arg(
            Arg::new("target")
                .long("target")
                .value_name("TRIPLE")
                .action(ArgAction::Set)
                .help("Build for the target triple")
        )
        .arg(
            Arg::new("target-dir")
                .long("target-dir")
                .value_name("DIRECTORY")
                .action(ArgAction::Set)
                .help("Directory for all generated artifacts")
        )
        .next_help_heading("Manifest Options")
        .arg(
            Arg::new("manifest-path")
                .long("manifest-path")
                .value_name("PATH")
                .action(ArgAction::Set)
                .help("Path to Cargo.toml")
        )
        .arg(
            Arg::new("frozen")
                .long("frozen")
                .action(ArgAction::SetTrue)
                .help("Require that Cargo.lock and cache are both up to date")
        )
        .arg(
            Arg::new("locked")
                .long("locked")
                .action(ArgAction::SetTrue)
                .help("Require that Cargo.lock is up to date")
        )
        .arg(
            Arg::new("offline")
                .long("offline")
                .action(ArgAction::SetTrue)
                .help("Run without cargo accessing the network")
        )
    }

    pub fn new(mut args: ArgMatches) -> Result<Self> {
        let env = env::vars().filter(|(key, _)| key.starts_with("CARGO_MAELSTROM_"));

        let config_files = match args.remove_one::<String>("config-file").as_deref() {
            Some("-") => vec![],
            Some(config_file) => vec![PathBuf::from(config_file)],
            None => BaseDirectories::with_prefix("maelstrom/cargo-maelstrom")
                .context("searching for config files")?
                .find_config_files("config.toml")
                .rev()
                .collect(),
        };
        let mut files = vec![];
        for config_file in config_files {
            let contents = fs::read_to_string(&config_file).with_context(|| {
                format!("reading config file `{}`", config_file.to_string_lossy())
            })?;
            files.push((config_file.clone(), contents));
        }

        let print_config = args.remove_one::<bool>("print-config").unwrap();

        let config = maelstrom_config::ConfigBag::new(args, "CARGO_MAELSTROM_", env, files)
            .context("loading configuration from environment variables and config files")?;

        let config = Self {
            broker: config.get("broker")?,
            log_level: config.get_or("log-level", LogLevel::Info)?,
            quiet: config.get_flag("quiet")?.unwrap_or(Quiet::from(false)),
            timeout: config.get_option("timeout")?,
            cargo_feature_selection_options: FeatureSelectionOptions {
                features: config.get_option("features")?,
                all_features: config.get_flag("all-features")?.unwrap_or(false),
                no_default_features: config.get_flag("no-default-features")?.unwrap_or(false),
            },
            cargo_compilation_options: CompilationOptions {
                profile: config.get_option("profile")?,
                target: config.get_option("target")?,
                target_dir: config.get_option("target-dir")?,
            },
            cargo_manifest_options: ManifestOptions {
                manifest_path: config.get_option("manifest-path")?,
                frozen: config.get_flag("frozen")?.unwrap_or(false),
                locked: config.get_flag("locked")?.unwrap_or(false),
                offline: config.get_flag("offline")?.unwrap_or(false),
            },
        };

        if print_config {
            println!("{config:#?}");
            process::exit(0);
        }

        Ok(config)
    }
}
