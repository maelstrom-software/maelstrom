use crate::ui::UiKind;
use clap::{command, Args};
use derive_more::From;
use maelstrom_client::{AcceptInvalidRemoteContainerTlsCerts, ContainerImageDepotDir};
use maelstrom_macro::Config;
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots},
    root::RootBuf,
};
use serde::Deserialize;
use std::num::NonZeroUsize;
use std::{
    fmt::{self, Debug, Formatter},
    result, str,
};
use xdg::BaseDirectories;

#[derive(Copy, Clone, Deserialize, From)]
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

#[derive(Copy, Clone, Debug, Deserialize, From)]
#[serde(transparent)]
pub struct Repeat(NonZeroUsize);

impl From<Repeat> for usize {
    fn from(s: Repeat) -> usize {
        s.0.into()
    }
}

impl TryFrom<usize> for Repeat {
    type Error = <NonZeroUsize as TryFrom<usize>>::Error;

    fn try_from(t: usize) -> result::Result<Self, Self::Error> {
        Ok(Self(t.try_into()?))
    }
}

impl Default for Repeat {
    fn default() -> Self {
        Self::from(NonZeroUsize::new(1).unwrap())
    }
}

impl str::FromStr for Repeat {
    type Err = <NonZeroUsize as str::FromStr>::Err;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        Ok(Self::from(NonZeroUsize::from_str(s)?))
    }
}

impl fmt::Display for Repeat {
    fn fmt(&self, f: &mut Formatter<'_>) -> result::Result<(), fmt::Error> {
        fmt::Display::fmt(&self.0, f)
    }
}

#[derive(Config, Debug)]
pub struct Config {
    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative. SI and binary suffixes are supported.
    #[config(
        value_name = "BYTES",
        default = "CacheSize::default()",
        next_help_heading = "Local Worker Config Options"
    )]
    pub cache_size: CacheSize,

    /// The maximum amount of bytes to return inline for captured stdout and stderr.
    #[config(value_name = "BYTES", default = "InlineLimit::default()")]
    pub inline_limit: InlineLimit,

    /// The number of job slots available.
    #[config(value_name = "N", default = "Slots::default()")]
    pub slots: Slots,

    /// Directory in which to put cached container images.
    #[config(
        value_name = "PATH",
        default = r#"|bd: &BaseDirectories| {
            bd.get_cache_home()
                .parent()
                .unwrap()
                .join("container/")
                .into_os_string()
                .into_string()
                .unwrap()
        }"#,
        next_help_heading = "Container Image Config Options"
    )]
    pub container_image_depot_root: RootBuf<ContainerImageDepotDir>,

    /// Accept invalid TLS certificates when downloading container images.
    #[config(flag)]
    pub accept_invalid_remote_container_tls_certs: AcceptInvalidRemoteContainerTlsCerts,

    /// Socket address of broker. If not provided, all tests will be run locally.
    #[config(
        option,
        short = 'b',
        value_name = "SOCKADDR",
        default = r#""standalone mode""#,
        next_help_heading = "Test Runner Config Options"
    )]
    pub broker: Option<BrokerAddr>,

    /// Minimum log level to output.
    #[config(short = 'l', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,

    /// Don't output information about the tests being run.
    #[config(flag, short = 'q')]
    pub quiet: Quiet,

    /// The UI style to use. Options are "auto", "simple", and "fancy".
    #[config(value_name = "UI-STYLE", default = "UiKind::Auto")]
    pub ui: UiKind,

    /// Runs all of the selected tests this many times. Must be non-zero.
    #[config(alias = "loop", value_name = "COUNT", default = "Repeat::default()")]
    pub repeat: Repeat,

    /// Override timeout value for all tests specified (O indicates no timeout).
    #[config(
        option,
        short = 't',
        value_name = "SECONDS",
        default = r#""whatever individual tests specify""#,
        next_help_heading = "Test Override Config Options"
    )]
    pub timeout: Option<u32>,
}

#[derive(Args, Default)]
#[command(next_help_heading = "Test Selection Options")]
#[group(id = "TestRunnerExtraCommandLineOptions")]
pub struct ExtraCommandLineOptions {
    #[arg(
        long,
        short = 'i',
        value_name = "FILTER-EXPRESSION",
        default_value = "all",
        help = "Only include tests which match the given filter. Can be specified multiple times.",
        help_heading = "Test Selection Options"
    )]
    pub include: Vec<String>,

    #[arg(
        long,
        short = 'x',
        value_name = "FILTER-EXPRESSION",
        help = "Only include tests which don't match the given filter. Can be specified multiple times.",
        help_heading = "Test Selection Options"
    )]
    pub exclude: Vec<String>,

    #[arg(
        long,
        help = "Write out a starter test metadata file if one does not exist, then exit.",
        help_heading = "Test Metadata Options"
    )]
    pub init: bool,

    #[arg(long, hide(true), help = "Only used for testing purposes.")]
    pub client_bg_proc: bool,
}
