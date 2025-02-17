use crate::ui::UiKind;
use clap::{command, Args};
use derive_more::From;
use maelstrom_client::{AcceptInvalidRemoteContainerTlsCerts, ContainerImageDepotDir};
use maelstrom_macro::Config;
use maelstrom_util::{
    config::common::{
        ArtifactTransferStrategy, BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots,
    },
    root::RootBuf,
};
use serde::Deserialize;
use std::num::NonZeroUsize;
use std::{
    fmt::{self, Debug, Formatter},
    result, str,
};
use xdg::BaseDirectories;

macro_rules! non_zero_usize_wrapper {
    ($name:ident) => {
        #[derive(Copy, Clone, Debug, Deserialize, From)]
        #[serde(transparent)]
        pub struct $name(NonZeroUsize);

        impl From<$name> for usize {
            fn from(s: $name) -> usize {
                s.0.into()
            }
        }

        impl TryFrom<usize> for $name {
            type Error = <NonZeroUsize as TryFrom<usize>>::Error;

            fn try_from(t: usize) -> result::Result<Self, Self::Error> {
                Ok(Self(t.try_into()?))
            }
        }

        impl str::FromStr for $name {
            type Err = <NonZeroUsize as str::FromStr>::Err;

            fn from_str(s: &str) -> result::Result<Self, Self::Err> {
                Ok(Self::from(NonZeroUsize::from_str(s)?))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> result::Result<(), fmt::Error> {
                fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

non_zero_usize_wrapper!(Repeat);

impl Default for Repeat {
    fn default() -> Self {
        Self::from(NonZeroUsize::new(1).unwrap())
    }
}

non_zero_usize_wrapper!(StopAfter);

pub trait IntoParts {
    type First;
    type Second;
    fn into_parts(self) -> (Self::First, Self::Second);
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

    /// Controls how we upload artifacts when communicating with a remote broker.
    #[config(
        value_name = "ARTIFACT_TRANSFER_STRATEGY",
        default = r#""tcp-upload""#,
        hide
    )]
    pub artifact_transfer_strategy: ArtifactTransferStrategy,

    /// Minimum log level to output.
    #[config(short = 'l', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,

    /// The UI style to use. Options are "auto", "simple", "quiet", and "fancy".
    #[config(value_name = "UI-STYLE", default = "UiKind::Auto")]
    pub ui: UiKind,

    /// The number of times to run each selected test. Must be non-zero.
    #[config(alias = "loop", value_name = "COUNT", default = "Repeat::default()")]
    pub repeat: Repeat,

    /// Stop running tests after the given number of failures are encountered.
    #[config(option, value_name = "NUM-FAILURES", default = r#""never stop""#)]
    pub stop_after: Option<StopAfter>,

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
    pub client_process: bool,

    #[arg(
        long,
        hide(true),
        help = "Keep running tests in a loop waiting for changes in-between"
    )]
    pub watch: bool,
}
