use crate::ui::UiKind;
use clap::{command, Args};
use derive_more::From;
use maelstrom_client::config::Config as ClientConfig;
use maelstrom_macro::Config;
use maelstrom_util::config::common::LogLevel;
use serde::Deserialize;
use std::num::NonZeroUsize;
use std::{
    fmt::{self, Debug, Formatter},
    result, str,
};

macro_rules! non_zero_usize_wrapper {
    ($name:ident) => {
        #[derive(Copy, Clone, Debug, Deserialize, From)]
        #[serde(transparent)]
        pub struct $name(NonZeroUsize);

        impl $name {
            pub fn as_usize(self) -> usize {
                self.0.get()
            }
        }

        impl From<$name> for usize {
            fn from(s: $name) -> usize {
                s.as_usize()
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

pub trait AsParts {
    type First;
    type Second;
    fn as_parts(&self) -> (&Self::First, &Self::Second);
}

#[derive(Config, Debug)]
pub struct Config {
    /// Minimum log level to output.
    #[config(short = 'L', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,

    /// The UI style to use. Options are "auto", "simple", "quiet", and "fancy".
    #[config(value_name = "UI-STYLE", default = "UiKind::Auto")]
    pub ui: UiKind,

    /// The number of times to run each selected test. Must be non-zero.
    #[config(
        short = 'r',
        alias = "loop",
        value_name = "COUNT",
        default = "Repeat::default()"
    )]
    pub repeat: Repeat,

    /// Stop running tests after the given number of failures are encountered.
    #[config(
        option,
        short = 's',
        value_name = "NUM-FAILURES",
        default = r#""never stop""#
    )]
    pub stop_after: Option<StopAfter>,

    /// Override timeout value for all tests specified (O indicates no timeout).
    #[config(
        option,
        short = 't',
        value_name = "SECONDS",
        default = r#""whatever individual tests specify""#,
        next_help_heading = "Test Override Config Values"
    )]
    pub timeout: Option<u32>,

    #[config(flatten, next_help_heading = "Cluster Communication Config Values")]
    pub client: ClientConfig,
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
        help_heading = "Test Runner Options"
    )]
    pub init: bool,

    #[arg(
        long,
        short = 'w',
        help = "Loop running tests then waiting for changes.",
        help_heading = "Test Runner Options"
    )]
    pub watch: bool,

    #[arg(long, hide(true), help = "Only used for testing purposes.")]
    pub client_process: bool,
}
