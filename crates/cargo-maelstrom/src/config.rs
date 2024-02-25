use derive_more::From;
use maelstrom_util::config::{BrokerAddr, LogLevel};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::fmt::{self, Debug, Formatter};

#[derive(Clone, Deserialize, From)]
#[serde(transparent)]
pub struct Quiet(bool);

impl Quiet {
    pub fn into_inner(self) -> bool {
        self.0
    }
}

impl Debug for Quiet {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub broker: BrokerAddr,
    pub run: RunConfig,
    pub log_level: LogLevel,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    pub quiet: Quiet,
}

#[skip_serializing_none]
#[derive(Serialize)]
pub struct ConfigOptions {
    pub broker: Option<String>,
    pub run: RunConfigOptions,
    pub log_level: Option<LogLevel>,
}

#[skip_serializing_none]
#[derive(Serialize)]
pub struct RunConfigOptions {
    pub quiet: Option<bool>,
}

impl Default for ConfigOptions {
    fn default() -> Self {
        ConfigOptions {
            broker: None,
            run: RunConfigOptions { quiet: Some(false) },
            log_level: Some(LogLevel::Error),
        }
    }
}
