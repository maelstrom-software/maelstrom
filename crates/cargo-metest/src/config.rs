use derive_more::From;
use meticulous_util::config::BrokerAddr;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::fmt::{self, Debug, Formatter};

#[derive(Deserialize, From)]
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
    pub quiet: Quiet,
}

#[skip_serializing_none]
#[derive(Serialize)]
pub struct ConfigOptions {
    pub broker: Option<String>,
    pub quiet: Option<bool>,
}

impl Default for ConfigOptions {
    fn default() -> Self {
        ConfigOptions {
            broker: None,
            quiet: Some(false),
        }
    }
}
