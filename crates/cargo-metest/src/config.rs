use serde::Deserialize;
use std::fmt::{self, Debug, Formatter};

#[derive(Deserialize)]
#[serde(transparent)]
pub struct Quiet(bool);

impl Quiet {
    pub fn into_inner(self) -> bool {
        self.0
    }
}

impl From<bool> for Quiet {
    fn from(value: bool) -> Self {
        Quiet(value)
    }
}

impl Debug for Quiet {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}
