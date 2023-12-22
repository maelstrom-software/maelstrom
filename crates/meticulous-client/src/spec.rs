pub mod substitute;

use anyhow::{Error, Result};
use enumset::{EnumSet, EnumSetType};
use serde::{de, Deserialize, Serialize};
use std::{
    env::{self, VarError},
    path::PathBuf,
};

pub fn std_env_lookup(var: &str) -> Result<Option<String>> {
    match env::var(var) {
        Ok(val) => Ok(Some(val)),
        Err(VarError::NotPresent) => Ok(None),
        Err(err) => Err(Error::new(err)),
    }
}

#[derive(Default)]
pub struct ImageConfig {
    pub layers: Vec<PathBuf>,
    pub working_directory: Option<PathBuf>,
    pub environment: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, EnumSetType, Serialize)]
#[serde(rename_all = "snake_case")]
#[enumset(serialize_repr = "list")]
pub enum ImageUse {
    Layers,
    Environment,
    WorkingDirectory,
}

#[derive(Deserialize)]
pub struct Image {
    pub name: String,
    #[serde(rename = "use")]
    pub use_: EnumSet<ImageUse>,
}

#[derive(PartialEq, Eq, Debug, Deserialize)]
pub enum PossiblyImage<T> {
    Image,
    Explicit(T),
}

pub fn incompatible<T, E>(field: &Option<T>, msg: &str) -> std::result::Result<(), E>
where
    E: de::Error,
{
    if field.is_some() {
        Err(E::custom(format_args!("{}", msg)))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn std_env_lookup_good() {
        let var = "AN_ENVIRONMENT_VARIABLE_1";
        let val = "foobar";
        env::set_var(var, val);
        assert_eq!(std_env_lookup(var).unwrap(), Some(val.to_string()));
    }

    #[test]
    fn std_env_lookup_missing() {
        let var = "AN_ENVIRONMENT_VARIABLE_TO_DELETE";
        env::remove_var(var);
        assert_eq!(std_env_lookup(var).unwrap(), None);
    }

    #[test]
    fn std_env_lookup_error() {
        let var = "AN_ENVIRONMENT_VARIABLE_2";
        let val = unsafe { std::ffi::OsString::from_encoded_bytes_unchecked(vec![0xff]) };
        env::set_var(var, &val);
        assert_eq!(
            format!("{}", std_env_lookup(var).unwrap_err()),
            r#"environment variable was not valid unicode: "\xFF""#
        );
    }
}
