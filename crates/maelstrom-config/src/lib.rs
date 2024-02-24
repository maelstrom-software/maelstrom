use anyhow::{bail, Context as _, Result};
use clap::{
    parser::{MatchesError, ValueSource},
    ArgMatches,
};
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf, str::FromStr};
use toml::Table;

pub struct Config {
    args: ArgMatches,
    env_prefix: &'static str,
    env: HashMap<String, String>,
    files: Vec<(PathBuf, Table)>,
}

impl Config {
    pub fn new(
        args: ArgMatches,
        env_prefix: &'static str,
        env: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
        files: impl IntoIterator<Item = (impl Into<PathBuf>, impl Into<String>)>,
    ) -> Result<Self> {
        let env = env.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        let files = files
            .into_iter()
            .map(|(path, contents)| {
                contents
                    .into()
                    .parse::<Table>()
                    .map(|table| (path.into(), table))
            })
            .collect::<std::result::Result<_, _>>()?;
        Ok(Self {
            args,
            env_prefix,
            env,
            files,
        })
    }

    pub fn get_internal<T, F>(&self, key: &str, default: Option<F>) -> Result<T>
    where
        T: FromStr + for<'a> Deserialize<'a>,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
        F: FnOnce() -> T,
    {
        let mut args_result = self.args.try_get_one::<String>(key);
        if let Err(MatchesError::UnknownArgument { .. }) = args_result {
            args_result = Ok(None);
        }
        let mut value = args_result
            .with_context(|| {
                format!("error getting matches data for command-line option `--{key}`")
            })?
            .map(String::as_str)
            .map(T::from_str)
            .transpose()
            .with_context(|| format!("error parsing command-line option `--{key}`"))?;
        if let Some(value) = value {
            return Ok(value);
        }

        let env_key: String = self
            .env_prefix
            .chars()
            .chain(key.chars())
            .map(|c| match c {
                '-' => '_',
                c => c.to_ascii_uppercase(),
            })
            .collect();
        value = self
            .env
            .get(&env_key)
            .map(String::as_str)
            .map(T::from_str)
            .transpose()
            .with_context(|| format!("error parsing environment variable `{env_key}`"))?;
        if let Some(value) = value {
            return Ok(value);
        }

        let toml_key: String = key
            .chars()
            .map(|c| match c {
                '-' => '_',
                c => c,
            })
            .collect();
        for (path, table) in &self.files {
            if let Some(value) = table.get(&toml_key) {
                return T::deserialize(value.clone()).with_context(|| {
                    format!(
                        "error parsing value for key `{toml_key}` in config file `{}`",
                        path.to_string_lossy()
                    )
                });
            }
        }

        if let Some(default) = default {
            Ok(default())
        } else {
            bail!(
                "config value `{key}` must be set via `--{key}` command-line option, \
                `{env_key}` environment variable, or `{toml_key}` key in config file"
            )
        }
    }

    fn noop<T>() -> T {
        panic!()
    }

    pub fn get<T>(&self, key: &str) -> Result<T>
    where
        T: FromStr + for<'a> Deserialize<'a>,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        let mut default = Some(|| Self::noop::<T>());
        default.take();
        self.get_internal(key, default)
    }

    pub fn get_or<T>(&self, key: &str, default: T) -> Result<T>
    where
        T: FromStr + for<'a> Deserialize<'a>,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        self.get_internal(key, Some(|| default))
    }

    pub fn get_or_else<T, F>(&self, key: &str, default: F) -> Result<T>
    where
        T: FromStr + for<'a> Deserialize<'a>,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
        F: FnMut() -> T,
    {
        self.get_internal(key, Some(default))
    }

    pub fn get_flag<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: From<bool> + for<'a> Deserialize<'a>,
    {
        let mut args_result = self.args.try_get_one::<bool>(key);
        if let Err(MatchesError::UnknownArgument { .. }) = args_result {
            args_result = Ok(None);
        }
        if let Ok(Some(_)) = args_result {
            if self.args.value_source(key).unwrap() == ValueSource::DefaultValue {
                args_result = Ok(None);
            }
        }
        let mut value = args_result?.copied().map(T::from);
        if value.is_some() {
            return Ok(value);
        }

        let env_key: String = self
            .env_prefix
            .chars()
            .chain(key.chars())
            .map(|c| match c {
                '-' => '_',
                c => c.to_ascii_uppercase(),
            })
            .collect();
        value = self
            .env
            .get(&env_key)
            .map(String::as_str)
            .map(bool::from_str)
            .transpose()?
            .map(T::from);

        if value.is_some() {
            return Ok(value);
        }

        let toml_key: String = key
            .chars()
            .map(|c| match c {
                '-' => '_',
                c => c,
            })
            .collect();
        for (_, table) in &self.files {
            if let Some(value) = table.get(&toml_key) {
                return Ok(Some(T::deserialize(value.clone())).transpose()?);
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Arg, ArgAction, Command};
    use indoc::indoc;

    fn get_config() -> Config {
        let args = Command::new("command")
            .arg(Arg::new("key-1").long("key-1").action(ArgAction::Set))
            .arg(
                Arg::new("int-key-1")
                    .long("int-key-1")
                    .action(ArgAction::Set),
            )
            .arg(
                Arg::new("bool-key-1")
                    .long("bool-key-1")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("bool-key-2")
                    .long("bool-key-2")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("bool-key-3")
                    .long("bool-key-3")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("bool-key-4")
                    .long("bool-key-4")
                    .action(ArgAction::SetTrue),
            )
            .get_matches_from([
                "command",
                "--key-1=value-1",
                "--int-key-1=1",
                "--bool-key-1",
            ]);
        Config::new(
            args,
            "prefix_",
            [
                ("PREFIX_KEY_2", "value-2"),
                ("PREFIX_INT_KEY_2", "2"),
                ("PREFIX_BOOL_KEY_2", "true"),
            ],
            [
                (
                    "config-1.toml",
                    indoc! {r#"
                        key_3 = "value-3"
                        int_key_3 = 3
                        bool_key_3 = true
                    "#},
                ),
                (
                    "config-2.toml",
                    indoc! {r#"
                        key_4 = "value-4"
                        int_key_4 = 4
                        bool_key_4 = true
                    "#},
                ),
            ],
        )
        .unwrap()
    }

    #[test]
    fn string_values() {
        let config = get_config();
        assert_eq!(
            config.get::<String>("key-1").unwrap(),
            "value-1".to_string()
        );
        assert_eq!(
            config.get::<String>("key-2").unwrap(),
            "value-2".to_string()
        );
        assert_eq!(
            config.get::<String>("key-3").unwrap(),
            "value-3".to_string()
        );
        assert_eq!(
            config.get::<String>("key-4").unwrap(),
            "value-4".to_string()
        );
    }

    #[test]
    fn int_values() {
        let config = get_config();
        assert_eq!(config.get::<i32>("int-key-1").unwrap(), 1);
        assert_eq!(config.get::<i32>("int-key-2").unwrap(), 2);
        assert_eq!(config.get::<i32>("int-key-3").unwrap(), 3);
        assert_eq!(config.get::<i32>("int-key-4").unwrap(), 4);
    }

    #[test]
    fn bool_values() {
        let config = get_config();
        assert_eq!(config.get_flag("bool-key-1").unwrap(), Some(true));
        assert_eq!(config.get_flag("bool-key-2").unwrap(), Some(true));
        assert_eq!(config.get_flag("bool-key-3").unwrap(), Some(true));
        assert_eq!(config.get_flag("bool-key-4").unwrap(), Some(true));
    }
}
