pub mod common;

use anyhow::{anyhow, Context as _, Result};
use clap::{Arg, ArgAction, ArgMatches, Args, Command, FromArgMatches};
use heck::{ToKebabCase as _, ToShoutySnakeCase as _};
use serde::Deserialize;
use std::{
    collections::HashMap, env, ffi::OsString, fmt::Debug, fs, iter, path::PathBuf, process, result,
    str::FromStr,
};
use toml::Table;
use xdg::BaseDirectories;

pub struct ConfigBag {
    args: ArgMatches,
    env_prefix: String,
    env: HashMap<String, String>,
    files: Vec<(PathBuf, Table)>,
}

enum GetResult<T> {
    Some(T),
    None { key: String, env_var: String },
}

impl ConfigBag {
    pub fn new(
        args: ArgMatches,
        env_prefix: impl Into<String>,
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
            env_prefix: env_prefix.into(),
            env,
            files,
        })
    }

    pub fn into_args(self) -> ArgMatches {
        self.args
    }

    fn get_internal<T>(&self, field: &str) -> Result<GetResult<T>>
    where
        T: FromStr + for<'a> Deserialize<'a>,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        let key = field.to_kebab_case();
        let env_var = format!("{}{}", self.env_prefix, field.to_shouty_snake_case());

        let mut value = self
            .args
            .try_get_one::<String>(&key)
            .with_context(|| {
                format!("error getting matches data for command-line option `--{key}`")
            })?
            .map(String::as_str)
            .map(T::from_str)
            .transpose()
            .with_context(|| format!("error parsing command-line option `--{key}`"))?;
        if let Some(value) = value {
            return Ok(GetResult::Some(value));
        }

        value = self
            .env
            .get(&env_var)
            .map(String::as_str)
            .map(T::from_str)
            .transpose()
            .with_context(|| format!("error parsing environment variable `{env_var}`"))?;
        if let Some(value) = value {
            return Ok(GetResult::Some(value));
        }

        for (path, table) in &self.files {
            if let Some(value) = table.get(&key) {
                return Ok(GetResult::Some(
                    T::deserialize(value.clone()).with_context(|| {
                        format!(
                            "error parsing value for key `{key}` in config file `{}`",
                            path.to_string_lossy()
                        )
                    })?,
                ));
            }
        }

        Ok(GetResult::None { key, env_var })
    }

    pub fn get<T>(&self, field: &str) -> Result<T>
    where
        T: FromStr + for<'a> Deserialize<'a>,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        match self.get_internal(field) {
            Err(err) => Err(err),
            Ok(GetResult::Some(v)) => Ok(v),
            Ok(GetResult::None { key, env_var }) => Err(anyhow!(
                "config value `{key}` must be set via `--{key}` command-line option, \
                `{env_var}` environment variable, or `{key}` key in config file"
            )),
        }
    }

    pub fn get_or_else<T, F>(&self, field: &str, mut default: F) -> Result<T>
    where
        T: FromStr + for<'a> Deserialize<'a>,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
        F: FnMut() -> T,
    {
        self.get_internal(field).map(|v| match v {
            GetResult::Some(v) => v,
            GetResult::None { .. } => default(),
        })
    }

    pub fn get_option<T>(&self, field: &str) -> Result<Option<T>>
    where
        T: FromStr + for<'a> Deserialize<'a>,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        self.get_internal(field).map(|v| match v {
            GetResult::Some(v) => Some(v),
            GetResult::None { .. } => None,
        })
    }

    pub fn get_flag<T>(&self, field: &str) -> Result<Option<T>>
    where
        T: From<bool> + for<'a> Deserialize<'a>,
    {
        let key = field.to_kebab_case();
        let env_var = format!("{}{}", self.env_prefix, field.to_shouty_snake_case());

        let Some(&args_result) = self.args.get_one::<bool>(&key) else {
            panic!("didn't expect None")
        };
        if args_result {
            return Ok(Some(T::from(args_result)));
        }

        let value = self
            .env
            .get(&env_var)
            .map(String::as_str)
            .map(bool::from_str)
            .transpose()
            .with_context(|| format!("error parsing environment variable `{env_var}`"))?
            .map(T::from);

        if value.is_some() {
            return Ok(value);
        }

        for (path, table) in &self.files {
            if let Some(value) = table.get(&key) {
                return Some(T::deserialize(value.clone()))
                    .transpose()
                    .with_context(|| {
                        format!(
                            "error parsing value for key `{key}` in config file `{}`",
                            path.to_string_lossy(),
                        )
                    });
            }
        }

        Ok(None)
    }
}

pub trait Config: Sized {
    fn add_command_line_options(
        builder: CommandBuilder,
        base_directories: &BaseDirectories,
    ) -> CommandBuilder;
    fn from_config_bag(config: &mut ConfigBag, base_directories: &BaseDirectories) -> Result<Self>;
}

pub struct CommandBuilder {
    command: Command,
    env_var_prefix: &'static str,
}

impl CommandBuilder {
    pub fn new(
        command: Command,
        base_directories: &BaseDirectories,
        env_var_prefix: &'static str,
    ) -> Self {
        let config_files = iter::once(base_directories.get_config_home())
            .chain(base_directories.get_config_dirs())
            .map(|pb| pb.join("config.toml").to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let command = command
            .disable_help_flag(true)
            .disable_version_flag(true)
            .styles(crate::clap::styles())
            .after_help(format!(
                "Configuration values can be specified in three ways: fields in a configuration \
                file, environment variables, or command-line options. Command-line options have the \
                highest precendence, followed by environment variables.\n\
                \n\
                The hypothetical configuration value \"config-value\" would be set via the \
                --config-value command-line option, the {env_var_prefix}_CONFIG_VALUE \
                environment variable, and the \"config-value\" key in a configuration file.\n\
                \n\
                See the help information for --config-file for more information."))
            .next_help_heading("Print-and-Exit Options")
            .arg(
                Arg::new("help")
                    .long("help")
                    .short('h')
                    .action(ArgAction::HelpLong)
                    .help("Print help and exit."),
            )
            .arg(
                Arg::new("version")
                    .long("version")
                    .action(ArgAction::Version)
                    .help("Print version and exit."),
            )
            .arg(
                Arg::new("print-config")
                    .long("print-config")
                    .short('P')
                    .action(ArgAction::SetTrue)
                    .help("Print all configuration values and exit."),
            )
            .next_help_heading("Config File Options")
            .arg(
                Arg::new("config-file")
                    .long("config-file")
                    .short('c')
                    .value_name("PATH")
                    .action(ArgAction::Set)
                    .next_line_help(true)
                    .help(format!(
                        "File to read configuration values from. Must be in TOML format.\n\
                        \n\
                        The special path \"-\" indicates that no configuration file should be read.\n\
                        \n\
                        If this option is not set, multiple configuration files will be read, \
                        as described in the XDG Base Directories specification. With the present \
                        settings, the list of files to be read, in order, is: {config_files}.\n\
                        \n\
                        Values set by earlier files override values set by later files. \
                        Values set by environment variables override values set by files. \
                        Values set by command-line-options override values set by environment \
                        variables and files."
                    ))
            )
            .next_help_heading("Config Options")
            ;

        Self {
            command,
            env_var_prefix,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn _value(
        mut self,
        field: &'static str,
        short: Option<char>,
        alias: Option<String>,
        value_name: &'static str,
        default: Option<String>,
        help: &'static str,
        action: ArgAction,
    ) -> Self {
        let name = field.to_kebab_case();
        let env_var = format!("{}_{}", self.env_var_prefix, field.to_shouty_snake_case());
        let default = default.unwrap_or("no default, must be specified".to_string());
        let help = if let Some(alias) = &alias {
            format!("{help} [default: {default}] [alias: {alias}] [env: {env_var}]")
        } else {
            format!("{help} [default: {default}] [env: {env_var}]")
        };
        let mut arg = Arg::new(name.clone())
            .long(name)
            .value_name(value_name)
            .action(action)
            .help(help);
        if let Some(short) = short {
            arg = arg.short(short);
        }
        if let Some(alias) = alias {
            arg = arg.alias(alias);
        }
        self.command = self.command.arg(arg);
        self
    }

    pub fn value(
        self,
        field: &'static str,
        short: Option<char>,
        alias: Option<String>,
        value_name: &'static str,
        default: Option<String>,
        help: &'static str,
    ) -> Self {
        self._value(
            field,
            short,
            alias,
            value_name,
            default,
            help,
            ArgAction::Set,
        )
    }

    pub fn flag_value(
        self,
        field: &'static str,
        short: Option<char>,
        alias: Option<String>,
        help: &'static str,
    ) -> Self {
        self._value(
            field,
            short,
            alias,
            "",
            Some("false".to_string()),
            help,
            ArgAction::SetTrue,
        )
    }

    pub fn next_help_heading(mut self, heading: &'static str) -> Self {
        self.command = self.command.next_help_heading(heading);
        self
    }

    pub fn build(self) -> Command {
        self.command
    }
}

pub fn new_config_with_extra_from_args<T, U, AI, AT>(
    command: Command,
    base_directories_prefix: &'static str,
    env_var_prefix: &'static str,
    args: AI,
) -> Result<(T, U)>
where
    T: Config + Debug,
    U: Args,
    AI: IntoIterator<Item = AT>,
    AT: Into<OsString> + Clone,
{
    let base_directories = BaseDirectories::with_prefix(base_directories_prefix)
        .context("searching for config files")?;
    let builder = CommandBuilder::new(command, &base_directories, env_var_prefix);
    let builder = T::add_command_line_options(builder, &base_directories);
    let command = U::augment_args(builder.build());
    let mut args = command.get_matches_from(args);
    let env_var_prefix = env_var_prefix.to_string() + "_";
    let env = env::vars().filter(|(key, _)| key.starts_with(&env_var_prefix));

    let config_files = match args.remove_one::<String>("config-file").as_deref() {
        Some("-") => vec![],
        Some(config_file) => vec![PathBuf::from(config_file)],
        None => base_directories
            .find_config_files("config.toml")
            .rev()
            .collect(),
    };
    let mut files = vec![];
    for config_file in config_files {
        let contents = fs::read_to_string(&config_file)
            .with_context(|| format!("reading config file `{}`", config_file.to_string_lossy()))?;
        files.push((config_file.clone(), contents));
    }

    let print_config = args.remove_one::<bool>("print-config").unwrap();

    let mut config_bag = ConfigBag::new(args, &env_var_prefix, env, files)
        .context("loading configuration from environment variables and config files")?;

    let config = T::from_config_bag(&mut config_bag, &base_directories)?;
    let extra = U::from_arg_matches(&config_bag.into_args())?;

    if print_config {
        println!("{config:#?}");
        process::exit(0);
    }

    Ok((config, extra))
}

struct NoExtraCommandLineOptions;

impl FromArgMatches for NoExtraCommandLineOptions {
    fn from_arg_matches(_matches: &ArgMatches) -> result::Result<Self, clap::Error> {
        Ok(NoExtraCommandLineOptions)
    }

    fn update_from_arg_matches(
        &mut self,
        _matches: &ArgMatches,
    ) -> result::Result<(), clap::Error> {
        Ok(())
    }
}

impl Args for NoExtraCommandLineOptions {
    fn augment_args(cmd: Command) -> Command {
        cmd
    }

    fn augment_args_for_update(cmd: Command) -> Command {
        cmd
    }
}

pub fn new_config<T: Config + Debug>(
    command: Command,
    base_directories_prefix: &'static str,
    env_var_prefix: &'static str,
) -> Result<T> {
    let (config, _): (_, NoExtraCommandLineOptions) = new_config_with_extra_from_args(
        command,
        base_directories_prefix,
        env_var_prefix,
        env::args_os(),
    )?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Arg, ArgAction, Command};
    use indoc::indoc;

    fn get_config() -> ConfigBag {
        let args = Command::new("command")
            .arg(Arg::new("key-1").long("key-1").action(ArgAction::Set))
            .arg(Arg::new("key-2").long("key-2").action(ArgAction::Set))
            .arg(Arg::new("key-3").long("key-3").action(ArgAction::Set))
            .arg(Arg::new("key-4").long("key-4").action(ArgAction::Set))
            .arg(
                Arg::new("int-key-1")
                    .long("int-key-1")
                    .action(ArgAction::Set),
            )
            .arg(
                Arg::new("int-key-2")
                    .long("int-key-2")
                    .action(ArgAction::Set),
            )
            .arg(
                Arg::new("int-key-3")
                    .long("int-key-3")
                    .action(ArgAction::Set),
            )
            .arg(
                Arg::new("int-key-4")
                    .long("int-key-4")
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
        ConfigBag::new(
            args,
            "PREFIX_",
            [
                ("PREFIX_KEY_2", "value-2"),
                ("PREFIX_INT_KEY_2", "2"),
                ("PREFIX_BOOL_KEY_2", "true"),
            ],
            [
                (
                    "config-1.toml",
                    indoc! {r#"
                        key-3 = "value-3"
                        int-key-3 = 3
                        bool-key-3 = true
                    "#},
                ),
                (
                    "config-2.toml",
                    indoc! {r#"
                        key-4 = "value-4"
                        int-key-4 = 4
                        bool-key-4 = true
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
            config.get::<String>("key_1").unwrap(),
            "value-1".to_string()
        );
        assert_eq!(
            config.get::<String>("key_2").unwrap(),
            "value-2".to_string()
        );
        assert_eq!(
            config.get::<String>("key_3").unwrap(),
            "value-3".to_string()
        );
        assert_eq!(
            config.get::<String>("key_4").unwrap(),
            "value-4".to_string()
        );
    }

    #[test]
    fn int_values() {
        let config = get_config();
        assert_eq!(config.get::<i32>("int_key_1").unwrap(), 1);
        assert_eq!(config.get::<i32>("int_key_2").unwrap(), 2);
        assert_eq!(config.get::<i32>("int_key_3").unwrap(), 3);
        assert_eq!(config.get::<i32>("int_key_4").unwrap(), 4);
    }

    #[test]
    fn bool_values() {
        let config = get_config();
        assert_eq!(config.get_flag("bool_key_1").unwrap(), Some(true));
        assert_eq!(config.get_flag("bool_key_2").unwrap(), Some(true));
        assert_eq!(config.get_flag("bool_key_3").unwrap(), Some(true));
        assert_eq!(config.get_flag("bool_key_4").unwrap(), Some(true));
    }
}
