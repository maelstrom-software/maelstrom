pub mod common;

use anyhow::{anyhow, Context as _, Result};
use clap::{Arg, ArgAction, ArgMatches, Args, Command, FromArgMatches, Subcommand};
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
    env_prefixes: Vec<String>,
    env: HashMap<String, String>,
    files: Vec<(PathBuf, Table)>,
}

enum GetResult<T> {
    Some(T),
    None { key: String, env_vars: Vec<String> },
}

impl ConfigBag {
    pub fn new(
        args: ArgMatches,
        env_prefixes: impl IntoIterator<Item = impl Into<String>>,
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
        let env_prefixes = Vec::from_iter(env_prefixes.into_iter().map(Into::into));
        Ok(Self {
            args,
            env_prefixes,
            env,
            files,
        })
    }

    pub fn into_args(self) -> ArgMatches {
        self.args
    }

    fn get_from_args<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: FromStr,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        self.args
            .try_get_one::<String>(key)
            .with_context(|| {
                format!("error getting matches data for command-line option `--{key}`")
            })?
            .map(String::as_str)
            .map(T::from_str)
            .transpose()
            .with_context(|| format!("error parsing command-line option `--{key}`"))
    }

    fn get_from_env<T>(&self, env_var: &str) -> Result<Option<T>>
    where
        T: FromStr,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        self.env
            .get(env_var)
            .map(String::as_str)
            .map(T::from_str)
            .transpose()
            .with_context(|| format!("error parsing environment variable `{env_var}`"))
    }

    fn get_from_file<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: for<'a> Deserialize<'a>,
    {
        for (path, table) in &self.files {
            if let Some(value) = table.get(key) {
                return Ok(Some(T::deserialize(value.clone()).with_context(|| {
                    format!(
                        "error parsing value for key `{key}` in config file `{}`",
                        path.to_string_lossy()
                    )
                })?));
            }
        }
        Ok(None)
    }

    fn get_internal<T>(&self, field: &str) -> Result<GetResult<T>>
    where
        T: FromStr + for<'a> Deserialize<'a>,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        let key = field.to_kebab_case();

        if let Some(value) = self.get_from_args(&key)? {
            return Ok(GetResult::Some(value));
        }

        let mut env_vars = vec![];
        for env_prefix in &self.env_prefixes {
            let env_var = format!("{env_prefix}_{}", field.to_shouty_snake_case());
            if let Some(value) = self.get_from_env(&env_var)? {
                return Ok(GetResult::Some(value));
            }
            env_vars.push(env_var);
        }

        if let Some(value) = self.get_from_file(&key)? {
            return Ok(GetResult::Some(value));
        }

        Ok(GetResult::None { key, env_vars })
    }

    fn get_flag_from_args<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: From<bool>,
    {
        let Some(&args_result) = self.args.get_one::<bool>(key) else {
            panic!("didn't expect None")
        };
        if args_result {
            Ok(Some(T::from(args_result)))
        } else {
            Ok(None)
        }
    }

    fn get_flag_from_env<T>(&self, env_var: &str) -> Result<Option<T>>
    where
        T: From<bool>,
    {
        Ok(self
            .env
            .get(env_var)
            .map(String::as_str)
            .map(bool::from_str)
            .transpose()
            .with_context(|| format!("error parsing environment variable `{env_var}`"))?
            .map(T::from))
    }

    fn get_var_arg_from_args<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: FromIterator<String>,
    {
        if let Some(args_result) = self.args.get_many::<String>(key) {
            Ok(Some(args_result.cloned().collect()))
        } else {
            Ok(None)
        }
    }

    fn get_var_arg_from_env<T>(&self, env_var: &str) -> Result<Option<T>>
    where
        T: FromIterator<String>,
    {
        if let Some(v) = self.env.get(env_var) {
            Ok(Some(
                v.split(' ')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_owned())
                    .collect(),
            ))
        } else {
            Ok(None)
        }
    }

    /// Get something which can be converted from a string from the bag. Returns an error if not
    /// present.
    pub fn get<T>(&self, field: &str) -> Result<T>
    where
        T: FromStr + for<'a> Deserialize<'a>,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        match self.get_internal(field) {
            Err(err) => Err(err),
            Ok(GetResult::Some(v)) => Ok(v),
            Ok(GetResult::None { key, env_vars }) => {
                let env_vars = itertools::join(
                    env_vars.into_iter().map(|env_var| format!("`{env_var}`")),
                    " or ",
                );
                Err(anyhow!(
                    "config value `{key}` must be set via `--{key}` command-line option, \
                    {env_vars} environment variables, or `{key}` key in config file"
                ))
            }
        }
    }

    /// Get something which can be converted from a string from the bag. Returns the result of
    /// the `default` argument if not present.
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

    /// Get something which can be converted from a string from the bag. Returns [`None`] if not
    /// present.
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

    /// Get something which can be converted from a `bool` from the bag. Returns [`None`] if not
    /// present.
    pub fn get_flag<T>(&self, field: &str) -> Result<Option<T>>
    where
        T: From<bool> + for<'a> Deserialize<'a>,
    {
        let key = field.to_kebab_case();

        if let Some(v) = self.get_flag_from_args(&key)? {
            return Ok(Some(v));
        }

        for env_prefix in &self.env_prefixes {
            let env_var = format!("{env_prefix}_{}", field.to_shouty_snake_case());
            if let Some(v) = self.get_flag_from_env(&env_var)? {
                return Ok(Some(v));
            }
        }

        if let Some(v) = self.get_from_file(&key)? {
            return Ok(Some(v));
        }

        Ok(None)
    }

    /// Get something that can be converted from a list of strings from the bag.
    /// Returns [`Default::default()`] if not present.
    pub fn get_list<T>(&self, field: &str) -> Result<T>
    where
        T: FromIterator<String> + for<'a> Deserialize<'a> + Default,
    {
        let key = field.to_kebab_case();

        if let Some(v) = self.get_var_arg_from_args(&key)? {
            return Ok(v);
        }

        for env_prefix in &self.env_prefixes {
            let env_var = format!("{env_prefix}_{}", field.to_shouty_snake_case());
            if let Some(v) = self.get_var_arg_from_env(&env_var)? {
                return Ok(v);
            }
        }

        if let Some(v) = self.get_from_file(&key)? {
            return Ok(v);
        }

        Ok(Default::default())
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
    env_var_prefixes: Vec<String>,
}

impl CommandBuilder {
    pub fn new(
        command: Command,
        base_directories: &BaseDirectories,
        env_var_prefixes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let config_files = iter::once(base_directories.get_config_home())
            .chain(base_directories.get_config_dirs())
            .map(|pb| pb.join("config.toml").to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let env_var_prefixes = Vec::from_iter(env_var_prefixes.into_iter().map(Into::into));
        let env_vars = Vec::from_iter(
            env_var_prefixes
                .iter()
                .map(|prefix| format!("{prefix}_CONFIG_VALUE")),
        );
        let env_vars_help_phrase = match &env_vars[..] {
            [] => {
                panic!("at least one env_var_prefix must be provided");
            }
            [one] => format!("the {one} environment variable"),
            [one, two] => format!("the {one} or {two} environment variables"),
            [first @ .., last] => {
                let first = first.join(", ");
                format!("the {first}, or {last} environment variables")
            }
        };
        let command = command
            .disable_help_flag(true)
            .disable_version_flag(true)
            .styles(crate::clap::styles())
            .after_help(format!(
                "Configuration values can be specified in three ways: fields in a configuration \
                file, environment variables, or command-line options. Command-line options have the \
                highest precedence, followed by environment variables.\n\
                \n\
                The hypothetical configuration value \"config-value\" would be set via the \
                --config-value command-line option, {env_vars_help_phrase}, \
                and the \"config-value\" key in a configuration file.\n\
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
            .next_help_heading("Config Values")
            ;

        Self {
            command,
            env_var_prefixes,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn _value(
        mut self,
        field: &'static str,
        is_var_arg: bool,
        short: Option<char>,
        alias: Option<String>,
        value_name: &'static str,
        default: Option<String>,
        hide: bool,
        help: &'static str,
        action: ArgAction,
    ) -> Self {
        let name = field.to_kebab_case();
        let env_vars = itertools::join(
            self.env_var_prefixes
                .iter()
                .map(|prefix| format!("{prefix}_{}", field.to_shouty_snake_case())),
            ", ",
        );
        let default = default.unwrap_or("no default, must be specified".to_string());
        let help = if let Some(alias) = &alias {
            format!("{help} [default: {default}] [alias: {alias}] [env: {env_vars}]")
        } else {
            format!("{help} [default: {default}] [env: {env_vars}]")
        };
        let mut arg = Arg::new(name.clone())
            .action(action)
            .global(true)
            .help(help)
            .hide(hide)
            .value_name(value_name);
        if is_var_arg {
            arg = arg.last(true);
        } else {
            arg = arg.long(name);
        }
        if let Some(short) = short {
            arg = arg.short(short);
        }
        if let Some(alias) = alias {
            arg = arg.alias(alias);
        }
        self.command = self.command.arg(arg);
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub fn value(
        self,
        field: &'static str,
        short: Option<char>,
        alias: Option<String>,
        value_name: &'static str,
        default: Option<String>,
        hide: bool,
        help: &'static str,
    ) -> Self {
        self._value(
            field,
            false, /* is_var_arg */
            short,
            alias,
            value_name,
            default,
            hide,
            help,
            ArgAction::Set,
        )
    }

    pub fn flag_value(
        self,
        field: &'static str,
        short: Option<char>,
        alias: Option<String>,
        hide: bool,
        help: &'static str,
    ) -> Self {
        self._value(
            field,
            false, /* is_var_arg */
            short,
            alias,
            "",
            Some("false".to_string()),
            hide,
            help,
            ArgAction::SetTrue,
        )
    }

    pub fn var_arg(
        self,
        field: &'static str,
        value_name: &'static str,
        default: Option<String>,
        hide: bool,
        help: &'static str,
    ) -> Self {
        self._value(
            field,
            true, /* is_var_arg */
            None, /* short */
            None, /* alias */
            value_name,
            default,
            hide,
            help,
            ArgAction::Append,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn list(
        self,
        field: &'static str,
        short: Option<char>,
        alias: Option<String>,
        value_name: &'static str,
        default: Option<String>,
        hide: bool,
        help: &'static str,
    ) -> Self {
        self._value(
            field,
            false, /* is_var_arg */
            short,
            alias,
            value_name,
            default,
            hide,
            help,
            ArgAction::Append,
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
    env_var_prefixes: impl IntoIterator<Item = impl Into<String>>,
    args: AI,
) -> Result<(T, U)>
where
    T: Config + Debug,
    U: Args,
    AI: IntoIterator<Item = AT>,
    AT: Into<OsString> + Clone,
{
    let env_var_prefixes = Vec::from_iter(env_var_prefixes.into_iter().map(Into::into));
    let base_directories = BaseDirectories::with_prefix(base_directories_prefix)
        .context("searching for config files")?;
    let builder = CommandBuilder::new(command, &base_directories, &env_var_prefixes);
    let builder = T::add_command_line_options(builder, &base_directories);
    let command = U::augment_args(builder.build());
    let mut args = command.get_matches_from(args);
    let env = env::vars().filter(|(key, _)| {
        env_var_prefixes
            .iter()
            .any(|prefix| key.starts_with(prefix))
    });

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

    let mut config_bag = ConfigBag::new(args, &env_var_prefixes, env, files)
        .context("loading configuration from environment variables and config files")?;

    let config = T::from_config_bag(&mut config_bag, &base_directories)?;
    let extra = U::from_arg_matches(&config_bag.into_args())?;

    if print_config {
        println!("{config:#?}");
        process::exit(0);
    }

    Ok((config, extra))
}

pub fn new_config_with_subcommand_from_args<T, U, AI, AT>(
    command: Command,
    base_directories_prefix: &'static str,
    env_var_prefixes: impl IntoIterator<Item = impl Into<String>>,
    args: AI,
) -> Result<(T, U)>
where
    T: Config + Debug,
    U: Subcommand,
    AI: IntoIterator<Item = AT>,
    AT: Into<OsString> + Clone,
{
    let env_var_prefixes = Vec::from_iter(env_var_prefixes.into_iter().map(Into::into));
    let base_directories = BaseDirectories::with_prefix(base_directories_prefix)
        .context("searching for config files")?;
    let builder = CommandBuilder::new(command, &base_directories, &env_var_prefixes);
    let builder = T::add_command_line_options(builder, &base_directories);
    let command = U::augment_subcommands(builder.build());
    let mut args = command.get_matches_from(args);
    let env = env::vars().filter(|(key, _)| {
        env_var_prefixes
            .iter()
            .any(|prefix| key.starts_with(prefix))
    });

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

    let mut config_bag = ConfigBag::new(args, &env_var_prefixes, env, files)
        .context("loading configuration from environment variables and config files")?;

    let config = T::from_config_bag(&mut config_bag, &base_directories)?;
    let subcommand = U::from_arg_matches(&config_bag.into_args())?;

    if print_config {
        println!("{config:#?}");
        process::exit(0);
    }

    Ok((config, subcommand))
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
    env_var_prefixes: impl IntoIterator<Item = impl Into<String>>,
) -> Result<T> {
    let (config, _): (_, NoExtraCommandLineOptions) = new_config_with_extra_from_args(
        command,
        base_directories_prefix,
        env_var_prefixes,
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
        let cmd = Command::new("command")
            .arg(Arg::new("key-1").long("key-1").action(ArgAction::Set))
            .arg(Arg::new("key-2").long("key-2").action(ArgAction::Set))
            .arg(Arg::new("key-3").long("key-3").action(ArgAction::Set))
            .arg(Arg::new("key-4").long("key-4").action(ArgAction::Set))
            .arg(Arg::new("key-5").long("key-5").action(ArgAction::Set))
            .arg(Arg::new("key-6").long("key-6").action(ArgAction::Set))
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
                Arg::new("int-key-5")
                    .long("int-key-5")
                    .action(ArgAction::Set),
            )
            .arg(
                Arg::new("int-key-6")
                    .long("int-key-6")
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
            .arg(
                Arg::new("bool-key-5")
                    .long("bool-key-5")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("bool-key-6")
                    .long("bool-key-6")
                    .action(ArgAction::SetTrue),
            );
        let args = cmd.clone().get_matches_from([
            "command",
            "--key-1=value-1",
            "--int-key-1=1",
            "--bool-key-1",
        ]);
        ConfigBag::new(
            args,
            ["PREFIX", "OTHER_PREFIX"],
            [
                ("PREFIX_KEY_2", "value-2"),
                ("PREFIX_INT_KEY_2", "2"),
                ("PREFIX_BOOL_KEY_2", "true"),
                ("OTHER_PREFIX_KEY_2", "shadowed-value"),
                ("OTHER_PREFIX_INT_KEY_2", "shadowed-value"),
                ("OTHER_PREFIX_BOOL_KEY_2", "shadowed-value"),
                ("OTHER_PREFIX_KEY_3", "value-3"),
                ("OTHER_PREFIX_INT_KEY_3", "3"),
                ("OTHER_PREFIX_BOOL_KEY_3", "false"),
            ],
            [
                (
                    "config-1.toml",
                    indoc! {r#"
                        key-4 = "value-4"
                        int-key-4 = 4
                        bool-key-4 = true
                    "#},
                ),
                (
                    "config-2.toml",
                    indoc! {r#"
                        key-5 = "value-5"
                        int-key-5 = 5
                        bool-key-5 = false
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
            "value-1".to_string(),
        );
        assert_eq!(
            config.get::<String>("key_2").unwrap(),
            "value-2".to_string(),
        );
        assert_eq!(
            config.get::<String>("key_3").unwrap(),
            "value-3".to_string(),
        );
        assert_eq!(
            config.get::<String>("key_4").unwrap(),
            "value-4".to_string(),
        );
        assert_eq!(
            config.get::<String>("key_5").unwrap(),
            "value-5".to_string(),
        );
        assert_eq!(
            config.get::<String>("key_6").unwrap_err().to_string(),
            "config value `key-6` must be set via `--key-6` command-line option, \
            `PREFIX_KEY_6` or `OTHER_PREFIX_KEY_6` environment variables, \
            or `key-6` key in config file",
        );
    }

    #[test]
    fn string_option_values() {
        let config = get_config();
        assert_eq!(
            config.get_option::<String>("key_1").unwrap(),
            Some("value-1".to_string()),
        );
        assert_eq!(
            config.get_option::<String>("key_2").unwrap(),
            Some("value-2".to_string()),
        );
        assert_eq!(
            config.get_option::<String>("key_3").unwrap(),
            Some("value-3".to_string()),
        );
        assert_eq!(
            config.get_option::<String>("key_4").unwrap(),
            Some("value-4".to_string()),
        );
        assert_eq!(
            config.get_option::<String>("key_5").unwrap(),
            Some("value-5".to_string()),
        );
        assert_eq!(config.get_option::<String>("key_6").unwrap(), None,);
    }

    #[test]
    fn string_or_else_values() {
        let config = get_config();
        assert_eq!(
            config.get_or_else("key_1", || "else".to_string()).unwrap(),
            "value-1".to_string(),
        );
        assert_eq!(
            config.get_or_else("key_2", || "else".to_string()).unwrap(),
            "value-2".to_string(),
        );
        assert_eq!(
            config.get_or_else("key_3", || "else".to_string()).unwrap(),
            "value-3".to_string(),
        );
        assert_eq!(
            config.get_or_else("key_4", || "else".to_string()).unwrap(),
            "value-4".to_string(),
        );
        assert_eq!(
            config.get_or_else("key_5", || "else".to_string()).unwrap(),
            "value-5".to_string(),
        );
        assert_eq!(
            config.get_or_else("key_6", || "else".to_string()).unwrap(),
            "else".to_string(),
        );
    }

    #[test]
    fn int_values() {
        let config = get_config();
        assert_eq!(config.get::<i32>("int_key_1").unwrap(), 1);
        assert_eq!(config.get::<i32>("int_key_2").unwrap(), 2);
        assert_eq!(config.get::<i32>("int_key_3").unwrap(), 3);
        assert_eq!(config.get::<i32>("int_key_4").unwrap(), 4);
        assert_eq!(config.get::<i32>("int_key_5").unwrap(), 5);
        assert_eq!(
            config.get::<i32>("int_key_6").unwrap_err().to_string(),
            "config value `int-key-6` must be set via `--int-key-6` command-line option, \
            `PREFIX_INT_KEY_6` or `OTHER_PREFIX_INT_KEY_6` environment variables, \
            or `int-key-6` key in config file",
        );
    }

    #[test]
    fn int_option_values() {
        let config = get_config();
        assert_eq!(config.get_option::<i32>("int_key_1").unwrap(), Some(1));
        assert_eq!(config.get_option::<i32>("int_key_2").unwrap(), Some(2));
        assert_eq!(config.get_option::<i32>("int_key_3").unwrap(), Some(3));
        assert_eq!(config.get_option::<i32>("int_key_4").unwrap(), Some(4));
        assert_eq!(config.get_option::<i32>("int_key_5").unwrap(), Some(5));
        assert_eq!(config.get_option::<i32>("int_key_6").unwrap(), None);
    }

    #[test]
    fn int_or_else_values() {
        let config = get_config();
        assert_eq!(config.get_or_else("int_key_1", || 0).unwrap(), 1);
        assert_eq!(config.get_or_else("int_key_2", || 0).unwrap(), 2);
        assert_eq!(config.get_or_else("int_key_3", || 0).unwrap(), 3);
        assert_eq!(config.get_or_else("int_key_4", || 0).unwrap(), 4);
        assert_eq!(config.get_or_else("int_key_5", || 0).unwrap(), 5);
        assert_eq!(config.get_or_else("int_key_6", || 0).unwrap(), 0);
    }

    #[test]
    fn bool_values() {
        let config = get_config();
        assert_eq!(config.get_flag("bool_key_1").unwrap(), Some(true));
        assert_eq!(config.get_flag("bool_key_2").unwrap(), Some(true));
        assert_eq!(config.get_flag("bool_key_3").unwrap(), Some(false));
        assert_eq!(config.get_flag("bool_key_4").unwrap(), Some(true));
        assert_eq!(config.get_flag("bool_key_5").unwrap(), Some(false));
        assert_eq!(config.get_flag::<bool>("bool_key_6").unwrap(), None);
    }

    #[test]
    fn list_from_arg() {
        let cmd =
            Command::new("command").arg(Arg::new("var-args").last(true).action(ArgAction::Append));
        let args = cmd
            .clone()
            .get_matches_from(["command", "--", "--a", "--b"]);
        let config = ConfigBag::new(
            args,
            ["PREFIX", "OTHER_PREFIX"],
            Vec::<(String, String)>::new(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert_eq!(
            config.get_list::<Vec<String>>("var_args").unwrap(),
            vec!["--a".to_owned(), "--b".to_owned()],
        );
    }

    #[test]
    fn list_from_env() {
        let cmd =
            Command::new("command").arg(Arg::new("var-args").last(true).action(ArgAction::Append));
        let args = cmd.clone().get_matches_from(["command"]);
        let config = ConfigBag::new(
            args,
            ["PREFIX", "OTHER_PREFIX"],
            [
                ("PREFIX_VAR_ARGS", "--a --b"),
                ("OTHER_PREFIX_VAR_ARGS", "--c --d"),
            ],
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert_eq!(
            config.get_list::<Vec<String>>("var_args").unwrap(),
            vec!["--a".to_owned(), "--b".to_owned()],
        );
    }

    #[test]
    fn list_from_env_2() {
        let cmd =
            Command::new("command").arg(Arg::new("var-args").last(true).action(ArgAction::Append));
        let args = cmd.clone().get_matches_from(["command"]);
        let config = ConfigBag::new(
            args,
            ["PREFIX", "OTHER_PREFIX"],
            [("OTHER_PREFIX_VAR_ARGS", "--a --b")],
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert_eq!(
            config.get_list::<Vec<String>>("var_args").unwrap(),
            vec!["--a".to_owned(), "--b".to_owned()],
        );
    }

    #[test]
    fn list_from_file() {
        let cmd =
            Command::new("command").arg(Arg::new("var-args").last(true).action(ArgAction::Append));
        let args = cmd.clone().get_matches_from(["command"]);
        let config = ConfigBag::new(
            args,
            ["PREFIX", "OTHER_PREFIX"],
            Vec::<(String, String)>::new(),
            [(
                "config-1.toml",
                indoc! {r#"
                    var-args = ["--a", "--b"]
                "#},
            )],
        )
        .unwrap();

        assert_eq!(
            config.get_list::<Vec<String>>("var_args").unwrap(),
            vec!["--a".to_owned(), "--b".to_owned()],
        );
    }
}
