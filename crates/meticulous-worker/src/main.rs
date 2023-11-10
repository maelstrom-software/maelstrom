use anyhow::{Context as _, Result};
use clap::Parser;
use figment::{
    error::Kind,
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use meticulous_util::config::LogLevel;
use meticulous_worker::config::Config;
use nix::{
    errno::Errno,
    sys::{
        signal,
        wait::{self, WaitStatus},
    },
    unistd::{self, Pid},
};
use serde::{
    ser::{SerializeMap, Serializer},
    Serialize,
};
use slog::{o, Drain, Level, LevelFilter, Logger};
use slog_async::Async;
use slog_term::{FullFormat, TermDecorator};
use std::{fs, mem, path::PathBuf, process, slice};
use tokio::runtime::Runtime;

/// The meticulous worker. This process executes jobs as directed by the broker.
#[derive(Parser)]
#[command(
    after_help = r#"Configuration values can be specified in three ways: fields in a config file, environment variables, or command-line options. Command-line options have the highest precendence, followed by environment variables.

The configuration value 'config_value' would be set via the '--config-value' command-line option, the METICULOUS_WORKER_CONFIG_VALUE environment variable, and the 'config_value' key in a configuration file.

All values except for 'broker' have reasonable defaults.
"#
)]
#[command(version)]
struct CliOptions {
    /// Configuration file. Values set in the configuration file will be overridden by values set
    /// through environment variables and values set on the command line.
    #[arg(short = 'c', long, default_value=PathBuf::from(".config/meticulous-worker.toml").into_os_string())]
    config_file: PathBuf,

    /// Print configuration and exit
    #[arg(short = 'P', long)]
    print_config: bool,

    /// Socket address of broker. Examples: 127.0.0.1:5000 host.example.com:2000"
    #[arg(short = 'b', long)]
    broker: Option<String>,

    /// The number of job slots available. Most jobs will take one job slot
    #[arg(short = 's', long)]
    slots: Option<u16>,

    /// The directory to use for the cache
    #[arg(short = 'r', long)]
    cache_root: Option<PathBuf>,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative
    #[arg(short = 'B', long)]
    cache_bytes_used_target: Option<u64>,

    /// The maximum amount of bytes to return inline for captured stdout and stderr.
    #[arg(short = 'i', long)]
    inline_limit: Option<u64>,

    /// Minimum log level to output.
    #[arg(short = 'l', long, value_enum)]
    log_level: Option<LogLevel>,
}

impl Default for CliOptions {
    fn default() -> Self {
        CliOptions {
            config_file: "".into(),
            print_config: false,
            broker: None,
            slots: Some(num_cpus::get().try_into().unwrap()),
            cache_root: Some(".cache/meticulous-worker".into()),
            cache_bytes_used_target: Some(1_000_000_000),
            inline_limit: Some(1_000_000),
            log_level: Some(LogLevel::Info),
        }
    }
}

impl Serialize for CliOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        // Don't serialize 'config_file'.
        // Don't serialize 'print_config'.
        if let Some(broker) = &self.broker {
            map.serialize_entry("broker", broker)?;
        }
        if let Some(slots) = &self.slots {
            map.serialize_entry("slots", slots)?;
        }
        if let Some(cache_root) = &self.cache_root {
            map.serialize_entry("cache_root", cache_root)?;
        }
        if let Some(cache_bytes_used_target) = &self.cache_bytes_used_target {
            map.serialize_entry("cache_bytes_used_target", cache_bytes_used_target)?;
        }
        if let Some(inline_limit) = &self.inline_limit {
            map.serialize_entry("inline_limit", inline_limit)?;
        }
        if let Some(log_level) = &self.log_level {
            map.serialize_entry("log_level", log_level)?;
        }
        map.end()
    }
}

/// Clone a child process and continue executing in the child. The child process will be in a new
/// pid namespace, meaning when it terminates all of its descendant processes will also terminate.
/// The child process will also be in a new user namespace, and have uid 0, gid 0 in that
/// namespace. The user namespace is required in order to create the pid namespace.
///
/// WARNING: This function must only be called while the program is single-threaded.
fn clone_into_pid_and_user_namespace() -> Result<()> {
    let parent_pid = unistd::getpid();
    let parent_uid = unistd::getuid();
    let parent_gid = unistd::getgid();

    // Create a parent pidfd. We'll use this in the child to see if the parent has terminated
    // early.
    let parent_pidfd =
        unsafe { nc::pidfd_open(parent_pid.as_raw(), 0) }.map_err(Errno::from_i32)?;

    // Clone a new process into new user and pid namespaces.
    let mut clone_args = nc::clone_args_t {
        flags: nc::CLONE_NEWUSER as u64 | nc::CLONE_NEWPID as u64,
        exit_signal: nc::SIGCHLD as u64,
        ..Default::default()
    };
    match unsafe { nc::clone3(&mut clone_args, mem::size_of::<nc::clone_args_t>()) }
        .map_err(Errno::from_i32)?
    {
        0 => {
            // Child.

            // Set parent death signal.
            unsafe { nc::prctl(nc::PR_SET_PDEATHSIG, nc::types::SIGKILL as usize, 0, 0, 0) }
                .map_err(Errno::from_i32)?;

            // Check if the parent has already terminated.
            let mut pollfd = nc::pollfd_t {
                fd: parent_pidfd,
                events: nc::POLLIN,
                revents: 0,
            };
            if unsafe { nc::poll(slice::from_mut(&mut pollfd), 0) }.map_err(Errno::from_i32)? == 1 {
                process::abort();
            }

            // We are done with the parent_pidfd now.
            unsafe { nc::close(parent_pidfd) }.map_err(Errno::from_i32)?;

            // Map uid and guid.
            fs::write("/proc/self/setgroups", "deny\n")?;
            fs::write("/proc/self/uid_map", format!("0 {parent_uid} 1\n"))?;
            fs::write("/proc/self/gid_map", format!("0 {parent_gid} 1\n"))?;

            Ok(())
        }
        child_pid => {
            // Parent.

            // The parent_pidfd is only used in the child.
            unsafe { nc::close(parent_pidfd) }.unwrap_or_else(|e| {
                panic!("unexpected error closing pidfd: {}", Errno::from_i32(e))
            });

            // Wait for the child and mimick how it terminated.
            match wait::waitpid(Some(Pid::from_raw(child_pid)), None).unwrap_or_else(|e| {
                panic!("unexpected error waiting on child process {child_pid}: {e}")
            }) {
                WaitStatus::Exited(_, code) => {
                    process::exit(code);
                }
                WaitStatus::Signaled(_, signal, _) => {
                    signal::raise(signal).unwrap_or_else(|e| {
                        panic!("unexpected error raising signal {signal}: {e}")
                    });
                    process::abort();
                }
                unknown_status => {
                    panic!("child terminated with unexpected status: {unknown_status:?}");
                }
            }
        }
    }
}

fn main() -> Result<()> {
    let cli_options = CliOptions::parse();
    let print_config = cli_options.print_config;
    let config: Config = Figment::new()
        .merge(Serialized::defaults(CliOptions::default()))
        .merge(Toml::file(&cli_options.config_file))
        .merge(Env::prefixed("METICULOUS_WORKER_"))
        .merge(Serialized::globals(cli_options))
        .extract()
        .map_err(|mut e| {
            if let Kind::MissingField(field) = &e.kind {
                e.kind = Kind::Message(format!("configuration value \"{field}\" was no provided"));
                e
            } else {
                e
            }
        })
        .context("reading configuration")?;
    if print_config {
        println!("{config:#?}");
        return Ok(());
    }
    clone_into_pid_and_user_namespace()?;
    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let level = match config.log_level {
        LogLevel::Error => Level::Error,
        LogLevel::Warning => Level::Warning,
        LogLevel::Info => Level::Info,
        LogLevel::Debug => Level::Debug,
    };
    let drain = LevelFilter::new(drain, level).fuse();
    let log = Logger::root(drain, o!());
    Runtime::new()
        .context("starting tokio runtime")?
        .block_on(async move { meticulous_worker::main(config, log).await })?;
    Ok(())
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    CliOptions::command().debug_assert()
}
