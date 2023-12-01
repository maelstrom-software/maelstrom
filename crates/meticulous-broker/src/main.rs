use anyhow::{Context, Result};
use clap::Parser;
use figment::{
    error::Kind,
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use meticulous_broker::config::{Config, ConfigOptions};
use meticulous_util::config::LogLevel;
use slog::{info, o, Drain, Level, LevelFilter, Logger};
use slog_async::Async;
use slog_term::{FullFormat, TermDecorator};
use std::{
    net::{Ipv6Addr, SocketAddrV6},
    path::PathBuf,
    process,
};
use tokio::{net::TcpListener, runtime::Runtime};

/// The meticulous broker. This process coordinates between clients and workers.
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
    #[arg(short = 'c', long, default_value=PathBuf::from(".config/meticulous-broker.toml").into_os_string())]
    config_file: PathBuf,

    /// Print configuration and exit
    #[arg(short = 'P', long)]
    print_config: bool,

    /// The port the broker listens for connections from workers and clients on
    #[arg(short = 'p', long)]
    port: Option<u16>,

    /// The port the HTTP UI is served up on
    #[arg(short = 'H', long)]
    http_port: Option<u16>,

    /// The directory to use for the cache
    #[arg(short = 'r', long)]
    cache_root: Option<PathBuf>,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative
    #[arg(short = 'B', long)]
    cache_bytes_used_target: Option<u64>,

    /// Minimum log level to output.
    #[arg(short = 'l', long, value_enum)]
    log_level: Option<LogLevel>,
}

impl Default for CliOptions {
    fn default() -> Self {
        CliOptions {
            config_file: "".into(),
            print_config: false,
            port: Some(0),
            http_port: Some(0),
            cache_root: Some(".cache/meticulous-broker".into()),
            cache_bytes_used_target: Some(1_000_000_000),
            log_level: Some(LogLevel::Info),
        }
    }
}

impl CliOptions {
    fn to_config_options(&self) -> ConfigOptions {
        ConfigOptions {
            port: self.port,
            http_port: self.http_port,
            cache_root: self.cache_root.clone(),
            cache_bytes_used_target: self.cache_bytes_used_target,
            log_level: self.log_level,
        }
    }
}

fn main() -> Result<()> {
    let cli_options = CliOptions::parse();
    let print_config = cli_options.print_config;
    let config: Config = Figment::new()
        .merge(Serialized::defaults(ConfigOptions::default()))
        .merge(Toml::file(&cli_options.config_file))
        .merge(Env::prefixed("METICULOUS_BROKER_"))
        .merge(Serialized::globals(cli_options.to_config_options()))
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
        .block_on(async {
            let sock_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, *config.port.inner(), 0, 0);
            let listener = TcpListener::bind(sock_addr)
                .await
                .context("binding listener socket")?;

            let sock_addr =
                SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, *config.http_port.inner(), 0, 0);
            let http_listener = TcpListener::bind(sock_addr)
                .await
                .context("binding http listener socket")?;

            let listener_addr = listener
                .local_addr()
                .context("retrieving listener local address")?;
            let http_listener_addr = http_listener
                .local_addr()
                .context("retrieving listener local address")?;
            info!(log, "started";
                "config" => ?config,
                "addr" => listener_addr,
                "http_addr" => http_listener_addr,
                "pid" => process::id());

            meticulous_broker::main(
                listener,
                http_listener,
                config.cache_root,
                config.cache_bytes_used_target,
                log.clone(),
            )
            .await;
            info!(log, "exiting");
            Ok(())
        })
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    CliOptions::command().debug_assert()
}
