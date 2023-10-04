use anyhow::Result;
use clap::Parser;
use figment::{
    error::Kind,
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{
    ser::{SerializeMap, Serializer},
    Serialize,
};
use std::{net::SocketAddrV6, path::PathBuf};

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
    #[arg(short = 'c', long, default_value=PathBuf::from("meticulous-broker.toml").into_os_string())]
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
}

impl Default for CliOptions {
    fn default() -> Self {
        CliOptions {
            config_file: "".into(),
            print_config: false,
            port: Some(0),
            http_port: Some(0),
            cache_root: Some("var/cache/meticulous-broker".into()),
            cache_bytes_used_target: Some(100000000),
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
        if let Some(port) = &self.port {
            map.serialize_entry("port", port)?;
        }
        if let Some(http_port) = &self.http_port {
            map.serialize_entry("http_port", http_port)?;
        }
        if let Some(cache_root) = &self.cache_root {
            map.serialize_entry("cache_root", cache_root)?;
        }
        if let Some(cache_bytes_used_target) = &self.cache_bytes_used_target {
            map.serialize_entry("cache_bytes_used_target", cache_bytes_used_target)?;
        }
        map.end()
    }
}

fn main() -> Result<()> {
    let cli_options = CliOptions::parse();
    let print_config = cli_options.print_config;
    let config: meticulous_broker::config::Config = Figment::new()
        .merge(Serialized::defaults(CliOptions::default()))
        .merge(Toml::file(&cli_options.config_file))
        .merge(Env::prefixed("METICULOUS_BROKER_"))
        .merge(Serialized::globals(cli_options))
        .extract()
        .map_err(|mut e| {
            if let Kind::MissingField(field) = &e.kind {
                e.kind = Kind::Message(format!("configuration value \"{field}\" was no provided"));
                e
            } else {
                e
            }
        })?;
    if print_config {
        println!("{config:#?}");
        return Ok(());
    }
    tokio::runtime::Runtime::new()?.block_on(async {
        let sock_addr = std::net::SocketAddrV6::new(
            std::net::Ipv6Addr::UNSPECIFIED,
            *config.port.inner(),
            0,
            0,
        );
        let broker_listener = tokio::net::TcpListener::bind(sock_addr).await?;
        println!("broker listening on: {:?}", broker_listener.local_addr()?);

        let sock_addr = SocketAddrV6::new(
            std::net::Ipv6Addr::UNSPECIFIED,
            *config.http_port.inner(),
            0,
            0,
        );
        let http_listener = tokio::net::TcpListener::bind(sock_addr).await?;
        println!("web UI listing on {:?}", http_listener.local_addr()?);

        meticulous_broker::main(
            broker_listener,
            http_listener,
            config.cache_root,
            config.cache_bytes_used_target,
        )
        .await;
        Ok(())
    })
}

#[test]
fn test_cli() {
    use clap::CommandFactory;
    CliOptions::command().debug_assert()
}
