use maelstrom_macro::Config;
use maelstrom_util::{
    config::common::{BrokerAddr, BrokerConnection, CacheSize, InlineLimit, LogLevel, Slots},
    root::RootBuf,
};
use url::Url;
use xdg::BaseDirectories;

pub struct CacheDir;

#[derive(Config, Debug)]
pub struct Config {
    /// Socket address of broker.
    #[config(
        option,
        short = 'b',
        value_name = "SOCKADDR",
        default = r#""no default, must be specified if broker-connection is tcp (the default)""#
    )]
    pub broker: Option<BrokerAddr>,

    /// The number of job slots available.
    #[config(value_name = "N", default = "Slots::default()")]
    pub slots: Slots,

    /// The directory to use for the cache.
    #[config(
        value_name = "PATH",
        default = r#"|bd: &BaseDirectories| {
            bd.get_cache_home()
                .into_os_string()
                .into_string()
                .unwrap()
        }"#
    )]
    pub cache_root: RootBuf<CacheDir>,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative. SI and binary suffixes are supported.
    #[config(value_name = "BYTES", default = "CacheSize::default()")]
    pub cache_size: CacheSize,

    /// The maximum amount of bytes to return inline for captured stdout and stderr.
    #[config(value_name = "BYTES", default = "InlineLimit::default()")]
    pub inline_limit: InlineLimit,

    /// Minimum log level to output.
    #[config(short = 'L', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,

    /// Controls how we connect to the broker.
    #[config(value_name = "BROKER_CONNECTION", default = r#""tcp""#, hide)]
    pub broker_connection: BrokerConnection,

    /// This is required with `broker-conection=github`. This is passed to JavaScript GitHub
    /// actions as `ACTIONS_RUNTIME_TOKEN`.
    #[config(
        option,
        value_name = "GITHUB_ACTIONS_TOKEN",
        default = r#""no default, must be specified if broker-connection is github""#,
        hide,
    )]
    pub github_actions_token: Option<String>,

    /// This is required with `broker-conection=github`. This is passed to JavaScript GitHub
    /// actions as `ACTIONS_RESULTS_URL`.
    #[config(
        option,
        value_name = "GITHUB_ACTIONS_URL",
        default = r#""no default, must be specified if broker-connection is github""#,
        hide,
    )]
    pub github_actions_url: Option<Url>,
}
