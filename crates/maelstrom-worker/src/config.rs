use maelstrom_macro::Config;
use maelstrom_util::{
    config::common::{
        ArtifactTransferStrategy, BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots,
    },
    root::RootBuf,
};
use xdg::BaseDirectories;

pub struct CacheDir;

#[derive(Config, Debug)]
pub struct Config {
    /// Socket address of broker.
    #[config(short = 'b', value_name = "SOCKADDR")]
    pub broker: BrokerAddr,

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
    #[config(short = 'l', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,

    /// Controls how we upload artifacts when communicating with the broker.
    #[config(
        value_name = "ARTIFACT_TRANSFER_STRATEGY",
        default = r#""tcp-upload""#,
        hide
    )]
    pub artifact_transfer_strategy: ArtifactTransferStrategy,
}
