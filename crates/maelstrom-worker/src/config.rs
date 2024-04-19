use maelstrom_macro::Config;
use maelstrom_util::config::common::{
    BrokerAddr, CacheRoot, CacheSize, InlineLimit, LogLevel, Slots,
};
use xdg::BaseDirectories;

#[derive(Config, Debug)]
pub struct Config {
    /// Socket address of broker.
    #[config(short = 'b', value_name = "SOCKADDR")]
    pub broker: BrokerAddr,

    /// The number of job slots available.
    #[config(short = 'S', value_name = "N", default = "num_cpus::get()")]
    pub slots: Slots,

    /// The directory to use for the cache.
    #[config(
        short = 'r',
        value_name = "PATH",
        default = "|bd: &BaseDirectories| bd.get_cache_home().into_os_string().into_string().unwrap()"
    )]
    pub cache_root: CacheRoot,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative. SI and binary suffixes are supported.
    #[config(
        short = 's',
        value_name = "BYTES",
        default = "bytesize::ByteSize::gb(1)"
    )]
    pub cache_size: CacheSize,

    /// The maximum amount of bytes to return inline for captured stdout and stderr.
    #[config(
        short = 'i',
        value_name = "BYTES",
        default = "bytesize::ByteSize::mb(1)"
    )]
    pub inline_limit: InlineLimit,

    /// Minimum log level to output.
    #[config(short = 'l', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,
}
