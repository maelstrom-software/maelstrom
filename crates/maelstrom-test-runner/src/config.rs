use derive_more::From;
use maelstrom_client::{AcceptInvalidRemoteContainerTlsCerts, ContainerImageDepotDir};
use maelstrom_macro::Config;
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots},
    root::RootBuf,
};
use serde::Deserialize;
use std::{
    fmt::{self, Debug, Formatter},
    result,
};
use xdg::BaseDirectories;

#[derive(Clone, Deserialize, From)]
#[serde(transparent)]
pub struct Quiet(bool);

impl Quiet {
    pub fn into_inner(self) -> bool {
        self.0
    }
}

impl Debug for Quiet {
    fn fmt(&self, f: &mut Formatter<'_>) -> result::Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

#[derive(Config, Debug)]
pub struct Config {
    /// Socket address of broker. If not provided, all tests will be run locally.
    #[config(
        option,
        short = 'b',
        value_name = "SOCKADDR",
        default = r#""standalone mode""#
    )]
    pub broker: Option<BrokerAddr>,

    /// Minimum log level to output.
    #[config(short = 'l', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,

    /// Don't output information about the tests being run.
    #[config(flag, short = 'q')]
    pub quiet: Quiet,

    /// Directory in which to put cached container images.
    #[config(
        value_name = "PATH",
        default = r#"|bd: &BaseDirectories| {
            bd.get_cache_home()
                .parent()
                .unwrap()
                .join("container/")
                .into_os_string()
                .into_string()
                .unwrap()
        }"#
    )]
    pub container_image_depot_root: RootBuf<ContainerImageDepotDir>,

    /// Override timeout value for all tests specified (O indicates no timeout).
    #[config(
        option,
        short = 't',
        value_name = "SECONDS",
        default = r#""whatever individual tests specify""#,
        next_help_heading = "Test Override Config Options"
    )]
    pub timeout: Option<u32>,

    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative. SI and binary suffixes are supported.
    #[config(
        value_name = "BYTES",
        default = "CacheSize::default()",
        next_help_heading = "Local Worker Options"
    )]
    pub cache_size: CacheSize,

    /// The maximum amount of bytes to return inline for captured stdout and stderr.
    #[config(value_name = "BYTES", default = "InlineLimit::default()")]
    pub inline_limit: InlineLimit,

    /// The number of job slots available.
    #[config(value_name = "N", default = "Slots::default()")]
    pub slots: Slots,

    /// Whether to accept invalid TLS certificates when downloading container images.
    #[config(flag, value_name = "ACCEPT_INVALID_REMOTE_CONTAINER_TLS_CERTS")]
    pub accept_invalid_remote_container_tls_certs: AcceptInvalidRemoteContainerTlsCerts,
}
