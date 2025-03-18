use crate::AcceptInvalidRemoteContainerTlsCerts;
use maelstrom_container::ContainerImageDepotDir;
use maelstrom_macro::Config;
use maelstrom_util::{
    config::common::{ArtifactTransferStrategy, BrokerAddr, CacheSize, InlineLimit, Slots},
    root::RootBuf,
};
use xdg::BaseDirectories;

#[derive(Config, Debug)]
pub struct Config {
    /// The target amount of disk space to use for the cache. This bound won't be followed
    /// strictly, so it's best to be conservative. SI and binary suffixes are supported.
    #[config(
        value_name = "BYTES",
        default = "CacheSize::default()",
        next_help_heading = "Local Worker Config Options"
    )]
    pub cache_size: CacheSize,

    /// The maximum amount of bytes to return inline for captured stdout and stderr.
    #[config(value_name = "BYTES", default = "InlineLimit::default()")]
    pub inline_limit: InlineLimit,

    /// The number of job slots available.
    #[config(value_name = "N", default = "Slots::default()")]
    pub slots: Slots,

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
        }"#,
        next_help_heading = "Container Image Config Options"
    )]
    pub container_image_depot_root: RootBuf<ContainerImageDepotDir>,

    /// Accept invalid TLS certificates when downloading container images.
    #[config(flag)]
    pub accept_invalid_remote_container_tls_certs: AcceptInvalidRemoteContainerTlsCerts,

    /// Socket address of broker. If not provided, all tests will be run locally.
    #[config(
        option,
        short = 'b',
        value_name = "SOCKADDR",
        default = r#""standalone mode""#,
        next_help_heading = "Test Runner Config Options"
    )]
    pub broker: Option<BrokerAddr>,

    /// Controls how we upload artifacts when communicating with a remote broker.
    #[config(
        value_name = "ARTIFACT_TRANSFER_STRATEGY",
        default = r#""tcp-upload""#,
        hide
    )]
    pub artifact_transfer_strategy: ArtifactTransferStrategy,
}
