use maelstrom_macro::Config;
use maelstrom_util::config::common::{BrokerAddr, ClusterCommunicationStrategy, LogLevel};
use url::Url;

#[derive(Config, Debug)]
pub struct Config {
    /// Minimum log level to output.
    #[config(short = 'L', value_name = "LEVEL", default = r#""info""#)]
    pub log_level: LogLevel,

    /// Socket address of broker. If not provided, all tests will be run locally.
    #[config(
        option,
        short = 'b',
        value_name = "SOCKADDR",
        default = r#""standalone mode""#
    )]
    pub broker: Option<BrokerAddr>,

    /// The cluster communication strategy: TCP or GitHub.
    #[config(
        value_name = "CLUSTER_COMMUNICATION_STRATEGY",
        default = r#""tcp""#,
        hide
    )]
    pub cluster_communication_strategy: ClusterCommunicationStrategy,

    /// This is required with `cluster-communication-strategy=github`. This is passed to JavaScript
    /// GitHub actions as `ACTIONS_RUNTIME_TOKEN`.
    #[config(
        option,
        value_name = "GITHUB_ACTIONS_TOKEN",
        default = r#""no default, must be specified if cluster-communication-strategy is github""#,
        hide
    )]
    pub github_actions_token: Option<String>,

    /// This is required with `cluster-communication-strategy=github`. This is passed to JavaScript
    /// GitHub actions as `ACTIONS_RESULTS_URL`.
    #[config(
        option,
        value_name = "GITHUB_ACTIONS_URL",
        default = r#""no default, must be specified if cluster-communication-strategy is github""#,
        hide
    )]
    pub github_actions_url: Option<Url>,
}
