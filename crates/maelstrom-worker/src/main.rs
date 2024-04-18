use anyhow::{Context as _, Result};
use maelstrom_worker::config::Config;
use slog::{o, Drain, LevelFilter, Logger};
use slog_async::Async;
use slog_term::{FullFormat, TermDecorator};
use tokio::runtime::Runtime;

fn run_with_logger(config: Config, f: impl FnOnce(Config, Logger) -> Result<()>) -> Result<()> {
    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let drain = LevelFilter::new(drain, config.log_level.as_slog_level()).fuse();
    let log = Logger::root(drain, o!());
    f(config, log)
}

fn main() -> Result<()> {
    let config = Config::new("maelstrom/worker", "MAELSTROM_WORKER")?;
    maelstrom_worker::clone_into_pid_and_user_namespace()?;
    run_with_logger(config, |config, log| {
        Runtime::new()
            .context("starting tokio runtime")?
            .block_on(async move { maelstrom_worker::main(config, log).await })
    })
}
