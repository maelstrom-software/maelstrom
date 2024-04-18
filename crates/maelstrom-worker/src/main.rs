use anyhow::Result;
use maelstrom_worker::config::Config;

fn main() -> Result<()> {
    let config = Config::new("maelstrom/worker", "MAELSTROM_WORKER")?;
    maelstrom_worker::clone_into_pid_and_user_namespace()?;
    maelstrom_util::log::run_with_logger(config.log_level, |log| {
        maelstrom_worker::main(config, log)
    })
}
