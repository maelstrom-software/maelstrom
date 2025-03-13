use anyhow::Result;
use maelstrom_broker::config::Config;

fn main() -> Result<()> {
    let config = Config::new("maelstrom/broker", ["MAELSTROM_BROKER", "MAELSTROM"])?;
    maelstrom_util::log::run_with_logger(config.log_level, |log| {
        maelstrom_broker::main(config, log)
    })
}
