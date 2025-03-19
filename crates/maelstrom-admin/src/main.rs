use anyhow::Result;
use maelstrom_admin::config::Config;

fn main() -> Result<()> {
    let config = Config::new("maelstrom/admin", ["MAELSTROM_ADMIN", "MAELSTROM"])?;
    maelstrom_util::log::run_with_logger(config.log_level, |log| maelstrom_admin::main(config, log))
}
