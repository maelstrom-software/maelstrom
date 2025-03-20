use anyhow::Result;
use maelstrom_admin::{config::Config, Subcommand};
use std::env;

fn main() -> Result<()> {
    let (config, subcommand): (_, Subcommand) = Config::new_with_subcommand(
        "maelstrom/admin",
        ["MAELSTROM_ADMIN", "MAELSTROM"],
        env::args(),
    )?;

    maelstrom_util::log::run_with_logger(config.log_level, |log| {
        maelstrom_admin::main(config, log, subcommand)
    })
}
