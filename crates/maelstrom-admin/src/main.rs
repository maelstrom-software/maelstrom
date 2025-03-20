use anyhow::Result;
use clap::command;
use maelstrom_admin::{config::Config, Subcommand};
use maelstrom_util::config;
use std::env;

fn main() -> Result<()> {
    let (config, subcommand): (Config, Subcommand) = config::new_config_with_subcommand_from_args(
        command!(),
        "maelstrom/admin",
        ["MAELSTROM_ADMIN", "MAELSTROM"],
        env::args(),
    )?;

    maelstrom_util::log::run_with_logger(config.log_level, |log| {
        maelstrom_admin::main(config, log, subcommand)
    })
}
