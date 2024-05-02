use anyhow::Result;
use cargo_maelstrom::{cli::ExtraCommandLineOptions, config::Config, Logger};
use console::Term;
use maelstrom_client::ClientBgProcess;
use maelstrom_util::process::ExitCode;
use std::{env, io::IsTerminal as _};

pub fn main() -> Result<ExitCode> {
    let mut args = Vec::from_iter(env::args());
    if args.len() > 1 && args[0].ends_with(format!("cargo-{}", args[1]).as_str()) {
        args.remove(1);
    }

    let (config, extra_options): (_, ExtraCommandLineOptions) =
        Config::new_with_extra_from_args("maelstrom/cargo-maelstrom", "CARGO_MAELSTROM", args)?;

    let bg_proc = ClientBgProcess::new_from_fork(config.log_level)?;

    let logger = Logger::DefaultLogger(config.log_level);

    let stderr_is_tty = std::io::stderr().is_terminal();
    let stdout_is_tty = std::io::stdout().is_terminal();
    let terminal = Term::buffered_stdout();

    cargo_maelstrom::main(
        config,
        extra_options,
        bg_proc,
        logger,
        stderr_is_tty,
        stdout_is_tty,
        terminal,
    )
}
