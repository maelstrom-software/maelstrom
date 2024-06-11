use anyhow::Result;
use console::Term;
use maelstrom_client::ClientBgProcess;
use maelstrom_pytest::{cli::ExtraCommandLineOptions, Config, Logger};
use maelstrom_util::process::ExitCode;
use std::{env, io::IsTerminal as _};

pub fn main() -> Result<ExitCode> {
    let args = Vec::from_iter(env::args());
    let (config, extra_options): (_, ExtraCommandLineOptions) =
        Config::new_with_extra_from_args("maelstrom/maelstrom-pytest", "MAELSTROM_PYTEST", args)?;

    let bg_proc = ClientBgProcess::new_from_fork(config.parent.log_level)?;

    let logger = Logger::DefaultLogger(config.parent.log_level);

    let stderr_is_tty = std::io::stderr().is_terminal();
    let stdout_is_tty = std::io::stdout().is_terminal();
    let terminal = Term::buffered_stdout();

    maelstrom_pytest::main(
        config,
        extra_options,
        bg_proc,
        logger,
        stderr_is_tty,
        stdout_is_tty,
        terminal,
    )
}
