use anyhow::Result;
use cargo_maelstrom::{cli::ExtraCommandLineOptions, config::Config, Logger};
use maelstrom_client::ClientBgProcess;
use maelstrom_test_runner::ui;
use maelstrom_util::process::ExitCode;
use std::{env, io::IsTerminal as _};

pub fn main() -> Result<ExitCode> {
    let mut args = Vec::from_iter(env::args());
    if args.len() > 1 && args[0].ends_with(format!("cargo-{}", args[1]).as_str()) {
        args.remove(1);
    }

    let (config, extra_options): (_, ExtraCommandLineOptions) =
        Config::new_with_extra_from_args("maelstrom/cargo-maelstrom", "CARGO_MAELSTROM", args)?;

    let bg_proc = ClientBgProcess::new_from_fork(config.parent.log_level)?;

    let logger = Logger::DefaultLogger(config.parent.log_level);

    let stderr_is_tty = std::io::stderr().is_terminal();
    let stdout_is_tty = std::io::stdout().is_terminal();

    let list =
        extra_options.list.tests || extra_options.list.binaries || extra_options.list.packages;
    let ui = ui::factory(config.parent.ui, list, stdout_is_tty, config.parent.quiet);

    cargo_maelstrom::main(config, extra_options, bg_proc, logger, stderr_is_tty, ui)
}
