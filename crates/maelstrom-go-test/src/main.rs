use anyhow::Result;
use maelstrom_client::{ClientBgProcess, ProjectDir};
use maelstrom_go_test::{cli::ExtraCommandLineOptions, Config, Logger};
use maelstrom_test_runner::ui;
use maelstrom_util::{process::ExitCode, root::Root};
use std::path::Path;
use std::{env, io::IsTerminal as _};

pub fn main() -> Result<ExitCode> {
    let args = Vec::from_iter(env::args());

    let (config, extra_options): (_, ExtraCommandLineOptions) =
        Config::new_with_extra_from_args("maelstrom/maelstrom-go-test", "MAELSTROM_GO_TEST", args)?;

    let bg_proc = ClientBgProcess::new_from_fork(config.parent.log_level)?;

    let logger = Logger::DefaultLogger(config.parent.log_level);

    let stderr_is_tty = std::io::stderr().is_terminal();
    let stdout_is_tty = std::io::stdout().is_terminal();

    let cwd = Path::new(".").canonicalize()?;
    let project_dir = Root::<ProjectDir>::new(&cwd);

    let ui = ui::factory(
        config.parent.ui,
        extra_options.list,
        stdout_is_tty,
        config.parent.quiet,
    );

    maelstrom_go_test::main(
        config,
        extra_options,
        project_dir,
        bg_proc,
        logger,
        stderr_is_tty,
        std::io::stderr(),
        ui,
    )
}
