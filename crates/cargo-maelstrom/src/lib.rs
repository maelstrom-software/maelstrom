pub use maelstrom_test_runner::{cargo, cli, config, Logger};

pub fn main<TermT>(
    config: config::Config,
    extra_options: cli::ExtraCommandLineOptions,
    bg_proc: maelstrom_client::ClientBgProcess,
    logger: Logger,
    stderr_is_tty: bool,
    stdout_is_tty: bool,
    terminal: TermT,
) -> anyhow::Result<maelstrom_util::process::ExitCode>
where
    TermT: indicatif::TermLike
        + Clone
        + Send
        + Sync
        + std::panic::UnwindSafe
        + std::panic::RefUnwindSafe
        + 'static,
{
    maelstrom_test_runner::main(
        config,
        extra_options,
        bg_proc,
        logger,
        stderr_is_tty,
        stdout_is_tty,
        terminal,
    )
}
