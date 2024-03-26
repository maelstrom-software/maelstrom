use anyhow::{Context as _, Result};
use cargo_maelstrom::{
    config::Config, main_app_new, progress::DefaultProgressDriver, ListAction, Logger, MainAppDeps,
};
use cargo_metadata::Metadata as CargoMetadata;
use clap::{command, Args};
use console::Term;
use maelstrom_base::Timeout;
use maelstrom_client::ClientBgProcess;
use maelstrom_util::process::ExitCode;
use std::{env, io::IsTerminal as _, process};

#[derive(Args)]
#[command(next_help_heading = "Test Selection Options")]
struct ExtraCommandLineOptions {
    #[arg(
        long,
        short = 'i',
        value_name = "FILTER-EXPRESSION",
        default_value = "all",
        help = "Only include tests which match the given filter. Can be specified multiple times."
    )]
    include: Vec<String>,

    #[arg(
        long,
        short = 'x',
        value_name = "FILTER-EXPRESSION",
        help = "Only include tests which don't match the given filter. Can be specified multiple times."
    )]
    exclude: Vec<String>,

    #[command(flatten)]
    list: ListOptions,
}

#[derive(Args)]
#[group(multiple = false)]
#[command(next_help_heading = "List Options")]
struct ListOptions {
    #[arg(
        long = "list-tests",
        visible_alias = "list",
        help = "Instead of running tests, print the tests that would have been run. \
            May require building test binaries."
    )]
    tests: bool,

    #[arg(
        long = "list-binaries",
        help = "Instead of running tests, print the test binaries of those tests that would \
            have been run."
    )]
    binaries: bool,

    #[arg(
        long = "list-packages",
        help = "Instead of running tests, print the packages of those tests that would \
            have been run."
    )]
    packages: bool,
}

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub fn main() -> Result<ExitCode> {
    let mut args = Vec::from_iter(env::args());
    if args.len() > 1 && args[0].ends_with(format!("cargo-{}", args[1]).as_str()) {
        args.remove(1);
    }

    let (config, extra_options): (Config, ExtraCommandLineOptions) =
        maelstrom_util::config::new_config_with_extra_from_args(
            command!(),
            "maelstrom/cargo-maelstrom",
            "CARGO_MAELSTROM",
            args,
        )?;

    let list_action = match (
        extra_options.list.tests,
        extra_options.list.binaries,
        extra_options.list.packages,
    ) {
        (true, _, _) => Some(ListAction::ListTests),
        (_, true, _) => Some(ListAction::ListBinaries),
        (_, _, true) => Some(ListAction::ListPackages),
        (_, _, _) => None,
    };

    let bg_proc = ClientBgProcess::new_from_fork()?;

    let cargo_metadata = process::Command::new("cargo")
        .args(["metadata", "--format-version=1"])
        .args(config.cargo_feature_selection_options.iter())
        .args(config.cargo_manifest_options.iter())
        .output()
        .context("getting cargo metadata")?;
    let cargo_metadata: CargoMetadata =
        serde_json::from_slice(&cargo_metadata.stdout).context("parsing cargo metadata")?;

    let deps = MainAppDeps::new(
        bg_proc,
        "cargo".into(),
        extra_options.include,
        extra_options.exclude,
        list_action,
        std::io::stderr(),
        std::io::stderr().is_terminal(),
        &cargo_metadata.workspace_root,
        &cargo_metadata.workspace_packages(),
        config.broker,
        Default::default(),
        config.cargo_feature_selection_options,
        config.cargo_compilation_options,
        config.cargo_manifest_options,
        Logger::DefaultLogger(config.log_level),
    )?;

    let stdout_tty = std::io::stdout().is_terminal();
    std::thread::scope(|scope| {
        let mut app = main_app_new(
            &deps,
            stdout_tty,
            config.quiet,
            Term::buffered_stdout(),
            DefaultProgressDriver::new(scope),
            config.timeout.map(Timeout::new),
        )?;
        while !app.enqueue_one()?.is_done() {}
        app.drain()?;
        app.finish()
    })
}
