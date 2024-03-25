use anyhow::{Context as _, Result};
use cargo_maelstrom::{
    config::Config, main_app_new, progress::DefaultProgressDriver, ListAction, Logger, MainAppDeps,
};
use cargo_metadata::Metadata as CargoMetadata;
use clap::{command, Arg, ArgAction, ArgGroup, Command};
use console::Term;
use maelstrom_base::Timeout;
use maelstrom_client::ClientBgProcess;
use maelstrom_util::process::ExitCode;
use std::{env, io::IsTerminal as _, process};

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub fn main() -> Result<ExitCode> {
    let add_more_command_line_options = |command: Command| {
        command
            .next_help_heading("Test Selection Options")
            .arg(
                Arg::new("include")
                    .long("include")
                    .short('i')
                    .value_name("FILTER-EXPRESSION")
                    .action(ArgAction::Append)
                    .help("Only include tests which match the given filter. Can be specified multiple times.")
            )
            .arg(
                Arg::new("exclude")
                    .long("exclude")
                    .short('x')
                    .value_name("FILTER-EXPRESSION")
                    .action(ArgAction::Append)
                    .help("Only include tests which don't match the given filter. Can be specified multiple times.")
            )
            .next_help_heading("List Options")
            .arg(
                Arg::new("list-tests")
                    .long("list-tests")
                    .visible_alias("list")
                    .action(ArgAction::SetTrue)
                    .help(
                        "Instead of running tests, print the tests that would have been run. \
                        May require building test binaries."
                    )
            )
            .arg(
                Arg::new("list-binaries")
                    .long("list-binaries")
                    .action(ArgAction::SetTrue)
                    .help(
                        "Instead of running tests, print the test binaries of those tests \
                        that would have been run."
                    )
            )
            .arg(
                Arg::new("list-packages")
                    .long("list-packages")
                    .action(ArgAction::SetTrue)
                    .help(
                        "Instead of running tests, print the packages of those tests \
                        that would have been run."
                    )
            )
            .group(
                ArgGroup::new("list").args(["list-tests", "list-binaries", "list-packages"]))
    };

    let mut args = Vec::from_iter(env::args());
    if args.len() > 1 && args[0].ends_with(format!("cargo-{}", args[1]).as_str()) {
        args.remove(1);
    }

    let (config, mut args): (Config, _) = maelstrom_util::config::new_config2(
        command!(),
        "maelstrom/cargo-maelstrom",
        "CARGO_MAELSTROM",
        add_more_command_line_options,
        args,
    )?;

    let include = if let Some(values) = args.remove_many::<String>("include") {
        Vec::from_iter(values)
    } else {
        vec!["all".to_string()]
    };
    let exclude = if let Some(values) = args.remove_many::<String>("exclude") {
        Vec::from_iter(values)
    } else {
        vec![]
    };
    let list_action = match (
        args.remove_one::<bool>("list-tests").unwrap(),
        args.remove_one::<bool>("list-binaries").unwrap(),
        args.remove_one::<bool>("list-packages").unwrap(),
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
        include,
        exclude,
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
