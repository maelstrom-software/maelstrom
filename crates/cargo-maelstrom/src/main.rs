use anyhow::{bail, Context as _, Result};
use cargo_maelstrom::{
    cargo::CargoBuildError, config::Config, main_app_new,
    metadata::maybe_write_default_test_metadata, progress::DefaultProgressDriver,
    DefaultMainAppDeps, ListAction, Logger, LoggingOutput, MainAppState,
};
use cargo_metadata::Metadata as CargoMetadata;
use clap::{command, Args};
use console::Term;
use maelstrom_base::Timeout;
use maelstrom_client::ClientBgProcess;
use maelstrom_util::{fs::Fs, process::ExitCode};
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

    #[command(flatten)]
    test_metadata: TestMetadataOptions,
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

#[derive(Args)]
#[command(next_help_heading = "Test Metadata Options")]
struct TestMetadataOptions {
    #[arg(
        long,
        help = "Write out a starter test metadata file if one does not exist, then exit."
    )]
    init: bool,
}

fn maybe_print_build_error(res: Result<ExitCode>) -> Result<ExitCode> {
    if let Err(e) = &res {
        if let Some(e) = e.downcast_ref::<CargoBuildError>() {
            eprintln!("{}", &e.stderr);
            return Ok(e.exit_code);
        }
    }
    res
}

/// The main function for the client. This should be called on a task of its own. It will return
/// when a signal is received or when all work has been processed by the broker.
pub fn main() -> Result<ExitCode> {
    let mut args = Vec::from_iter(env::args());
    if args.len() > 1 && args[0].ends_with(format!("cargo-{}", args[1]).as_str()) {
        args.remove(1);
    }

    let (config, extra_options): (_, ExtraCommandLineOptions) =
        Config::new_with_extra_from_args("maelstrom/cargo-maelstrom", "CARGO_MAELSTROM", args)?;

    let fs = Fs::new();
    let bg_proc = ClientBgProcess::new_from_fork(config.log_level)?;

    let logging_output = LoggingOutput::default();
    let logger = Logger::DefaultLogger(config.log_level);
    let log = logger.build(logging_output.clone());

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

    let output = process::Command::new("cargo")
        .args(["metadata", "--format-version=1"])
        .args(config.cargo_feature_selection_options.iter())
        .args(config.cargo_manifest_options.iter())
        .output()
        .context("getting cargo metadata")?;
    if !output.status.success() {
        bail!(String::from_utf8(output.stderr)
            .context("reading stderr")?
            .trim_end()
            .trim_start_matches("error: ")
            .to_owned());
    }
    let cargo_metadata: CargoMetadata =
        serde_json::from_slice(&output.stdout).context("parsing cargo metadata")?;

    if extra_options.test_metadata.init {
        maybe_write_default_test_metadata(&cargo_metadata.workspace_root)?;
        return Ok(ExitCode::SUCCESS);
    }

    let target_dir = &cargo_metadata.target_directory;
    fs.create_dir_all(target_dir)?;

    let deps = DefaultMainAppDeps::new(
        bg_proc,
        target_dir,
        &cargo_metadata.workspace_root,
        config.broker,
        config.cache_size,
        config.inline_limit,
        config.slots,
        log.clone(),
    )?;

    let state = MainAppState::new(
        deps,
        extra_options.include,
        extra_options.exclude,
        list_action,
        std::io::stderr().is_terminal(),
        &cargo_metadata.workspace_root,
        &cargo_metadata.workspace_packages(),
        &cargo_metadata.target_directory,
        config.cargo_feature_selection_options,
        config.cargo_compilation_options,
        config.cargo_manifest_options,
        logging_output,
        log,
    )?;

    let stdout_tty = std::io::stdout().is_terminal();
    let res = std::thread::scope(|scope| {
        let mut app = main_app_new(
            &state,
            stdout_tty,
            config.quiet,
            Term::buffered_stdout(),
            DefaultProgressDriver::new(scope),
            config.timeout.map(Timeout::new),
        )?;
        while !app.enqueue_one()?.is_done() {}
        app.drain()?;
        app.finish()
    });
    drop(state);
    maybe_print_build_error(res)
}
