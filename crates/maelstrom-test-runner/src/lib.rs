mod alternative_mains;
mod app;
pub mod config;
mod deps;
pub mod metadata;
pub mod test_db;
pub mod ui;

#[cfg(test)]
pub mod fake_test_framework;

pub use app::run_app_with_ui_multithreaded;
pub use deps::*;

use anyhow::Result;
use clap::{Args, Command};
use maelstrom_base::Utf8PathBuf;
use maelstrom_client::ClientBgProcess;
use maelstrom_util::{config::common::LogLevel, config::Config, process::ExitCode};
use slog::Drain as _;
use std::{
    ffi::OsString,
    fmt::{self, Debug},
    io::{self, IsTerminal as _},
    str,
    sync::{Arc, Mutex},
};
use ui::{Ui, UiSender, UiSlogDrain};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NotRunEstimate {
    About(u64),
    Exactly(u64),
    GreaterThan(u64),
    Unknown,
}

impl fmt::Display for NotRunEstimate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::About(n) => fmt::Display::fmt(&format!("~{n}"), f),
            Self::Exactly(n) => fmt::Display::fmt(&n, f),
            Self::GreaterThan(n) => fmt::Display::fmt(&format!(">{n}"), f),
            Self::Unknown => fmt::Display::fmt("unknown", f),
        }
    }
}

#[derive(Debug)]
pub enum ListAction {
    ListTests,
}

type CaseMetadataM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::CaseMetadata;

type ArtifactKeyM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::ArtifactKey;

type CollectOptionsM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::Options;

type TestFilterM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::TestFilter;

/// This is where cached data goes. If there is build output it is also here.
pub struct BuildDir;

type TermDrain = slog::Fuse<slog_async::Async>;

enum LoggingOutputInner {
    Ui(UiSlogDrain),
    Term(TermDrain),
}

impl Default for LoggingOutputInner {
    fn default() -> Self {
        let decorator = slog_term::TermDecorator::new().stdout().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        Self::Term(drain)
    }
}

/// This object exists to allow us to switch where log messages go at run-time.
///
/// When the UI is running, it owns the terminal, so we need to send log messages to the UI during
/// this time. Before or after the UI is running though, we just display log messages more normally
/// directly to the terminal.
///
/// When this object is created via `[Default::default]`, it starts out sending messages directly
/// to the terminal.
#[derive(Clone, Default)]
pub struct LoggingOutput {
    inner: Arc<Mutex<LoggingOutputInner>>,
}

impl LoggingOutput {
    /// Send any future log messages to the given UI sender. Probably best to call this when the
    /// process is single-threaded.
    pub fn display_on_ui(&self, ui: UiSender) {
        *self.inner.lock().unwrap() = LoggingOutputInner::Ui(UiSlogDrain::new(ui));
    }

    /// Send any future log messages directly to the terminal on `stdout`. Probably best to call
    /// this when the process is single-threaded.
    pub fn display_on_term(&self) {
        *self.inner.lock().unwrap() = LoggingOutputInner::default();
    }
}

impl slog::Drain for LoggingOutput {
    type Ok = ();
    type Err = <TermDrain as slog::Drain>::Err;

    fn log(
        &self,
        record: &slog::Record<'_>,
        values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        match &mut *self.inner.lock().unwrap() {
            LoggingOutputInner::Ui(d) => d.log(record, values),
            LoggingOutputInner::Term(d) => d.log(record, values),
        }
    }
}

/// A way to represent an already instantiated `[slog::Logger]` or the arguments to create a
/// `[slog::Logger]`.
///
/// This is used when invoking the main entry-point for test runners. The caller either wants the
/// test runner to create it own logger, or (in the case of the tests) use the given one.
pub enum Logger {
    DefaultLogger(LogLevel),
    GivenLogger(slog::Logger),
}

impl Logger {
    pub fn build(&self, out: LoggingOutput) -> slog::Logger {
        match self {
            Self::DefaultLogger(level) => {
                let drain = slog::LevelFilter::new(out, level.as_slog_level()).fuse();
                slog::Logger::root(drain, slog::o!())
            }
            Self::GivenLogger(logger) => logger.clone(),
        }
    }
}

pub trait TestRunner {
    type Config: Config + Debug + AsRef<config::Config>;
    type ExtraCommandLineOptions: Args + AsRef<config::ExtraCommandLineOptions>;

    fn get_base_directories_prefix(&self) -> &'static str;
    fn get_environment_variable_prefix(&self) -> &'static str;
    fn get_test_metadata_file_name(&self) -> &str;
    fn get_test_metadata_default_contents(&self) -> &str;
    fn get_project_directory(&self, config: &Self::Config) -> Result<Utf8PathBuf>;
    fn is_list(&self, extra_options: &Self::ExtraCommandLineOptions) -> bool;
    fn main(
        &self,
        config: Self::Config,
        extra_options: Self::ExtraCommandLineOptions,
        bg_proc: ClientBgProcess,
        logger: Logger,
        stdout_is_tty: bool,
        ui: Box<dyn Ui>,
    ) -> Result<ExitCode>;
}

/// Helper that does common work for test-runner main functions and then forwards on to the given
/// underlying function.
///
/// Mostly it deals with the `--init` and `--client-bg-proc` flags
#[allow(clippy::too_many_arguments)]
pub fn main<ArgsT, ArgsIntoIterT, TestRunnerT>(
    command: Command,
    args: ArgsIntoIterT,
    test_runner: TestRunnerT,
) -> Result<ExitCode>
where
    ArgsIntoIterT: IntoIterator<Item = ArgsT>,
    ArgsT: Into<OsString> + Clone,
    TestRunnerT: TestRunner,
{
    let (config, extra_options): (TestRunnerT::Config, TestRunnerT::ExtraCommandLineOptions) =
        maelstrom_util::config::new_config_with_extra_from_args(
            command,
            test_runner.get_base_directories_prefix(),
            test_runner.get_environment_variable_prefix(),
            args,
        )?;

    if extra_options.as_ref().client_bg_proc {
        return alternative_mains::client_bg_proc();
    } else if extra_options.as_ref().init {
        return alternative_mains::init(
            &test_runner.get_project_directory(&config)?,
            test_runner.get_test_metadata_file_name(),
            test_runner.get_test_metadata_default_contents(),
        );
    }

    let config_parent = config.as_ref();
    let bg_proc = ClientBgProcess::new_from_fork(config_parent.log_level)?;
    let logger = Logger::DefaultLogger(config_parent.log_level);
    let stdout_is_tty = io::stdout().is_terminal();
    let ui = ui::factory(
        config_parent.ui,
        test_runner.is_list(&extra_options),
        stdout_is_tty,
    )?;
    test_runner.main(config, extra_options, bg_proc, logger, stdout_is_tty, ui)
}
