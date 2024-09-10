mod alternative_mains;
mod app;
pub mod config;
mod deps;
mod introspect_driver;
pub mod metadata;
pub mod test_db;
pub mod ui;
pub mod visitor;

#[cfg(test)]
pub mod fake_test_framework;

pub use app::{run_app_with_ui_multithreaded, MainAppCombinedDeps};
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

#[derive(Copy, Clone, PartialEq, Eq)]
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

type TestDb<TestCollectorT> = test_db::TestDb<
    <TestCollectorT as CollectTests>::ArtifactKey,
    <TestCollectorT as CollectTests>::CaseMetadata,
>;

type CaseIter<CaseMetadataT> = <Vec<(String, CaseMetadataT)> as IntoIterator>::IntoIter;

type ArtifactM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::Artifact;

type CaseMetadataM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::CaseMetadata;

type ArtifactKeyM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::ArtifactKey;

type PackageM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::Package;

type PackageIdM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::PackageId;

type CollectOptionsM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::Options;

type TestFilterM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::TestFilter;

type BuildHandleM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::BuildHandle;

type ArtifactStreamM<MainAppDepsT> =
    <<MainAppDepsT as MainAppDeps>::TestCollector as CollectTests>::ArtifactStream;

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

/// Helper that does common work for test-runner main functions and then forwards on to the given
/// underlying function.
///
/// Mostly it deals with the `--init` and `--client-bg-proc` flags
#[allow(clippy::too_many_arguments)]
pub fn main<
    ConfigT,
    ExtraCommandLineOptionsT,
    ArgsT,
    ArgsIntoIterT,
    IsListFn,
    GetProjectDirFn,
    MainFn,
>(
    command: Command,
    base_directories_prefix: &'static str,
    env_var_prefix: &'static str,
    args: ArgsIntoIterT,
    is_list: IsListFn,
    get_project_dir: GetProjectDirFn,
    test_metadata_file_name: &str,
    test_metadata_default_contents: &str,
    main: MainFn,
) -> Result<ExitCode>
where
    ConfigT: Config + Debug + AsRef<config::Config>,
    ExtraCommandLineOptionsT: Args + AsRef<config::ExtraCommandLineOptions>,
    ArgsIntoIterT: IntoIterator<Item = ArgsT>,
    ArgsT: Into<OsString> + Clone,
    IsListFn: FnOnce(&ExtraCommandLineOptionsT) -> bool,
    GetProjectDirFn: FnOnce(&ConfigT) -> Result<Utf8PathBuf>,
    MainFn: FnOnce(
        ConfigT,
        ExtraCommandLineOptionsT,
        ClientBgProcess,
        Logger,
        bool,
        Box<dyn Ui>,
    ) -> Result<ExitCode>,
{
    let (config, extra_options): (ConfigT, ExtraCommandLineOptionsT) =
        maelstrom_util::config::new_config_with_extra_from_args(
            command,
            base_directories_prefix,
            env_var_prefix,
            args,
        )?;

    let config_parent = config.as_ref();

    let bg_proc = ClientBgProcess::new_from_fork(config_parent.log_level)?;
    let logger = Logger::DefaultLogger(config_parent.log_level);

    let stderr_is_tty = io::stderr().is_terminal();
    let stdout_is_tty = io::stdout().is_terminal();

    let ui = ui::factory(config_parent.ui, is_list(&extra_options), stdout_is_tty)?;

    if extra_options.as_ref().client_bg_proc {
        alternative_mains::client_bg_proc()
    } else if extra_options.as_ref().init {
        alternative_mains::init(
            &get_project_dir(&config)?,
            test_metadata_file_name,
            test_metadata_default_contents,
        )
    } else {
        main(config, extra_options, bg_proc, logger, stderr_is_tty, ui)
    }
}
