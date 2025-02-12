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
use maelstrom_base::{Timeout, Utf8PathBuf};
use maelstrom_client::{CacheDir, Client, ClientBgProcess, ProjectDir, StateDir};
use maelstrom_util::{
    config::common::LogLevel, config::Config, fs::Fs, process::ExitCode, root::RootBuf,
    template::TemplateVars,
};
use slog::Drain as _;
use std::{
    ffi::OsString,
    fmt::{self, Debug},
    io::{self, IsTerminal as _},
    path::PathBuf,
    str,
    sync::{Arc, Mutex},
};
use ui::{Ui, UiHandle, UiSender, UiSlogDrain};

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

type CaseMetadataM<TestCollectorT> = <TestCollectorT as CollectTests>::CaseMetadata;

type ArtifactKeyM<TestCollectorT> = <TestCollectorT as CollectTests>::ArtifactKey;

type CollectOptionsM<TestCollectorT> = <TestCollectorT as CollectTests>::Options;

type TestFilterM<TestCollectorT> = <TestCollectorT as CollectTests>::TestFilter;

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

#[derive(Clone)]
pub struct Directories {
    pub build: RootBuf<BuildDir>,
    pub cache: RootBuf<CacheDir>,
    pub project: RootBuf<ProjectDir>,
    pub state: RootBuf<StateDir>,
}

pub trait TestRunner {
    type Config: Config + Debug + AsRef<config::Config>;
    type ExtraCommandLineOptions: Args + AsRef<config::ExtraCommandLineOptions>;
    type Metadata;
    type TestCollector<'client>: CollectTests<Options = Self::CollectorOptions> + Sync;
    type CollectorOptions;

    const BASE_DIRECTORIES_PREFIX: &'static str;
    const ENVIRONMENT_VARIABLE_PREFIX: &'static str;
    const TEST_METADATA_FILE_NAME: &'static str;
    const TEST_METADATA_DEFAULT_CONTENTS: &'static str;

    fn get_project_directory(config: &Self::Config) -> Result<Utf8PathBuf>;

    fn is_list(extra_options: &Self::ExtraCommandLineOptions) -> bool;

    fn is_list_tests(extra_options: &Self::ExtraCommandLineOptions) -> bool;

    fn get_directories_and_metadata(config: &Self::Config)
        -> Result<(Directories, Self::Metadata)>;

    fn execute_alternative_main(
        _config: &Self::Config,
        _extra_options: &Self::ExtraCommandLineOptions,
        _start_ui: impl FnOnce() -> (UiHandle, UiSender),
    ) -> Option<Result<ExitCode>> {
        None
    }

    fn get_test_collector<'client>(
        client: &'client Client,
        directories: &Directories,
        log: &slog::Logger,
        metadata: Self::Metadata,
    ) -> Result<Self::TestCollector<'client>>;

    fn get_watch_exclude_paths(directories: &Directories) -> Vec<PathBuf>;

    fn split_config(config: Self::Config) -> (config::Config, Self::CollectorOptions);

    fn extra_options_into_parent(
        extra_options: Self::ExtraCommandLineOptions,
    ) -> config::ExtraCommandLineOptions;

    fn get_template_vars(
        _collector_options: &Self::CollectorOptions,
        _directories: &Directories,
    ) -> Result<TemplateVars> {
        Ok(TemplateVars::default())
    }
}

/// Helper that does common work for test-runner main functions and then forwards on to the given
/// underlying function.
///
/// Mostly it deals with the `--init` and `--client-bg-proc` flags
#[allow(clippy::too_many_arguments)]
pub fn main<ArgsT, ArgsIntoIterT, TestRunnerT>(
    command: Command,
    args: ArgsIntoIterT,
    _test_runner: TestRunnerT,
) -> Result<ExitCode>
where
    ArgsIntoIterT: IntoIterator<Item = ArgsT>,
    ArgsT: Into<OsString> + Clone,
    TestRunnerT: TestRunner,
{
    let (config, extra_options): (TestRunnerT::Config, TestRunnerT::ExtraCommandLineOptions) =
        maelstrom_util::config::new_config_with_extra_from_args(
            command,
            TestRunnerT::BASE_DIRECTORIES_PREFIX,
            TestRunnerT::ENVIRONMENT_VARIABLE_PREFIX,
            args,
        )?;

    if extra_options.as_ref().client_bg_proc {
        return alternative_mains::client_bg_proc();
    } else if extra_options.as_ref().init {
        return alternative_mains::init(
            &TestRunnerT::get_project_directory(&config)?,
            TestRunnerT::TEST_METADATA_FILE_NAME,
            TestRunnerT::TEST_METADATA_DEFAULT_CONTENTS,
        );
    }

    let config_parent = config.as_ref();
    let bg_proc = ClientBgProcess::new_from_fork(config_parent.log_level)?;
    let logger = Logger::DefaultLogger(config_parent.log_level);
    let stdout_is_tty = io::stdout().is_terminal();
    let ui = ui::factory(
        config_parent.ui,
        TestRunnerT::is_list(&extra_options),
        stdout_is_tty,
    )?;

    let mut ui = Some(ui);
    let start_ui = || {
        let logging_output = LoggingOutput::default();
        let log = logger.build(logging_output.clone());
        ui.take().unwrap().start_ui_thread(logging_output, log)
    };
    if let Some(result) = TestRunnerT::execute_alternative_main(&config, &extra_options, start_ui) {
        return result;
    }

    let (directories, metadata) = TestRunnerT::get_directories_and_metadata(&config)?;

    Fs.create_dir_all(&directories.state)?;
    Fs.create_dir_all(&directories.cache)?;

    let logging_output = LoggingOutput::default();
    let log = logger.build(logging_output.clone());

    let (parent_config, collector_config) = TestRunnerT::split_config(config);
    let client = Client::new(
        bg_proc,
        parent_config.broker,
        &directories.project,
        &directories.state,
        parent_config.container_image_depot_root,
        &directories.cache,
        parent_config.cache_size,
        parent_config.inline_limit,
        parent_config.slots,
        parent_config.accept_invalid_remote_container_tls_certs,
        parent_config.artifact_transfer_strategy,
        log.clone(),
    )?;

    let list_action = TestRunnerT::is_list(&extra_options).then_some(ListAction::ListTests);
    let parent_extra_options = TestRunnerT::extra_options_into_parent(extra_options);
    let template_vars = TestRunnerT::get_template_vars(&collector_config, &directories)?;
    let test_collector = TestRunnerT::get_test_collector(&client, &directories, &log, metadata)?;
    run_app_with_ui_multithreaded(
        logging_output,
        parent_config.timeout.map(Timeout::new),
        ui.unwrap(),
        &test_collector,
        parent_extra_options.include,
        parent_extra_options.exclude,
        list_action,
        parent_config.repeat,
        parent_config.stop_after,
        parent_extra_options.watch,
        stdout_is_tty,
        &directories.project,
        &directories.state,
        TestRunnerT::get_watch_exclude_paths(&directories),
        collector_config,
        log,
        &client,
        TestRunnerT::TEST_METADATA_FILE_NAME,
        TestRunnerT::TEST_METADATA_DEFAULT_CONTENTS,
        template_vars,
    )
}
