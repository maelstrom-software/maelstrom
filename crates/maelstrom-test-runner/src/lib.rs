mod alternative_mains;
mod app;
pub mod config;
mod deps;
pub mod log;
pub mod metadata;
pub mod test_db;
pub mod ui;
pub mod util;

#[cfg(test)]
pub mod fake_test_framework;

pub use app::run_app_with_ui_multithreaded;
pub use deps::*;

use anyhow::Result;
use clap::{Args, Command};
use derive_more::{From, Into};
use log::{LogDestination, LoggerBuilder};
use maelstrom_base::{Timeout, Utf8PathBuf};
use maelstrom_client::{CacheDir, Client, ClientBgProcess, ProjectDir, StateDir};
use maelstrom_util::{
    config::Config, fs::Fs, process::ExitCode, root::RootBuf, template::TemplateVars,
};
use std::{
    ffi::OsString,
    fmt::Debug,
    io::{self, IsTerminal as _},
    path::PathBuf,
    str,
};
use ui::{Ui, UiHandle, UiSender};

#[derive(Clone, Copy, derive_more::Debug, derive_more::Display, Eq, From, PartialEq)]
pub struct ListTests(bool);

impl ListTests {
    pub fn as_bool(self) -> bool {
        self.0
    }
}

/// This is the directory that contains build artifacts. The [`CacheDir`] and [`StateDir`]
/// directories will be descendents of this directory. On the other hand, this directory will
/// always be a descendant of the [`ProjectDir`].
pub struct BuildDir;

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
    const DEFAULT_TEST_METADATA_FILE_CONTENTS: &'static str;

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
        let (directories, _) = TestRunnerT::get_directories_and_metadata(&config)?;
        return alternative_mains::init(
            &directories.project,
            TestRunnerT::TEST_METADATA_FILE_NAME,
            TestRunnerT::DEFAULT_TEST_METADATA_FILE_CONTENTS,
        );
    }

    let config_parent = config.as_ref();
    let bg_proc = ClientBgProcess::new_from_fork(config_parent.log_level)?;
    let logger = LoggerBuilder::DefaultLogger(config_parent.log_level);
    let stdout_is_tty = io::stdout().is_terminal();
    let ui = ui::factory(
        config_parent.ui,
        TestRunnerT::is_list(&extra_options),
        stdout_is_tty,
    )?;

    let mut ui = Some(ui);
    let start_ui = || {
        let log_destination = LogDestination::default();
        let log = logger.build(log_destination.clone());
        ui.take().unwrap().start_ui_thread(log_destination, log)
    };
    if let Some(result) = TestRunnerT::execute_alternative_main(&config, &extra_options, start_ui) {
        return result;
    }

    let (directories, metadata) = TestRunnerT::get_directories_and_metadata(&config)?;

    Fs.create_dir_all(&directories.state)?;
    Fs.create_dir_all(&directories.cache)?;

    let log_destination = LogDestination::default();
    let log = logger.build(log_destination.clone());

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

    let list_tests = TestRunnerT::is_list_tests(&extra_options).into();
    let parent_extra_options = TestRunnerT::extra_options_into_parent(extra_options);
    let template_vars = TestRunnerT::get_template_vars(&collector_config, &directories)?;
    let test_collector = TestRunnerT::get_test_collector(&client, &directories, &log, metadata)?;
    run_app_with_ui_multithreaded(
        log_destination,
        parent_config.timeout.map(Timeout::new),
        ui.unwrap(),
        &test_collector,
        parent_extra_options.include,
        parent_extra_options.exclude,
        list_tests,
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
        TestRunnerT::DEFAULT_TEST_METADATA_FILE_CONTENTS,
        template_vars,
    )
}
