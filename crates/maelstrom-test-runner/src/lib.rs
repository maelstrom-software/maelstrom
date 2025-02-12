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
use config::IntoParts;
use derive_more::{From, Into};
use log::{LogDestination, LoggerBuilder};
use maelstrom_base::Timeout;
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
use ui::{Ui, UiSender};
use util::ListTests;

/// The listing mode of the test runner. This determines whether the test runner runs normally, or
/// instead lists tests, binaries, packages, etc.
pub enum ListingMode {
    /// Run normally. Execute specified tests, potentially building them first.
    None,

    /// Instead of running specified tests, list them instead. This may require building the test
    /// executables first. This will always run with the full UI.
    Tests,

    /// Instead of running normally, list something like the selected packages or binaries. Do so
    /// with the full UI. This is used when listing the packages or binaries or whatever may take a
    /// while. This happens, for example, with Golang, as listing all of the packages may cause it
    /// to download and install dependencies.
    OtherWithUi,

    /// Instead of running normally, list something like the selected packages or binaries. Do so
    /// without the full UI. This is used when listing the packages or binaries or whatever is very
    /// fast. This happens, for example, with Rust, as all of the relevant information can be
    /// quickly acquired from `cargo metadata`.
    OtherWithoutUi,
}

/// This is the directory that contains build artifacts. The [`CacheDir`] and [`StateDir`]
/// directories will be descendents of this directory. On the other hand, this directory will
/// always be a descendant of the [`ProjectDir`].
pub struct BuildDir;

/// The relevant directories for the project. It must be the case that `project` is an ancestor of
/// `build`, `build` is an ancestor of `cache`, and `build` is an ancestor of `state`.
#[derive(Clone)]
pub struct Directories {
    pub build: RootBuf<BuildDir>,
    pub cache: RootBuf<CacheDir>,
    pub project: RootBuf<ProjectDir>,
    pub state: RootBuf<StateDir>,
}

/// Top-level trait that defines a test runner.
///
/// This trait is mostly responsible for startup and configuration concerns. It hands most of the
/// heavy lifting off to [`Self::TestCollector`].
pub trait TestRunner {
    /// Configuration values for the test runner. This consists of configuration values shared
    /// between all test runners ([`config::Config`]), and those specific to this test runner
    /// ([`Self::CollectorOptions`]).
    type Config: Config
        + Debug
        + AsRef<config::Config>
        + IntoParts<First = config::Config, Second = Self::CollectorOptions>;

    /// Extra command-line options for the test runner. This consists of extra command-line options
    /// shared between all test runners ([`config::ExtraCommandLineOptions`]), and those specific
    /// to this test runner. The extra command-line options specific to this test runner are used
    /// to control the listing mode.
    type ExtraCommandLineOptions: Args
        + AsRef<config::ExtraCommandLineOptions>
        + IntoParts<First = config::ExtraCommandLineOptions>;

    /// Project metadata specific to the test runner. This is called out as a separate type so that
    /// it can be acquired once, and then used later. In particular, for Cargo, this is the result
    /// of running `cargo metadata`. This is assumed to be required for determining the directories
    /// [`Self::get_metadata_and_directories`], and is then used by [`Self::get_test_collector`].
    type Metadata;

    /// The test collector does most of the actual work for the test runner. Currently, it is
    /// provided a reference to the [`Client`]. However, in the future, we want to remove this
    /// direct dependency. When that happens, this type will no longer need to be a GAT.
    type TestCollector<'client>: CollectTests<Options = Self::CollectorOptions> + Sync;

    /// The test-collector-specific configuration values. Currently, this is a separate type since
    /// [`Self::TestCollector`] is a GAT. This type must be the same as [`CollectTests::Options`]
    /// for all [`Self::TestCollector`]s. When [`Self::TestCollector`] is no longer a GAT, this
    /// type can go away.
    type CollectorOptions;

    /// The prefix used when looking for files defined by the XDG base directories specification.
    /// This is something like `maelstrom/maelstrom-go-test`.
    const BASE_DIRECTORIES_PREFIX: &'static str;

    /// The environment variable prefix used when determining configuration values. This is
    /// something like `MAELSTROM_GO_TEST`.
    const ENVIRONMENT_VARIABLE_PREFIX: &'static str;

    /// The file where the test runner looks for test metadata. This is something like
    /// "maelstrom-go-test.toml".
    const TEST_METADATA_FILE_NAME: &'static str;

    /// When the test metadata file doesn't exist, these are the contents used instead. They can be
    /// persisted with the `--init` command-line option.
    const DEFAULT_TEST_METADATA_FILE_CONTENTS: &'static str;

    /// Determine the listing mode. Based on the potential values that can be returned by this,
    /// it's possible that [`Self::execute_listing_without_ui`] or
    /// [`Self::execute_listing_with_ui`] may need to be implemented.
    fn get_listing_mode(extra_options: &Self::ExtraCommandLineOptions) -> ListingMode;

    /// Execute an "other", with the UI. This must be implemented if [`Self::get_listing_mode`]
    /// can return [`ListingMode::OtherWithUi`].
    fn execute_listing_with_ui(
        _config: &Self::Config,
        _extra_options: &Self::ExtraCommandLineOptions,
        _ui_sender: UiSender,
    ) -> Result<ExitCode> {
        unimplemented!()
    }

    /// Execute an "other", without the UI. This must be implemented if [`Self::get_listing_mode`]
    /// can return [`ListingMode::OtherWithoutUi`].
    fn execute_listing_without_ui(
        _config: &Self::Config,
        _extra_options: &Self::ExtraCommandLineOptions,
    ) -> Result<ExitCode> {
        unimplemented!()
    }

    /// Return the directories and metadata for the project.
    fn get_metadata_and_directories(config: &Self::Config)
        -> Result<(Self::Metadata, Directories)>;

    fn build_test_collector<'client>(
        client: &'client Client,
        directories: &Directories,
        log: &slog::Logger,
        metadata: Self::Metadata,
    ) -> Result<Self::TestCollector<'client>>;

    fn get_watch_exclude_paths(directories: &Directories) -> Vec<PathBuf>;

    fn get_template_vars(
        collector_options: &Self::CollectorOptions,
        directories: &Directories,
    ) -> Result<TemplateVars>;
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
        let (_, directories) = TestRunnerT::get_metadata_and_directories(&config)?;
        return alternative_mains::init(
            &directories.project,
            TestRunnerT::TEST_METADATA_FILE_NAME,
            TestRunnerT::DEFAULT_TEST_METADATA_FILE_CONTENTS,
        );
    }

    let config_parent = config.as_ref();

    let list_tests: ListTests = match TestRunnerT::get_listing_mode(&extra_options) {
        ListingMode::None => false.into(),
        ListingMode::Tests => true.into(),
        ListingMode::OtherWithoutUi => {
            return TestRunnerT::execute_listing_without_ui(&config, &extra_options);
        }
        ListingMode::OtherWithUi => {
            let ui = ui::factory(config_parent.ui, true, io::stdout().is_terminal())?;
            let log_destination = LogDestination::default();
            let log_builder = LoggerBuilder::DefaultLogger(config_parent.log_level);
            let log = log_builder.build(log_destination.clone());
            let (ui_handle, ui_sender) = ui.start_ui_thread(log_destination, log);
            let result = TestRunnerT::execute_listing_with_ui(&config, &extra_options, ui_sender);
            ui_handle.join()?;
            return result;
        }
    };

    let bg_proc = ClientBgProcess::new_from_fork(config_parent.log_level)?;

    let stdout_is_tty = io::stdout().is_terminal();
    let ui = ui::factory(config_parent.ui, list_tests.as_bool(), stdout_is_tty)?;

    let (metadata, directories) = TestRunnerT::get_metadata_and_directories(&config)?;

    Fs.create_dir_all(&directories.state)?;
    Fs.create_dir_all(&directories.cache)?;

    let log_builder = LoggerBuilder::DefaultLogger(config_parent.log_level);
    let log_destination = LogDestination::default();
    let log = log_builder.build(log_destination.clone());

    let (parent_config, collector_config) = config.into_parts();
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

    let (extra_options, _) = extra_options.into_parts();
    let template_vars = TestRunnerT::get_template_vars(&collector_config, &directories)?;
    let test_collector = TestRunnerT::build_test_collector(&client, &directories, &log, metadata)?;
    run_app_with_ui_multithreaded(
        log_destination,
        parent_config.timeout.map(Timeout::new),
        ui,
        &test_collector,
        extra_options.include,
        extra_options.exclude,
        list_tests,
        parent_config.repeat,
        parent_config.stop_after,
        extra_options.watch,
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
