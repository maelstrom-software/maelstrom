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

pub use deps::*;

use anyhow::{Context as _, Result};
use app::TestingOptions;
use app::{
    main_app::MainApp, watch::Watcher, ControlMessage, Deps, MainAppDepsAdapter, MainAppMessage,
    MainAppMessageM, TestDbM,
};
use clap::{Args, Command};
use config::IntoParts;
use derive_more::{From, Into};
use log::{LogDestination, LoggerBuilder};
use maelstrom_base::Timeout;
use maelstrom_client::{CacheDir, Client, ClientBgProcess, ProjectDir, StateDir};
use maelstrom_util::{
    config::{common::LogLevel, Config},
    fs::Fs,
    process::ExitCode,
    root::RootBuf,
    sync::Event,
};
use metadata::Store as MetadataStore;
use std::{
    fmt::Debug,
    io::{self, IsTerminal as _},
    path::PathBuf,
    str,
    sync::mpsc::Receiver,
    time::Duration,
};
use std_semaphore::Semaphore;
use test_db::TestDbStore;
use ui::{Ui, UiKind, UiMessage, UiSender};
use util::{IsListing, ListTests, StdoutTty, UseColor};

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
    /// ([`Self::TestCollectorConfig`]).
    type Config: Config
        + Debug
        + AsRef<config::Config>
        + IntoParts<First = config::Config, Second = Self::TestCollectorConfig>;

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
    /// [`Self::get_metadata_and_directories`], and is then used by [`Self::build_test_collector`].
    type Metadata;

    /// The test collector does most of the actual work for the test runner. Currently, it is
    /// provided a reference to the [`Client`]. However, in the future, we want to remove this
    /// direct dependency. When that happens, this type will no longer need to be a GAT.
    type TestCollector<'client>: TestCollector + Sync;

    /// The test-collector-specific configuration values. Used to build the test collector with
    /// [`Self::build_test_collector`].
    type TestCollectorConfig;

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

    /// Return the metadata for the project and the project directory.
    fn get_metadata_and_project_directory(
        config: &Self::Config,
    ) -> Result<(Self::Metadata, RootBuf<ProjectDir>)>;

    /// Return the directories computed from the metadata and project directory.
    fn get_directories(metadata: &Self::Metadata, project_dir: RootBuf<ProjectDir>) -> Directories;

    /// Build the test collector. This will be used for the rest of the execution.
    fn build_test_collector<'client>(
        client: &'client Client,
        config: Self::TestCollectorConfig,
        directories: &Directories,
        log: &slog::Logger,
        metadata: Self::Metadata,
    ) -> Result<Self::TestCollector<'client>>;
}

/// Helper that does common work for test-runner main functions and then forwards on to the given
/// underlying function.
///
/// Mostly it deals with the `--init` and `--client-bg-proc` flags
pub fn main<TestRunnerT: TestRunner>(
    command: Command,
    args: impl IntoIterator<Item = String>,
) -> Result<ExitCode> {
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
        let (_, project_dir) = TestRunnerT::get_metadata_and_project_directory(&config)?;
        return alternative_mains::init(
            &project_dir,
            TestRunnerT::TEST_METADATA_FILE_NAME,
            TestRunnerT::DEFAULT_TEST_METADATA_FILE_CONTENTS,
        );
    }

    main_inner::<TestRunnerT>(
        ClientBgProcess::new_from_fork,
        config,
        extra_options,
        TestRunnerT::get_metadata_and_project_directory,
        LoggerBuilder::DefaultLogger,
        StdoutTty::from(io::stdout().is_terminal()),
        ui::factory,
    )
}

pub fn main_for_test<TestRunnerT: TestRunner>(
    client_bg_process_factory: impl FnOnce(LogLevel) -> Result<ClientBgProcess>,
    config: TestRunnerT::Config,
    extra_options: TestRunnerT::ExtraCommandLineOptions,
    get_metadata_and_project_directory: impl FnOnce(
        &TestRunnerT::Config,
    ) -> Result<(
        TestRunnerT::Metadata,
        RootBuf<ProjectDir>,
    )>,
    logger_builder: LoggerBuilder,
    ui_factory: impl FnOnce(UiKind, IsListing, StdoutTty) -> Result<Box<dyn Ui>>,
) -> Result<ExitCode> {
    main_inner::<TestRunnerT>(
        client_bg_process_factory,
        config,
        extra_options,
        get_metadata_and_project_directory,
        |_| logger_builder,
        StdoutTty::from(false),
        ui_factory,
    )
}

#[allow(clippy::too_many_arguments)]
fn main_inner<TestRunnerT: TestRunner>(
    client_bg_process_factory: impl FnOnce(LogLevel) -> Result<ClientBgProcess>,
    config: TestRunnerT::Config,
    extra_options: TestRunnerT::ExtraCommandLineOptions,
    get_metadata_and_project_directory: impl FnOnce(
        &TestRunnerT::Config,
    ) -> Result<(
        TestRunnerT::Metadata,
        RootBuf<ProjectDir>,
    )>,
    logger_builder_builder: impl FnOnce(LogLevel) -> LoggerBuilder,
    stdout_tty: StdoutTty,
    ui_factory: impl FnOnce(UiKind, IsListing, StdoutTty) -> Result<Box<dyn Ui>>,
) -> Result<ExitCode> {
    let parent_config = config.as_ref();

    // Deal with other test listings.
    let list_tests = match TestRunnerT::get_listing_mode(&extra_options) {
        ListingMode::None => ListTests::from(false),
        ListingMode::Tests => ListTests::from(true),
        ListingMode::OtherWithoutUi => {
            return TestRunnerT::execute_listing_without_ui(&config, &extra_options);
        }
        ListingMode::OtherWithUi => {
            let ui = ui_factory(
                parent_config.ui,
                IsListing::from(true),
                StdoutTty::from(io::stdout().is_terminal()),
            )?;
            let logger_builder = logger_builder_builder(parent_config.log_level);
            let log_destination = LogDestination::default();
            let log = logger_builder.build(log_destination.clone());
            let (ui_handle, ui_sender) = ui.start_ui_thread(log_destination, log);
            let result = TestRunnerT::execute_listing_with_ui(&config, &extra_options, ui_sender);
            ui_handle.join()?;
            return result;
        }
    };

    // From this point on, we're going to be building all tests and either running or listing them.

    let client_bg_process = client_bg_process_factory(parent_config.log_level)?;

    let (metadata, project_dir) = get_metadata_and_project_directory(&config)?;
    let directories = TestRunnerT::get_directories(&metadata, project_dir);
    let (parent_config, test_collector_config) = config.into_parts();
    let (extra_options, _) = extra_options.into_parts();

    Fs.create_dir_all(&directories.state)?;
    Fs.create_dir_all(&directories.cache)?;

    let logger_builder = logger_builder_builder(parent_config.log_level);
    let log_destination = LogDestination::default();
    let log = logger_builder.build(log_destination.clone());

    let ui = ui_factory(
        parent_config.ui,
        IsListing::from(list_tests.as_bool()),
        stdout_tty,
    )?;

    let client = Client::new(
        client_bg_process,
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

    let test_collector = TestRunnerT::build_test_collector(
        &client,
        test_collector_config,
        &directories,
        &log,
        metadata,
    )?;

    let fs = Fs::new();

    let metadata_template_variables = test_collector.get_template_variables()?;
    let metadata_path = directories
        .project
        .join::<()>(TestRunnerT::TEST_METADATA_FILE_NAME);
    let metadata_store = if let Some(contents) = fs.read_to_string_if_exists(&metadata_path)? {
        MetadataStore::load(&contents, &metadata_template_variables)
            .with_context(|| format!("parsing metadata file {}", metadata_path.display()))?
    } else {
        MetadataStore::load(
            TestRunnerT::DEFAULT_TEST_METADATA_FILE_CONTENTS,
            &metadata_template_variables,
        )
        .expect("embedded default test metadata TOML to be valid")
    };

    // TODO: There are a few things wrong with this from an efficiency point of view.
    //
    // First, we're doing everything serially with synchronous RPCs. We could fix that by
    // sending all of the RPCs in parallel and then waiting for them all to complete.
    //
    // Second, we're blocking the rest of startup by doing this here. We could fix that by
    // pushing this logic into the main app and have it not enqueue any jobs until all of the
    // containers have been registered.
    //
    // Third, we're unnecessarily registering all containers that we find in in the
    // configuration file. We could fix that by only registering containers when we need them.
    // We'd block sending jobs to the client until all of the required containers were
    // registered. The downside of this "fix" is that it's probably overkill, and it could hurt
    // performance. If we implemented the fix above in the second point, we'd be registering
    // all of the containers while we were doing other startup. In all likelihood, the number
    // of containers would be small and we'd be done long before we started submitting jobs.
    for (name, spec) in metadata_store.containers() {
        client.add_container(name.clone(), spec.clone())?;
    }

    let test_db_store = TestDbStore::new(fs, &directories.state);

    let filter = test_filter_compile(
        &test_collector,
        &extra_options.include,
        &extra_options.exclude,
    )?;

    let watch_exclude_paths = test_collector
        .get_paths_to_exclude_from_watch()
        .into_iter()
        .chain([
            directories
                .project
                .to_path_buf()
                .join(maelstrom_container::TAG_FILE_NAME),
            directories.project.to_path_buf().join(".git"),
        ])
        .collect();

    let (ui_handle, ui_sender) = ui.start_ui_thread(log_destination, log.clone());
    let result = main_with_ui_thread(
        &test_collector,
        log,
        test_db_store,
        directories.project,
        TestingOptions {
            test_metadata: metadata_store,
            filter,
            timeout_override: parent_config.timeout.map(Timeout::new),
            use_color: UseColor::from(stdout_tty.as_bool()),
            repeat: parent_config.repeat,
            stop_after: parent_config.stop_after,
            list_tests,
        },
        extra_options.watch,
        watch_exclude_paths,
        ui_sender,
        &client,
    );
    ui_handle.join()?;
    result
}

fn test_filter_compile<TestCollectorT: TestCollector>(
    _test_collector: &TestCollectorT,
    include: &[String],
    exclude: &[String],
) -> Result<TestCollectorT::TestFilter> {
    TestCollectorT::TestFilter::compile(include, exclude)
}

const MAX_NUM_BACKGROUND_THREADS: isize = 200;

#[allow(clippy::too_many_arguments)]
fn main_with_ui_thread<TestCollectorT: TestCollector + Sync>(
    test_collector: &TestCollectorT,
    log: slog::Logger,
    test_db_store: TestDbStore<TestCollectorT::ArtifactKey, TestCollectorT::CaseMetadata>,
    project_dir: RootBuf<ProjectDir>,
    options: TestingOptions<TestCollectorT::TestFilter>,
    watch: bool,
    watch_exclude_paths: Vec<PathBuf>,
    ui_sender: UiSender,
    client: &Client,
) -> Result<ExitCode> {
    // This is where the pytest runner builds pip packages.
    test_collector.build_test_layers(options.test_metadata.get_all_images(), &ui_sender)?;

    let sem = Semaphore::new(MAX_NUM_BACKGROUND_THREADS);
    let done = Event::new();
    let files_changed = Event::new();

    std::thread::scope(|scope| {
        let res = (|| -> Result<_> {
            let watcher = Watcher::new(
                scope,
                log.clone(),
                &project_dir,
                &watch_exclude_paths,
                &sem,
                &done,
                &files_changed,
            );
            if watch {
                watcher.watch_for_changes()?;
            }

            loop {
                let exit_code = run_app_once(
                    test_collector,
                    &test_db_store,
                    &options,
                    scope,
                    &sem,
                    ui_sender.clone(),
                    client,
                )?;

                if watch {
                    client.restart()?;

                    ui_sender.send(UiMessage::UpdateEnqueueStatus(
                        "waiting for changes...".into(),
                    ));
                    watcher.wait_for_changes();

                    continue;
                } else {
                    break Ok(exit_code);
                }
            }
        })();

        done.set();
        res
    })
}

fn run_app_once<'scope, 'deps, TestCollectorT: TestCollector + Sync>(
    test_collector: &'deps TestCollectorT,
    test_db_store: &'deps TestDbStore<TestCollectorT::ArtifactKey, TestCollectorT::CaseMetadata>,
    options: &'deps TestingOptions<TestCollectorT::TestFilter>,
    scope: &'scope std::thread::Scope<'scope, 'deps>,
    sem: &'deps Semaphore,
    ui: UiSender,
    client: &'deps Client,
) -> Result<ExitCode> {
    let (main_app_sender, main_app_receiver) = std::sync::mpsc::channel();

    let done = Event::new();
    std::thread::scope(|inner_scope| {
        inner_scope.spawn(|| introspect_loop(&done, client, ui.clone()));

        let res = (|| -> Result<_> {
            let deps = MainAppDepsAdapter::new(
                test_collector,
                scope,
                main_app_sender.clone(),
                ui.clone(),
                sem,
                client,
            );

            main_app_sender.send(MainAppMessage::Start.into()).unwrap();

            let test_db = test_db_store.load()?;
            let app = MainApp::new(&deps, options, test_db);

            let (exit_code, test_db) = main_app_channel_reader(app, &main_app_receiver)?;
            test_db_store.save(test_db)?;

            Ok(exit_code)
        })();

        done.set();
        res
    })
}

/// Grab introspect data from the client process periodically and send it to the UI. Exit when the
/// done event has been set.
fn introspect_loop(done: &Event, client: &maelstrom_client::Client, ui: UiSender) {
    loop {
        let Ok(introspect_resp) = client.introspect() else {
            break;
        };
        ui.send(UiMessage::UpdateIntrospectState(introspect_resp));

        if !done.wait_timeout(Duration::from_millis(500)).timed_out() {
            break;
        }
    }
}

fn main_app_channel_reader<DepsT: Deps>(
    mut app: MainApp<DepsT>,
    main_app_receiver: &Receiver<ControlMessage<MainAppMessageM<DepsT>>>,
) -> Result<(ExitCode, TestDbM<DepsT>)> {
    loop {
        match main_app_receiver.recv()? {
            ControlMessage::Shutdown => {
                let (exit_code, test_db) = app.main_return_value()?;
                break Ok((exit_code, test_db));
            }
            ControlMessage::App { msg } => {
                app.receive_message(msg);
            }
        }
    }
}
