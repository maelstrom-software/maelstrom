mod job_output;
mod main_app;
mod watch;

#[cfg(test)]
mod tests;

use crate::{
    config::{ExtraCommandLineOptions, Repeat, StopAfter},
    deps::{KillOnDrop, TestArtifact as _, TestCollector, TestFilter as _, Wait as _, WaitStatus},
    log::LogDestination,
    metadata::Store as MetadataStore,
    test_db::{TestDb, TestDbStore},
    ui::{Ui, UiJobId as JobId, UiMessage, UiSender},
    util::{ListTests, UseColor},
    Directories,
};
use anyhow::{Context as _, Result};
use maelstrom_base::Timeout;
use maelstrom_client::{spec::JobSpec, Client, JobStatus, ProjectDir};
use maelstrom_util::{fs::Fs, process::ExitCode, root::RootBuf, sync::Event};
use main_app::MainApp;
use std::{
    path::PathBuf,
    sync::{
        mpsc::{Receiver, Sender},
        Arc, Mutex,
    },
    time::Duration,
};
use std_semaphore::Semaphore;
use watch::Watcher;

type ArtifactM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::Artifact;
type ArtifactKeyM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::ArtifactKey;
type CaseMetadataM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::CaseMetadata;
type PackageM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::Package;
type PackageIdM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::PackageId;
type TestFilterM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::TestFilter;
type TestDbM<DepsT> = TestDb<ArtifactKeyM<DepsT>, CaseMetadataM<DepsT>>;
type TestingOptionsM<DepsT> = TestingOptions<TestFilterM<DepsT>>;

trait Deps {
    type TestCollector: TestCollector + Sync;

    fn start_collection(&self, use_color: UseColor, packages: Vec<&PackageM<Self>>);
    fn get_packages(&self);
    fn add_job(&self, job_id: JobId, spec: JobSpec);
    fn list_tests(&self, artifact: ArtifactM<Self>);
    fn start_shutdown(&self);
    fn send_ui_msg(&self, msg: UiMessage);
}

/// Immutable information used to control the testing invocation.
struct TestingOptions<TestFilterT> {
    test_metadata: MetadataStore<TestFilterT>,
    filter: TestFilterT,
    timeout_override: Option<Option<Timeout>>,
    use_color: UseColor,
    repeat: Repeat,
    stop_after: Option<StopAfter>,
    list_tests: ListTests,
}

enum MainAppMessage<PackageT: 'static, ArtifactT: 'static, CaseMetadataT: 'static> {
    Start,
    Packages {
        packages: Vec<PackageT>,
    },
    ArtifactBuilt {
        artifact: ArtifactT,
    },
    TestsListed {
        artifact: ArtifactT,
        listing: Vec<(String, CaseMetadataT)>,
        ignored_listing: Vec<String>,
    },
    FatalError {
        error: anyhow::Error,
    },
    JobUpdate {
        job_id: JobId,
        result: Result<JobStatus>,
    },
    CollectionFinished {
        wait_status: WaitStatus,
    },
}

type MainAppMessageM<DepsT> =
    MainAppMessage<PackageM<DepsT>, ArtifactM<DepsT>, CaseMetadataM<DepsT>>;

enum ControlMessage<MessageT> {
    Shutdown,
    App { msg: MessageT },
}

impl<MessageT> From<MessageT> for ControlMessage<MessageT> {
    fn from(msg: MessageT) -> Self {
        Self::App { msg }
    }
}

type WaitM<DepsT> = <<DepsT as Deps>::TestCollector as TestCollector>::BuildHandle;

struct MainAppDepsAdapter<'deps, 'scope, TestCollectorT: TestCollector + Sync> {
    test_collector: &'deps TestCollectorT,
    scope: &'scope std::thread::Scope<'scope, 'deps>,
    main_app_sender: Sender<ControlMessage<MainAppMessageM<Self>>>,
    ui: UiSender,
    collect_killer: Mutex<Option<KillOnDrop<WaitM<Self>>>>,
    semaphore: &'deps Semaphore,
    client: &'deps Client,
}

const MAX_NUM_BACKGROUND_THREADS: isize = 200;

impl<'deps, 'scope, TestCollectorT: TestCollector + Sync>
    MainAppDepsAdapter<'deps, 'scope, TestCollectorT>
{
    fn new(
        test_collector: &'deps TestCollectorT,
        scope: &'scope std::thread::Scope<'scope, 'deps>,
        main_app_sender: Sender<ControlMessage<MainAppMessageM<Self>>>,
        ui: UiSender,
        semaphore: &'deps Semaphore,
        client: &'deps Client,
    ) -> Self {
        Self {
            test_collector,
            scope,
            main_app_sender,
            ui,
            collect_killer: Mutex::new(None),
            semaphore,
            client,
        }
    }
}

impl<TestCollectorT: TestCollector + Sync> Deps for MainAppDepsAdapter<'_, '_, TestCollectorT> {
    type TestCollector = TestCollectorT;

    fn start_collection(&self, use_color: UseColor, packages: Vec<&PackageM<Self>>) {
        let sem = self.semaphore;
        let sender = self.main_app_sender.clone();
        match self.test_collector.start(use_color, packages, &self.ui) {
            Ok((build_handle, artifact_stream)) => {
                let build_handle = Arc::new(build_handle);
                let killer = KillOnDrop::new(build_handle.clone());
                let _ = self.collect_killer.lock().unwrap().replace(killer);

                self.scope.spawn(move || {
                    let _guard = sem.access();
                    for artifact in artifact_stream {
                        match artifact {
                            Ok(artifact) => {
                                if sender
                                    .send(MainAppMessage::ArtifactBuilt { artifact }.into())
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(error) => {
                                let _ = sender.send(MainAppMessage::FatalError { error }.into());
                                break;
                            }
                        }
                    }
                    match build_handle.wait() {
                        Ok(wait_status) => {
                            let _ = sender
                                .send(MainAppMessage::CollectionFinished { wait_status }.into());
                        }
                        Err(error) => {
                            let _ = sender.send(MainAppMessage::FatalError { error }.into());
                        }
                    }
                });
            }
            Err(error) => {
                let _ = sender.send(MainAppMessage::FatalError { error }.into());
            }
        }
    }

    fn get_packages(&self) {
        let sem = self.semaphore;
        let test_collector = self.test_collector;
        let sender = self.main_app_sender.clone();
        let ui = self.ui.clone();
        self.scope.spawn(move || {
            let _guard = sem.access();
            match test_collector.get_packages(&ui) {
                Ok(packages) => {
                    let _ = sender.send(MainAppMessage::Packages { packages }.into());
                }
                Err(error) => {
                    let _ = sender.send(MainAppMessage::FatalError { error }.into());
                }
            }
        });
    }

    fn add_job(&self, job_id: JobId, spec: JobSpec) {
        let sender = self.main_app_sender.clone();

        let cb_sender = sender.clone();
        let res = self.client.add_job(spec, move |result| {
            let _ = cb_sender.send(MainAppMessage::JobUpdate { job_id, result }.into());
        });
        if let Err(error) = res {
            let _ = sender.send(MainAppMessage::FatalError { error }.into());
        }
    }

    fn list_tests(&self, artifact: ArtifactM<Self>) {
        let sem = self.semaphore;
        let sender = self.main_app_sender.clone();
        self.scope.spawn(move || {
            let _guard = sem.access();
            let listing = match artifact.list_tests() {
                Ok(listing) => listing,
                Err(error) => {
                    let _ = sender.send(MainAppMessage::FatalError { error }.into());
                    return;
                }
            };
            let ignored_listing = match artifact.list_ignored_tests() {
                Ok(listing) => listing,
                Err(error) => {
                    let _ = sender.send(MainAppMessage::FatalError { error }.into());
                    return;
                }
            };

            let _ = sender.send(
                MainAppMessage::TestsListed {
                    artifact,
                    listing,
                    ignored_listing,
                }
                .into(),
            );
        });
    }

    fn start_shutdown(&self) {
        let _ = self.main_app_sender.send(ControlMessage::Shutdown);
    }

    fn send_ui_msg(&self, msg: UiMessage) {
        self.ui.send(msg);
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

#[allow(clippy::too_many_arguments)]
fn run_app_in_loop<TestCollectorT: TestCollector + Sync>(
    test_collector: &TestCollectorT,
    log: slog::Logger,
    test_db_store: TestDbStore<TestCollectorT::ArtifactKey, TestCollectorT::CaseMetadata>,
    project_dir: RootBuf<ProjectDir>,
    options: TestingOptions<TestCollectorT::TestFilter>,
    watch: bool,
    watch_exclude_paths: Vec<PathBuf>,
    ui: UiSender,
    client: &Client,
) -> Result<ExitCode> {
    // This is where the pytest runner builds pip packages.
    test_collector.build_test_layers(options.test_metadata.get_all_images(), &ui)?;

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
                    ui.clone(),
                    client,
                )?;

                if watch {
                    client.restart()?;

                    ui.send(UiMessage::UpdateEnqueueStatus(
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

/// Run the given [`Ui`] implementation on a background thread, and run the main test-runner
/// application on this thread using the UI until it is completed.
#[allow(clippy::too_many_arguments)]
pub fn run_app_with_ui_multithreaded<TestCollectorT: TestCollector + Sync>(
    log_destination: LogDestination,
    timeout_override: Option<Option<Timeout>>,
    ui: impl Ui,
    test_collector: TestCollectorT,
    list_tests: ListTests,
    repeat: Repeat,
    stop_after: Option<StopAfter>,
    use_color: UseColor,
    log: slog::Logger,
    client: &Client,
    test_metadata_file_name: &'static str,
    test_metadata_default_contents: &'static str,
    directories: Directories,
    extra_options: ExtraCommandLineOptions,
) -> Result<ExitCode> {
    let fs = Fs::new();

    let metadata_template_variables = test_collector.get_template_variables()?;
    let metadata_path = directories.project.join::<()>(test_metadata_file_name);
    let metadata_store = if let Some(contents) = fs.read_to_string_if_exists(&metadata_path)? {
        MetadataStore::load(&contents, &metadata_template_variables)
            .with_context(|| format!("parsing metadata file {}", metadata_path.display()))?
    } else {
        MetadataStore::load(test_metadata_default_contents, &metadata_template_variables)
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

    let filter =
        <TestCollectorT::TestFilter>::compile(&extra_options.include, &extra_options.exclude)?;

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

    let (ui_handle, ui) = ui.start_ui_thread(log_destination, log.clone());

    let main_res = run_app_in_loop(
        &test_collector,
        log,
        test_db_store,
        directories.project,
        TestingOptions {
            test_metadata: metadata_store,
            filter,
            timeout_override,
            use_color,
            repeat,
            stop_after,
            list_tests,
        },
        extra_options.watch,
        watch_exclude_paths,
        ui,
        client,
    );
    ui_handle.join()?;

    let exit_code = main_res?;
    Ok(exit_code)
}
