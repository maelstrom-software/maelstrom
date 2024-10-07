mod job_output;
mod main_app;

#[cfg(test)]
mod tests;

use crate::config::{Repeat, StopAfter};
use crate::metadata::AllMetadata;
use crate::test_db::{TestDb, TestDbStore};
use crate::ui::{Ui, UiJobId as JobId, UiMessage};
use crate::*;
use maelstrom_base::Timeout;
use maelstrom_client::{spec::JobSpec, JobStatus, ProjectDir, StateDir};
use maelstrom_util::{fs::Fs, process::ExitCode, root::Root, sync::Event};
use main_app::MainApp;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Mutex;
use std::time::Duration;
use std_semaphore::Semaphore;

type ArtifactM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::Artifact;
type ArtifactKeyM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::ArtifactKey;
type CaseMetadataM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::CaseMetadata;
type CollectOptionsM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::Options;
type PackageM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::Package;
type PackageIdM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::PackageId;
type TestFilterM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::TestFilter;
type TestDbM<DepsT> = TestDb<ArtifactKeyM<DepsT>, CaseMetadataM<DepsT>>;
type TestingOptionsM<DepsT> = TestingOptions<TestFilterM<DepsT>, CollectOptionsM<DepsT>>;

trait Deps {
    type TestCollector: CollectTests;

    fn start_collection(
        &self,
        color: bool,
        options: &CollectOptionsM<Self>,
        packages: Vec<&PackageM<Self>>,
    );
    fn get_packages(&self);
    fn add_job(&self, job_id: JobId, spec: JobSpec);
    fn list_tests(&self, artifact: ArtifactM<Self>);
    fn start_shutdown(&self);
    fn send_ui_msg(&self, msg: UiMessage);
}

/// Immutable information used to control the testing invocation.
struct TestingOptions<TestFilterT, CollectOptionsT> {
    test_metadata: AllMetadata<TestFilterT>,
    filter: TestFilterT,
    collector_options: CollectOptionsT,
    timeout_override: Option<Option<Timeout>>,
    stdout_color: bool,
    repeat: Repeat,
    stop_after: Option<StopAfter>,
    listing: bool,
}

pub struct MainAppCombinedDeps<MainAppDepsT: MainAppDeps> {
    abstract_deps: MainAppDepsT,
    log: slog::Logger,
    test_db_store:
        TestDbStore<super::ArtifactKeyM<MainAppDepsT>, super::CaseMetadataM<MainAppDepsT>>,
    options: TestingOptions<super::TestFilterM<MainAppDepsT>, super::CollectOptionsM<MainAppDepsT>>,
}

impl<MainAppDepsT: MainAppDeps> MainAppCombinedDeps<MainAppDepsT> {
    /// Creates a new `MainAppCombinedDeps`
    ///
    /// `bg_proc`: handle to background client process
    /// `include_filter`: tests which match any of the patterns in this filter are run
    /// `exclude_filter`: tests which match any of the patterns in this filter are not run
    /// `list_action`: if some, tests aren't run, instead tests or other things are listed
    /// `stdout_color`: should terminal color codes be written to `stdout` or not
    /// `project_dir`: the path to the root of the project
    /// `broker_addr`: the network address of the broker which we connect to
    /// `client_driver`: an object which drives the background work of the `Client`
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        abstract_deps: MainAppDepsT,
        include_filter: Vec<String>,
        exclude_filter: Vec<String>,
        list_action: Option<ListAction>,
        repeat: Repeat,
        stop_after: Option<StopAfter>,
        stdout_color: bool,
        project_dir: impl AsRef<Root<ProjectDir>>,
        state_dir: impl AsRef<Root<StateDir>>,
        collector_options: super::CollectOptionsM<MainAppDepsT>,
        log: slog::Logger,
    ) -> Result<Self> {
        let mut test_metadata = AllMetadata::load(
            log.clone(),
            project_dir,
            MainAppDepsT::TEST_METADATA_FILE_NAME,
            MainAppDepsT::DEFAULT_TEST_METADATA_CONTENTS,
        )?;
        let test_db_store = TestDbStore::new(Fs::new(), &state_dir);

        let vars = abstract_deps.get_template_vars(&collector_options)?;
        test_metadata.replace_template_vars(&vars)?;

        let filter = super::TestFilterM::<MainAppDepsT>::compile(&include_filter, &exclude_filter)?;

        Ok(Self {
            abstract_deps,
            log,
            test_db_store,
            options: TestingOptions {
                test_metadata,
                filter,
                collector_options,
                timeout_override: None,
                stdout_color,
                repeat,
                stop_after,
                listing: list_action.is_some(),
            },
        })
    }
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
    Shutdown,
}

type MainAppMessageM<DepsT> =
    MainAppMessage<PackageM<DepsT>, ArtifactM<DepsT>, CaseMetadataM<DepsT>>;

type WaitM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::BuildHandle;

struct MainAppDepsAdapter<'deps, 'scope, MainAppDepsT: MainAppDeps> {
    deps: &'deps MainAppDepsT,
    scope: &'scope std::thread::Scope<'scope, 'deps>,
    main_app_sender: Sender<MainAppMessageM<Self>>,
    ui: UiSender,
    collect_killer: Mutex<Option<KillOnDrop<WaitM<Self>>>>,
    semaphore: &'deps Semaphore,
}

const MAX_NUM_BACKGROUND_THREADS: isize = 200;

impl<'deps, 'scope, MainAppDepsT: MainAppDeps> MainAppDepsAdapter<'deps, 'scope, MainAppDepsT> {
    fn new(
        deps: &'deps MainAppDepsT,
        scope: &'scope std::thread::Scope<'scope, 'deps>,
        main_app_sender: Sender<MainAppMessageM<Self>>,
        ui: UiSender,
        semaphore: &'deps Semaphore,
    ) -> Self {
        Self {
            deps,
            scope,
            main_app_sender,
            ui,
            collect_killer: Mutex::new(None),
            semaphore,
        }
    }
}

impl<'deps, 'scope, MainAppDepsT: MainAppDeps> Deps
    for MainAppDepsAdapter<'deps, 'scope, MainAppDepsT>
{
    type TestCollector = MainAppDepsT::TestCollector;

    fn start_collection(
        &self,
        color: bool,
        options: &CollectOptionsM<Self>,
        packages: Vec<&PackageM<Self>>,
    ) {
        let sem = self.semaphore;
        let sender = self.main_app_sender.clone();
        match self
            .deps
            .test_collector()
            .start(color, options, packages, &self.ui)
        {
            Ok((build_handle, artifact_stream)) => {
                let build_handle = Arc::new(build_handle);
                let killer = KillOnDrop::new(build_handle.clone());
                let existing_killer = self.collect_killer.lock().unwrap().replace(killer);
                assert!(existing_killer.is_none());

                self.scope.spawn(move || {
                    let _guard = sem.access();
                    for artifact in artifact_stream {
                        match artifact {
                            Ok(artifact) => {
                                if sender
                                    .send(MainAppMessage::ArtifactBuilt { artifact })
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(error) => {
                                let _ = sender.send(MainAppMessage::FatalError { error });
                                break;
                            }
                        }
                    }
                    match build_handle.wait() {
                        Ok(wait_status) => {
                            let _ = sender.send(MainAppMessage::CollectionFinished { wait_status });
                        }
                        Err(error) => {
                            let _ = sender.send(MainAppMessage::FatalError { error });
                        }
                    }
                });
            }
            Err(error) => {
                let _ = sender.send(MainAppMessage::FatalError { error });
            }
        }
    }

    fn get_packages(&self) {
        let sem = self.semaphore;
        let deps = self.deps;
        let sender = self.main_app_sender.clone();
        let ui = self.ui.clone();
        self.scope.spawn(move || {
            let _guard = sem.access();
            match deps.test_collector().get_packages(&ui) {
                Ok(packages) => {
                    let _ = sender.send(MainAppMessage::Packages { packages });
                }
                Err(error) => {
                    let _ = sender.send(MainAppMessage::FatalError { error });
                }
            }
        });
    }

    fn add_job(&self, job_id: JobId, spec: JobSpec) {
        let sender = self.main_app_sender.clone();
        let res = self.deps.client().add_job(spec, move |result| {
            let _ = sender.send(MainAppMessage::JobUpdate { job_id, result });
        });
        if let Err(error) = res {
            let _ = self
                .main_app_sender
                .send(MainAppMessage::FatalError { error });
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
                    let _ = sender.send(MainAppMessage::FatalError { error });
                    return;
                }
            };
            let ignored_listing = match artifact.list_ignored_tests() {
                Ok(listing) => listing,
                Err(error) => {
                    let _ = sender.send(MainAppMessage::FatalError { error });
                    return;
                }
            };

            let _ = sender.send(MainAppMessage::TestsListed {
                artifact,
                listing,
                ignored_listing,
            });
        });
    }

    fn start_shutdown(&self) {
        let _ = self.main_app_sender.send(MainAppMessage::Shutdown);
    }

    fn send_ui_msg(&self, msg: UiMessage) {
        self.ui.send(msg);
    }
}

fn main_app_channel_reader<DepsT: Deps>(
    mut app: MainApp<DepsT>,
    main_app_receiver: Receiver<MainAppMessageM<DepsT>>,
) -> Result<(ExitCode, TestDbM<DepsT>)> {
    loop {
        let msg = main_app_receiver.recv()?;
        if matches!(msg, MainAppMessage::Shutdown) {
            break app.main_return_value();
        } else {
            app.receive_message(msg);
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

/// Run the given `[Ui]` implementation on a background thread, and run the main test-runner
/// application on this thread using the UI until it is completed.
pub fn run_app_with_ui_multithreaded<MainAppDepsT>(
    mut deps: MainAppCombinedDeps<MainAppDepsT>,
    logging_output: LoggingOutput,
    timeout_override: Option<Option<Timeout>>,
    ui: impl Ui,
) -> Result<ExitCode>
where
    MainAppDepsT: MainAppDeps,
{
    let (main_app_sender, main_app_receiver) = std::sync::mpsc::channel();
    let (ui_handle, ui) = ui.start_ui_thread(logging_output, deps.log.clone());

    deps.options.timeout_override = timeout_override;
    let abs_deps = &deps.abstract_deps;
    let options = &deps.options;
    let client = abs_deps.client();

    // This is where the pytest runner builds pip packages.
    abs_deps
        .test_collector()
        .build_test_layers(deps.options.test_metadata.get_all_images(), &ui)?;

    let test_db = deps.test_db_store.load()?;

    let done = Event::new();
    let sem = Semaphore::new(MAX_NUM_BACKGROUND_THREADS);

    let main_res = std::thread::scope(|scope| {
        main_app_sender.send(MainAppMessage::Start).unwrap();
        let deps = MainAppDepsAdapter::new(abs_deps, scope, main_app_sender, ui.clone(), &sem);

        scope.spawn(|| introspect_loop(&done, client, ui));

        let app = MainApp::new(&deps, options, test_db);
        let res = main_app_channel_reader(app, main_app_receiver);
        done.set();

        res
    });

    ui_handle.join()?;

    let (exit_code, test_db) = main_res?;
    deps.test_db_store.save(test_db)?;

    Ok(exit_code)
}
