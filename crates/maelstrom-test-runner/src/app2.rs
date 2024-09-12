#[cfg(test)]
mod tests;

use crate::config::{Repeat, StopAfter};
use crate::metadata::{AllMetadata, TestMetadata};
use crate::test_db::{CaseOutcome, TestDb, TestDbStore};
use crate::ui::{Ui, UiJobId as JobId, UiJobResult, UiJobStatus, UiMessage};
use crate::*;
use maelstrom_base::{JobRootOverlay, Timeout};
use maelstrom_client::{
    spec::{JobSpec, LayerSpec},
    ContainerSpec, JobStatus, ProjectDir, StateDir,
};
use maelstrom_util::{ext::OptionExt as _, fs::Fs, process::ExitCode, root::Root};
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc::{Receiver, Sender};

type ArtifactM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::Artifact;
type ArtifactKeyM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::ArtifactKey;
type CaseMetadataM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::CaseMetadata;
type CollectOptionsM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::Options;
type PackageM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::Package;
type PackageIdM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::PackageId;
type TestFilterM<DepsT> = <<DepsT as Deps>::TestCollector as CollectTests>::TestFilter;
type AllMetadataM<DepsT> = AllMetadata<TestFilterM<DepsT>>;
type TestDbM<DepsT> = TestDb<ArtifactKeyM<DepsT>, CaseMetadataM<DepsT>>;

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
    fn get_test_layers(
        &self,
        artifact: &ArtifactM<Self>,
        metadata: &TestMetadata,
    ) -> Vec<LayerSpec>;
    fn send_ui_msg(&self, msg: UiMessage);
}

struct MainApp<'deps, DepsT: Deps> {
    deps: &'deps DepsT,
    packages: BTreeMap<PackageIdM<DepsT>, PackageM<DepsT>>,
    next_job_id: u32,
    test_metadata: &'deps AllMetadataM<DepsT>,
    test_db: TestDbM<DepsT>,
    timeout_override: Option<Option<Timeout>>,
    jobs: HashMap<JobId, String>,
    collection_finished: bool,
    pending_listings: u64,
    collector_options: &'deps CollectOptionsM<DepsT>,
    num_enqueued: u64,
}

impl<'deps, DepsT: Deps> MainApp<'deps, DepsT> {
    fn new(
        deps: &'deps DepsT,
        test_metadata: &'deps AllMetadataM<DepsT>,
        test_db: TestDbM<DepsT>,
        timeout_override: Option<Option<Timeout>>,
        collector_options: &'deps CollectOptionsM<DepsT>,
    ) -> Self {
        Self {
            deps,
            packages: BTreeMap::new(),
            next_job_id: 1,
            test_metadata,
            test_db,
            timeout_override,
            jobs: HashMap::new(),
            collection_finished: false,
            pending_listings: 0,
            collector_options,
            num_enqueued: 0,
        }
    }

    fn start(&mut self) {
        self.deps.get_packages();
    }

    fn main_return_value(&self) -> Result<ExitCode> {
        Ok(ExitCode::SUCCESS)
    }

    fn check_for_done(&mut self) {
        if self.jobs.is_empty() && self.pending_listings == 0 && self.collection_finished {
            self.deps.start_shutdown();
        }
    }

    fn receive_packages(&mut self, packages: Vec<PackageM<DepsT>>) {
        self.packages = packages.into_iter().map(|p| (p.id(), p)).collect();

        let packages: Vec<_> = self.packages.values().collect();

        if !packages.is_empty() {
            let color = false;
            self.deps
                .start_collection(color, self.collector_options, packages);
        } else {
            self.collection_finished = true;
        }

        self.check_for_done();
    }

    fn receive_artifact_built(&mut self, artifact: ArtifactM<DepsT>) {
        self.pending_listings += 1;
        self.deps.list_tests(artifact)
    }

    fn vend_job_id(&mut self) -> JobId {
        let id = JobId::from(self.next_job_id);
        self.next_job_id += 1;
        id
    }

    fn enqueue_test(
        &mut self,
        artifact: &ArtifactM<DepsT>,
        case_name: &str,
        case_metadata: &CaseMetadataM<DepsT>,
    ) {
        let package = self
            .packages
            .get(&artifact.package())
            .expect("artifact for unknown package");

        let test_metadata = self
            .test_metadata
            .get_metadata_for_test_with_env(package, &artifact.to_key(), (case_name, case_metadata))
            .expect("XXX this error isn't real");

        let case_str = artifact.format_case(package.name(), case_name, case_metadata);

        let mut layers = test_metadata.layers.clone();
        layers.extend(self.deps.get_test_layers(artifact, &test_metadata));

        let get_timing_result =
            self.test_db
                .get_case(package.name(), &artifact.to_key(), case_name);
        let (priority, estimated_duration) = match get_timing_result {
            None => (1, None),
            Some((CaseOutcome::Success, duration)) => (0, Some(duration)),
            Some((CaseOutcome::Failure, duration)) => (1, Some(duration)),
        };

        let (program, arguments) = artifact.build_command(case_name, case_metadata);
        let container = ContainerSpec {
            image: test_metadata.image,
            environment: test_metadata.environment,
            layers,
            mounts: test_metadata.mounts,
            network: test_metadata.network,
            root_overlay: if test_metadata.enable_writable_file_system {
                JobRootOverlay::Tmp
            } else {
                JobRootOverlay::None
            },
            working_directory: test_metadata.working_directory,
            user: test_metadata.user,
            group: test_metadata.group,
        }
        .into();
        let spec = JobSpec {
            container,
            program,
            arguments,
            timeout: self.timeout_override.unwrap_or(test_metadata.timeout),
            estimated_duration,
            allocate_tty: None,
            priority,
        };

        let job_id = self.vend_job_id();
        self.deps.add_job(job_id, spec);
        self.jobs.insert(job_id, case_str).assert_is_none();

        self.num_enqueued += 1;
        self.deps
            .send_ui_msg(UiMessage::UpdatePendingJobsCount(self.num_enqueued));
    }

    fn receive_tests_listed(
        &mut self,
        artifact: ArtifactM<DepsT>,
        listing: Vec<(String, CaseMetadataM<DepsT>)>,
    ) {
        self.pending_listings -= 1;
        for (case_name, case_metadata) in &listing {
            self.enqueue_test(&artifact, case_name, case_metadata);
        }

        self.check_for_done();
    }

    fn receive_fatal_error(&mut self, _error: anyhow::Error) {
        todo!()
    }

    fn receive_job_update(&mut self, job_id: JobId, result: Result<JobStatus>) {
        if matches!(result, Err(_) | Ok(JobStatus::Completed { .. })) {
            let name = self.jobs.remove(&job_id).expect("job finishes only once");
            self.deps.send_ui_msg(UiMessage::JobFinished(UiJobResult {
                name,
                job_id,
                duration: None,
                status: UiJobStatus::Ok,
                stdout: vec![],
                stderr: vec![],
            }));
        }

        self.check_for_done();
    }

    fn receive_collection_finished(&mut self) {
        self.collection_finished = true;
        self.deps.send_ui_msg(UiMessage::DoneQueuingJobs);

        self.check_for_done();
    }

    fn receive_message(&mut self, message: MainAppMessageM<DepsT>) {
        match message {
            MainAppMessage::Start => self.start(),
            MainAppMessage::Packages { packages } => self.receive_packages(packages),
            MainAppMessage::ArtifactBuilt { artifact } => self.receive_artifact_built(artifact),
            MainAppMessage::TestsListed { artifact, listing } => {
                self.receive_tests_listed(artifact, listing)
            }
            MainAppMessage::FatalError { error } => self.receive_fatal_error(error),
            MainAppMessage::JobUpdate { job_id, result } => self.receive_job_update(job_id, result),
            MainAppMessage::CollectionFinished => self.receive_collection_finished(),
            MainAppMessage::Shutdown => unimplemented!(),
        }
    }
}

pub struct MainAppCombinedDeps<MainAppDepsT: MainAppDeps> {
    abstract_deps: MainAppDepsT,
    log: slog::Logger,
    test_metadata: AllMetadata<super::TestFilterM<MainAppDepsT>>,
    collector_options: super::CollectOptionsM<MainAppDepsT>,
    test_db_store:
        TestDbStore<super::ArtifactKeyM<MainAppDepsT>, super::CaseMetadataM<MainAppDepsT>>,
}

impl<MainAppDepsT: MainAppDeps> MainAppCombinedDeps<MainAppDepsT> {
    /// Creates a new `MainAppCombinedDeps`
    ///
    /// `bg_proc`: handle to background client process
    /// `include_filter`: tests which match any of the patterns in this filter are run
    /// `exclude_filter`: tests which match any of the patterns in this filter are not run
    /// `list_action`: if some, tests aren't run, instead tests or other things are listed
    /// `stderr_color`: should terminal color codes be written to `stderr` or not
    /// `project_dir`: the path to the root of the project
    /// `broker_addr`: the network address of the broker which we connect to
    /// `client_driver`: an object which drives the background work of the `Client`
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        abstract_deps: MainAppDepsT,
        _include_filter: Vec<String>,
        _exclude_filter: Vec<String>,
        _list_action: Option<ListAction>,
        _repeat: Repeat,
        _stop_after: Option<StopAfter>,
        _stderr_color: bool,
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

        Ok(Self {
            abstract_deps,
            log,
            test_metadata,
            test_db_store,
            collector_options,
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
    },
    FatalError {
        error: anyhow::Error,
    },
    JobUpdate {
        job_id: JobId,
        result: Result<JobStatus>,
    },
    CollectionFinished,
    Shutdown,
}

type MainAppMessageM<DepsT> =
    MainAppMessage<PackageM<DepsT>, ArtifactM<DepsT>, CaseMetadataM<DepsT>>;

struct MainAppDepsAdapter<'deps, 'scope, MainAppDepsT: MainAppDeps> {
    deps: &'deps MainAppDepsT,
    scope: &'scope std::thread::Scope<'scope, 'deps>,
    main_app_sender: Sender<MainAppMessageM<Self>>,
    ui: UiSender,
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
        let sender = self.main_app_sender.clone();
        match self
            .deps
            .test_collector()
            .start(color, options, packages, &self.ui)
        {
            Ok((build_handle, artifact_stream)) => {
                self.scope.spawn(move || {
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
                    if let Err(error) = build_handle.wait() {
                        let _ = sender.send(MainAppMessage::FatalError { error });
                    } else {
                        let _ = sender.send(MainAppMessage::CollectionFinished);
                    }
                });
            }
            Err(error) => {
                let _ = sender.send(MainAppMessage::FatalError { error });
            }
        }
    }

    fn get_packages(&self) {
        let deps = self.deps;
        let sender = self.main_app_sender.clone();
        let ui = self.ui.clone();
        self.scope
            .spawn(move || match deps.test_collector().get_packages(&ui) {
                Ok(packages) => {
                    let _ = sender.send(MainAppMessage::Packages { packages });
                }
                Err(error) => {
                    let _ = sender.send(MainAppMessage::FatalError { error });
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
        let sender = self.main_app_sender.clone();
        self.scope.spawn(move || match artifact.list_tests() {
            Ok(listing) => {
                let _ = sender.send(MainAppMessage::TestsListed { artifact, listing });
            }
            Err(error) => {
                let _ = sender.send(MainAppMessage::FatalError { error });
            }
        });
    }

    fn start_shutdown(&self) {
        let _ = self.main_app_sender.send(MainAppMessage::Shutdown);
    }

    fn get_test_layers(
        &self,
        artifact: &ArtifactM<Self>,
        metadata: &TestMetadata,
    ) -> Vec<LayerSpec> {
        self.deps
            .test_collector()
            .get_test_layers(artifact, metadata, &self.ui)
            .expect("XXX the python pip package creation needs to change")
    }

    fn send_ui_msg(&self, msg: UiMessage) {
        self.ui.send_raw(msg);
    }
}

fn main_app_channel_reader<DepsT: Deps>(
    app: &mut MainApp<DepsT>,
    main_app_receiver: Receiver<MainAppMessageM<DepsT>>,
) -> Result<ExitCode> {
    loop {
        let msg = main_app_receiver.recv()?;
        if matches!(msg, MainAppMessage::Shutdown) {
            break app.main_return_value();
        } else {
            app.receive_message(msg);
        }
    }
}

/// Run the given `[Ui]` implementation on a background thread, and run the main test-runner
/// application on this thread using the UI until it is completed.
pub fn run_app_with_ui_multithreaded<MainAppDepsT>(
    deps: MainAppCombinedDeps<MainAppDepsT>,
    logging_output: LoggingOutput,
    timeout_override: Option<Option<Timeout>>,
    ui: impl Ui,
) -> Result<ExitCode>
where
    MainAppDepsT: MainAppDeps,
{
    let (main_app_sender, main_app_receiver) = std::sync::mpsc::channel();
    let (ui_handle, ui_sender) = ui.start_ui_thread(logging_output, deps.log.clone());

    let test_metadata = &deps.test_metadata;
    let collector_options = &deps.collector_options;
    let test_db = deps.test_db_store.load()?;
    let abs_deps = &deps.abstract_deps;

    let exit_code = std::thread::scope(move |scope| {
        main_app_sender.send(MainAppMessage::Start).unwrap();
        let deps = MainAppDepsAdapter {
            deps: abs_deps,
            scope,
            main_app_sender,
            ui: ui_sender,
        };

        let mut app = MainApp::new(
            &deps,
            test_metadata,
            test_db,
            timeout_override,
            collector_options,
        );
        main_app_channel_reader(&mut app, main_app_receiver)
    })?;

    ui_handle.join()?;

    Ok(exit_code)
}
