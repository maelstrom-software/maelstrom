mod alternative_mains;
pub mod config;
mod deps;
mod introspect_driver;
pub mod metadata;
pub mod test_db;
pub mod ui;
pub mod visitor;

#[cfg(test)]
mod tests;

pub use deps::*;

use anyhow::Result;
use clap::{Args, Command};
use config::{Repeat, StopAfter};
use derive_more::From;
use introspect_driver::{DefaultIntrospectDriver, IntrospectDriver};
use maelstrom_base::{JobRootOverlay, Timeout, Utf8PathBuf};
use maelstrom_client::{
    spec::{ContainerSpec, JobSpec},
    ClientBgProcess, ProjectDir, StateDir,
};
use maelstrom_util::{
    config::common::LogLevel, config::Config, fs::Fs, process::ExitCode, root::Root,
};
use metadata::AllMetadata;
use slog::Drain as _;
use std::{
    collections::{BTreeMap, HashSet},
    ffi::OsString,
    fmt::{self, Debug},
    io::{self, IsTerminal as _},
    mem, str,
    sync::{Arc, Mutex},
};
use test_db::{CaseOutcome, TestDbStore};
use ui::{Ui, UiJobEnqueued, UiJobId, UiSender, UiSlogDrain};
use visitor::{JobStatusTracker, JobStatusVisitor};

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

/// A collection of dependencies that are used while enqueuing jobs.
struct JobQueuingDeps<TestCollectorT: CollectTests> {
    filter: TestCollectorT::TestFilter,
    stderr_color: bool,
    test_metadata: AllMetadata<TestCollectorT::TestFilter>,
    test_db: Arc<Mutex<Option<TestDb<TestCollectorT>>>>,
    list_action: Option<ListAction>,
    repeat: Repeat,
    stop_after: Option<StopAfter>,
    collector_options: TestCollectorT::Options,
}

impl<TestCollectorT: CollectTests> JobQueuingDeps<TestCollectorT> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        filter: TestCollectorT::TestFilter,
        stderr_color: bool,
        test_metadata: AllMetadata<TestCollectorT::TestFilter>,
        test_db: TestDb<TestCollectorT>,
        list_action: Option<ListAction>,
        repeat: Repeat,
        stop_after: Option<StopAfter>,
        collector_options: TestCollectorT::Options,
    ) -> Result<Self> {
        Ok(Self {
            filter,
            stderr_color,
            test_metadata,
            test_db: Arc::new(Mutex::new(Some(test_db))),
            list_action,
            repeat,
            stop_after,
            collector_options,
        })
    }
}

/// This is state about the jobs being enqueued used the MainApp.
struct JobQueuingState<'a, TestCollectorT: CollectTests> {
    deps: &'a JobQueuingDeps<TestCollectorT>,
    tracker: Arc<JobStatusTracker>,
    jobs_queued: u64,
    expected_job_count: u64,
    all_jobs_queued: bool,
    next_ui_job_id: u32,
}

impl<'a, TestCollectorT: CollectTests> JobQueuingState<'a, TestCollectorT> {
    fn new(deps: &'a JobQueuingDeps<TestCollectorT>, expected_job_count: u64) -> Self {
        Self {
            deps,
            tracker: Arc::new(JobStatusTracker::new(deps.stop_after)),
            jobs_queued: 0,
            expected_job_count,
            all_jobs_queued: false,
            next_ui_job_id: 1,
        }
    }

    fn next_ui_job_id(&mut self) -> UiJobId {
        let next_id = self.next_ui_job_id;
        self.next_ui_job_id += 1;
        next_id.into()
    }

    fn track_outstanding(&mut self, case_str: &str, ui: &UiSender) -> UiJobId {
        self.jobs_queued += 1;
        ui.update_length(std::cmp::max(self.expected_job_count, self.jobs_queued));
        let ui_job_id = self.next_ui_job_id();
        ui.job_enqueued(UiJobEnqueued {
            job_id: ui_job_id,
            name: case_str.into(),
        });
        self.tracker.add_outstanding();
        ui_job_id
    }

    fn not_run_estimate(&self) -> NotRunEstimate {
        let completed = self.tracker.completed();

        if self.all_jobs_queued {
            // If we queued everything, we know exactly how many tests didn't run
            NotRunEstimate::Exactly(self.jobs_queued - completed)
        } else if self.expected_job_count >= self.jobs_queued {
            // If our expectation looks okay, then lets trust it
            NotRunEstimate::About(self.expected_job_count - completed)
        } else if self.jobs_queued > 0 && self.jobs_queued > completed {
            // Otherwise, if we have any jobs we queued but didn't complete, we can say it is at
            // least that much
            NotRunEstimate::GreaterThan(self.jobs_queued - completed)
        } else {
            NotRunEstimate::Unknown
        }
    }
}

/// Collects test cases for an artifact as jobs to be run on the client.
///
/// This object is like an iterator, it maintains a position in the test listing and returns the
/// next thing when asked.
///
/// This object is stored inside `JobQueuing` and is used to keep track of which artifact it is
/// currently enqueuing from.
struct ArtifactQueuing<'a, MainAppDepsT: MainAppDeps> {
    log: slog::Logger,
    queuing_deps: &'a JobQueuingDeps<MainAppDepsT::TestCollector>,
    deps: &'a MainAppDepsT,
    ui: UiSender,
    artifact: ArtifactM<MainAppDepsT>,
    ignored_cases: HashSet<String>,
    package: PackageM<MainAppDepsT>,
    cases: CaseIter<CaseMetadataM<MainAppDepsT>>,
    timeout_override: Option<Option<Timeout>>,
}

#[derive(Default)]
struct TestListingResult<CaseMetadataT> {
    cases: Vec<(String, CaseMetadataT)>,
    ignored_cases: HashSet<String>,
}

fn list_test_cases<TestCollectorT: CollectTests>(
    log: slog::Logger,
    queuing_deps: &JobQueuingDeps<TestCollectorT>,
    ui: &UiSender,
    artifact: &TestCollectorT::Artifact,
    package: &TestCollectorT::Package,
) -> Result<TestListingResult<TestCollectorT::CaseMetadata>> {
    ui.update_enqueue_status(format!("getting test list for {}", package.name()));

    slog::debug!(log, "listing ignored tests"; "artifact" => ?artifact);
    let ignored_cases: HashSet<_> = artifact.list_ignored_tests()?.into_iter().collect();

    slog::debug!(log, "listing tests"; "artifact" => ?artifact);
    let mut cases = artifact.list_tests()?;

    let artifact_key = artifact.to_key();
    let mut test_db = queuing_deps.test_db.lock().unwrap();
    test_db.as_mut().unwrap().update_artifact_cases(
        package.name(),
        artifact_key.clone(),
        cases.clone(),
    );

    cases.retain(|(c, cd)| {
        queuing_deps
            .filter
            .filter(package, Some(&artifact_key), Some((c.as_str(), cd)))
            .expect("should have case")
    });
    Ok(TestListingResult {
        cases,
        ignored_cases,
    })
}

impl<'a, MainAppDepsT> ArtifactQueuing<'a, MainAppDepsT>
where
    MainAppDepsT: MainAppDeps,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        log: slog::Logger,
        queuing_deps: &'a JobQueuingDeps<MainAppDepsT::TestCollector>,
        deps: &'a MainAppDepsT,
        ui: UiSender,
        artifact: ArtifactM<MainAppDepsT>,
        package: PackageM<MainAppDepsT>,
        timeout_override: Option<Option<Timeout>>,
    ) -> Result<Self> {
        let listing = list_test_cases(log.clone(), queuing_deps, &ui, &artifact, &package)?;

        ui.update_enqueue_status(format!("generating artifacts for {}", package.name()));
        slog::debug!(
            log,
            "generating artifacts";
            "package_name" => package.name(),
            "artifact" => ?artifact);

        Ok(Self {
            log,
            queuing_deps,
            deps,
            ui,
            artifact,
            ignored_cases: listing.ignored_cases,
            package,
            cases: listing.cases.into_iter(),
            timeout_override,
        })
    }

    fn build_job_from_case(
        &mut self,
        queuing_state: &mut JobQueuingState<'a, MainAppDepsT::TestCollector>,
        case_name: &str,
        case_metadata: &CaseMetadataM<MainAppDepsT>,
    ) -> Result<CollectionResult<MainAppDepsT>> {
        let case_str = self
            .artifact
            .format_case(self.package.name(), case_name, case_metadata);
        self.ui
            .update_enqueue_status(format!("processing {case_str}"));
        slog::debug!(self.log, "enqueuing test case"; "case" => &case_str);

        if self.queuing_deps.list_action.is_some() {
            self.ui.list(case_str);
            return Ok(NotCollected::Listed.into());
        }

        let test_metadata = self
            .queuing_deps
            .test_metadata
            .get_metadata_for_test_with_env(
                &self.package,
                &self.artifact.to_key(),
                (case_name, case_metadata),
            )?;

        let visitor = JobStatusVisitor::new(
            queuing_state.tracker.clone(),
            self.queuing_deps.test_db.clone(),
            self.package.name().into(),
            self.artifact.to_key(),
            case_name.to_owned(),
            case_str.clone(),
            self.ui.downgrade(),
            MainAppDepsT::TestCollector::remove_fixture_output
                as fn(&str, Vec<String>) -> Vec<String>,
            MainAppDepsT::TestCollector::was_test_ignored as fn(&str, &[String]) -> bool,
        );

        if self.ignored_cases.contains(case_name) || test_metadata.ignore {
            let ui_job_id = queuing_state.track_outstanding(&case_str, &self.ui);
            visitor.job_ignored(ui_job_id);
            return Ok(NotCollected::Ignored.into());
        }

        self.ui
            .update_enqueue_status(format!("calculating layers for {case_str}"));
        slog::debug!(&self.log, "calculating job layers"; "case" => &case_str);
        let mut layers = test_metadata.layers.clone();
        layers.extend(self.deps.test_collector().get_test_layers(
            &self.artifact,
            &test_metadata,
            &self.ui,
        )?);

        let get_timing_result = self
            .queuing_deps
            .test_db
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .get_timing(self.package.name(), &self.artifact.to_key(), case_name);
        let (priority, estimated_duration) = match get_timing_result {
            None => (1, None),
            Some((CaseOutcome::Success, duration)) => (0, Some(duration)),
            Some((CaseOutcome::Failure, duration)) => (1, Some(duration)),
        };

        let (program, arguments) = self.artifact.build_command(case_name, case_metadata);
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
        Ok(TestToEnqueue {
            package_name: self.package.name().into(),
            case: case_name.into(),
            case_str,
            spec: JobSpec {
                container,
                program,
                arguments,
                timeout: self.timeout_override.unwrap_or(test_metadata.timeout),
                estimated_duration,
                allocate_tty: None,
                priority,
            },
            visitor,
        }
        .into())
    }

    /// Attempt to collect the next test as a job to run on the client.
    ///
    /// Returns an [`CollectionResult`] describing what happened.
    ///
    /// When a test is successfully collected it returns `CollectionResult::Collected(...)`
    ///
    /// Meant to be called until it returns `CollectionResult::NotCollected(NotCollected::Done)`
    fn collect_one(
        &mut self,
        queuing_state: &mut JobQueuingState<'a, MainAppDepsT::TestCollector>,
    ) -> Result<CollectionResult<MainAppDepsT>> {
        let Some((case_name, case_metadata)) = self.cases.next() else {
            return Ok(NotCollected::Done.into());
        };
        self.build_job_from_case(queuing_state, &case_name, &case_metadata)
    }
}

/// Collects test cases as jobs to be run on the client.
///
/// This object is like an iterator, it maintains a position in the test listing and returns the
/// next thing when asked.
struct JobQueuing<'a, MainAppDepsT: MainAppDeps> {
    log: slog::Logger,
    state: JobQueuingState<'a, MainAppDepsT::TestCollector>,
    deps: &'a MainAppDepsT,
    ui: UiSender,
    wait_handle: Option<BuildHandleM<MainAppDepsT>>,
    packages: BTreeMap<PackageIdM<MainAppDepsT>, PackageM<MainAppDepsT>>,
    package_match: bool,
    artifacts: Option<ArtifactStreamM<MainAppDepsT>>,
    artifact_queuing: Option<ArtifactQueuing<'a, MainAppDepsT>>,
    timeout_override: Option<Option<Timeout>>,
}

impl<'a, MainAppDepsT> JobQueuing<'a, MainAppDepsT>
where
    MainAppDepsT: MainAppDeps,
{
    fn new(
        log: slog::Logger,
        queuing_deps: &'a JobQueuingDeps<MainAppDepsT::TestCollector>,
        deps: &'a MainAppDepsT,
        ui: UiSender,
        timeout_override: Option<Option<Timeout>>,
    ) -> Result<Self> {
        ui.update_enqueue_status(MainAppDepsT::TestCollector::ENQUEUE_MESSAGE);

        let packages = deps.test_collector().get_packages(&ui)?;

        let mut locked_test_db = queuing_deps.test_db.lock().unwrap();
        let test_db = locked_test_db.as_mut().unwrap();
        test_db.retain_packages_and_artifacts(packages.iter().map(|p| (p.name(), p.artifacts())));

        let package_map: BTreeMap<_, _> = packages
            .iter()
            .map(|p| (p.name().into(), p.clone()))
            .collect();

        let mut expected_job_count = test_db.expected_job_count(&package_map, &queuing_deps.filter);
        drop(locked_test_db);

        expected_job_count *= usize::from(queuing_deps.repeat) as u64;
        ui.update_length(expected_job_count);

        let selected_packages: BTreeMap<_, _> = packages
            .iter()
            .filter(|p| queuing_deps.filter.filter(p, None, None).unwrap_or(true))
            .map(|p| (p.id(), p.clone()))
            .collect();

        slog::debug!(
            &log, "filtered packages";
            "selected_packages" => ?Vec::from_iter(selected_packages.keys()),
        );

        let building_tests = !selected_packages.is_empty()
            && matches!(queuing_deps.list_action, None | Some(ListAction::ListTests));

        let (wait_handle, artifacts) = building_tests
            .then(|| {
                deps.test_collector().start(
                    queuing_deps.stderr_color,
                    &queuing_deps.collector_options,
                    selected_packages.values().collect(),
                    &ui,
                )
            })
            .transpose()?
            .unzip();

        Ok(Self {
            log,
            state: JobQueuingState::new(queuing_deps, expected_job_count),
            deps,
            ui,
            packages: selected_packages,
            package_match: false,
            artifacts,
            artifact_queuing: None,
            wait_handle,
            timeout_override,
        })
    }

    fn start_queuing_from_artifact(&mut self) -> Result<bool> {
        self.ui
            .update_enqueue_status(MainAppDepsT::TestCollector::ENQUEUE_MESSAGE);

        slog::debug!(self.log, "getting artifacts");
        let Some(ref mut artifacts) = self.artifacts else {
            return Ok(false);
        };
        let Some(artifact) = artifacts.next() else {
            return Ok(false);
        };
        let artifact = artifact?;

        slog::debug!(self.log, "got artifact"; "artifact" => ?artifact);
        let package = self
            .packages
            .get(&artifact.package())
            .expect("artifact for unknown package");

        self.artifact_queuing = Some(ArtifactQueuing::new(
            self.log.clone(),
            self.state.deps,
            self.deps,
            self.ui.clone(),
            artifact,
            package.clone(),
            self.timeout_override,
        )?);

        Ok(true)
    }

    /// Meant to be called when the user has enqueued all the jobs they want. Checks for deferred
    /// errors from collecting tests or otherwise
    fn finish(&mut self) -> Result<()> {
        slog::debug!(self.log, "checking for collection errors");
        if let Some(wh) = self.wait_handle.take() {
            wh.wait()?;
        }
        Ok(())
    }

    /// Attempt to collect the next test as a job to run on the client.
    ///
    /// Returns an [`CollectionResult`] describing what happened.
    ///
    /// When a test is successfully collected it returns `CollectionResult::Collected(...)`
    ///
    /// Meant to be called until it returns `CollectionResult::NotCollected(NotCollected::Done)`
    fn collect_one(&mut self) -> Result<CollectionResult<MainAppDepsT>> {
        slog::debug!(self.log, "enqueuing a job");

        if self.artifact_queuing.is_none() && !self.start_queuing_from_artifact()? {
            self.finish()?;
            return Ok(NotCollected::Done.into());
        }
        self.package_match = true;

        let res = self
            .artifact_queuing
            .as_mut()
            .unwrap()
            .collect_one(&mut self.state)?;
        if res.is_done() {
            self.artifact_queuing = None;
            return self.collect_one();
        }

        Ok(res)
    }
}

/// This is where cached data goes. If there is build output it is also here.
pub struct BuildDir;

/// A collection of objects that are used to run the MainApp. This is useful as a separate object
/// since it can contain things which live longer than scoped threads and thus shared among them.
pub struct MainAppCombinedDeps<MainAppDepsT: MainAppDeps> {
    abstract_deps: MainAppDepsT,
    queuing_deps: JobQueuingDeps<MainAppDepsT::TestCollector>,
    test_db_store: TestDbStore<ArtifactKeyM<MainAppDepsT>, CaseMetadataM<MainAppDepsT>>,
    log: slog::Logger,
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
        include_filter: Vec<String>,
        exclude_filter: Vec<String>,
        list_action: Option<ListAction>,
        repeat: Repeat,
        stop_after: Option<StopAfter>,
        stderr_color: bool,
        project_dir: impl AsRef<Root<ProjectDir>>,
        state_dir: impl AsRef<Root<StateDir>>,
        collector_options: CollectOptionsM<MainAppDepsT>,
        log: slog::Logger,
    ) -> Result<Self> {
        slog::debug!(
            log, "creating app state";
            "include_filter" => ?include_filter,
            "exclude_filter" => ?exclude_filter,
            "list_action" => ?list_action,
            "repeat" => ?repeat,
            "stop_after" => ?stop_after
        );

        let mut test_metadata = AllMetadata::load(
            log.clone(),
            project_dir,
            MainAppDepsT::TEST_METADATA_FILE_NAME,
            MainAppDepsT::DEFAULT_TEST_METADATA_CONTENTS,
        )?;
        let test_db_store = TestDbStore::new(Fs::new(), &state_dir);
        let test_db = test_db_store.load()?;
        let filter = TestFilterM::<MainAppDepsT>::compile(&include_filter, &exclude_filter)?;

        let vars = abstract_deps.get_template_vars(&collector_options)?;
        test_metadata.replace_template_vars(&vars)?;

        Ok(Self {
            abstract_deps,
            queuing_deps: JobQueuingDeps::new(
                filter,
                stderr_color,
                test_metadata,
                test_db,
                list_action,
                repeat,
                stop_after,
                collector_options,
            )?,
            test_db_store,
            log,
        })
    }
}

/// The `MainApp` enqueues tests as jobs. With each attempted job enqueued, if we for some reason
/// didn't enqueue the test we get this status instead.
pub enum NotCollected {
    /// No job was enqueued, instead the test that would have been enqueued has been ignored
    /// because it has been marked as such.
    Ignored,
    /// No job was enqueued, we have run out of tests to run.
    Done,
    /// No job was enqueued, we listed the test case instead.
    Listed,
}

/// The `MainApp` enqueues tests as jobs. With each attempted job enqueued this object is returned
/// and describes what happened.
#[derive(From)]
pub enum EnqueueResult {
    /// A job successfully enqueued with the following information.
    Enqueued { package_name: String, case: String },
    /// A job was not enqueued, instead something else happened.
    NotEnqueued(NotCollected),
}

impl EnqueueResult {
    /// Is this `EnqueueResult` the `Done` variant
    pub fn is_done(&self) -> bool {
        matches!(self, Self::NotEnqueued(NotCollected::Done))
    }

    /// Is this `EnqueueResult` the `Ignored` variant
    pub fn is_ignored(&self) -> bool {
        matches!(self, Self::NotEnqueued(NotCollected::Ignored))
    }
}

struct TestToEnqueue<MainAppDepsT: MainAppDeps> {
    /// The name of the package containing this test
    package_name: String,
    /// The name of the case containing this test
    case: String,
    /// This is a kind-of FQDN for the test which we can display to the user
    case_str: String,
    /// The spec to submit to the client
    spec: JobSpec,
    /// This is used with the callback to give to the client
    visitor: JobStatusVisitor<ArtifactKeyM<MainAppDepsT>, CaseMetadataM<MainAppDepsT>>,
}

impl<MainAppDepsT: MainAppDeps> Clone for TestToEnqueue<MainAppDepsT> {
    fn clone(&self) -> Self {
        Self {
            package_name: self.package_name.clone(),
            case: self.case.clone(),
            case_str: self.case_str.clone(),
            spec: self.spec.clone(),
            visitor: self.visitor.clone(),
        }
    }
}

impl<MainAppDepsT: MainAppDeps> TestToEnqueue<MainAppDepsT> {
    fn enqueue(
        self,
        state: &MainAppCombinedDeps<MainAppDepsT>,
        queuing_state: &mut JobQueuingState<'_, MainAppDepsT::TestCollector>,
        ui: &UiSender,
    ) -> Result<EnqueueResult> {
        let case_str = self.case_str;
        let ui_job_id = queuing_state.track_outstanding(&case_str, ui);
        ui.update_enqueue_status(format!("submitting job for {case_str}"));
        slog::debug!(&state.log, "submitting job"; "case" => &case_str);
        let cb = move |res| self.visitor.job_update(ui_job_id, res);
        state.abstract_deps.client().add_job(self.spec, cb)?;
        Ok(EnqueueResult::Enqueued {
            package_name: self.package_name,
            case: self.case,
        })
    }
}

/// Returned internally when we attempt to collect the next test job to run. If we get a test then
/// [`TestToEnqueue`] is returned with a test that can be run. Otherwise we get back a
/// [`NotCollected`] explaining what happened.
#[derive(From)]
enum CollectionResult<MainAppDepsT: MainAppDeps> {
    Collected(TestToEnqueue<MainAppDepsT>),
    NotCollected(NotCollected),
}

impl<MainAppDepsT: MainAppDeps> CollectionResult<MainAppDepsT> {
    /// Does this `CollectionResult` contain [`NotCollected::Done`].
    pub fn is_done(&self) -> bool {
        matches!(self, Self::NotCollected(NotCollected::Done))
    }
}

#[derive(Default)]
enum EnqueueStage<MainAppDepsT: MainAppDeps> {
    /// We are ready to collect the next test.
    #[default]
    NeedTest,
    /// We are in the middle of repeating a test.
    Repeating {
        test: TestToEnqueue<MainAppDepsT>,
        times: usize,
    },
    /// We are all done.
    Done,
}

struct MainApp<'deps, IntrospectDriverT, MainAppDepsT: MainAppDeps> {
    deps: &'deps MainAppCombinedDeps<MainAppDepsT>,
    queuing: JobQueuing<'deps, MainAppDepsT>,
    introspect_driver: IntrospectDriverT,
    ui: UiSender,
    stage: EnqueueStage<MainAppDepsT>,
}

impl<'deps, 'scope, IntrospectDriverT, MainAppDepsT> MainApp<'deps, IntrospectDriverT, MainAppDepsT>
where
    IntrospectDriverT: IntrospectDriver<'scope>,
    MainAppDepsT: MainAppDeps,
{
    pub fn new(
        deps: &'deps MainAppCombinedDeps<MainAppDepsT>,
        ui: UiSender,
        mut introspect_driver: IntrospectDriverT,
        timeout_override: Option<Option<Timeout>>,
    ) -> Result<Self>
    where
        'deps: 'scope,
    {
        introspect_driver.drive(deps.abstract_deps.client(), ui.clone());

        slog::debug!(deps.log, "main app created");

        let queuing = JobQueuing::new(
            deps.log.clone(),
            &deps.queuing_deps,
            &deps.abstract_deps,
            ui.clone(),
            timeout_override,
        )?;
        Ok(Self {
            deps,
            queuing,
            introspect_driver,
            ui,
            stage: EnqueueStage::NeedTest,
        })
    }

    /// Enqueue one test as a job on the `Client`. This is meant to be called repeatedly until
    /// `EnqueueResult::Done` is returned, or an error is encountered.
    pub fn enqueue_one(&mut self) -> Result<EnqueueResult> {
        if self.queuing.state.tracker.is_failure_limit_reached() {
            return Ok(NotCollected::Done.into());
        }

        let repeat_times = usize::from(self.deps.queuing_deps.repeat);
        match mem::take(&mut self.stage) {
            EnqueueStage::NeedTest => match self.queuing.collect_one()? {
                CollectionResult::Collected(test) => {
                    if repeat_times > 1 {
                        self.stage = EnqueueStage::Repeating {
                            times: repeat_times - 1,
                            test: test.clone(),
                        };
                    }
                    test.enqueue(self.deps, &mut self.queuing.state, &self.ui)
                }
                CollectionResult::NotCollected(NotCollected::Done) => {
                    self.stage = EnqueueStage::Done;
                    self.queuing.state.all_jobs_queued = true;
                    Ok(NotCollected::Done.into())
                }
                CollectionResult::NotCollected(res) => Ok(res.into()),
            },
            EnqueueStage::Repeating { test, times } => {
                if times == 1 {
                    self.stage = EnqueueStage::NeedTest;
                } else {
                    self.stage = EnqueueStage::Repeating {
                        test: test.clone(),
                        times: times - 1,
                    };
                }
                test.enqueue(self.deps, &mut self.queuing.state, &self.ui)
            }
            EnqueueStage::Done => Ok(NotCollected::Done.into()),
        }
    }

    /// Indicates that we have finished enqueuing jobs.
    fn done_queuing(&mut self) -> Result<()> {
        slog::debug!(self.queuing.log, "draining");
        self.ui.update_length(self.queuing.state.jobs_queued);
        self.ui.done_queuing_jobs();
        Ok(())
    }

    /// Waits for all outstanding jobs to finish.
    fn wait_for_tests(&mut self) -> Result<()> {
        slog::debug!(self.queuing.log, "waiting for outstanding jobs");
        self.queuing
            .state
            .tracker
            .wait_for_outstanding_or_failure_limit_reached();
        self.introspect_driver.stop()?;
        Ok(())
    }

    /// Displays a summary, and obtains an `ExitCode`
    fn finish(self) -> Result<ExitCode> {
        let nre = self.queuing.state.not_run_estimate();
        let summary = self.queuing.state.tracker.ui_summary(nre);
        self.ui.finished(summary);

        self.deps.test_db_store.save(
            self.deps
                .queuing_deps
                .test_db
                .lock()
                .unwrap()
                .take()
                .unwrap(),
        )?;

        Ok(self.queuing.state.tracker.exit_code())
    }
}

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
    let (ui_handle, ui_sender) = ui.start_ui_thread(logging_output, deps.log.clone());

    let exit_code_res = std::thread::scope(|scope| {
        let mut app = MainApp::new(
            &deps,
            ui_sender,
            DefaultIntrospectDriver::new(scope),
            timeout_override,
        )?;
        while !app.enqueue_one()?.is_done() {}
        app.done_queuing()?;
        app.wait_for_tests()?;
        app.finish()
    });
    let log = deps.log.clone();
    drop(deps);
    slog::debug!(log, "MainAppCombinedDeps destroyed");

    let ui_res = ui_handle.join();
    let exit_code = exit_code_res?;
    ui_res?;

    Ok(exit_code)
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
