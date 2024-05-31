pub mod artifacts;
pub mod config;
mod deps;
pub mod metadata;
pub mod progress;
pub mod test_listing;
pub mod visitor;

#[cfg(test)]
mod tests;

use anyhow::Result;
use artifacts::GeneratedArtifacts;
use config::Quiet;
pub use deps::*;
use indicatif::TermLike;
use maelstrom_base::{ArtifactType, JobRootOverlay, Sha256Digest, Timeout};
use maelstrom_client::{spec::JobSpec, ProjectDir, StateDir};
use maelstrom_util::{config::common::LogLevel, fs::Fs, process::ExitCode, root::Root};
use metadata::{AllMetadata, TestMetadata};
use progress::{
    MultipleProgressBars, NoBar, ProgressDriver, ProgressIndicator, ProgressPrinter as _,
    QuietNoBar, QuietProgressBar, TestListingProgress, TestListingProgressNoSpinner,
};
use slog::Drain as _;
use std::{
    collections::{BTreeMap, HashSet},
    io,
    panic::{RefUnwindSafe, UnwindSafe},
    str,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use test_listing::{TestListing, TestListingStore};
use visitor::{JobStatusTracker, JobStatusVisitor};

#[derive(Debug)]
pub enum ListAction {
    ListTests,
}

/// A collection of objects that are used while enqueuing jobs. This is useful as a separate object
/// since it can contain things which live longer than the scoped threads and thus can be shared
/// among them.
///
/// This object is separate from `MainAppState` because it is lent to `JobQueuing`
struct JobQueuingState<TestCollectorT: CollectTests> {
    packages: BTreeMap<TestCollectorT::PackageId, TestCollectorT::Package>,
    filter: TestCollectorT::TestFilter,
    stderr_color: bool,
    tracker: Arc<JobStatusTracker>,
    jobs_queued: AtomicU64,
    test_metadata: AllMetadata<TestCollectorT::TestFilter>,
    expected_job_count: u64,
    test_listing: Arc<Mutex<Option<TestListing<TestCollectorT::ArtifactKey>>>>,
    list_action: Option<ListAction>,
    collector_options: TestCollectorT::Options,
}

impl<TestCollectorT: CollectTests> JobQueuingState<TestCollectorT> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        packages: BTreeMap<TestCollectorT::PackageId, TestCollectorT::Package>,
        filter: TestCollectorT::TestFilter,
        stderr_color: bool,
        test_metadata: AllMetadata<TestCollectorT::TestFilter>,
        test_listing: TestListing<TestCollectorT::ArtifactKey>,
        list_action: Option<ListAction>,
        collector_options: TestCollectorT::Options,
    ) -> Result<Self> {
        let expected_job_count = test_listing.expected_job_count(&filter);

        Ok(Self {
            packages,
            filter,
            stderr_color,
            tracker: Arc::new(JobStatusTracker::default()),
            jobs_queued: AtomicU64::new(0),
            test_metadata,
            expected_job_count,
            test_listing: Arc::new(Mutex::new(Some(test_listing))),
            list_action,
            collector_options,
        })
    }
}

type StringIter = <Vec<String> as IntoIterator>::IntoIter;

/// Enqueues test cases as jobs in the given client from the given artifact
///
/// This object is like an iterator, it maintains a position in the test listing and enqueues the
/// next thing when asked.
///
/// This object is stored inside `JobQueuing` and is used to keep track of which artifact it is
/// currently enqueuing from.
struct ArtifactQueuing<'a, ProgressIndicatorT, MainAppDepsT: MainAppDeps> {
    log: slog::Logger,
    queuing_state: &'a JobQueuingState<MainAppDepsT::TestCollector>,
    deps: &'a MainAppDepsT,
    width: usize,
    ind: ProgressIndicatorT,
    artifact: <MainAppDepsT::TestCollector as CollectTests>::Artifact,
    ignored_cases: HashSet<String>,
    package_name: String,
    cases: StringIter,
    timeout_override: Option<Option<Timeout>>,
    generated_artifacts: Option<GeneratedArtifacts>,
}

#[derive(Default)]
struct TestListingResult {
    cases: Vec<String>,
    ignored_cases: HashSet<String>,
}

fn list_test_cases<TestCollectorT: CollectTests>(
    log: slog::Logger,
    queuing_state: &JobQueuingState<TestCollectorT>,
    ind: &impl ProgressIndicator,
    artifact: &TestCollectorT::Artifact,
    package_name: &str,
) -> Result<TestListingResult> {
    ind.update_enqueue_status(format!("getting test list for {package_name}"));

    slog::debug!(log, "listing ignored tests"; "artifact" => ?artifact);
    let ignored_cases: HashSet<_> = artifact.list_ignored_tests()?.into_iter().collect();

    slog::debug!(log, "listing tests"; "artifact" => ?artifact);
    let mut cases = artifact.list_tests()?;

    let artifact_key = artifact.to_key();
    let mut listing = queuing_state.test_listing.lock().unwrap();
    listing
        .as_mut()
        .unwrap()
        .update_artifact_cases(package_name, artifact_key.clone(), &cases);

    cases.retain(|c| {
        queuing_state
            .filter
            .filter(package_name, Some(&artifact_key), Some(c.as_str()))
            .expect("should have case")
    });
    Ok(TestListingResult {
        cases,
        ignored_cases,
    })
}

impl<'a, ProgressIndicatorT, MainAppDepsT> ArtifactQueuing<'a, ProgressIndicatorT, MainAppDepsT>
where
    ProgressIndicatorT: ProgressIndicator,
    MainAppDepsT: MainAppDeps,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        log: slog::Logger,
        queuing_state: &'a JobQueuingState<MainAppDepsT::TestCollector>,
        deps: &'a MainAppDepsT,
        width: usize,
        ind: ProgressIndicatorT,
        artifact: <MainAppDepsT::TestCollector as CollectTests>::Artifact,
        package_name: String,
        timeout_override: Option<Option<Timeout>>,
    ) -> Result<Self> {
        let listing = list_test_cases(log.clone(), queuing_state, &ind, &artifact, &package_name)?;

        ind.update_enqueue_status(format!("generating artifacts for {package_name}"));
        slog::debug!(
            log,
            "generating artifacts";
            "package_name" => &package_name,
            "artifact" => ?artifact);

        Ok(Self {
            log,
            queuing_state,
            deps,
            width,
            ind,
            artifact,
            ignored_cases: listing.ignored_cases,
            package_name,
            cases: listing.cases.into_iter(),
            timeout_override,
            generated_artifacts: None,
        })
    }

    fn calculate_job_layers(
        &mut self,
        test_metadata: &TestMetadata,
    ) -> Result<Vec<(Sha256Digest, ArtifactType)>> {
        test_metadata
            .layers
            .iter()
            .map(|layer| {
                slog::debug!(self.log, "adding layer"; "layer" => ?layer);
                self.deps.client().add_layer(layer.clone())
            })
            .collect::<Result<Vec<_>>>()
    }

    fn format_case_str(&self, case: &str) -> String {
        let mut s = self.package_name.to_string();
        s += " ";

        let artifact_name = self.artifact.name();
        if artifact_name != self.package_name {
            s += artifact_name;
            s += " ";
        }
        s += case;
        s
    }

    fn generate_artifacts(&mut self) -> Result<GeneratedArtifacts> {
        if let Some(generated_artifacts) = &self.generated_artifacts {
            return Ok(generated_artifacts.clone());
        }
        let generated_artifacts = artifacts::add_generated_artifacts(
            self.deps.client(),
            self.artifact.path(),
            self.log.clone(),
        )?;
        self.generated_artifacts = Some(generated_artifacts.clone());
        Ok(generated_artifacts)
    }

    fn queue_job_from_case(&mut self, case: &str) -> Result<EnqueueResult> {
        let case_str = self.format_case_str(case);
        self.ind
            .update_enqueue_status(format!("processing {case_str}"));
        slog::debug!(self.log, "enqueuing test case"; "case" => &case_str);

        if self.queuing_state.list_action.is_some() {
            self.ind.lock_printing().println(case_str);
            return Ok(EnqueueResult::Listed);
        }

        let test_metadata = self
            .queuing_state
            .test_metadata
            .get_metadata_for_test_with_env(&self.package_name, &self.artifact.to_key(), case)?;
        self.ind
            .update_enqueue_status(format!("calculating layers for {case_str}"));
        slog::debug!(&self.log, "calculating job layers"; "case" => &case_str);
        let mut layers = self.calculate_job_layers(&test_metadata)?;

        match self.deps.test_collector().get_test_layers(&test_metadata)? {
            TestLayers::GenerateForBinary => {
                let dep = self.generate_artifacts()?;
                layers.push((dep.binary, ArtifactType::Manifest));
                if test_metadata.include_shared_libraries() {
                    layers.push((dep.deps, ArtifactType::Manifest));
                }
            }
            TestLayers::Provided(layer_specs) => {
                for layer_spec in layer_specs {
                    layers.push(self.deps.client().add_layer(layer_spec)?);
                }
            }
        }

        // N.B. Must do this before we enqueue the job, but after we know we can't fail
        let count = self
            .queuing_state
            .jobs_queued
            .fetch_add(1, Ordering::AcqRel);
        self.ind.update_length(std::cmp::max(
            self.queuing_state.expected_job_count,
            count + 1,
        ));
        self.queuing_state.tracker.add_outstanding();

        let visitor = JobStatusVisitor::new(
            self.queuing_state.tracker.clone(),
            self.queuing_state.test_listing.clone(),
            self.package_name.clone(),
            self.artifact.to_key(),
            case.to_owned(),
            case_str.clone(),
            self.width,
            self.ind.clone(),
            MainAppDepsT::TestCollector::remove_fixture_output
                as fn(&str, Vec<String>) -> Vec<String>,
        );

        if self.ignored_cases.contains(case) {
            visitor.job_ignored();
            return Ok(EnqueueResult::Ignored);
        }

        let estimated_duration = self
            .queuing_state
            .test_listing
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .get_timing(&self.package_name, &self.artifact.to_key(), case);

        self.ind
            .update_enqueue_status(format!("submitting job for {case_str}"));
        slog::debug!(&self.log, "submitting job"; "case" => &case_str);
        let (program, arguments) = self.artifact.build_command(case);
        self.deps.client().add_job(
            JobSpec {
                program,
                arguments,
                image: test_metadata.image,
                environment: test_metadata.environment,
                layers,
                devices: test_metadata.devices,
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
                timeout: self.timeout_override.unwrap_or(test_metadata.timeout),
                estimated_duration,
            },
            move |res| visitor.job_finished(res),
        )?;

        Ok(EnqueueResult::Enqueued {
            package_name: self.package_name.clone(),
            case: case.into(),
        })
    }

    /// Attempt to enqueue the next test as a job in the client
    ///
    /// Returns an `EnqueueResult` describing what happened. Meant to be called until it returns
    /// `EnqueueResult::Done`
    fn enqueue_one(&mut self) -> Result<EnqueueResult> {
        let Some(case) = self.cases.next() else {
            return Ok(EnqueueResult::Done);
        };
        self.queue_job_from_case(&case)
    }
}

/// Enqueues tests as jobs using the given deps.
///
/// This object is like an iterator, it maintains a position in the test listing and enqueues the
/// next thing when asked.
struct JobQueuing<'a, ProgressIndicatorT, MainAppDepsT: MainAppDeps> {
    log: slog::Logger,
    queuing_state: &'a JobQueuingState<MainAppDepsT::TestCollector>,
    deps: &'a MainAppDepsT,
    width: usize,
    ind: ProgressIndicatorT,
    wait_handle: Option<<MainAppDepsT::TestCollector as CollectTests>::BuildHandle>,
    package_match: bool,
    artifacts: Option<<MainAppDepsT::TestCollector as CollectTests>::ArtifactStream>,
    artifact_queuing: Option<ArtifactQueuing<'a, ProgressIndicatorT, MainAppDepsT>>,
    timeout_override: Option<Option<Timeout>>,
}

impl<'a, ProgressIndicatorT: ProgressIndicator, MainAppDepsT>
    JobQueuing<'a, ProgressIndicatorT, MainAppDepsT>
where
    ProgressIndicatorT: ProgressIndicator,
    MainAppDepsT: MainAppDeps,
{
    fn new(
        log: slog::Logger,
        queuing_state: &'a JobQueuingState<MainAppDepsT::TestCollector>,
        deps: &'a MainAppDepsT,
        width: usize,
        ind: ProgressIndicatorT,
        timeout_override: Option<Option<Timeout>>,
    ) -> Result<Self> {
        let package_names: Vec<_> = queuing_state
            .packages
            .values()
            .map(|p| format!("{}@{}", p.name(), p.version()))
            .collect();

        let building_tests = !package_names.is_empty()
            && matches!(
                queuing_state.list_action,
                None | Some(ListAction::ListTests)
            );

        let (wait_handle, artifacts) = building_tests
            .then(|| {
                deps.test_collector().start(
                    queuing_state.stderr_color,
                    &queuing_state.collector_options,
                    package_names,
                )
            })
            .transpose()?
            .unzip();

        Ok(Self {
            log,
            queuing_state,
            deps,
            width,
            ind,
            package_match: false,
            artifacts,
            artifact_queuing: None,
            wait_handle,
            timeout_override,
        })
    }

    fn start_queuing_from_artifact(&mut self) -> Result<bool> {
        self.ind.update_enqueue_status("building artifacts...");

        slog::debug!(self.log, "getting artifacts");
        let Some(ref mut artifacts) = self.artifacts else {
            return Ok(false);
        };
        let Some(artifact) = artifacts.next() else {
            return Ok(false);
        };
        let artifact = artifact?;

        slog::debug!(self.log, "got artifact"; "artifact" => ?artifact);
        let package_name = self
            .queuing_state
            .packages
            .get(&artifact.package())
            .expect("artifact for unknown package")
            .name();

        self.artifact_queuing = Some(ArtifactQueuing::new(
            self.log.clone(),
            self.queuing_state,
            self.deps,
            self.width,
            self.ind.clone(),
            artifact,
            package_name.into(),
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

    /// Attempt to enqueue the next test as a job in the client
    ///
    /// Returns an `EnqueueResult` describing what happened. Meant to be called it returns
    /// `EnqueueResult::Done`
    fn enqueue_one(&mut self) -> Result<EnqueueResult> {
        slog::debug!(self.log, "enqueuing a job");

        if self.artifact_queuing.is_none() && !self.start_queuing_from_artifact()? {
            self.finish()?;
            return Ok(EnqueueResult::Done);
        }
        self.package_match = true;

        let res = self.artifact_queuing.as_mut().unwrap().enqueue_one()?;
        if res.is_done() {
            self.artifact_queuing = None;
            return self.enqueue_one();
        }

        Ok(res)
    }
}

/// This is where cached data goes. If there is build output it is also here.
pub struct BuildDir;

/// A collection of objects that are used to run the MainApp. This is useful as a separate object
/// since it can contain things which live longer than scoped threads and thus shared among them.
pub struct MainAppState<MainAppDepsT: MainAppDeps> {
    deps: MainAppDepsT,
    queuing_state: JobQueuingState<MainAppDepsT::TestCollector>,
    test_listing_store:
        TestListingStore<<MainAppDepsT::TestCollector as CollectTests>::ArtifactKey>,
    logging_output: LoggingOutput,
    log: slog::Logger,
}

impl<MainAppDepsT: MainAppDeps> MainAppState<MainAppDepsT> {
    /// Creates a new `MainAppState`
    ///
    /// `bg_proc`: handle to background client process
    /// `include_filter`: tests which match any of the patterns in this filter are run
    /// `exclude_filter`: tests which match any of the patterns in this filter are not run
    /// `list_action`: if some, tests aren't run, instead tests or other things are listed
    /// `stderr_color`: should terminal color codes be written to `stderr` or not
    /// `project_dir`: the path to the root of the project
    /// `packages`: a listing of all the packages
    /// `broker_addr`: the network address of the broker which we connect to
    /// `client_driver`: an object which drives the background work of the `Client`
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        deps: MainAppDepsT,
        include_filter: Vec<String>,
        exclude_filter: Vec<String>,
        list_action: Option<ListAction>,
        stderr_color: bool,
        project_dir: impl AsRef<Root<ProjectDir>>,
        packages: &[<MainAppDepsT::TestCollector as CollectTests>::Package],
        state_dir: impl AsRef<Root<StateDir>>,
        build_dir: impl AsRef<Root<BuildDir>>,
        collector_options: <MainAppDepsT::TestCollector as CollectTests>::Options,
        logging_output: LoggingOutput,
        log: slog::Logger,
    ) -> Result<Self> {
        slog::debug!(
            log, "creating app state";
            "include_filter" => ?include_filter,
            "exclude_filter" => ?exclude_filter,
            "list_action" => ?list_action,
        );

        let mut test_metadata =
            AllMetadata::load(log.clone(), project_dir, MainAppDepsT::MAELSTROM_TEST_TOML)?;
        let test_listing_store = TestListingStore::new(Fs::new(), &state_dir);
        let mut test_listing = test_listing_store.load()?;
        test_listing
            .retain_packages_and_artifacts(packages.iter().map(|p| (p.name(), p.artifacts())));

        let filter = <MainAppDepsT::TestCollector as CollectTests>::TestFilter::compile(
            &include_filter,
            &exclude_filter,
        )?;
        let selected_packages: BTreeMap<_, _> = packages
            .iter()
            .filter(|p| filter.filter(p.name(), None, None).unwrap_or(true))
            .map(|p| (p.id(), p.clone()))
            .collect();

        slog::debug!(
            log, "filtered packages";
            "selected_packages" => ?Vec::from_iter(selected_packages.keys()),
        );

        let vars = deps.get_template_vars(&collector_options, build_dir.as_ref())?;
        test_metadata.replace_template_vars(&vars)?;

        Ok(Self {
            deps,
            queuing_state: JobQueuingState::new(
                selected_packages,
                filter,
                stderr_color,
                test_metadata,
                test_listing,
                list_action,
                collector_options,
            )?,
            test_listing_store,
            logging_output,
            log,
        })
    }
}

/// The `MainApp` enqueues tests as jobs. With each attempted job enqueued this object is returned
/// and describes what happened.
pub enum EnqueueResult {
    /// A job successfully enqueued with the following information
    Enqueued { package_name: String, case: String },
    /// No job was enqueued, instead the test that would have been enqueued has been ignored
    /// because it has been marked as `#[ignored]`
    Ignored,
    /// No job was enqueued, we have run out of tests to run
    Done,
    /// No job was enqueued, we listed the test case instead
    Listed,
}

impl EnqueueResult {
    /// Is this `EnqueueResult` the `Done` variant
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }

    /// Is this `EnqueueResult` the `Ignored` variant
    pub fn is_ignored(&self) -> bool {
        matches!(self, Self::Ignored)
    }
}

/// This is the public API for the MainApp
///
/// N.B. This API is a trait only for type-erasure purposes
pub trait MainApp {
    /// Enqueue one test as a job on the `Client`. This is meant to be called repeatedly until
    /// `EnqueueResult::Done` is returned, or an error is encountered.
    fn enqueue_one(&mut self) -> Result<EnqueueResult>;

    /// Indicates that we have finished enqueuing jobs and starts tearing things down
    fn drain(&mut self) -> Result<()>;

    /// Waits for all outstanding jobs to finish, displays a summary, and obtains an `ExitCode`
    fn finish(&mut self) -> Result<ExitCode>;
}

struct MainAppImpl<'state, TermT, ProgressIndicatorT, ProgressDriverT, MainAppDepsT: MainAppDeps> {
    state: &'state MainAppState<MainAppDepsT>,
    queuing: JobQueuing<'state, ProgressIndicatorT, MainAppDepsT>,
    prog_driver: ProgressDriverT,
    prog: ProgressIndicatorT,
    term: TermT,
}

impl<'state, TermT, ProgressIndicatorT, ProgressDriverT, MainAppDepsT: MainAppDeps>
    MainAppImpl<'state, TermT, ProgressIndicatorT, ProgressDriverT, MainAppDepsT>
{
    fn new(
        state: &'state MainAppState<MainAppDepsT>,
        queuing: JobQueuing<'state, ProgressIndicatorT, MainAppDepsT>,
        prog_driver: ProgressDriverT,
        prog: ProgressIndicatorT,
        term: TermT,
    ) -> Self {
        Self {
            state,
            queuing,
            prog_driver,
            prog,
            term,
        }
    }
}

impl<'state, 'scope, TermT, ProgressIndicatorT, ProgressDriverT, MainAppDepsT> MainApp
    for MainAppImpl<'state, TermT, ProgressIndicatorT, ProgressDriverT, MainAppDepsT>
where
    ProgressIndicatorT: ProgressIndicator,
    TermT: TermLike + Clone + 'static,
    ProgressDriverT: ProgressDriver<'scope>,
    MainAppDepsT: MainAppDeps,
{
    fn enqueue_one(&mut self) -> Result<EnqueueResult> {
        self.queuing.enqueue_one()
    }

    fn drain(&mut self) -> Result<()> {
        slog::debug!(self.queuing.log, "draining");
        self.prog
            .update_length(self.state.queuing_state.jobs_queued.load(Ordering::Acquire));
        self.prog.done_queuing_jobs();
        self.prog_driver.stop()?;
        Ok(())
    }

    fn finish(&mut self) -> Result<ExitCode> {
        slog::debug!(self.queuing.log, "waiting for outstanding jobs");
        self.state.queuing_state.tracker.wait_for_outstanding();
        self.prog.finished()?;

        if self.state.queuing_state.list_action.is_none() {
            let width = self.term.width() as usize;
            self.state
                .queuing_state
                .tracker
                .print_summary(width, self.term.clone())?;
        }

        self.state.test_listing_store.save(
            self.state
                .queuing_state
                .test_listing
                .lock()
                .unwrap()
                .take()
                .unwrap(),
        )?;

        Ok(self.state.queuing_state.tracker.exit_code())
    }
}

#[derive(Default)]
struct LoggingOutputInner {
    prog: Option<Box<dyn io::Write + Send + Sync + 'static>>,
}

#[derive(Clone, Default)]
pub struct LoggingOutput {
    inner: Arc<Mutex<LoggingOutputInner>>,
}

impl LoggingOutput {
    fn update(&self, prog: impl io::Write + Send + Sync + 'static) {
        let mut inner = self.inner.lock().unwrap();
        inner.prog = Some(Box::new(prog));
    }
}

impl io::Write for LoggingOutput {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(prog) = &mut inner.prog {
            prog.write(buf)
        } else {
            io::stdout().write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(prog) = &mut inner.prog {
            prog.flush()
        } else {
            io::stdout().flush()
        }
    }
}

pub enum Logger {
    DefaultLogger(LogLevel),
    GivenLogger(slog::Logger),
}

impl Logger {
    pub fn build(&self, out: LoggingOutput) -> slog::Logger {
        match self {
            Self::DefaultLogger(level) => {
                let decorator = slog_term::PlainDecorator::new(out);
                let drain = slog_term::FullFormat::new(decorator).build().fuse();
                let drain = slog_async::Async::new(drain).build().fuse();
                let drain = slog::LevelFilter::new(drain, level.as_slog_level()).fuse();
                slog::Logger::root(drain, slog::o!())
            }
            Self::GivenLogger(logger) => logger.clone(),
        }
    }
}

fn new_helper<'state, 'scope, ProgressIndicatorT, TermT, MainAppDepsT>(
    state: &'state MainAppState<MainAppDepsT>,
    prog_factory: impl FnOnce(TermT) -> ProgressIndicatorT,
    term: TermT,
    mut prog_driver: impl ProgressDriver<'scope> + 'scope,
    timeout_override: Option<Option<Timeout>>,
) -> Result<Box<dyn MainApp + 'scope>>
where
    ProgressIndicatorT: ProgressIndicator,
    TermT: TermLike + Clone + 'static,
    MainAppDepsT: MainAppDeps,
    'state: 'scope,
{
    let width = term.width() as usize;
    let prog = prog_factory(term.clone());

    prog_driver.drive(state.deps.client(), prog.clone());
    prog.update_length(state.queuing_state.expected_job_count);

    state
        .logging_output
        .update(progress::ProgressWriteAdapter::new(prog.clone()));
    slog::debug!(state.log, "main app created");

    let queuing = JobQueuing::new(
        state.log.clone(),
        &state.queuing_state,
        &state.deps,
        width,
        prog.clone(),
        timeout_override,
    )?;
    Ok(Box::new(MainAppImpl::new(
        state,
        queuing,
        prog_driver,
        prog,
        term,
    )))
}

/// Construct a `MainApp`
///
/// `state`: The shared state for the main app
/// `stdout_tty`: should terminal color codes be printed to stdout (provided via `term`)
/// `quiet`: indicates whether quiet mode should be used or not
/// `term`: represents the terminal
/// `driver`: drives the background work needed for updating the progress bars
pub fn main_app_new<'state, 'scope, TermT, MainAppDepsT>(
    state: &'state MainAppState<MainAppDepsT>,
    stdout_tty: bool,
    quiet: Quiet,
    term: TermT,
    driver: impl ProgressDriver<'scope> + 'scope,
    timeout_override: Option<Option<Timeout>>,
) -> Result<Box<dyn MainApp + 'scope>>
where
    TermT: TermLike + Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static,
    MainAppDepsT: MainAppDeps,
    'state: 'scope,
{
    if state.queuing_state.list_action.is_some() {
        return if stdout_tty {
            Ok(new_helper(
                state,
                TestListingProgress::new,
                term,
                driver,
                timeout_override,
            )?)
        } else {
            Ok(new_helper(
                state,
                TestListingProgressNoSpinner::new,
                term,
                driver,
                timeout_override,
            )?)
        };
    }

    match (stdout_tty, quiet.into_inner()) {
        (true, true) => Ok(new_helper(
            state,
            QuietProgressBar::new,
            term,
            driver,
            timeout_override,
        )?),
        (true, false) => Ok(new_helper(
            state,
            MultipleProgressBars::new,
            term,
            driver,
            timeout_override,
        )?),
        (false, true) => Ok(new_helper(
            state,
            QuietNoBar::new,
            term,
            driver,
            timeout_override,
        )?),
        (false, false) => Ok(new_helper(
            state,
            NoBar::new,
            term,
            driver,
            timeout_override,
        )?),
    }
}
