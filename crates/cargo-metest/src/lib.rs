pub mod artifacts;
pub mod cargo;
pub mod config;
pub mod metadata;
pub mod pattern;
pub mod progress;
pub mod test_listing;
pub mod visitor;

use anyhow::Result;
use cargo::{get_cases_from_binary, CargoBuild, TestArtifactStream};
use cargo_metadata::{Artifact as CargoArtifact, Package as CargoPackage};
use config::Quiet;
use indicatif::{ProgressBar, TermLike};
use metadata::{AllMetadata, TestMetadata};
use meticulous_base::{JobSpec, NonEmpty, Sha256Digest};
use meticulous_client::{spec::ImageConfig, Client, ClientDriver};
use meticulous_util::{config::BrokerAddr, process::ExitCode};
use progress::{
    MultipleProgressBars, NoBar, ProgressDriver, ProgressIndicator, QuietNoBar, QuietProgressBar,
};
use std::{
    collections::HashSet,
    io,
    path::{Path, PathBuf},
    str,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use test_listing::{load_test_listing, write_test_listing, TestListing, LAST_TEST_LISTING_NAME};
use visitor::{JobStatusTracker, JobStatusVisitor};

/// Returns `true` if the given `CargoPackage` matches the given pattern
fn filter_package(package: &CargoPackage, p: &pattern::Pattern) -> bool {
    let c = pattern::Context {
        package: package.name.clone(),
        artifact: None,
        case: None,
    };
    pattern::interpret_pattern(p, &c).unwrap_or(true)
}

/// Returns `true` if the given `CargoArtifact` and case matches the given pattern
fn filter_case(artifact: &CargoArtifact, case: &str, p: &pattern::Pattern) -> bool {
    let package_name = artifact.package_id.repr.split(' ').next().unwrap().into();
    let c = pattern::Context {
        package: package_name,
        artifact: Some(pattern::Artifact::from_target(&artifact.target)),
        case: Some(pattern::Case { name: case.into() }),
    };
    pattern::interpret_pattern(p, &c).expect("case is provided")
}

/// A collection of objects that are used while enqueuing jobs. This is useful as a separate object
/// since it can contain things which live longer than the scoped threads and thus can be shared
/// among them.
///
/// This object is separate from `MainAppDeps` because it is lent to `JobQueuing`
struct JobQueuingDeps<StdErrT> {
    cargo: String,
    packages: Vec<String>,
    filter: pattern::Pattern,
    stderr: Mutex<StdErrT>,
    stderr_color: bool,
    tracker: Arc<JobStatusTracker>,
    jobs_queued: AtomicU64,
    test_metadata: AllMetadata,
    expected_job_count: u64,
    test_listing: Mutex<TestListing>,
}

impl<StdErrT> JobQueuingDeps<StdErrT> {
    fn new(
        cargo: String,
        packages: Vec<String>,
        filter: pattern::Pattern,
        stderr: StdErrT,
        stderr_color: bool,
        test_metadata: AllMetadata,
        test_listing: TestListing,
    ) -> Self {
        let expected_job_count = test_listing.expected_job_count(&filter);

        Self {
            cargo,
            packages,
            filter,
            stderr: Mutex::new(stderr),
            stderr_color,
            tracker: Arc::new(JobStatusTracker::default()),
            jobs_queued: AtomicU64::new(0),
            test_metadata,
            expected_job_count,
            test_listing: Mutex::new(test_listing),
        }
    }
}

/// Enqueues test cases as jobs in the given client from the given `CargoArtifact`
///
/// This object is like an iterator, it maintains a position in the test listing and enqueues the
/// next thing when asked.
///
/// This object is stored inside `JobQueuing` and is used to keep track of which artifact it is
/// currently enqueuing from.
struct ArtifactQueuing<'a, StdErrT, ProgressIndicatorT> {
    queuing_deps: &'a JobQueuingDeps<StdErrT>,
    client: &'a Mutex<Client>,
    width: usize,
    ind: ProgressIndicatorT,
    artifact: CargoArtifact,
    binary: PathBuf,
    binary_artifact: Sha256Digest,
    deps_artifact: Sha256Digest,
    ignored_cases: HashSet<String>,
    package_name: String,
    cases: <Vec<String> as IntoIterator>::IntoIter,
}

impl<'a, StdErrT, ProgressIndicatorT> ArtifactQueuing<'a, StdErrT, ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
{
    fn new(
        queuing_deps: &'a JobQueuingDeps<StdErrT>,
        client: &'a Mutex<Client>,
        width: usize,
        ind: ProgressIndicatorT,
        artifact: CargoArtifact,
        package_name: String,
    ) -> Result<Self> {
        ind.update_enqueue_status(format!("processing {package_name}"));
        let binary = PathBuf::from(artifact.executable.clone().unwrap());
        let ignored_cases: HashSet<_> = get_cases_from_binary(&binary, &Some("--ignored".into()))?
            .into_iter()
            .collect();

        ind.update_enqueue_status(format!("tar {package_name}"));
        let (binary_artifact, deps_artifact) =
            artifacts::add_generated_artifacts(client, &binary, &ind)?;

        ind.update_enqueue_status(format!("processing {package_name}"));
        let mut cases = get_cases_from_binary(&binary, &None)?;

        let mut listing = queuing_deps.test_listing.lock().unwrap();
        listing.add_cases(&artifact, &cases[..]);

        cases.retain(|c| filter_case(&artifact, c, &queuing_deps.filter));

        Ok(Self {
            queuing_deps,
            client,
            width,
            ind,
            artifact,
            binary,
            binary_artifact,
            deps_artifact,
            ignored_cases,
            package_name,
            cases: cases.into_iter(),
        })
    }

    fn calculate_job_layers(
        &mut self,
        test_metadata: &TestMetadata,
    ) -> Result<NonEmpty<Sha256Digest>> {
        let mut layers = test_metadata
            .layers
            .iter()
            .map(|layer| {
                self.client
                    .lock()
                    .unwrap()
                    .add_artifact(PathBuf::from(layer).as_path())
            })
            .collect::<Result<Vec<_>>>()?;
        if test_metadata.include_shared_libraries() {
            layers.push(self.deps_artifact.clone());
        }
        layers.push(self.binary_artifact.clone());

        Ok(NonEmpty::try_from(layers).unwrap())
    }

    fn queue_job_from_case(&mut self, case: &str) -> Result<EnqueueResult> {
        let case_str = format!("{} {case}", &self.package_name);
        self.ind
            .update_enqueue_status(format!("processing {case_str}"));

        let image_lookup = |image: &str| {
            let (image, version) = image.split_once(':').unwrap_or((image, "latest"));
            let prog = self
                .ind
                .new_side_progress(format!("downloading image {image}"))
                .unwrap_or_else(ProgressBar::hidden);
            let mut client = self.client.lock().unwrap();
            let container_image_depot = client.container_image_depot_mut();
            let image = container_image_depot.get_container_image(image, version, prog)?;
            Ok(ImageConfig {
                layers: image.layers.clone(),
                environment: image.env().cloned(),
                working_directory: image.working_dir().map(From::from),
            })
        };

        let filter_context = pattern::Context {
            package: self.package_name.clone(),
            artifact: Some(pattern::Artifact::from_target(&self.artifact.target)),
            case: Some(pattern::Case { name: case.into() }),
        };

        let test_metadata = self
            .queuing_deps
            .test_metadata
            .get_metadata_for_test_with_env(&filter_context, image_lookup)?;
        let layers = self.calculate_job_layers(&test_metadata)?;

        // N.B. Must do this before we enqueue the job, but after we know we can't fail
        let count = self.queuing_deps.jobs_queued.fetch_add(1, Ordering::AcqRel);
        self.ind.update_length(std::cmp::max(
            self.queuing_deps.expected_job_count,
            count + 1,
        ));

        let visitor = JobStatusVisitor::new(
            self.queuing_deps.tracker.clone(),
            case_str,
            self.width,
            self.ind.clone(),
        );

        if self.ignored_cases.contains(case) {
            visitor.job_ignored();
            return Ok(EnqueueResult::Ignored);
        }

        let binary_name = self.binary.file_name().unwrap().to_str().unwrap();
        self.client.lock().unwrap().add_job(
            JobSpec {
                program: format!("/{binary_name}").into(),
                arguments: vec!["--exact".into(), "--nocapture".into(), case.into()],
                environment: test_metadata.environment(),
                layers,
                devices: test_metadata.devices,
                mounts: test_metadata.mounts,
                enable_loopback: test_metadata.enable_loopback,
                enable_writable_file_system: test_metadata.enable_writable_file_system,
                working_directory: test_metadata.working_directory,
                user: test_metadata.user,
                group: test_metadata.group,
            },
            Box::new(move |cjid, result| visitor.job_finished(cjid, result)),
        );

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

/// Enqueues tests as jobs in the given client.
///
/// This object is like an iterator, it maintains a position in the test listing and enqueues the
/// next thing when asked.
struct JobQueuing<'a, StdErrT, ProgressIndicatorT> {
    queuing_deps: &'a JobQueuingDeps<StdErrT>,
    client: &'a Mutex<Client>,
    width: usize,
    ind: ProgressIndicatorT,
    cargo_build: Option<CargoBuild>,
    package_match: bool,
    artifacts: TestArtifactStream,
    artifact_queuing: Option<ArtifactQueuing<'a, StdErrT, ProgressIndicatorT>>,
}

impl<'a, StdErrT, ProgressIndicatorT: ProgressIndicator> JobQueuing<'a, StdErrT, ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
    StdErrT: io::Write,
{
    fn new(
        queuing_deps: &'a JobQueuingDeps<StdErrT>,
        client: &'a Mutex<Client>,
        width: usize,
        ind: ProgressIndicatorT,
    ) -> Result<Self> {
        let mut cargo_build = CargoBuild::new(
            &queuing_deps.cargo,
            queuing_deps.stderr_color,
            queuing_deps.packages.clone(),
        )?;
        Ok(Self {
            queuing_deps,
            client,
            width,
            ind,
            package_match: false,
            artifacts: cargo_build.artifact_stream(),
            artifact_queuing: None,
            cargo_build: Some(cargo_build),
        })
    }

    fn start_queuing_from_artifact(&mut self) -> Result<bool> {
        self.ind.update_enqueue_status("building artifacts...");

        let Some(artifact) = self.artifacts.next() else {
            return Ok(false);
        };
        let artifact = artifact?;

        let package_name = artifact.package_id.repr.split(' ').next().unwrap().into();
        self.artifact_queuing = Some(ArtifactQueuing::new(
            self.queuing_deps,
            self.client,
            self.width,
            self.ind.clone(),
            artifact,
            package_name,
        )?);

        Ok(true)
    }

    /// Meant to be called when the user has enqueued all the jobs they want. Checks for deferred
    /// errors from cargo or otherwise
    fn finish(&mut self) -> Result<()> {
        self.cargo_build
            .take()
            .unwrap()
            .check_status(&mut *self.queuing_deps.stderr.lock().unwrap())?;

        Ok(())
    }

    /// Attempt to enqueue the next test as a job in the client
    ///
    /// Returns an `EnqueueResult` describing what happened. Meant to be called it returns
    /// `EnqueueResult::Done`
    fn enqueue_one(&mut self) -> Result<EnqueueResult> {
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

/// A collection of objects that are used to run the MainApp. This is useful as a separate object
/// since it can contain things which live longer than scoped threads and thus shared among them.
pub struct MainAppDeps<StdErrT> {
    pub client: Mutex<Client>,
    queuing_deps: JobQueuingDeps<StdErrT>,
    cache_dir: PathBuf,
}

impl<StdErrT> MainAppDeps<StdErrT> {
    /// Creates a new `MainAppDeps`
    ///
    /// `cargo`: the command to run when invoking cargo
    /// `include_filter`: tests which match any of the patterns in this filter are run
    /// `exclude_filter`: tests which match any of the patterns in this filter are not run
    /// `stderr`: is written to for error output
    /// `stderr_color`: should terminal color codes be written to `stderr` or not
    /// `workspace_root`: the path to the root of the workspace
    /// `workspace_packages`: a listing of the packages in the workspace
    /// `broker_addr`: the network address of the broker which we connect to
    /// `client_driver`: an object which drives the background work of the `Client`
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cargo: String,
        include_filter: Vec<String>,
        exclude_filter: Vec<String>,
        stderr: StdErrT,
        stderr_color: bool,
        workspace_root: &impl AsRef<Path>,
        workspace_packages: &[&CargoPackage],
        broker_addr: BrokerAddr,
        client_driver: impl ClientDriver + Send + Sync + 'static,
    ) -> Result<Self> {
        let cache_dir = workspace_root.as_ref().join("target");
        let client = Mutex::new(Client::new(
            client_driver,
            broker_addr,
            workspace_root,
            cache_dir.clone(),
        )?);
        let test_metadata = AllMetadata::load(workspace_root)?;
        let mut test_listing =
            load_test_listing(&cache_dir.join(LAST_TEST_LISTING_NAME))?.unwrap_or_default();
        test_listing.retain_packages(workspace_packages);

        let filter = pattern::compile_filter(&include_filter, &exclude_filter)?;
        let selected_packages = workspace_packages
            .iter()
            .filter(|p| filter_package(p, &filter))
            .map(|p| p.name.clone())
            .collect();

        Ok(Self {
            client,
            queuing_deps: JobQueuingDeps::new(
                cargo,
                selected_packages,
                filter,
                stderr,
                stderr_color,
                test_metadata,
                test_listing,
            ),
            cache_dir,
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

struct MainAppImpl<'deps, StdErrT, TermT, ProgressIndicatorT, ProgressDriverT> {
    deps: &'deps MainAppDeps<StdErrT>,
    queuing: JobQueuing<'deps, StdErrT, ProgressIndicatorT>,
    prog_driver: ProgressDriverT,
    prog: ProgressIndicatorT,
    term: TermT,
}

impl<'deps, StdErrT, TermT, ProgressIndicatorT, ProgressDriverT>
    MainAppImpl<'deps, StdErrT, TermT, ProgressIndicatorT, ProgressDriverT>
{
    fn new(
        deps: &'deps MainAppDeps<StdErrT>,
        queuing: JobQueuing<'deps, StdErrT, ProgressIndicatorT>,
        prog_driver: ProgressDriverT,
        prog: ProgressIndicatorT,
        term: TermT,
    ) -> Self {
        Self {
            deps,
            queuing,
            prog_driver,
            prog,
            term,
        }
    }
}

impl<'deps, 'scope, StdErrT, TermT, ProgressIndicatorT, ProgressDriverT> MainApp
    for MainAppImpl<'deps, StdErrT, TermT, ProgressIndicatorT, ProgressDriverT>
where
    StdErrT: io::Write + Send,
    ProgressIndicatorT: ProgressIndicator,
    TermT: TermLike + Clone + 'static,
    ProgressDriverT: ProgressDriver<'scope>,
{
    fn enqueue_one(&mut self) -> Result<EnqueueResult> {
        self.queuing.enqueue_one()
    }

    fn drain(&mut self) -> Result<()> {
        self.prog
            .update_length(self.deps.queuing_deps.jobs_queued.load(Ordering::Acquire));
        self.prog.done_queuing_jobs();
        self.prog_driver.stop()?;
        self.deps.client.lock().unwrap().stop_accepting()?;
        Ok(())
    }

    fn finish(&mut self) -> Result<ExitCode> {
        self.deps
            .client
            .lock()
            .unwrap()
            .wait_for_outstanding_jobs()?;
        self.prog.finished()?;

        let width = self.term.width() as usize;
        self.deps
            .queuing_deps
            .tracker
            .print_summary(width, self.term.clone())?;

        write_test_listing(
            &self.deps.cache_dir.join(LAST_TEST_LISTING_NAME),
            &self.deps.queuing_deps.test_listing.lock().unwrap(),
        )?;

        Ok(self.deps.queuing_deps.tracker.exit_code())
    }
}

fn new_helper<'deps, 'scope, StdErrT, ProgressIndicatorT, TermT>(
    deps: &'deps MainAppDeps<StdErrT>,
    prog_factory: impl FnOnce(TermT) -> ProgressIndicatorT,
    term: TermT,
    mut prog_driver: impl ProgressDriver<'scope> + 'scope,
) -> Result<Box<dyn MainApp + 'scope>>
where
    StdErrT: io::Write + Send,
    ProgressIndicatorT: ProgressIndicator,
    TermT: TermLike + Clone + 'static,
    'deps: 'scope,
{
    let width = term.width() as usize;
    let prog = prog_factory(term.clone());

    prog_driver.drive(&deps.client, prog.clone());
    prog.update_length(deps.queuing_deps.expected_job_count);

    let queuing = JobQueuing::new(&deps.queuing_deps, &deps.client, width, prog.clone())?;
    Ok(Box::new(MainAppImpl::new(
        deps,
        queuing,
        prog_driver,
        prog,
        term,
    )))
}

/// Construct a `MainApp`
///
/// `deps`: a collection of dependencies
/// `stdout_tty`: should terminal color codes be printed to stdout (provided via `term`)
/// `quiet`: indicates whether quiet mode should be used or not
/// `term`: represents the terminal
/// `driver`: drives the background work needed for updating the progress bars
pub fn main_app_new<'deps, 'scope, TermT, StdErrT>(
    deps: &'deps MainAppDeps<StdErrT>,
    stdout_tty: bool,
    quiet: Quiet,
    term: TermT,
    driver: impl ProgressDriver<'scope> + 'scope,
) -> Result<Box<dyn MainApp + 'scope>>
where
    StdErrT: io::Write + Send,
    TermT: TermLike + Clone + Send + Sync + 'static,
    'deps: 'scope,
{
    match (stdout_tty, quiet.into_inner()) {
        (true, true) => Ok(new_helper(deps, QuietProgressBar::new, term, driver)?),
        (true, false) => Ok(new_helper(deps, MultipleProgressBars::new, term, driver)?),
        (false, true) => Ok(new_helper(deps, QuietNoBar::new, term, driver)?),
        (false, false) => Ok(new_helper(deps, NoBar::new, term, driver)?),
    }
}
