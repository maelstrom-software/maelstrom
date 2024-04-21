pub mod artifacts;
pub mod cargo;
pub mod config;
pub mod metadata;
pub mod pattern;
pub mod progress;
pub mod test_listing;
pub mod visitor;

use anyhow::Result;
use artifacts::GeneratedArtifacts;
use cargo::{
    get_cases_from_binary, run_cargo_test, CompilationOptions, FeatureSelectionOptions,
    ManifestOptions, TestArtifactStream, WaitHandle,
};
use cargo_metadata::{Artifact as CargoArtifact, Package as CargoPackage, PackageId};
use config::Quiet;
use indicatif::TermLike;
use maelstrom_base::{ArtifactType, JobSpec, NonEmpty, Sha256Digest, Timeout};
use maelstrom_client::{spec::ImageConfig, Client, ClientBgProcess};
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots},
    process::ExitCode,
};
use metadata::{AllMetadata, TestMetadata};
use progress::{
    MultipleProgressBars, NoBar, ProgressDriver, ProgressIndicator, QuietNoBar, QuietProgressBar,
    TestListingProgress, TestListingProgressNoSpinner,
};
use slog::Drain as _;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::{
    collections::{BTreeMap, HashSet},
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

#[derive(Debug)]
pub enum ListAction {
    ListTests,
    ListBinaries,
    ListPackages,
}

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
fn filter_case(
    package_name: &str,
    artifact: &CargoArtifact,
    case: &str,
    p: &pattern::Pattern,
) -> bool {
    let c = pattern::Context {
        package: package_name.into(),
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
struct JobQueuingDeps {
    cargo: String,
    packages: BTreeMap<PackageId, CargoPackage>,
    filter: pattern::Pattern,
    stderr_color: bool,
    tracker: Arc<JobStatusTracker>,
    jobs_queued: AtomicU64,
    test_metadata: AllMetadata,
    expected_job_count: u64,
    test_listing: Mutex<TestListing>,
    list_action: Option<ListAction>,
    feature_selection_options: FeatureSelectionOptions,
    compilation_options: CompilationOptions,
    manifest_options: ManifestOptions,
}

impl JobQueuingDeps {
    #[allow(clippy::too_many_arguments)]
    fn new(
        cargo: String,
        packages: BTreeMap<PackageId, CargoPackage>,
        filter: pattern::Pattern,
        stderr_color: bool,
        test_metadata: AllMetadata,
        test_listing: TestListing,
        list_action: Option<ListAction>,
        feature_selection_options: FeatureSelectionOptions,
        compilation_options: CompilationOptions,
        manifest_options: ManifestOptions,
    ) -> Self {
        let expected_job_count = test_listing.expected_job_count(&filter);

        Self {
            cargo,
            packages,
            filter,
            stderr_color,
            tracker: Arc::new(JobStatusTracker::default()),
            jobs_queued: AtomicU64::new(0),
            test_metadata,
            expected_job_count,
            test_listing: Mutex::new(test_listing),
            list_action,
            feature_selection_options,
            compilation_options,
            manifest_options,
        }
    }
}

type StringIter = <Vec<String> as IntoIterator>::IntoIter;

/// Enqueues test cases as jobs in the given client from the given `CargoArtifact`
///
/// This object is like an iterator, it maintains a position in the test listing and enqueues the
/// next thing when asked.
///
/// This object is stored inside `JobQueuing` and is used to keep track of which artifact it is
/// currently enqueuing from.
struct ArtifactQueuing<'a, ProgressIndicatorT> {
    log: slog::Logger,
    queuing_deps: &'a JobQueuingDeps,
    client: &'a Client,
    width: usize,
    ind: ProgressIndicatorT,
    artifact: CargoArtifact,
    binary: PathBuf,
    generated_artifacts: Option<GeneratedArtifacts>,
    ignored_cases: HashSet<String>,
    package_name: String,
    cases: StringIter,
    timeout_override: Option<Option<Timeout>>,
}

#[derive(Default)]
struct TestListingResult {
    cases: Vec<String>,
    ignored_cases: HashSet<String>,
}

fn list_test_cases<ProgressIndicatorT>(
    log: slog::Logger,
    queuing_deps: &JobQueuingDeps,
    ind: &ProgressIndicatorT,
    artifact: &CargoArtifact,
    package_name: &str,
) -> Result<TestListingResult>
where
    ProgressIndicatorT: ProgressIndicator,
{
    ind.update_enqueue_status(format!("getting test list for {package_name}"));

    slog::debug!(log, "listing ignored tests"; "binary" => ?artifact.executable);
    let binary = PathBuf::from(artifact.executable.clone().unwrap());
    let ignored_cases: HashSet<_> = get_cases_from_binary(&binary, &Some("--ignored".into()))?
        .into_iter()
        .collect();

    slog::debug!(log, "listing tests"; "binary" => ?artifact.executable);
    let mut cases = get_cases_from_binary(&binary, &None)?;

    let mut listing = queuing_deps.test_listing.lock().unwrap();
    listing.add_cases(package_name, artifact, &cases[..]);

    cases.retain(|c| filter_case(package_name, artifact, c, &queuing_deps.filter));
    Ok(TestListingResult {
        cases,
        ignored_cases,
    })
}

fn generate_artifacts(
    client: &Client,
    artifact: &CargoArtifact,
    log: slog::Logger,
) -> Result<GeneratedArtifacts> {
    let binary = PathBuf::from(artifact.executable.clone().unwrap());
    artifacts::add_generated_artifacts(client, &binary, log)
}

impl<'a, ProgressIndicatorT> ArtifactQueuing<'a, ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        log: slog::Logger,
        queuing_deps: &'a JobQueuingDeps,
        client: &'a Client,
        width: usize,
        ind: ProgressIndicatorT,
        artifact: CargoArtifact,
        package_name: String,
        timeout_override: Option<Option<Timeout>>,
    ) -> Result<Self> {
        let binary = PathBuf::from(artifact.executable.clone().unwrap());

        let running_tests = queuing_deps.list_action.is_none();

        let listing = list_test_cases(log.clone(), queuing_deps, &ind, &artifact, &package_name)?;

        ind.update_enqueue_status(format!("generating artifacts for {package_name}"));
        slog::debug!(
            log,
            "generating artifacts";
            "package_name" => &package_name,
            "artifact" => ?artifact);
        let generated_artifacts = running_tests
            .then(|| generate_artifacts(client, &artifact, log.clone()))
            .transpose()?;

        Ok(Self {
            log,
            queuing_deps,
            client,
            width,
            ind,
            artifact,
            binary,
            generated_artifacts,
            ignored_cases: listing.ignored_cases,
            package_name,
            cases: listing.cases.into_iter(),
            timeout_override,
        })
    }

    fn calculate_job_layers(
        &mut self,
        test_metadata: &TestMetadata,
    ) -> Result<NonEmpty<(Sha256Digest, ArtifactType)>> {
        let mut layers = test_metadata
            .layers
            .iter()
            .map(|layer| {
                slog::debug!(self.log, "adding layer"; "layer" => ?layer);
                self.client.add_layer(layer.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let artifacts = self.generated_artifacts.as_ref().unwrap();
        if test_metadata.include_shared_libraries() {
            layers.push((artifacts.deps.clone(), ArtifactType::Manifest));
        }
        layers.push((artifacts.binary.clone(), ArtifactType::Manifest));

        Ok(NonEmpty::try_from(layers).unwrap())
    }

    fn format_case_str(&self, case: &str) -> String {
        let mut s = self.package_name.to_string();
        s += " ";

        let artifact_name = &self.artifact.target.name;
        if artifact_name != &self.package_name {
            s += artifact_name;
            s += " ";
        }
        s += case;
        s
    }

    fn queue_job_from_case(&mut self, case: &str) -> Result<EnqueueResult> {
        let case_str = self.format_case_str(case);
        self.ind
            .update_enqueue_status(format!("processing {case_str}"));
        slog::debug!(self.log, "enqueuing test case"; "case" => &case_str);

        if self.queuing_deps.list_action.is_some() {
            self.ind.println(case_str);
            return Ok(EnqueueResult::Listed);
        }

        let image_lookup = |image: &str| {
            self.ind
                .update_enqueue_status(format!("downloading image {image}"));
            let (image, version) = image.split_once(':').unwrap_or((image, "latest"));
            slog::debug!(
                self.log, "getting container image";
                "image" => &image,
                "version" => &version,
            );
            let image = self.client.get_container_image(image, version)?;
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
        self.ind
            .update_enqueue_status(format!("calculating layers for {case_str}"));
        slog::debug!(&self.log, "calculating job layers"; "case" => &case_str);
        let layers = self.calculate_job_layers(&test_metadata)?;

        // N.B. Must do this before we enqueue the job, but after we know we can't fail
        let count = self.queuing_deps.jobs_queued.fetch_add(1, Ordering::AcqRel);
        self.ind.update_length(std::cmp::max(
            self.queuing_deps.expected_job_count,
            count + 1,
        ));

        let visitor = JobStatusVisitor::new(
            self.queuing_deps.tracker.clone(),
            case_str.clone(),
            self.width,
            self.ind.clone(),
        );

        if self.ignored_cases.contains(case) {
            visitor.job_ignored();
            return Ok(EnqueueResult::Ignored);
        }

        self.ind
            .update_enqueue_status(format!("submitting job for {case_str}"));
        slog::debug!(&self.log, "submitting job"; "case" => &case_str);
        let binary_name = self.binary.file_name().unwrap().to_str().unwrap();
        self.client.add_job(
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
                timeout: self.timeout_override.unwrap_or(test_metadata.timeout),
            },
            move |cjid, result| visitor.job_finished(cjid, result),
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

/// Enqueues tests as jobs in the given client.
///
/// This object is like an iterator, it maintains a position in the test listing and enqueues the
/// next thing when asked.
struct JobQueuing<'a, ProgressIndicatorT> {
    log: slog::Logger,
    queuing_deps: &'a JobQueuingDeps,
    client: &'a Client,
    width: usize,
    ind: ProgressIndicatorT,
    wait_handle: Option<WaitHandle>,
    package_match: bool,
    artifacts: Option<TestArtifactStream>,
    artifact_queuing: Option<ArtifactQueuing<'a, ProgressIndicatorT>>,
    timeout_override: Option<Option<Timeout>>,
}

impl<'a, ProgressIndicatorT: ProgressIndicator> JobQueuing<'a, ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
{
    fn new(
        log: slog::Logger,
        queuing_deps: &'a JobQueuingDeps,
        client: &'a Client,
        width: usize,
        ind: ProgressIndicatorT,
        timeout_override: Option<Option<Timeout>>,
    ) -> Result<Self> {
        let package_names: Vec<_> = queuing_deps
            .packages
            .values()
            .map(|p| format!("{}@{}", &p.name, &p.version))
            .collect();

        let building_tests = !package_names.is_empty()
            && matches!(queuing_deps.list_action, None | Some(ListAction::ListTests));

        let (wait_handle, artifacts) = building_tests
            .then(|| {
                run_cargo_test(
                    &queuing_deps.cargo,
                    queuing_deps.stderr_color,
                    &queuing_deps.feature_selection_options,
                    &queuing_deps.compilation_options,
                    &queuing_deps.manifest_options,
                    package_names,
                )
            })
            .transpose()?
            .unzip();

        Ok(Self {
            log,
            queuing_deps,
            client,
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

        slog::debug!(self.log, "getting artifact from cargo");
        let Some(ref mut artifacts) = self.artifacts else {
            return Ok(false);
        };
        let Some(artifact) = artifacts.next() else {
            return Ok(false);
        };
        let artifact = artifact?;

        slog::debug!(self.log, "got artifact"; "artifact" => ?artifact);
        let package_name = &self
            .queuing_deps
            .packages
            .get(&artifact.package_id)
            .expect("artifact for unknown package")
            .name;

        self.artifact_queuing = Some(ArtifactQueuing::new(
            self.log.clone(),
            self.queuing_deps,
            self.client,
            self.width,
            self.ind.clone(),
            artifact,
            package_name.into(),
            self.timeout_override,
        )?);

        Ok(true)
    }

    /// Meant to be called when the user has enqueued all the jobs they want. Checks for deferred
    /// errors from cargo or otherwise
    fn finish(&mut self) -> Result<()> {
        slog::debug!(self.log, "checking for cargo errors");
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

/// A collection of objects that are used to run the MainApp. This is useful as a separate object
/// since it can contain things which live longer than scoped threads and thus shared among them.
pub struct MainAppDeps {
    pub client: Client,
    queuing_deps: JobQueuingDeps,
    cache_dir: PathBuf,
    logging_output: LoggingOutput,
    log: slog::Logger,
}

impl MainAppDeps {
    /// Creates a new `MainAppDeps`
    ///
    /// `bg_proc`: handle to background client process
    /// `cargo`: the command to run when invoking cargo
    /// `include_filter`: tests which match any of the patterns in this filter are run
    /// `exclude_filter`: tests which match any of the patterns in this filter are not run
    /// `list_action`: if some, tests aren't run, instead tests or other things are listed
    /// `stderr_color`: should terminal color codes be written to `stderr` or not
    /// `workspace_root`: the path to the root of the workspace
    /// `workspace_packages`: a listing of the packages in the workspace
    /// `broker_addr`: the network address of the broker which we connect to
    /// `client_driver`: an object which drives the background work of the `Client`
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bg_proc: ClientBgProcess,
        cargo: String,
        include_filter: Vec<String>,
        exclude_filter: Vec<String>,
        list_action: Option<ListAction>,
        stderr_color: bool,
        workspace_root: &impl AsRef<Path>,
        workspace_packages: &[&CargoPackage],
        broker_addr: Option<BrokerAddr>,
        cache_size: CacheSize,
        inline_limit: InlineLimit,
        slots: Slots,
        feature_selection_options: FeatureSelectionOptions,
        compilation_options: CompilationOptions,
        manifest_options: ManifestOptions,
        logger: Logger,
    ) -> Result<Self> {
        let logging_output = LoggingOutput::default();
        let log = logger.build(logging_output.clone());
        slog::debug!(
            log, "creating app dependencies";
            "include_filter" => ?include_filter,
            "exclude_filter" => ?exclude_filter,
            "list_action" => ?list_action,
            "broker_addr" => ?broker_addr
        );

        let cache_dir = workspace_root.as_ref().join("target");
        let client = Client::new(
            bg_proc,
            broker_addr,
            workspace_root,
            cache_dir.clone(),
            cache_size,
            inline_limit,
            slots,
            log.clone(),
        )?;
        let test_metadata = AllMetadata::load(log.clone(), workspace_root)?;
        let mut test_listing =
            load_test_listing(&cache_dir.join(LAST_TEST_LISTING_NAME))?.unwrap_or_default();
        test_listing.retain_packages(workspace_packages);

        let filter = pattern::compile_filter(&include_filter, &exclude_filter)?;
        let selected_packages: BTreeMap<_, _> = workspace_packages
            .iter()
            .filter(|p| filter_package(p, &filter))
            .map(|&p| (p.id.clone(), p.clone()))
            .collect();

        slog::debug!(
            log, "filtered packages";
            "selected_packages" => ?Vec::from_iter(selected_packages.keys()),
        );

        Ok(Self {
            client,
            queuing_deps: JobQueuingDeps::new(
                cargo,
                selected_packages,
                filter,
                stderr_color,
                test_metadata,
                test_listing,
                list_action,
                feature_selection_options,
                compilation_options,
                manifest_options,
            ),
            cache_dir,
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

struct MainAppImpl<'deps, TermT, ProgressIndicatorT, ProgressDriverT> {
    deps: &'deps MainAppDeps,
    queuing: JobQueuing<'deps, ProgressIndicatorT>,
    prog_driver: ProgressDriverT,
    prog: ProgressIndicatorT,
    term: TermT,
}

impl<'deps, TermT, ProgressIndicatorT, ProgressDriverT>
    MainAppImpl<'deps, TermT, ProgressIndicatorT, ProgressDriverT>
{
    fn new(
        deps: &'deps MainAppDeps,
        queuing: JobQueuing<'deps, ProgressIndicatorT>,
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

impl<'deps, 'scope, TermT, ProgressIndicatorT, ProgressDriverT> MainApp
    for MainAppImpl<'deps, TermT, ProgressIndicatorT, ProgressDriverT>
where
    ProgressIndicatorT: ProgressIndicator,
    TermT: TermLike + Clone + 'static,
    ProgressDriverT: ProgressDriver<'scope>,
{
    fn enqueue_one(&mut self) -> Result<EnqueueResult> {
        self.queuing.enqueue_one()
    }

    fn drain(&mut self) -> Result<()> {
        slog::debug!(self.queuing.log, "draining");
        self.prog
            .update_length(self.deps.queuing_deps.jobs_queued.load(Ordering::Acquire));
        self.prog.done_queuing_jobs();
        self.prog_driver.stop()?;
        Ok(())
    }

    fn finish(&mut self) -> Result<ExitCode> {
        slog::debug!(self.queuing.log, "waiting for outstanding jobs");
        self.deps.client.wait_for_outstanding_jobs()?;
        self.prog.finished()?;

        if self.deps.queuing_deps.list_action.is_none() {
            let width = self.term.width() as usize;
            self.deps
                .queuing_deps
                .tracker
                .print_summary(width, self.term.clone())?;
        }

        write_test_listing(
            &self.deps.cache_dir.join(LAST_TEST_LISTING_NAME),
            &self.deps.queuing_deps.test_listing.lock().unwrap(),
        )?;

        Ok(self.deps.queuing_deps.tracker.exit_code())
    }
}

fn list_packages<ProgressIndicatorT>(
    ind: &ProgressIndicatorT,
    packages: &BTreeMap<PackageId, CargoPackage>,
) where
    ProgressIndicatorT: ProgressIndicator,
{
    for pkg in packages.values() {
        ind.println(format!("package {}", &pkg.name));
    }
}

fn list_binaries<ProgressIndicatorT>(
    ind: &ProgressIndicatorT,
    packages: &BTreeMap<PackageId, CargoPackage>,
) where
    ProgressIndicatorT: ProgressIndicator,
{
    for pkg in packages.values() {
        for tgt in &pkg.targets {
            if tgt.test {
                let pkg_kind = pattern::ArtifactKind::from_target(tgt);
                let mut binary_name = String::new();
                if tgt.name != pkg.name {
                    binary_name += " ";
                    binary_name += &tgt.name;
                }
                ind.println(format!(
                    "binary {}{} ({})",
                    &pkg.name, binary_name, pkg_kind
                ));
            }
        }
    }
}

#[derive(Default)]
struct LoggingOutputInner {
    prog: Option<Box<dyn io::Write + Send + Sync + 'static>>,
}

#[derive(Clone, Default)]
struct LoggingOutput {
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
    fn build(&self, out: LoggingOutput) -> slog::Logger {
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

fn new_helper<'deps, 'scope, ProgressIndicatorT, TermT>(
    deps: &'deps MainAppDeps,
    prog_factory: impl FnOnce(TermT) -> ProgressIndicatorT,
    term: TermT,
    mut prog_driver: impl ProgressDriver<'scope> + 'scope,
    timeout_override: Option<Option<Timeout>>,
) -> Result<Box<dyn MainApp + 'scope>>
where
    ProgressIndicatorT: ProgressIndicator,
    TermT: TermLike + Clone + 'static,
    'deps: 'scope,
{
    let width = term.width() as usize;
    let prog = prog_factory(term.clone());

    prog_driver.drive(&deps.client, prog.clone());
    prog.update_length(deps.queuing_deps.expected_job_count);

    deps.logging_output
        .update(progress::ProgressWriteAdapter::new(prog.clone()));
    slog::debug!(deps.log, "main app created");

    match deps.queuing_deps.list_action {
        Some(ListAction::ListPackages) => list_packages(&prog, &deps.queuing_deps.packages),

        Some(ListAction::ListBinaries) => list_binaries(&prog, &deps.queuing_deps.packages),
        _ => {}
    }

    let queuing = JobQueuing::new(
        deps.log.clone(),
        &deps.queuing_deps,
        &deps.client,
        width,
        prog.clone(),
        timeout_override,
    )?;
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
pub fn main_app_new<'deps, 'scope, TermT>(
    deps: &'deps MainAppDeps,
    stdout_tty: bool,
    quiet: Quiet,
    term: TermT,
    driver: impl ProgressDriver<'scope> + 'scope,
    timeout_override: Option<Option<Timeout>>,
) -> Result<Box<dyn MainApp + 'scope>>
where
    TermT: TermLike + Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static,
    'deps: 'scope,
{
    if deps.queuing_deps.list_action.is_some() {
        return if stdout_tty {
            Ok(new_helper(
                deps,
                TestListingProgress::new,
                term,
                driver,
                timeout_override,
            )?)
        } else {
            Ok(new_helper(
                deps,
                TestListingProgressNoSpinner::new,
                term,
                driver,
                timeout_override,
            )?)
        };
    }

    match (stdout_tty, quiet.into_inner()) {
        (true, true) => Ok(new_helper(
            deps,
            QuietProgressBar::new,
            term,
            driver,
            timeout_override,
        )?),
        (true, false) => Ok(new_helper(
            deps,
            MultipleProgressBars::new,
            term,
            driver,
            timeout_override,
        )?),
        (false, true) => Ok(new_helper(
            deps,
            QuietNoBar::new,
            term,
            driver,
            timeout_override,
        )?),
        (false, false) => Ok(new_helper(
            deps,
            NoBar::new,
            term,
            driver,
            timeout_override,
        )?),
    }
}
