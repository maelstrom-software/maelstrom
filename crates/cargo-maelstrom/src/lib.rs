pub mod alternative_mains;
pub mod artifacts;
pub mod cargo;
pub mod cli;
pub mod config;
pub mod metadata;
pub mod pattern;
pub mod progress;
pub mod test_listing;
pub mod visitor;

#[cfg(test)]
mod tests;

use anyhow::{anyhow, bail, Context as _, Result};
use artifacts::GeneratedArtifacts;
use cargo::{CompilationOptions, FeatureSelectionOptions, ManifestOptions};
use cargo_metadata::{
    Artifact as CargoArtifact, Metadata as CargoMetadata, Package as CargoPackage, PackageId,
};
use config::Quiet;
use indicatif::TermLike;
use maelstrom_base::{ArtifactType, ClientJobId, JobOutcomeResult, Sha256Digest, Timeout};
use maelstrom_client::{
    spec::{JobSpec, Layer},
    CacheDir, Client, ClientBgProcess, ContainerImageDepotDir, IntrospectResponse, ProjectDir,
    StateDir,
};
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, LogLevel, Slots},
    fs::Fs,
    process::ExitCode,
    root::Root,
    template::TemplateVars,
};
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
    path::{Path, PathBuf},
    str,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use test_listing::{ArtifactKey, TestListing, TestListingStore};
use visitor::{JobStatusTracker, JobStatusVisitor};

#[derive(Debug)]
pub enum ListAction {
    ListTests,
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

fn do_template_replacement(
    test_metadata: &mut AllMetadata,
    compilation_options: &CompilationOptions,
    target_dir: &Root<TargetDir>,
) -> Result<()> {
    let profile = compilation_options.profile.clone().unwrap_or("dev".into());
    let mut target = (**target_dir).to_owned();
    match profile.as_str() {
        "dev" => target.push("debug"),
        other => target.push(other),
    }
    let build_dir = target
        .to_str()
        .ok_or_else(|| anyhow!("{} contains non-UTF8", target.display()))?;
    let vars = TemplateVars::new()
        .with_var("build-dir", build_dir)
        .unwrap();
    test_metadata.replace_template_vars(&vars)?;
    Ok(())
}

/// A collection of objects that are used while enqueuing jobs. This is useful as a separate object
/// since it can contain things which live longer than the scoped threads and thus can be shared
/// among them.
///
/// This object is separate from `MainAppState` because it is lent to `JobQueuing`
struct JobQueuingState {
    packages: BTreeMap<PackageId, CargoPackage>,
    filter: pattern::Pattern,
    stderr_color: bool,
    tracker: Arc<JobStatusTracker>,
    jobs_queued: AtomicU64,
    test_metadata: AllMetadata,
    expected_job_count: u64,
    test_listing: Arc<Mutex<Option<TestListing>>>,
    list_action: Option<ListAction>,
    feature_selection_options: FeatureSelectionOptions,
    compilation_options: CompilationOptions,
    manifest_options: ManifestOptions,
}

impl JobQueuingState {
    #[allow(clippy::too_many_arguments)]
    fn new(
        packages: BTreeMap<PackageId, CargoPackage>,
        filter: pattern::Pattern,
        stderr_color: bool,
        mut test_metadata: AllMetadata,
        test_listing: TestListing,
        list_action: Option<ListAction>,
        target_dir: impl AsRef<Root<TargetDir>>,
        feature_selection_options: FeatureSelectionOptions,
        compilation_options: CompilationOptions,
        manifest_options: ManifestOptions,
    ) -> Result<Self> {
        let expected_job_count = test_listing.expected_job_count(&filter);
        do_template_replacement(
            &mut test_metadata,
            &compilation_options,
            target_dir.as_ref(),
        )?;

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
            feature_selection_options,
            compilation_options,
            manifest_options,
        })
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
struct ArtifactQueuing<'a, ProgressIndicatorT, MainAppDepsT> {
    log: slog::Logger,
    queuing_state: &'a JobQueuingState,
    deps: &'a MainAppDepsT,
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

fn list_test_cases(
    deps: &impl MainAppDeps,
    log: slog::Logger,
    queuing_state: &JobQueuingState,
    ind: &impl ProgressIndicator,
    artifact: &CargoArtifact,
    package_name: &str,
) -> Result<TestListingResult> {
    ind.update_enqueue_status(format!("getting test list for {package_name}"));

    slog::debug!(log, "listing ignored tests"; "binary" => ?artifact.executable);
    let binary = PathBuf::from(artifact.executable.clone().unwrap());
    let ignored_cases: HashSet<_> = deps
        .get_cases_from_binary(&binary, &Some("--ignored".into()))?
        .into_iter()
        .collect();

    slog::debug!(log, "listing tests"; "binary" => ?artifact.executable);
    let mut cases = deps.get_cases_from_binary(&binary, &None)?;

    let mut listing = queuing_state.test_listing.lock().unwrap();
    listing
        .as_mut()
        .unwrap()
        .update_artifact_cases(package_name, &artifact.target, &cases);

    cases.retain(|c| filter_case(package_name, artifact, c, &queuing_state.filter));
    Ok(TestListingResult {
        cases,
        ignored_cases,
    })
}

fn generate_artifacts(
    deps: &impl MainAppDeps,
    artifact: &CargoArtifact,
    log: slog::Logger,
) -> Result<GeneratedArtifacts> {
    let binary = PathBuf::from(artifact.executable.clone().unwrap());
    artifacts::add_generated_artifacts(deps, &binary, log)
}

impl<'a, ProgressIndicatorT, MainAppDepsT> ArtifactQueuing<'a, ProgressIndicatorT, MainAppDepsT>
where
    ProgressIndicatorT: ProgressIndicator,
    MainAppDepsT: MainAppDeps,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        log: slog::Logger,
        queuing_state: &'a JobQueuingState,
        deps: &'a MainAppDepsT,
        width: usize,
        ind: ProgressIndicatorT,
        artifact: CargoArtifact,
        package_name: String,
        timeout_override: Option<Option<Timeout>>,
    ) -> Result<Self> {
        let binary = PathBuf::from(artifact.executable.clone().unwrap());

        let running_tests = queuing_state.list_action.is_none();

        let listing = list_test_cases(
            deps,
            log.clone(),
            queuing_state,
            &ind,
            &artifact,
            &package_name,
        )?;

        ind.update_enqueue_status(format!("generating artifacts for {package_name}"));
        slog::debug!(
            log,
            "generating artifacts";
            "package_name" => &package_name,
            "artifact" => ?artifact);
        let generated_artifacts = running_tests
            .then(|| generate_artifacts(deps, &artifact, log.clone()))
            .transpose()?;

        Ok(Self {
            log,
            queuing_state,
            deps,
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
    ) -> Result<Vec<(Sha256Digest, ArtifactType)>> {
        let mut layers = test_metadata
            .layers
            .iter()
            .map(|layer| {
                slog::debug!(self.log, "adding layer"; "layer" => ?layer);
                self.deps.add_layer(layer.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let artifacts = self.generated_artifacts.as_ref().unwrap();
        if test_metadata.include_shared_libraries() {
            layers.push((artifacts.deps.clone(), ArtifactType::Manifest));
        }
        layers.push((artifacts.binary.clone(), ArtifactType::Manifest));

        Ok(layers)
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

        if self.queuing_state.list_action.is_some() {
            self.ind.lock_printing().println(case_str);
            return Ok(EnqueueResult::Listed);
        }

        let filter_context = pattern::Context {
            package: self.package_name.clone(),
            artifact: Some(pattern::Artifact::from_target(&self.artifact.target)),
            case: Some(pattern::Case { name: case.into() }),
        };

        let test_metadata = self
            .queuing_state
            .test_metadata
            .get_metadata_for_test_with_env(&filter_context)?;
        self.ind
            .update_enqueue_status(format!("calculating layers for {case_str}"));
        slog::debug!(&self.log, "calculating job layers"; "case" => &case_str);
        let layers = self.calculate_job_layers(&test_metadata)?;

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
            ArtifactKey::from(&self.artifact.target),
            case.to_owned(),
            case_str.clone(),
            self.width,
            self.ind.clone(),
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
            .get_timing(
                &self.package_name,
                &ArtifactKey::from(&self.artifact.target),
                case,
            );

        self.ind
            .update_enqueue_status(format!("submitting job for {case_str}"));
        slog::debug!(&self.log, "submitting job"; "case" => &case_str);
        let binary_name = self.binary.file_name().unwrap().to_str().unwrap();
        self.deps.add_job(
            JobSpec {
                program: format!("/{binary_name}").into(),
                arguments: vec!["--exact".into(), "--nocapture".into(), case.into()],
                image: test_metadata.image,
                environment: test_metadata.environment,
                layers,
                devices: test_metadata.devices,
                mounts: test_metadata.mounts,
                network: test_metadata.network,
                enable_writable_file_system: test_metadata.enable_writable_file_system,
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
    queuing_state: &'a JobQueuingState,
    deps: &'a MainAppDepsT,
    width: usize,
    ind: ProgressIndicatorT,
    wait_handle: Option<MainAppDepsT::CargoWaitHandle>,
    package_match: bool,
    artifacts: Option<MainAppDepsT::CargoTestArtifactStream>,
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
        queuing_state: &'a JobQueuingState,
        deps: &'a MainAppDepsT,
        width: usize,
        ind: ProgressIndicatorT,
        timeout_override: Option<Option<Timeout>>,
    ) -> Result<Self> {
        let package_names: Vec<_> = queuing_state
            .packages
            .values()
            .map(|p| format!("{}@{}", &p.name, &p.version))
            .collect();

        let building_tests = !package_names.is_empty()
            && matches!(
                queuing_state.list_action,
                None | Some(ListAction::ListTests)
            );

        let (wait_handle, artifacts) = building_tests
            .then(|| {
                deps.run_cargo_test(
                    queuing_state.stderr_color,
                    &queuing_state.feature_selection_options,
                    &queuing_state.compilation_options,
                    &queuing_state.manifest_options,
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
            .queuing_state
            .packages
            .get(&artifact.package_id)
            .expect("artifact for unknown package")
            .name;

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

pub trait Wait {
    fn wait(self) -> Result<()>;
}

impl Wait for cargo::WaitHandle {
    fn wait(self) -> Result<()> {
        cargo::WaitHandle::wait(self)
    }
}

pub trait MainAppDeps: Sync {
    fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)>;

    fn introspect(&self) -> Result<IntrospectResponse>;

    fn add_job(
        &self,
        spec: JobSpec,
        handler: impl FnOnce(Result<(ClientJobId, JobOutcomeResult)>) + Send + Sync + 'static,
    ) -> Result<()>;

    type CargoWaitHandle: Wait;
    type CargoTestArtifactStream: Iterator<Item = Result<CargoArtifact>>;

    fn run_cargo_test(
        &self,
        color: bool,
        feature_selection_options: &FeatureSelectionOptions,
        compilation_options: &CompilationOptions,
        manifest_options: &ManifestOptions,
        packages: Vec<String>,
    ) -> Result<(Self::CargoWaitHandle, Self::CargoTestArtifactStream)>;

    fn get_cases_from_binary(&self, binary: &Path, filter: &Option<String>) -> Result<Vec<String>>;
}

pub struct DefaultMainAppDeps {
    client: Client,
}

/// The workspace directory is the top-level Cargo workspace directory. If workspaces aren't
/// explicilty being used, it's the top-level directory for the package.
pub struct WorkspaceDir;

/// The target directory is usually <workspace-dir>/target, but Cargo lets "target" be overridden.
pub struct TargetDir;

/// The Maelstrom target directory is <target-dir>/maelstrom.
pub struct MaelstromTargetDir;

impl DefaultMainAppDeps {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bg_proc: ClientBgProcess,
        broker_addr: Option<BrokerAddr>,
        project_dir: impl AsRef<Root<ProjectDir>>,
        state_dir: impl AsRef<Root<StateDir>>,
        container_image_depot_dir: impl AsRef<Root<ContainerImageDepotDir>>,
        cache_dir: impl AsRef<Root<CacheDir>>,
        cache_size: CacheSize,
        inline_limit: InlineLimit,
        slots: Slots,
        log: slog::Logger,
    ) -> Result<Self> {
        let project_dir = project_dir.as_ref();
        let state_dir = state_dir.as_ref();
        let container_image_depot_dir = container_image_depot_dir.as_ref();
        let cache_dir = cache_dir.as_ref();
        slog::debug!(
            log, "creating app dependencies";
            "broker_addr" => ?broker_addr,
            "project_dir" => ?project_dir,
            "state_dir" => ?state_dir,
            "container_image_depot_dir" => ?container_image_depot_dir,
            "cache_dir" => ?cache_dir,
            "cache_size" => ?cache_size,
            "inline_limit" => ?inline_limit,
            "slots" => ?slots,
        );
        let client = Client::new(
            bg_proc,
            broker_addr,
            project_dir,
            state_dir,
            container_image_depot_dir,
            cache_dir,
            cache_size,
            inline_limit,
            slots,
            log,
        )?;
        Ok(Self { client })
    }
}

impl MainAppDeps for DefaultMainAppDeps {
    fn add_layer(&self, layer: Layer) -> Result<(Sha256Digest, ArtifactType)> {
        self.client.add_layer(layer)
    }

    fn introspect(&self) -> Result<IntrospectResponse> {
        self.client.introspect()
    }

    fn add_job(
        &self,
        spec: JobSpec,
        handler: impl FnOnce(Result<(ClientJobId, JobOutcomeResult)>) + Send + Sync + 'static,
    ) -> Result<()> {
        self.client.add_job(spec, handler)
    }

    type CargoWaitHandle = cargo::WaitHandle;
    type CargoTestArtifactStream = cargo::TestArtifactStream;

    fn run_cargo_test(
        &self,
        color: bool,
        feature_selection_options: &FeatureSelectionOptions,
        compilation_options: &CompilationOptions,
        manifest_options: &ManifestOptions,
        packages: Vec<String>,
    ) -> Result<(cargo::WaitHandle, cargo::TestArtifactStream)> {
        cargo::run_cargo_test(
            color,
            feature_selection_options,
            compilation_options,
            manifest_options,
            packages,
        )
    }

    fn get_cases_from_binary(&self, binary: &Path, filter: &Option<String>) -> Result<Vec<String>> {
        cargo::get_cases_from_binary(binary, filter)
    }
}

/// A collection of objects that are used to run the MainApp. This is useful as a separate object
/// since it can contain things which live longer than scoped threads and thus shared among them.
pub struct MainAppState<MainAppDepsT> {
    deps: MainAppDepsT,
    queuing_state: JobQueuingState,
    test_listing_store: TestListingStore,
    logging_output: LoggingOutput,
    log: slog::Logger,
}

impl<MainAppDepsT> MainAppState<MainAppDepsT> {
    /// Creates a new `MainAppState`
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
        deps: MainAppDepsT,
        include_filter: Vec<String>,
        exclude_filter: Vec<String>,
        list_action: Option<ListAction>,
        stderr_color: bool,
        workspace_root: impl AsRef<Root<WorkspaceDir>>,
        workspace_packages: &[&CargoPackage],
        state_dir: impl AsRef<Root<StateDir>>,
        target_directory: impl AsRef<Root<TargetDir>>,
        feature_selection_options: FeatureSelectionOptions,
        compilation_options: CompilationOptions,
        manifest_options: ManifestOptions,
        logging_output: LoggingOutput,
        log: slog::Logger,
    ) -> Result<Self> {
        slog::debug!(
            log, "creating app state";
            "include_filter" => ?include_filter,
            "exclude_filter" => ?exclude_filter,
            "list_action" => ?list_action,
        );

        let test_metadata = AllMetadata::load(log.clone(), workspace_root)?;
        let test_listing_store = TestListingStore::new(Fs::new(), &state_dir);
        let mut test_listing = test_listing_store.load()?;
        test_listing.retain_packages_and_artifacts(
            workspace_packages.iter().map(|p| (&*p.name, &p.targets)),
        );

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
            deps,
            queuing_state: JobQueuingState::new(
                selected_packages,
                filter,
                stderr_color,
                test_metadata,
                test_listing,
                list_action,
                target_directory,
                feature_selection_options,
                compilation_options,
                manifest_options,
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

    prog_driver.drive(&state.deps, prog.clone());
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

fn maybe_print_build_error(res: Result<ExitCode>) -> Result<ExitCode> {
    if let Err(e) = &res {
        if let Some(e) = e.downcast_ref::<cargo::CargoBuildError>() {
            eprintln!("{}", &e.stderr);
            return Ok(e.exit_code);
        }
    }
    res
}

fn read_cargo_metadata(config: &config::Config) -> Result<CargoMetadata> {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version=1"])
        .args(config.cargo_feature_selection_options.iter())
        .args(config.cargo_manifest_options.iter())
        .output()
        .context("getting cargo metadata")?;
    if !output.status.success() {
        bail!(String::from_utf8(output.stderr)
            .context("reading stderr")?
            .trim_end()
            .trim_start_matches("error: ")
            .to_owned());
    }
    let cargo_metadata: CargoMetadata =
        serde_json::from_slice(&output.stdout).context("parsing cargo metadata")?;
    Ok(cargo_metadata)
}

pub fn main<TermT>(
    config: config::Config,
    extra_options: cli::ExtraCommandLineOptions,
    bg_proc: ClientBgProcess,
    logger: Logger,
    stderr_is_tty: bool,
    stdout_is_tty: bool,
    terminal: TermT,
) -> Result<ExitCode>
where
    TermT: TermLike + Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static,
{
    let cargo_metadata = read_cargo_metadata(&config)?;
    if extra_options.test_metadata.init {
        alternative_mains::init(&cargo_metadata.workspace_root)
    } else if extra_options.list.packages {
        alternative_mains::list_packages(
            &cargo_metadata.workspace_packages(),
            &extra_options.include,
            &extra_options.exclude,
            &mut io::stdout().lock(),
        )
    } else if extra_options.list.binaries {
        alternative_mains::list_binaries(
            &cargo_metadata.workspace_packages(),
            &extra_options.include,
            &extra_options.exclude,
            &mut io::stdout().lock(),
        )
    } else {
        let workspace_dir = Root::<WorkspaceDir>::new(cargo_metadata.workspace_root.as_std_path());
        let logging_output = LoggingOutput::default();
        let log = logger.build(logging_output.clone());

        let list_action = match (extra_options.list.tests, extra_options.list.binaries) {
            (true, _) => Some(ListAction::ListTests),
            (_, _) => None,
        };

        let target_dir = Root::<TargetDir>::new(cargo_metadata.target_directory.as_std_path());
        let maelstrom_target_dir = target_dir.join::<MaelstromTargetDir>("maelstrom");
        let state_dir = maelstrom_target_dir.join::<StateDir>("state");
        let cache_dir = maelstrom_target_dir.join::<CacheDir>("cache");

        Fs.create_dir_all(&state_dir)?;
        Fs.create_dir_all(&cache_dir)?;

        let deps = DefaultMainAppDeps::new(
            bg_proc,
            config.broker,
            workspace_dir.transmute::<ProjectDir>(),
            &state_dir,
            config.container_image_depot_root,
            cache_dir,
            config.cache_size,
            config.inline_limit,
            config.slots,
            log.clone(),
        )?;

        let state = MainAppState::new(
            deps,
            extra_options.include,
            extra_options.exclude,
            list_action,
            stderr_is_tty,
            workspace_dir,
            &cargo_metadata.workspace_packages(),
            &state_dir,
            target_dir,
            config.cargo_feature_selection_options,
            config.cargo_compilation_options,
            config.cargo_manifest_options,
            logging_output,
            log,
        )?;

        let res = std::thread::scope(|scope| {
            let mut app = main_app_new(
                &state,
                stdout_is_tty,
                config.quiet,
                terminal,
                progress::DefaultProgressDriver::new(scope),
                config.timeout.map(Timeout::new),
            )?;
            while !app.enqueue_one()?.is_done() {}
            app.drain()?;
            app.finish()
        });
        drop(state);
        maybe_print_build_error(res)
    }
}
