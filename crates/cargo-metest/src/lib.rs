use anyhow::{anyhow, Result};
use cargo::{get_cases_from_binary, CargoBuild, TestArtifactStream};
use cargo_metadata::Artifact as CargoArtifact;
use config::Quiet;
use indicatif::TermLike;
use metadata::{AllMetadata, TestMetadata};
use meticulous_base::{JobSpec, NonEmpty, Sha256Digest};
use meticulous_client::{Client, ClientDriver};
use meticulous_util::{config::BrokerAddr, fs::Fs, process::ExitCode};
use progress::{MultipleProgressBars, NoBar, ProgressIndicator, QuietNoBar, QuietProgressBar};
use std::{
    collections::HashSet,
    env, io,
    path::{Path, PathBuf},
    str,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};
use visitor::{JobStatusTracker, JobStatusVisitor};

pub mod artifacts;
pub mod cargo;
pub mod config;
pub mod metadata;
pub mod progress;
pub mod substitute;
pub mod visitor;

struct JobQueuer<StdErr> {
    cargo: String,
    package: Option<String>,
    filter: Option<String>,
    stderr: Mutex<StdErr>,
    stderr_color: bool,
    tracker: Arc<JobStatusTracker>,
    jobs_queued: AtomicU64,
    test_metadata: AllMetadata,
    expected_job_count: Option<u64>,
}

impl<StdErr> JobQueuer<StdErr> {
    fn new(
        cargo: String,
        package: Option<String>,
        filter: Option<String>,
        stderr: StdErr,
        stderr_color: bool,
        test_metadata: AllMetadata,
        expected_job_count: Option<u64>,
    ) -> Self {
        Self {
            cargo,
            package,
            filter,
            stderr: Mutex::new(stderr),
            stderr_color,
            tracker: Arc::new(JobStatusTracker::default()),
            jobs_queued: AtomicU64::new(0),
            test_metadata,
            expected_job_count,
        }
    }
}

fn collect_environment_vars() -> Vec<String> {
    let mut env = vec![];
    for (key, value) in env::vars() {
        if key.starts_with("RUST_") {
            env.push(format!("{key}={value}"));
        }
    }
    env
}

struct ArtifactQueing<'a, StdErr, ProgressIndicatorT> {
    job_queuer: &'a JobQueuer<StdErr>,
    client: &'a Mutex<Client>,
    width: usize,
    ind: ProgressIndicatorT,
    binary: PathBuf,
    binary_artifact: Sha256Digest,
    deps_artifact: Sha256Digest,
    ignored_cases: HashSet<String>,
    package_name: String,
    cases: <Vec<String> as IntoIterator>::IntoIter,
}

impl<'a, StdErr, ProgressIndicatorT> ArtifactQueing<'a, StdErr, ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
{
    fn new(
        job_queuer: &'a JobQueuer<StdErr>,
        client: &'a Mutex<Client>,
        width: usize,
        ind: ProgressIndicatorT,
        artifact: CargoArtifact,
        package_name: String,
    ) -> Result<Self> {
        ind.update_enqueue_status(format!("processing {package_name}"));
        let binary = PathBuf::from(artifact.executable.unwrap());
        let ignored_cases: HashSet<_> = get_cases_from_binary(&binary, &Some("--ignored".into()))?
            .into_iter()
            .collect();

        let (binary_artifact, deps_artifact) = artifacts::add_generated_artifacts(client, &binary)?;

        let cases = get_cases_from_binary(&binary, &job_queuer.filter)?;

        Ok(Self {
            job_queuer,
            client,
            width,
            ind,
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
        let mut layers = vec![];
        for layer in &test_metadata.layers {
            let mut client = self.client.lock().unwrap();
            if layer.starts_with("docker:") {
                let pkg = layer.split(':').nth(1).unwrap();
                let prog = self.ind.new_container_progress();
                layers.extend(client.add_container(pkg, "latest", prog)?);
            } else {
                layers.push(client.add_artifact(PathBuf::from(layer).as_path())?);
            }
        }

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

        let test_metadata = self
            .job_queuer
            .test_metadata
            .get_metadata_for_test_with_env(&self.package_name, case)?;
        let layers = self.calculate_job_layers(&test_metadata)?;

        // N.B. Must do this before we enqueue the job, but after we know we can't fail
        let count = self.job_queuer.jobs_queued.fetch_add(1, Ordering::AcqRel);
        self.ind.update_length(std::cmp::max(
            self.job_queuer.expected_job_count.unwrap_or(0),
            count + 1,
        ));

        let visitor = JobStatusVisitor::new(
            self.job_queuer.tracker.clone(),
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
                program: format!("/{binary_name}"),
                arguments: vec!["--exact".into(), "--nocapture".into(), case.into()],
                environment: collect_environment_vars(),
                layers,
                devices: test_metadata.devices,
                mounts: test_metadata.mounts,
                enable_loopback: test_metadata.enable_loopback,
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

    fn enqueue_one(&mut self) -> Result<EnqueueResult> {
        let Some(case) = self.cases.next() else {
            return Ok(EnqueueResult::Done);
        };
        self.queue_job_from_case(&case)
    }
}

struct JobQueing<'a, StdErr, ProgressIndicatorT> {
    job_queuer: &'a JobQueuer<StdErr>,
    client: &'a Mutex<Client>,
    width: usize,
    ind: ProgressIndicatorT,
    cargo_build: Option<CargoBuild>,
    package_match: bool,
    artifacts: TestArtifactStream,
    artifact_queing: Option<ArtifactQueing<'a, StdErr, ProgressIndicatorT>>,
}

impl<'a, StdErr, ProgressIndicatorT: ProgressIndicator> JobQueing<'a, StdErr, ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
    StdErr: io::Write,
{
    fn new(
        job_queuer: &'a JobQueuer<StdErr>,
        client: &'a Mutex<Client>,
        width: usize,
        ind: ProgressIndicatorT,
    ) -> Result<Self> {
        let mut cargo_build = CargoBuild::new(
            &job_queuer.cargo,
            job_queuer.stderr_color,
            job_queuer.package.clone(),
        )?;
        Ok(Self {
            job_queuer,
            client,
            width,
            ind,
            package_match: false,
            artifacts: cargo_build.artifact_stream(),
            artifact_queing: None,
            cargo_build: Some(cargo_build),
        })
    }

    fn start_queuing_from_artifact(&mut self) -> Result<bool> {
        let mut stream = (&mut self.artifacts).filter(|artifact_res: &Result<CargoArtifact>| {
            artifact_res.is_err()
                || artifact_res.as_ref().is_ok_and(|artifact| {
                    let package_name = artifact.package_id.repr.split(' ').next().unwrap().into();
                    let filtered_package = self.job_queuer.package.as_ref();
                    filtered_package.is_none() || Some(&package_name) == filtered_package
                })
        });
        let Some(artifact) = stream.next() else {
            return Ok(false);
        };
        let artifact = artifact?;

        let package_name = artifact.package_id.repr.split(' ').next().unwrap().into();
        self.artifact_queing = Some(ArtifactQueing::new(
            self.job_queuer,
            self.client,
            self.width,
            self.ind.clone(),
            artifact,
            package_name,
        )?);

        Ok(true)
    }

    fn finish(&mut self) -> Result<()> {
        self.cargo_build
            .take()
            .unwrap()
            .check_status(&mut *self.job_queuer.stderr.lock().unwrap())?;

        if let Some(package) = &self.job_queuer.package {
            if !self.package_match {
                return Err(anyhow!("package {package:?} unknown"));
            }
        }
        Ok(())
    }

    fn enqueue_one(&mut self) -> Result<EnqueueResult> {
        if self.artifact_queing.is_none() && !self.start_queuing_from_artifact()? {
            self.finish()?;
            return Ok(EnqueueResult::Done);
        }
        self.package_match = true;

        let res = self.artifact_queing.as_mut().unwrap().enqueue_one()?;
        if res.is_done() {
            self.artifact_queing = None;
            return self.enqueue_one();
        }

        Ok(res)
    }
}

const EXPECTED_JOB_COUNT_NAME: &str = ".meticulous_expected_job_count";

fn load_expected_job_count(path: &Path) -> Result<Option<u64>> {
    let fs = Fs::new();
    if let Some(contents) = fs.read_to_string_if_exists(path)? {
        Ok(Some(contents.trim().parse()?))
    } else {
        Ok(None)
    }
}

fn write_expected_job_count(path: &Path, count: u64) -> Result<()> {
    let fs = Fs::new();
    fs.write(path, count.to_string().as_bytes())?;
    Ok(())
}

pub struct MainAppDeps<StdErr> {
    pub client: Mutex<Client>,
    queuer: JobQueuer<StdErr>,
    cache_dir: PathBuf,
}

impl<StdErr> MainAppDeps<StdErr> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cargo: String,
        package: Option<String>,
        filter: Option<String>,
        stderr: StdErr,
        stderr_color: bool,
        workspace_root: &impl AsRef<Path>,
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
        let expected_job_count = load_expected_job_count(&cache_dir.join(EXPECTED_JOB_COUNT_NAME))?;

        Ok(Self {
            client,
            queuer: JobQueuer::new(
                cargo,
                package,
                filter,
                stderr,
                stderr_color,
                test_metadata,
                expected_job_count,
            ),
            cache_dir,
        })
    }
}

pub trait ProgressDriver<'scope> {
    fn drive<'dep, ProgressIndicatorT>(
        &mut self,
        client: &'dep Mutex<Client>,
        ind: ProgressIndicatorT,
    ) where
        ProgressIndicatorT: ProgressIndicator,
        'dep: 'scope;

    fn stop(&mut self) -> Result<()>;
}

pub struct DefaultProgressDriver<'scope, 'env> {
    scope: &'scope thread::Scope<'scope, 'env>,
    handle: Option<thread::ScopedJoinHandle<'scope, Result<()>>>,
}

impl<'scope, 'env> DefaultProgressDriver<'scope, 'env> {
    pub fn new(scope: &'scope thread::Scope<'scope, 'env>) -> Self {
        Self {
            scope,
            handle: None,
        }
    }
}

impl<'scope, 'env> ProgressDriver<'scope> for DefaultProgressDriver<'scope, 'env> {
    fn drive<'dep, ProgressIndicatorT>(
        &mut self,
        client: &'dep Mutex<Client>,
        ind: ProgressIndicatorT,
    ) where
        ProgressIndicatorT: ProgressIndicator,
        'dep: 'scope,
    {
        self.handle = Some(self.scope.spawn(move || {
            loop {
                let counts = client.lock().unwrap().get_job_state_counts_async()?;
                if !ind.update_job_states(counts.recv()?)? {
                    break;
                }

                // Don't hammer server with requests, also this function updates the spinner
                thread::sleep(Duration::from_millis(500));
            }
            Ok(())
        }));
    }

    fn stop(&mut self) -> Result<()> {
        self.handle.take().unwrap().join().unwrap()
    }
}

pub enum EnqueueResult {
    Enqueued { package_name: String, case: String },
    Ignored,
    Done,
}

impl EnqueueResult {
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }

    pub fn is_ignored(&self) -> bool {
        matches!(self, Self::Ignored)
    }
}

// This trait exists only for type-erasure purposes
pub trait MainApp {
    fn enqueue_one(&mut self) -> Result<EnqueueResult>;
    fn drain(&mut self) -> Result<()>;
    fn finish(&mut self) -> Result<ExitCode>;
}

struct MainAppImpl<'deps, StdErr, Term, ProgressIndicatorT, ProgressDriverT> {
    deps: &'deps MainAppDeps<StdErr>,
    queing: JobQueing<'deps, StdErr, ProgressIndicatorT>,
    prog_driver: ProgressDriverT,
    prog: ProgressIndicatorT,
    term: Term,
}

impl<'deps, StdErr, Term, ProgressIndicatorT, ProgressDriverT>
    MainAppImpl<'deps, StdErr, Term, ProgressIndicatorT, ProgressDriverT>
{
    fn new(
        deps: &'deps MainAppDeps<StdErr>,
        queing: JobQueing<'deps, StdErr, ProgressIndicatorT>,
        prog_driver: ProgressDriverT,
        prog: ProgressIndicatorT,
        term: Term,
    ) -> Self {
        Self {
            deps,
            queing,
            prog_driver,
            prog,
            term,
        }
    }
}

impl<'deps, 'scope, StdErr, Term, ProgressIndicatorT, ProgressDriverT> MainApp
    for MainAppImpl<'deps, StdErr, Term, ProgressIndicatorT, ProgressDriverT>
where
    StdErr: io::Write + Send,
    ProgressIndicatorT: ProgressIndicator,
    Term: TermLike + Clone + 'static,
    ProgressDriverT: ProgressDriver<'scope>,
{
    fn enqueue_one(&mut self) -> Result<EnqueueResult> {
        self.queing.enqueue_one()
    }

    fn drain(&mut self) -> Result<()> {
        self.prog
            .update_length(self.deps.queuer.jobs_queued.load(Ordering::Acquire));
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
            .queuer
            .tracker
            .print_summary(width, self.term.clone())?;

        write_expected_job_count(
            &self.deps.cache_dir.join(EXPECTED_JOB_COUNT_NAME),
            self.deps.queuer.jobs_queued.load(Ordering::Acquire),
        )?;

        Ok(self.deps.queuer.tracker.exit_code())
    }
}

fn new_helper<'deps, 'scope, StdErr, ProgressIndicatorT, Term>(
    deps: &'deps MainAppDeps<StdErr>,
    prog_factory: impl FnOnce(Term) -> ProgressIndicatorT,
    term: Term,
    mut prog_driver: impl ProgressDriver<'scope> + 'scope,
) -> Result<Box<dyn MainApp + 'scope>>
where
    StdErr: io::Write + Send,
    ProgressIndicatorT: ProgressIndicator,
    Term: TermLike + Clone + 'static,
    'deps: 'scope,
{
    let width = term.width() as usize;
    let prog = prog_factory(term.clone());

    prog_driver.drive(&deps.client, prog.clone());

    if let Some(len) = &deps.queuer.expected_job_count {
        prog.update_length(*len);
    }

    let queing = JobQueing::new(&deps.queuer, &deps.client, width, prog.clone())?;
    Ok(Box::new(MainAppImpl::new(
        deps,
        queing,
        prog_driver,
        prog,
        term,
    )))
}

pub fn main_app_new<'deps, 'scope, Term, StdErr>(
    deps: &'deps MainAppDeps<StdErr>,
    stdout_tty: bool,
    quiet: Quiet,
    term: Term,
    driver: impl ProgressDriver<'scope> + 'scope,
) -> Result<Box<dyn MainApp + 'scope>>
where
    StdErr: io::Write + Send,
    Term: TermLike + Clone + Send + Sync + 'static,
    'deps: 'scope,
{
    match (stdout_tty, quiet.into_inner()) {
        (true, true) => Ok(new_helper(deps, QuietProgressBar::new, term, driver)?),
        (true, false) => Ok(new_helper(deps, MultipleProgressBars::new, term, driver)?),
        (false, true) => Ok(new_helper(deps, QuietNoBar::new, term, driver)?),
        (false, false) => Ok(new_helper(deps, NoBar::new, term, driver)?),
    }
}
