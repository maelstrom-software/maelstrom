use anyhow::{anyhow, Result};
use cargo::{get_cases_from_binary, CargoBuild};
use cargo_metadata::Artifact as CargoArtifact;
use config::Quiet;
use indicatif::TermLike;
use metadata::{AllMetadata, TestMetadata};
use meticulous_base::{JobSpec, NonEmpty, Sha256Digest};
use meticulous_client::{Client, DefaultClientDriver};
use meticulous_util::{config::BrokerAddr, process::ExitCode};
use progress::{
    MultipleProgressBars, NoBar, ProgressIndicator, ProgressIndicatorScope, QuietNoBar,
    QuietProgressBar,
};
use std::{
    collections::HashSet,
    io,
    path::{Path, PathBuf},
    str,
    sync::{Arc, Mutex},
};
use visitor::{JobStatusTracker, JobStatusVisitor};

pub mod artifacts;
pub mod cargo;
pub mod config;
pub mod metadata;
pub mod progress;
pub mod visitor;

struct JobQueuer<StdErr> {
    cargo: String,
    package: Option<String>,
    filter: Option<String>,
    stderr: StdErr,
    stderr_color: bool,
    tracker: Arc<JobStatusTracker>,
    jobs_queued: u64,
    test_metadata: AllMetadata,
}

impl<StdErr> JobQueuer<StdErr> {
    fn new(
        cargo: String,
        package: Option<String>,
        filter: Option<String>,
        stderr: StdErr,
        stderr_color: bool,
        test_metadata: AllMetadata,
    ) -> Self {
        Self {
            cargo,
            package,
            filter,
            stderr,
            stderr_color,
            tracker: Arc::new(JobStatusTracker::default()),
            jobs_queued: 0,
            test_metadata,
        }
    }
}

fn collect_environment_vars() -> Vec<String> {
    let mut env = vec![];
    for (key, value) in std::env::vars() {
        if key.starts_with("RUST_") {
            env.push(format!("{key}={value}"));
        }
    }
    env
}

struct ArtifactQueing<'a, ProgressIndicatorT> {
    client: &'a Mutex<Client>,
    width: usize,
    ind: ProgressIndicatorT,
    binary: PathBuf,
    binary_artifact: Sha256Digest,
    deps_artifact: Sha256Digest,
    ignored_cases: HashSet<String>,
    package_name: String,
    tracker: Arc<JobStatusTracker>,
    test_metadata: &'a AllMetadata,
}

impl<'a, ProgressIndicatorT> ArtifactQueing<'a, ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicatorScope,
{
    fn new(
        client: &'a Mutex<Client>,
        width: usize,
        ind: ProgressIndicatorT,
        artifact: CargoArtifact,
        package_name: String,
        tracker: Arc<JobStatusTracker>,
        test_metadata: &'a AllMetadata,
    ) -> Result<Self> {
        let binary = PathBuf::from(artifact.executable.unwrap());
        let ignored_cases: HashSet<_> = get_cases_from_binary(&binary, &Some("--ignored".into()))?
            .into_iter()
            .collect();

        let (binary_artifact, deps_artifact) = artifacts::add_generated_artifacts(client, &binary)?;

        Ok(Self {
            client,
            width,
            ind,
            binary,
            binary_artifact,
            deps_artifact,
            ignored_cases,
            package_name,
            tracker,
            test_metadata,
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

    fn queue_job_from_case(&mut self, case: &str, enqueue_cb: impl FnOnce()) -> Result<()> {
        let test_metadata = self
            .test_metadata
            .get_metadata_for_test(&self.package_name, &case);
        let layers = self.calculate_job_layers(&test_metadata)?;

        // N.B. Must do this before we enqueue the job, but after we know we can't fail
        enqueue_cb();

        let package_name = &self.package_name;
        let case_str = format!("{package_name} {case}");
        let visitor =
            JobStatusVisitor::new(self.tracker.clone(), case_str, self.width, self.ind.clone());

        if self.ignored_cases.contains(case) {
            visitor.job_ignored();
            return Ok(());
        }

        let binary_name = self.binary.file_name().unwrap().to_str().unwrap();
        self.client.lock().unwrap().add_job(
            JobSpec {
                program: format!("/{binary_name}"),
                arguments: vec!["--exact".into(), "--nocapture".into(), case.into()],
                environment: collect_environment_vars(),
                layers: layers,
                devices: test_metadata.devices,
                mounts: test_metadata.mounts,
                loopback: test_metadata.loopback_enabled,
            },
            Box::new(move |cjid, result| visitor.job_finished(cjid, result)),
        );

        Ok(())
    }
}

impl<StdErr: io::Write> JobQueuer<StdErr> {
    fn queue_jobs_from_artifact<ProgressIndicatorT>(
        &mut self,
        client: &Mutex<Client>,
        width: usize,
        ind: ProgressIndicatorT,
        cb: &mut impl FnMut(u64),
        artifact: CargoArtifact,
    ) -> Result<bool>
    where
        ProgressIndicatorT: ProgressIndicatorScope,
    {
        let package_name = artifact.package_id.repr.split(' ').next().unwrap().into();
        if let Some(package) = &self.package {
            if &package_name != package {
                return Ok(false);
            }
        }

        let mut artifact_queing = ArtifactQueing::new(
            client,
            width,
            ind.clone(),
            artifact,
            package_name,
            self.tracker.clone(),
            &self.test_metadata,
        )?;

        for case in get_cases_from_binary(&artifact_queing.binary, &self.filter)? {
            artifact_queing.queue_job_from_case(&case, || {
                self.jobs_queued += 1;
                cb(self.jobs_queued);
            })?;
        }

        Ok(true)
    }

    fn queue_jobs<ProgressIndicatorT>(
        mut self,
        client: &Mutex<Client>,
        width: usize,
        ind: ProgressIndicatorT,
        mut cb: impl FnMut(u64),
    ) -> Result<()>
    where
        ProgressIndicatorT: ProgressIndicatorScope,
    {
        let mut cargo_build =
            CargoBuild::new(&self.cargo, self.stderr_color, self.package.clone())?;

        let mut package_match = false;

        for artifact in cargo_build.artifact_stream() {
            let artifact = artifact?;
            package_match |=
                self.queue_jobs_from_artifact(client, width, ind.clone(), &mut cb, artifact)?;
        }

        cargo_build.check_status(self.stderr)?;

        if let Some(package) = self.package {
            if !package_match {
                return Err(anyhow!("package {package:?} unknown"));
            }
        }

        Ok(())
    }
}

pub struct MainApp<StdErr> {
    client: Mutex<Client>,
    queuer: JobQueuer<StdErr>,
}

impl<StdErr> MainApp<StdErr> {
    pub fn new(
        cargo: String,
        package: Option<String>,
        filter: Option<String>,
        stderr: StdErr,
        stderr_color: bool,
        workspace_root: &impl AsRef<Path>,
        broker_addr: BrokerAddr,
    ) -> Result<Self> {
        let cache_dir = workspace_root.as_ref().join("target");
        let client = Mutex::new(Client::new(
            DefaultClientDriver::default(),
            broker_addr,
            workspace_root,
            cache_dir,
        )?);
        let test_metadata = AllMetadata::load(workspace_root)?;
        Ok(Self {
            client,
            queuer: JobQueuer::new(cargo, package, filter, stderr, stderr_color, test_metadata),
        })
    }
}

impl<StdErr: io::Write> MainApp<StdErr> {
    fn run_with_progress<ProgressIndicatorT, Term>(
        self,
        prog_factory: impl FnOnce(Term) -> ProgressIndicatorT,
        term: Term,
    ) -> Result<ExitCode>
    where
        ProgressIndicatorT: ProgressIndicator,
        Term: TermLike + Clone + 'static,
    {
        let width = term.width() as usize;
        let prog = prog_factory(term.clone());
        let tracker = self.queuer.tracker.clone();

        prog.run(self.client, |client, bar_scope| {
            let cb = |num_jobs| bar_scope.update_length(num_jobs);
            self.queuer.queue_jobs(client, width, bar_scope.clone(), cb)
        })?;

        tracker.print_summary(width, term)?;
        Ok(tracker.exit_code())
    }

    pub fn run<Term>(self, stdout_tty: bool, quiet: Quiet, term: Term) -> Result<ExitCode>
    where
        Term: TermLike + Clone + Send + Sync + 'static,
    {
        match (stdout_tty, quiet.into_inner()) {
            (true, true) => Ok(self.run_with_progress(QuietProgressBar::new, term)?),
            (true, false) => Ok(self.run_with_progress(MultipleProgressBars::new, term)?),
            (false, true) => Ok(self.run_with_progress(QuietNoBar::new, term)?),
            (false, false) => Ok(self.run_with_progress(NoBar::new, term)?),
        }
    }
}
