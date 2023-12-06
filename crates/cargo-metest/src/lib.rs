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
pub mod substitute;
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

impl<StdErr: io::Write> JobQueuer<StdErr> {
    #[allow(clippy::too_many_arguments)]
    fn queue_job_from_case<ProgressIndicatorT>(
        &mut self,
        client: &Mutex<Client>,
        width: usize,
        ind: ProgressIndicatorT,
        ignored_cases: &HashSet<String>,
        package_name: &str,
        case: &str,
        binary: &Path,
        layers: NonEmpty<Sha256Digest>,
        metadata: TestMetadata,
    ) where
        ProgressIndicatorT: ProgressIndicatorScope,
    {
        let case_str = format!("{package_name} {case}");
        let visitor = JobStatusVisitor::new(self.tracker.clone(), case_str, width, ind);

        if ignored_cases.contains(case) {
            visitor.job_ignored();
            return;
        }

        let binary_name = binary.file_name().unwrap().to_str().unwrap();
        client.lock().unwrap().add_job(
            JobSpec {
                program: format!("/{}", binary_name),
                arguments: vec!["--exact".into(), "--nocapture".into(), case.into()],
                environment: metadata.environment(),
                layers,
                devices: metadata.devices,
                mounts: metadata.mounts,
                enable_loopback: metadata.enable_loopback,
            },
            Box::new(move |cjid, result| visitor.job_finished(cjid, result)),
        );
    }

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
        let package_name = artifact.package_id.repr.split(' ').next().unwrap();
        if let Some(package) = &self.package {
            if package_name != package {
                return Ok(false);
            }
        }

        let binary = PathBuf::from(artifact.executable.unwrap());
        let ignored_cases: HashSet<_> = get_cases_from_binary(&binary, &Some("--ignored".into()))?
            .into_iter()
            .collect();

        let (binary_artifact, deps_artifact) = artifacts::add_generated_artifacts(client, &binary)?;

        for case in get_cases_from_binary(&binary, &self.filter)? {
            let test_metadata = self
                .test_metadata
                .get_metadata_for_test_with_env(package_name, &case)?;
            let mut layers = vec![];
            for layer in &test_metadata.layers {
                let mut client = client.lock().unwrap();
                if layer.starts_with("docker:") {
                    let pkg = layer.split(':').nth(1).unwrap();
                    let prog = ind.new_container_progress();
                    layers.extend(client.add_container(pkg, "latest", prog)?);
                } else {
                    layers.push(client.add_artifact(PathBuf::from(layer).as_path())?);
                }
            }

            if test_metadata.include_shared_libraries() {
                layers.push(deps_artifact.clone());
            }
            layers.push(binary_artifact.clone());

            self.jobs_queued += 1;
            cb(self.jobs_queued);

            self.queue_job_from_case(
                client,
                width,
                ind.clone(),
                &ignored_cases,
                package_name,
                &case,
                &binary,
                NonEmpty::try_from(layers).unwrap(),
                test_metadata,
            );
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
