use anyhow::{anyhow, Result};
use cargo::{get_cases_from_binary, CargoBuild};
use cargo_metadata::Artifact as CargoArtifact;
use config::Quiet;
use indicatif::TermLike;
use metadata::TestMetadata;
use meticulous_base::{EnumSet, JobDevice, JobMount, JobSpec, NonEmpty, Sha256Digest};
use meticulous_client::Client;
use meticulous_util::{config::BrokerAddr, fs::Fs, process::ExitCode};
use progress::{
    MultipleProgressBars, NoBar, ProgressIndicator, ProgressIndicatorScope, QuietNoBar,
    QuietProgressBar,
};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io,
    path::{Path, PathBuf},
    str,
    sync::{Arc, Mutex},
};
use visitor::{JobStatusTracker, JobStatusVisitor};

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
    test_metadata: TestMetadata,
}

impl<StdErr> JobQueuer<StdErr> {
    fn new(
        cargo: String,
        package: Option<String>,
        filter: Option<String>,
        stderr: StdErr,
        stderr_color: bool,
        test_metadata: TestMetadata,
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

fn create_artifact_for_binary(binary_path: &Path) -> Result<PathBuf> {
    let fs = Fs::new();
    let binary = fs.open_file(binary_path)?;

    let mut tar_path = PathBuf::from(binary_path);
    assert!(tar_path.set_extension("tar"));

    if fs.exists(&tar_path) {
        let binary_mtime = binary.metadata()?.modified()?;
        let tar_mtime = fs.metadata(&tar_path)?.modified()?;
        if binary_mtime < tar_mtime {
            return Ok(tar_path);
        }
    }

    let tar_file = fs.create_file(&tar_path)?;
    let mut a = tar::Builder::new(tar_file);

    let binary_path_in_tar = Path::new("./").join(binary_path.file_name().unwrap());
    a.append_file(binary_path_in_tar, &mut binary.into_inner())?;
    a.finish()?;

    Ok(tar_path)
}

fn create_artifact_for_binary_deps(binary_path: &Path) -> Result<PathBuf> {
    let fs = Fs::new();

    let mut tar_path = PathBuf::from(binary_path);
    assert!(tar_path.set_extension("deps.tar"));

    if fs.exists(&tar_path) {
        let binary_mtime = fs.metadata(binary_path)?.modified()?;
        let tar_mtime = fs.metadata(&tar_path)?.modified()?;

        if binary_mtime < tar_mtime {
            return Ok(tar_path);
        }
    }

    let dep_tree = lddtree::DependencyAnalyzer::new("/".into());
    let deps = dep_tree.analyze(binary_path)?;

    let mut paths = BTreeSet::new();
    if let Some(p) = deps.interpreter {
        if let Some(lib) = deps.libraries.get(&p) {
            paths.insert(lib.path.clone());
        }
    }

    fn walk_deps(
        deps: &[String],
        libraries: &HashMap<String, lddtree::Library>,
        paths: &mut BTreeSet<PathBuf>,
    ) {
        for dep in deps {
            if let Some(lib) = libraries.get(dep) {
                paths.insert(lib.path.clone());
            }
            if let Some(lib) = libraries.get(dep) {
                walk_deps(&lib.needed, libraries, paths);
            }
        }
    }
    walk_deps(&deps.needed, &deps.libraries, &mut paths);

    fn remove_root(path: &Path) -> PathBuf {
        path.components().skip(1).collect()
    }

    let tar_file = fs.create_file(&tar_path)?;
    let mut a = tar::Builder::new(tar_file);

    for path in paths {
        a.append_path_with_name(&path, &remove_root(&path))?;
    }

    a.finish()?;

    Ok(tar_path)
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
        devices: EnumSet<JobDevice>,
        mounts: Vec<JobMount>,
        loopback: bool,
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
                environment: collect_environment_vars(),
                layers,
                devices,
                mounts,
                loopback,
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

        let binary_artifact = client
            .lock()
            .unwrap()
            .add_artifact(&create_artifact_for_binary(&binary)?)?;
        let deps_artifact = client
            .lock()
            .unwrap()
            .add_artifact(&create_artifact_for_binary_deps(&binary)?)?;

        for case in get_cases_from_binary(&binary, &self.filter)? {
            let mut layers = vec![];
            for layer in self.test_metadata.get_layers_for_test(package_name, &case) {
                let mut client = client.lock().unwrap();
                if layer.starts_with("docker:") {
                    let pkg = layer.split(':').nth(1).unwrap();
                    let prog = ind.new_container_progress();
                    layers.extend(client.add_container(pkg, "latest", prog)?);
                } else {
                    layers.push(client.add_artifact(PathBuf::from(layer).as_path())?);
                }
            }

            if self
                .test_metadata
                .include_shared_libraries_for_test(package_name, &case)
            {
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
                self.test_metadata.get_devices_for_test(package_name, &case),
                self.test_metadata.get_mounts_for_test(package_name, &case),
                self.test_metadata
                    .get_loopback_for_test(package_name, &case),
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
        let client = Mutex::new(Client::new(broker_addr, workspace_root, cache_dir)?);
        let test_metadata = TestMetadata::load(workspace_root)?;
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
