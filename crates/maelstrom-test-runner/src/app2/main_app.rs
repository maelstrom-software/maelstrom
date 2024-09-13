use super::{
    job_output::{build_ignored_ui_job_result, build_ui_job_result_and_exit_code},
    AllMetadataM, ArtifactM, CaseMetadataM, CollectOptionsM, Deps, MainAppMessage, MainAppMessageM,
    PackageIdM, PackageM, TestDbM,
};
use crate::test_db::CaseOutcome;
use crate::ui::{UiJobId as JobId, UiMessage};
use crate::*;
use maelstrom_base::{ClientJobId, JobOutcomeResult, JobRootOverlay, Timeout};
use maelstrom_client::{spec::JobSpec, ContainerSpec, JobStatus};
use maelstrom_util::{ext::OptionExt as _, process::ExitCode};
use std::collections::{BTreeMap, HashMap};
use std::mem;

pub struct MainApp<'deps, DepsT: Deps> {
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
    fatal_error: Result<()>,
    exit_code: ExitCode,
}

impl<'deps, DepsT: Deps> MainApp<'deps, DepsT> {
    pub fn new(
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
            fatal_error: Ok(()),
            exit_code: ExitCode::SUCCESS,
        }
    }

    fn start(&mut self) {
        self.deps.get_packages();
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

    fn handle_ignored_test(
        &mut self,
        artifact: &ArtifactM<DepsT>,
        case_name: &str,
        case_metadata: &CaseMetadataM<DepsT>,
    ) {
        let package = self
            .packages
            .get(&artifact.package())
            .expect("artifact for unknown package");

        let case_str = artifact.format_case(package.name(), case_name, case_metadata);

        let job_id = self.vend_job_id();
        self.num_enqueued += 1;
        self.deps
            .send_ui_msg(UiMessage::UpdatePendingJobsCount(self.num_enqueued));
        let res = build_ignored_ui_job_result(job_id, &case_str);
        self.deps.send_ui_msg(UiMessage::JobFinished(res));
    }

    fn receive_tests_listed(
        &mut self,
        artifact: ArtifactM<DepsT>,
        listing: Vec<(String, CaseMetadataM<DepsT>)>,
        ignored_listing: Vec<String>,
    ) {
        self.pending_listings -= 1;
        for (case_name, case_metadata) in &listing {
            if ignored_listing.contains(case_name) {
                self.handle_ignored_test(&artifact, case_name, case_metadata);
            } else {
                self.enqueue_test(&artifact, case_name, case_metadata);
            }
        }

        self.check_for_done();
    }

    fn receive_fatal_error(&mut self, error: anyhow::Error) {
        self.fatal_error = Err(error);
        self.deps.start_shutdown();
    }

    fn receive_job_finished(
        &mut self,
        job_id: JobId,
        result: Result<(ClientJobId, JobOutcomeResult)>,
    ) {
        let name = self.jobs.remove(&job_id).expect("job finishes only once");
        let (ui_job_res, exit_code) =
            build_ui_job_result_and_exit_code::<DepsT::TestCollector>(job_id, &name, result);
        self.deps.send_ui_msg(UiMessage::JobFinished(ui_job_res));

        if self.exit_code == ExitCode::SUCCESS {
            self.exit_code = exit_code;
        }

        self.check_for_done();
    }

    fn receive_job_update(&mut self, job_id: JobId, result: Result<JobStatus>) {
        match result {
            Ok(JobStatus::Completed {
                client_job_id,
                result,
            }) => self.receive_job_finished(job_id, Ok((client_job_id, result))),
            Err(err) => self.receive_job_finished(job_id, Err(err)),
            _ => {}
        }
    }

    fn receive_collection_finished(&mut self) {
        self.collection_finished = true;
        self.deps.send_ui_msg(UiMessage::DoneQueuingJobs);

        self.check_for_done();
    }

    pub fn main_return_value(&mut self) -> Result<ExitCode> {
        mem::replace(&mut self.fatal_error, Ok(()))?;
        Ok(self.exit_code)
    }

    pub fn receive_message(&mut self, message: MainAppMessageM<DepsT>) {
        match message {
            MainAppMessage::Start => self.start(),
            MainAppMessage::Packages { packages } => self.receive_packages(packages),
            MainAppMessage::ArtifactBuilt { artifact } => self.receive_artifact_built(artifact),
            MainAppMessage::TestsListed {
                artifact,
                listing,
                ignored_listing,
            } => self.receive_tests_listed(artifact, listing, ignored_listing),
            MainAppMessage::FatalError { error } => self.receive_fatal_error(error),
            MainAppMessage::JobUpdate { job_id, result } => self.receive_job_update(job_id, result),
            MainAppMessage::CollectionFinished => self.receive_collection_finished(),
            MainAppMessage::Shutdown => unimplemented!(),
        }
    }
}
