use super::{
    job_output::{build_ignored_ui_job_result, build_ui_job_result_and_exit_code},
    ArtifactKeyM, ArtifactM, CaseMetadataM, Deps, MainAppMessage, MainAppMessageM, PackageIdM,
    PackageM, TestDbM, TestingOptionsM,
};
use crate::metadata::TestMetadata;
use crate::test_db::CaseOutcome;
use crate::ui::{UiJobId as JobId, UiJobStatus, UiMessage};
use crate::*;
use maelstrom_base::{ClientJobId, JobOutcomeResult, JobRootOverlay};
use maelstrom_client::{spec::JobSpec, ContainerSpec, JobStatus};
use maelstrom_util::{ext::OptionExt as _, process::ExitCode};
use std::collections::{BTreeMap, HashMap};

struct JobInfo<ArtifactKeyT> {
    case_name: String,
    package_name: String,
    artifact_key: ArtifactKeyT,
    case_str: String,
}

pub struct MainApp<'deps, DepsT: Deps> {
    deps: &'deps DepsT,
    options: &'deps TestingOptionsM<DepsT>,
    packages: BTreeMap<PackageIdM<DepsT>, PackageM<DepsT>>,
    next_job_id: u32,
    jobs: HashMap<JobId, JobInfo<ArtifactKeyM<DepsT>>>,
    collection_finished: bool,
    pending_listings: u64,
    num_enqueued: u64,
    expected_job_count: u64,
    fatal_error: Result<()>,
    exit_code: ExitCode,
    test_db: TestDbM<DepsT>,
}

impl<'deps, DepsT: Deps> MainApp<'deps, DepsT> {
    pub fn new(
        deps: &'deps DepsT,
        options: &'deps TestingOptionsM<DepsT>,
        test_db: TestDbM<DepsT>,
    ) -> Self {
        Self {
            deps,
            options,
            packages: BTreeMap::new(),
            next_job_id: 1,
            test_db,
            jobs: HashMap::new(),
            collection_finished: false,
            pending_listings: 0,
            num_enqueued: 0,
            expected_job_count: 0,
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
        self.test_db
            .retain_packages_and_artifacts(packages.iter().map(|p| (p.name(), p.artifacts())));

        self.packages = packages
            .into_iter()
            .filter(|p| self.options.filter.filter(p, None, None).unwrap_or(true))
            .map(|p| (p.id(), p))
            .collect();

        if !self.packages.is_empty() {
            let color = false;
            let packages: Vec<_> = self.packages.values().collect();
            self.deps
                .start_collection(color, &self.options.collector_options, packages);
        } else {
            self.collection_finished = true;
        }

        let package_name_map: BTreeMap<_, _> = self
            .packages
            .values()
            .map(|p| (p.name().into(), p.clone()))
            .collect();
        self.expected_job_count = self
            .test_db
            .count_matching_cases(&package_name_map, &self.options.filter);
        if self.expected_job_count > 0 {
            self.deps
                .send_ui_msg(UiMessage::UpdatePendingJobsCount(self.expected_job_count));
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
        test_metadata: TestMetadata,
        package_name: &str,
        artifact: &ArtifactM<DepsT>,
        case_name: &str,
        case_metadata: &CaseMetadataM<DepsT>,
    ) {
        let case_str = artifact.format_case(package_name, case_name, case_metadata);

        let mut layers = test_metadata.layers.clone();
        layers.extend(self.deps.get_test_layers(artifact, &test_metadata));

        let get_timing_result = self
            .test_db
            .get_case(package_name, &artifact.to_key(), case_name);
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
            timeout: self
                .options
                .timeout_override
                .unwrap_or(test_metadata.timeout),
            estimated_duration,
            allocate_tty: None,
            priority,
        };

        let job_id = self.vend_job_id();
        self.deps.add_job(job_id, spec);
        let job_info = JobInfo {
            case_name: case_name.into(),
            case_str,
            package_name: package_name.into(),
            artifact_key: artifact.to_key(),
        };
        self.jobs.insert(job_id, job_info).assert_is_none();

        self.num_enqueued += 1;
        if self.num_enqueued > self.expected_job_count {
            self.deps
                .send_ui_msg(UiMessage::UpdatePendingJobsCount(self.num_enqueued));
        }
    }

    fn handle_ignored_test(
        &mut self,
        package_name: &str,
        artifact: &ArtifactM<DepsT>,
        case_name: &str,
        case_metadata: &CaseMetadataM<DepsT>,
    ) {
        let case_str = artifact.format_case(package_name, case_name, case_metadata);

        let job_id = self.vend_job_id();
        self.num_enqueued += 1;
        if self.num_enqueued > self.expected_job_count {
            self.deps
                .send_ui_msg(UiMessage::UpdatePendingJobsCount(self.num_enqueued));
        }
        let res = build_ignored_ui_job_result(job_id, &case_str);
        self.deps.send_ui_msg(UiMessage::JobFinished(res));
    }

    fn maybe_enqueue_test(
        &mut self,
        artifact: &ArtifactM<DepsT>,
        case_name: &String,
        case_metadata: &CaseMetadataM<DepsT>,
        ignored_listing: &[String],
    ) {
        let package = self
            .packages
            .get(&artifact.package())
            .expect("artifact for unknown package");
        let package_name = package.name().to_owned();

        let case_tuple = (case_name.as_str(), case_metadata);
        let selected = self
            .options
            .filter
            .filter(package, Some(&artifact.to_key()), Some(case_tuple))
            .expect("should have case");

        if !selected {
            return;
        }

        let test_metadata = self
            .options
            .test_metadata
            .get_metadata_for_test_with_env(package, &artifact.to_key(), case_tuple)
            .expect("we always parse valid test metadata");

        if ignored_listing.contains(case_name) || test_metadata.ignore {
            self.handle_ignored_test(&package_name, artifact, case_name, case_metadata);
        } else {
            self.enqueue_test(
                test_metadata,
                &package_name,
                artifact,
                case_name,
                case_metadata,
            );
        }
    }

    fn receive_tests_listed(
        &mut self,
        artifact: ArtifactM<DepsT>,
        listing: Vec<(String, CaseMetadataM<DepsT>)>,
        ignored_listing: Vec<String>,
    ) {
        let package = self
            .packages
            .get(&artifact.package())
            .expect("artifact for unknown package");
        self.test_db
            .update_artifact_cases(package.name(), artifact.to_key(), listing.clone());

        self.pending_listings -= 1;
        for (case_name, case_metadata) in &listing {
            self.maybe_enqueue_test(&artifact, case_name, case_metadata, &ignored_listing);
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
        let job_info = self.jobs.remove(&job_id).expect("job finishes only once");
        let (ui_job_res, exit_code) = build_ui_job_result_and_exit_code::<DepsT::TestCollector>(
            job_id,
            &job_info.case_str,
            result,
        );

        if self.exit_code == ExitCode::SUCCESS {
            self.exit_code = exit_code;
        }

        if let Some(duration) = ui_job_res.duration {
            self.test_db.update_case(
                &job_info.package_name,
                &job_info.artifact_key,
                &job_info.case_name,
                !matches!(ui_job_res.status, UiJobStatus::Ok | UiJobStatus::Ignored),
                duration,
            );
        }
        self.deps.send_ui_msg(UiMessage::JobFinished(ui_job_res));

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

    pub fn main_return_value(self) -> Result<(ExitCode, TestDbM<DepsT>)> {
        self.fatal_error?;
        Ok((self.exit_code, self.test_db))
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
