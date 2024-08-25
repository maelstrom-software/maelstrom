use crate::config::StopAfter;
use crate::test_listing::TestListing;
use crate::ui::{UiJobId, UiJobResult, UiJobStatus, UiJobSummary, UiJobUpdate, UiSender};
use crate::{NotRunEstimate, TestArtifactKey, TestCaseMetadata};
use anyhow::Result;
use maelstrom_base::{
    ClientJobId, JobCompleted, JobEffects, JobError, JobOutcome, JobOutcomeResult, JobOutputResult,
    JobTerminationStatus,
};
use maelstrom_client::JobStatus;
use maelstrom_util::process::{ExitCode, ExitCodeAccumulator};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone)]
enum CaseResult {
    Ignored,
    Ran(ExitCode),
}

#[derive(Clone, Default)]
struct Statuses {
    outstanding: u64,
    completed: Vec<(String, CaseResult)>,
    num_failed: u64,
}

pub struct JobStatusTracker {
    statuses: Mutex<Statuses>,
    stop_after: Option<StopAfter>,
    condvar: Condvar,
    exit_code: ExitCodeAccumulator,
}

fn failure_limit_reached(stop_after: &Option<StopAfter>, statuses: &Statuses) -> bool {
    stop_after.is_some_and(|limit| statuses.num_failed as usize >= usize::from(limit))
}

impl JobStatusTracker {
    pub fn new(stop_after: Option<StopAfter>) -> Self {
        Self {
            statuses: Mutex::new(Statuses::default()),
            stop_after,
            condvar: Condvar::new(),
            exit_code: ExitCodeAccumulator::default(),
        }
    }

    pub fn add_outstanding(&self) {
        let mut statuses = self.statuses.lock().unwrap();
        statuses.outstanding += 1;
    }

    pub fn job_exited(&self, case: String, exit_code: ExitCode) {
        let mut statuses = self.statuses.lock().unwrap();
        statuses.outstanding -= 1;
        statuses.completed.push((case, CaseResult::Ran(exit_code)));
        if exit_code != ExitCode::SUCCESS {
            statuses.num_failed += 1;
        }
        self.exit_code.add(exit_code);
        self.condvar.notify_one();
    }

    pub fn job_ignored(&self, case: String) {
        let mut statuses = self.statuses.lock().unwrap();
        statuses.outstanding -= 1;
        statuses.completed.push((case, CaseResult::Ignored));
        self.condvar.notify_one();
    }

    pub fn wait_for_outstanding_or_failure_limit_reached(&self) {
        let mut statuses = self.statuses.lock().unwrap();
        while !(statuses.outstanding == 0 || failure_limit_reached(&self.stop_after, &statuses)) {
            statuses = self.condvar.wait(statuses).unwrap();
        }
    }

    pub fn completed(&self) -> u64 {
        let statuses = self.statuses.lock().unwrap();
        statuses.completed.len() as u64
    }

    pub fn is_failure_limit_reached(&self) -> bool {
        let statuses = self.statuses.lock().unwrap();
        failure_limit_reached(&self.stop_after, &statuses)
    }

    pub fn ui_summary(&self, not_run_estimate: NotRunEstimate) -> UiJobSummary {
        let statuses = self.statuses.lock().unwrap();

        let failed: Vec<_> = statuses
            .completed
            .iter()
            .filter(|(_, res)| matches!(res, CaseResult::Ran(e) if e != &ExitCode::SUCCESS))
            .map(|(n, _)| n.clone())
            .collect();
        let ignored: Vec<_> = statuses
            .completed
            .iter()
            .filter(|(_, res)| matches!(res, CaseResult::Ignored))
            .map(|(n, _)| n.clone())
            .collect();

        let mut not_run =
            failure_limit_reached(&self.stop_after, &statuses).then_some(not_run_estimate);
        if not_run == Some(NotRunEstimate::Exactly(0)) {
            not_run = None;
        }

        let succeeded = statuses.completed.len() - failed.len() - ignored.len();
        UiJobSummary {
            succeeded,
            failed,
            ignored,
            not_run,
        }
    }

    pub fn exit_code(&self) -> ExitCode {
        self.exit_code.get()
    }
}

#[derive(Clone)]
pub struct JobStatusVisitor<ArtifactKeyT: TestArtifactKey, CaseMetadataT: TestCaseMetadata> {
    tracker: Arc<JobStatusTracker>,
    test_listing: Arc<Mutex<Option<TestListing<ArtifactKeyT, CaseMetadataT>>>>,
    package: String,
    artifact: ArtifactKeyT,
    case: String,
    case_str: String,
    ui: UiSender,
    remove_fixture_output: fn(&str, Vec<String>) -> Vec<String>,
    was_ignored: fn(&str, &[String]) -> bool,
}

impl<ArtifactKeyT, CaseMetadataT> JobStatusVisitor<ArtifactKeyT, CaseMetadataT>
where
    ArtifactKeyT: TestArtifactKey,
    CaseMetadataT: TestCaseMetadata,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tracker: Arc<JobStatusTracker>,
        test_listing: Arc<Mutex<Option<TestListing<ArtifactKeyT, CaseMetadataT>>>>,
        package: String,
        artifact: ArtifactKeyT,
        case: String,
        case_str: String,
        ui: UiSender,
        remove_fixture_output: fn(&str, Vec<String>) -> Vec<String>,
        was_ignored: fn(&str, &[String]) -> bool,
    ) -> Self {
        Self {
            tracker,
            test_listing,
            package,
            artifact,
            case,
            case_str,
            ui,
            remove_fixture_output,
            was_ignored,
        }
    }
}

fn was_ignored(
    res: &JobOutputResult,
    case_str: &str,
    was_ignored_fn: impl Fn(&str, &[String]) -> bool,
) -> bool {
    let (_, case_str) = case_str.rsplit_once(' ').unwrap_or(("", case_str));
    let lines = match res {
        JobOutputResult::None => vec![],
        JobOutputResult::Inline(bytes) => String::from_utf8_lossy(bytes)
            .split('\n')
            .map(ToOwned::to_owned)
            .collect(),
        JobOutputResult::Truncated { first, .. } => String::from_utf8_lossy(first)
            .split('\n')
            .map(ToOwned::to_owned)
            .collect(),
    };
    was_ignored_fn(case_str, &lines)
}

fn split_test_output_into_lines(output: &[u8]) -> Vec<String> {
    let mut cursor = output;

    let mut lines = vec![];
    while let Some(end) = cursor.iter().position(|&b| b == b'\n') {
        lines.push(String::from_utf8_lossy(&cursor[..end]).into());
        cursor = &cursor[end + 1..];
    }

    if !cursor.is_empty() {
        lines.push(String::from_utf8_lossy(cursor).into());
    }

    lines
}

fn format_test_output(
    res: &JobOutputResult,
    name: &str,
    cjid: ClientJobId,
    case_str: &str,
    remove_fixture_output: impl Fn(&str, Vec<String>) -> Vec<String>,
) -> Vec<String> {
    let (_, case_str) = case_str.rsplit_once(' ').unwrap_or(("", case_str));
    let mut test_output_lines = vec![];
    match res {
        JobOutputResult::None => {}
        JobOutputResult::Inline(bytes) => {
            test_output_lines.extend(split_test_output_into_lines(bytes));
            if name == "stdout" {
                test_output_lines = remove_fixture_output(case_str, test_output_lines);
            }
        }
        JobOutputResult::Truncated { first, truncated } => {
            test_output_lines.extend(split_test_output_into_lines(first));
            if name == "stdout" {
                test_output_lines = remove_fixture_output(case_str, test_output_lines);
            }
            test_output_lines.push(format!(
                "job {cjid}: {name} truncated, {truncated} bytes lost"
            ));
        }
    }
    test_output_lines
}

impl<ArtifactKeyT, CaseMetadataT> JobStatusVisitor<ArtifactKeyT, CaseMetadataT>
where
    ArtifactKeyT: TestArtifactKey,
    CaseMetadataT: TestCaseMetadata,
{
    fn job_finished(&self, ui_job_id: UiJobId, res: Result<(ClientJobId, JobOutcomeResult)>) {
        if self.tracker.is_failure_limit_reached() {
            return;
        }

        let test_status: UiJobStatus;
        let mut test_output_stderr: Vec<String> = vec![];
        let mut test_output_stdout: Vec<String> = vec![];
        let mut test_duration = None;
        let exit_code = match res {
            Ok((
                cjid,
                Ok(JobOutcome::Completed(JobCompleted {
                    status,
                    effects:
                        JobEffects {
                            stdout,
                            stderr,
                            duration,
                        },
                })),
            )) => {
                test_duration = Some(duration);
                let mut job_failed = true;
                let exit_code = match status {
                    JobTerminationStatus::Exited(code) => {
                        test_status = if code == 0 {
                            job_failed = false;
                            UiJobStatus::Ok
                        } else {
                            UiJobStatus::Failure(None)
                        };
                        ExitCode::from(code)
                    }
                    JobTerminationStatus::Signaled(signo) => {
                        test_status =
                            UiJobStatus::Failure(Some(format!("killed by signal {signo}")));
                        ExitCode::FAILURE
                    }
                };
                if job_failed {
                    test_output_stdout.extend(format_test_output(
                        &stdout,
                        "stdout",
                        cjid,
                        &self.case_str,
                        self.remove_fixture_output,
                    ));
                    test_output_stderr.extend(format_test_output(
                        &stderr,
                        "stderr",
                        cjid,
                        &self.case_str,
                        self.remove_fixture_output,
                    ));
                }

                if !job_failed && was_ignored(&stdout, &self.case_str, self.was_ignored) {
                    self.job_ignored(ui_job_id);
                    return;
                }

                self.test_listing
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .add_timing(
                        self.package.as_str(),
                        self.artifact.clone(),
                        self.case.as_str(),
                        duration,
                    );
                exit_code
            }
            Ok((
                cjid,
                Ok(JobOutcome::TimedOut(JobEffects {
                    stdout,
                    stderr,
                    duration,
                })),
            )) => {
                test_duration = Some(duration);
                test_status = UiJobStatus::TimedOut;
                test_output_stdout.extend(format_test_output(
                    &stdout,
                    "stdout",
                    cjid,
                    &self.case_str,
                    self.remove_fixture_output,
                ));
                test_output_stderr.extend(format_test_output(
                    &stderr,
                    "stderr",
                    cjid,
                    &self.case_str,
                    self.remove_fixture_output,
                ));
                self.test_listing
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .add_timing(
                        self.package.as_str(),
                        self.artifact.clone(),
                        self.case.as_str(),
                        duration,
                    );
                ExitCode::FAILURE
            }
            Ok((_, Err(JobError::Execution(err)))) => {
                test_status = UiJobStatus::Error(format!("execution error: {err}"));
                ExitCode::FAILURE
            }
            Ok((_, Err(JobError::System(err)))) => {
                test_status = UiJobStatus::Error(format!("system error: {err}"));
                ExitCode::FAILURE
            }
            Err(err) => {
                test_status = UiJobStatus::Error(format!("remote error: {err}"));
                ExitCode::FAILURE
            }
        };

        self.ui.job_finished(UiJobResult {
            job_id: ui_job_id,
            name: self.case_str.clone(),
            status: test_status,
            duration: test_duration,
            stdout: test_output_stdout,
            stderr: test_output_stderr,
        });

        // This call unblocks main thread, so it must go last
        self.tracker.job_exited(self.case_str.clone(), exit_code);
    }

    pub fn job_update(&self, ui_job_id: UiJobId, res: Result<JobStatus>) {
        if self.tracker.is_failure_limit_reached() {
            return;
        }

        match res {
            Ok(JobStatus::Completed {
                client_job_id,
                result,
            }) => self.job_finished(ui_job_id, Ok((client_job_id, result))),
            Ok(JobStatus::Running(status)) => self.ui.job_updated(UiJobUpdate {
                job_id: ui_job_id,
                status,
            }),
            Err(err) => self.job_finished(ui_job_id, Err(err)),
        }
    }

    pub fn job_ignored(&self, ui_job_id: UiJobId) {
        if self.tracker.is_failure_limit_reached() {
            return;
        }

        self.ui.job_finished(UiJobResult {
            name: self.case_str.clone(),
            job_id: ui_job_id,
            status: UiJobStatus::Ignored,
            duration: None,
            stdout: vec![],
            stderr: vec![],
        });

        // This call unblocks main thread, so it must go last
        self.tracker.job_ignored(self.case_str.clone());
    }
}
