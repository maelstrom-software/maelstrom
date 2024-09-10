use crate::config::StopAfter;
use crate::test_db::TestDb;
use crate::ui::{UiJobId, UiJobResult, UiJobStatus, UiJobSummary, UiJobUpdate, UiWeakSender};
use crate::{NotRunEstimate, TestArtifactKey, TestCaseMetadata};
use anyhow::Result;
use maelstrom_base::{
    ClientJobId, JobCompleted, JobEffects, JobError, JobOutcome, JobOutcomeResult, JobOutputResult,
    JobTerminationStatus,
};
use maelstrom_client::JobStatus;
use maelstrom_util::process::{ExitCode, ExitCodeAccumulator};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

#[derive(Clone)]
enum CaseResult {
    Ignored,
    Ran(ExitCode),
}

#[derive(Default)]
struct LockedJobStatusTracker {
    outstanding: u64,
    completed: Vec<(String, CaseResult)>,
    num_failed: u64,
    stop_after: Option<StopAfter>,
    exit_code: ExitCodeAccumulator,
}

impl LockedJobStatusTracker {
    fn job_exited(&mut self, case: String, exit_code: ExitCode) {
        self.outstanding -= 1;
        self.completed.push((case, CaseResult::Ran(exit_code)));
        if exit_code != ExitCode::SUCCESS {
            self.num_failed += 1;
        }
        self.exit_code.add(exit_code);
    }

    fn job_ignored(&mut self, case: String) {
        self.outstanding -= 1;
        self.completed.push((case, CaseResult::Ignored));
    }

    fn ui_summary(&self, not_run_estimate: NotRunEstimate) -> UiJobSummary {
        let failed: Vec<_> = self
            .completed
            .iter()
            .filter(|(_, res)| matches!(res, CaseResult::Ran(e) if e != &ExitCode::SUCCESS))
            .map(|(n, _)| n.clone())
            .collect();
        let ignored: Vec<_> = self
            .completed
            .iter()
            .filter(|(_, res)| matches!(res, CaseResult::Ignored))
            .map(|(n, _)| n.clone())
            .collect();

        let mut not_run = self.is_failure_limit_reached().then_some(not_run_estimate);
        if not_run == Some(NotRunEstimate::Exactly(0)) {
            not_run = None;
        }

        let succeeded = self.completed.len() - failed.len() - ignored.len();
        UiJobSummary {
            succeeded,
            failed,
            ignored,
            not_run,
        }
    }

    fn is_failure_limit_reached(&self) -> bool {
        self.stop_after
            .is_some_and(|limit| self.num_failed as usize >= usize::from(limit))
    }
}

pub struct JobStatusTracker {
    inner: Mutex<LockedJobStatusTracker>,
    condvar: Condvar,
}

impl JobStatusTracker {
    pub fn new(stop_after: Option<StopAfter>) -> Self {
        Self {
            inner: Mutex::new(LockedJobStatusTracker {
                stop_after,
                ..Default::default()
            }),
            condvar: Condvar::new(),
        }
    }

    pub fn add_outstanding(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.outstanding += 1;
    }

    pub fn wait_for_outstanding_or_failure_limit_reached(&self) {
        let mut inner = self.inner.lock().unwrap();
        while !(inner.outstanding == 0 || inner.is_failure_limit_reached()) {
            inner = self.condvar.wait(inner).unwrap();
        }
    }

    pub fn completed(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.completed.len() as u64
    }

    pub fn is_failure_limit_reached(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.is_failure_limit_reached()
    }

    pub fn ui_summary(&self, not_run_estimate: NotRunEstimate) -> UiJobSummary {
        let inner = self.inner.lock().unwrap();
        inner.ui_summary(not_run_estimate)
    }

    pub fn exit_code(&self) -> ExitCode {
        let inner = self.inner.lock().unwrap();
        inner.exit_code.get()
    }
}

#[derive(Clone)]
pub struct JobStatusVisitor<ArtifactKeyT: TestArtifactKey, CaseMetadataT: TestCaseMetadata> {
    tracker: Arc<JobStatusTracker>,
    test_db: Arc<Mutex<Option<TestDb<ArtifactKeyT, CaseMetadataT>>>>,
    package: String,
    artifact: ArtifactKeyT,
    case: String,
    case_str: String,
    ui: UiWeakSender,
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
        test_db: Arc<Mutex<Option<TestDb<ArtifactKeyT, CaseMetadataT>>>>,
        package: String,
        artifact: ArtifactKeyT,
        case: String,
        case_str: String,
        ui: UiWeakSender,
        remove_fixture_output: fn(&str, Vec<String>) -> Vec<String>,
        was_ignored: fn(&str, &[String]) -> bool,
    ) -> Self {
        Self {
            tracker,
            test_db,
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
    fn job_finished(
        &self,
        mut locked_tracker: MutexGuard<'_, LockedJobStatusTracker>,
        ui_job_id: UiJobId,
        res: Result<(ClientJobId, JobOutcomeResult)>,
    ) {
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
                    drop(locked_tracker);
                    self.job_ignored(ui_job_id);
                    return;
                }

                self.test_db.lock().unwrap().as_mut().unwrap().update_case(
                    self.package.as_str(),
                    &self.artifact,
                    self.case.as_str(),
                    job_failed,
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
                self.test_db.lock().unwrap().as_mut().unwrap().update_case(
                    self.package.as_str(),
                    &self.artifact,
                    self.case.as_str(),
                    true,
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

        self.ui_job_finished(UiJobResult {
            job_id: ui_job_id,
            name: self.case_str.clone(),
            status: test_status,
            duration: test_duration,
            stdout: test_output_stdout,
            stderr: test_output_stderr,
        });

        locked_tracker.job_exited(self.case_str.clone(), exit_code);

        // This call unblocks main thread, so it must go last
        self.tracker.condvar.notify_all();
    }

    pub fn job_update(&self, ui_job_id: UiJobId, res: Result<JobStatus>) {
        let locked_tracker = self.tracker.inner.lock().unwrap();
        if locked_tracker.is_failure_limit_reached() {
            return;
        }

        match res {
            Ok(JobStatus::Completed {
                client_job_id,
                result,
            }) => self.job_finished(locked_tracker, ui_job_id, Ok((client_job_id, result))),
            Ok(JobStatus::Running(status)) => self.ui_job_updated(UiJobUpdate {
                job_id: ui_job_id,
                status,
            }),
            Err(err) => self.job_finished(locked_tracker, ui_job_id, Err(err)),
        }
    }

    pub fn job_ignored(&self, ui_job_id: UiJobId) {
        let mut locked_tracker = self.tracker.inner.lock().unwrap();
        if locked_tracker.is_failure_limit_reached() {
            return;
        }

        self.ui_job_finished(UiJobResult {
            name: self.case_str.clone(),
            job_id: ui_job_id,
            status: UiJobStatus::Ignored,
            duration: None,
            stdout: vec![],
            stderr: vec![],
        });

        locked_tracker.job_ignored(self.case_str.clone());

        // This call unblocks main thread, so it must go last
        self.tracker.condvar.notify_all();
    }

    fn ui_job_finished(&self, res: UiJobResult) {
        if let Some(ui) = self.ui.upgrade() {
            ui.job_finished(res);
        }
    }

    fn ui_job_updated(&self, res: UiJobUpdate) {
        if let Some(ui) = self.ui.upgrade() {
            ui.job_updated(res);
        }
    }
}
