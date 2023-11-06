use crate::ProgressIndicatorScope;
use anyhow::Result;
use colored::{ColoredString, Colorize as _};
use indicatif::TermLike;
use meticulous_base::{ClientJobId, JobOutputResult, JobResult, JobStatus};
use meticulous_util::process::{ExitCode, ExitCodeAccumulator};
use std::sync::{Arc, Mutex};
use unicode_truncate::UnicodeTruncateStr as _;
use unicode_width::UnicodeWidthStr as _;

#[derive(Default)]
pub struct JobStatusTracker {
    statuses: Mutex<Vec<(String, ExitCode)>>,
    exit_code: ExitCodeAccumulator,
}

impl JobStatusTracker {
    pub fn job_exited(&self, case: String, exit_code: ExitCode) {
        let mut statuses = self.statuses.lock().unwrap();
        statuses.push((case, exit_code));
        self.exit_code.add(exit_code);
    }

    pub fn print_summary(&self, width: usize, term: impl TermLike) -> Result<()> {
        term.write_line("")?;

        let heading = " Test Summary ";
        let equal_width = (width - heading.width()) / 2;
        term.write_line(&format!(
            "{empty:=<equal_width$}{heading}{empty:=<equal_width$}",
            empty = ""
        ))?;

        let success = "Successful Tests";
        let failure = "Failed Tests";
        let column1_width = std::cmp::max(success.width(), failure.width());
        let max_digits = 9;
        let statuses = self.statuses.lock().unwrap();
        let failed = statuses
            .iter()
            .filter(|(_, exit_code)| exit_code != &ExitCode::SUCCESS);
        let num_failed = failed.clone().count();
        let num_succeeded = statuses.len() - num_failed;

        term.write_line(&format!(
            "{:<column1_width$}: {num_succeeded:>max_digits$}",
            success.green(),
        ))?;
        term.write_line(&format!(
            "{:<column1_width$}: {num_failed:>max_digits$}",
            failure.red(),
        ))?;
        let failed_width = failed.clone().map(|(n, _)| n.width()).max().unwrap_or(0);
        for (failed, _) in failed {
            term.write_line(&format!("    {failed:<failed_width$}: {}", "failure".red()))?;
        }

        term.flush()?;
        Ok(())
    }

    pub fn exit_code(&self) -> ExitCode {
        self.exit_code.get()
    }
}

pub struct JobStatusVisitor<ProgressIndicatorT> {
    tracker: Arc<JobStatusTracker>,
    case: String,
    width: usize,
    ind: ProgressIndicatorT,
}

impl<ProgressIndicatorT> JobStatusVisitor<ProgressIndicatorT> {
    pub fn new(
        tracker: Arc<JobStatusTracker>,
        case: String,
        width: usize,
        ind: ProgressIndicatorT,
    ) -> Self {
        Self {
            tracker,
            case,
            width,
            ind,
        }
    }
}

impl<ProgressIndicatorT: ProgressIndicatorScope> JobStatusVisitor<ProgressIndicatorT> {
    pub fn job_finished(&self, cjid: ClientJobId, result: JobResult) -> Result<()> {
        let result_str: ColoredString;
        let mut result_details: Option<String> = None;
        match result {
            JobResult::Ran { status, stderr, .. } => {
                let mut job_failed = true;
                match status {
                    JobStatus::Exited(code) => {
                        result_str = if code == 0 {
                            job_failed = false;
                            "OK".green()
                        } else {
                            "FAIL".red()
                        };
                        self.tracker
                            .job_exited(self.case.clone(), ExitCode::from(code));
                    }
                    JobStatus::Signaled(signo) => {
                        result_str = "FAIL".red();
                        result_details = Some(format!("killed by signal {signo}"));
                        self.tracker
                            .job_exited(self.case.clone(), ExitCode::FAILURE);
                    }
                };
                if job_failed {
                    match stderr {
                        JobOutputResult::None => {}
                        JobOutputResult::Inline(bytes) => {
                            self.ind.eprintln(String::from_utf8_lossy(&bytes));
                        }
                        JobOutputResult::Truncated { first, truncated } => {
                            self.ind.eprintln(String::from_utf8_lossy(&first));
                            self.ind.eprintln(format!(
                                "job {cjid}: stderr truncated, {truncated} bytes lost"
                            ));
                        }
                    }
                }
            }
            JobResult::ExecutionError(err) => {
                result_str = "ERR".yellow();
                result_details = Some(format!("execution error: {err}"));
                self.tracker
                    .job_exited(self.case.clone(), ExitCode::FAILURE);
            }
            JobResult::SystemError(err) => {
                result_str = "ERR".yellow();
                result_details = Some(format!("system error: {err}"));
                self.tracker
                    .job_exited(self.case.clone(), ExitCode::FAILURE);
            }
        }
        if self.width > 10 {
            let case_width = self.case.width();
            let result_width = result_str.width();
            if case_width + result_width < self.width {
                let dots_width = self.width - result_width - case_width;
                let case = self.case.bold();
                self.ind.println(format!(
                    "{case}{empty:.<dots_width$}{result_str}",
                    empty = ""
                ));
            } else {
                let (case, case_width) = self
                    .case
                    .unicode_truncate_start(self.width - 2 - result_width);
                let case = case.bold();
                let dots_width = self.width - result_width - case_width - 1;
                self.ind.println(format!(
                    "<{case}{empty:.<dots_width$}{result_str}",
                    empty = ""
                ));
            }
        } else {
            self.ind
                .println(format!("{case} {result_str}", case = self.case));
        }
        if let Some(details_str) = result_details {
            self.ind.println(details_str);
        }
        self.ind.job_finished();
        Ok(())
    }
}
