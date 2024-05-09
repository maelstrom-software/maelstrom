use crate::progress::{ProgressIndicator, ProgressPrinter};
use crate::test_listing::{ArtifactKey, TestListing};
use anyhow::Result;
use colored::{ColoredString, Colorize as _};
use indicatif::TermLike;
use maelstrom_base::{
    ClientJobId, JobCompleted, JobEffects, JobError, JobOutcome, JobOutcomeResult, JobOutputResult,
    JobStatus,
};
use maelstrom_util::process::{ExitCode, ExitCodeAccumulator};
use std::sync::{Arc, Condvar, Mutex};
use unicode_truncate::UnicodeTruncateStr as _;
use unicode_width::UnicodeWidthStr as _;

enum CaseResult {
    Ignored,
    Ran(ExitCode),
}

#[derive(Default)]
struct Statuses {
    outstanding: u64,
    completed: Vec<(String, CaseResult)>,
}

#[derive(Default)]
pub struct JobStatusTracker {
    statuses: Mutex<Statuses>,
    condvar: Condvar,
    exit_code: ExitCodeAccumulator,
}

impl JobStatusTracker {
    pub fn add_outstanding(&self) {
        let mut statuses = self.statuses.lock().unwrap();
        statuses.outstanding += 1;
    }

    pub fn job_exited(&self, case: String, exit_code: ExitCode) {
        let mut statuses = self.statuses.lock().unwrap();
        statuses.outstanding -= 1;
        statuses.completed.push((case, CaseResult::Ran(exit_code)));
        self.exit_code.add(exit_code);
        self.condvar.notify_one();
    }

    pub fn job_ignored(&self, case: String) {
        let mut statuses = self.statuses.lock().unwrap();
        statuses.outstanding -= 1;
        statuses.completed.push((case, CaseResult::Ignored));
        self.condvar.notify_one();
    }

    pub fn wait_for_outstanding(&self) {
        let mut statuses = self.statuses.lock().unwrap();
        while statuses.outstanding > 0 {
            statuses = self.condvar.wait(statuses).unwrap();
        }
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
        let ignore = "Ignored Tests";
        let mut column1_width = std::cmp::max(success.width(), failure.width());
        let max_digits = 9;
        let statuses = self.statuses.lock().unwrap();
        assert_eq!(statuses.outstanding, 0);
        let failed = statuses
            .completed
            .iter()
            .filter(|(_, res)| matches!(res, CaseResult::Ran(e) if e != &ExitCode::SUCCESS));
        let ignored = statuses
            .completed
            .iter()
            .filter(|(_, res)| matches!(res, CaseResult::Ignored));
        let num_failed = failed.clone().count();
        let num_ignored = ignored.clone().count();
        let num_succeeded = statuses.completed.len() - num_failed - num_ignored;

        if num_ignored > 0 {
            column1_width = std::cmp::max(column1_width, ignore.width());
        }

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

        if num_ignored > 0 {
            term.write_line(&format!(
                "{:<column1_width$}: {num_ignored:>max_digits$}",
                ignore.yellow(),
            ))?;
            let failed_width = ignored.clone().map(|(n, _)| n.width()).max().unwrap_or(0);
            for (ignored, _) in ignored {
                term.write_line(&format!(
                    "    {ignored:<failed_width$}: {}",
                    "ignored".yellow()
                ))?;
            }
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
    test_listing: Arc<Mutex<Option<TestListing>>>,
    package: String,
    artifact: ArtifactKey,
    case: String,
    case_str: String,
    width: usize,
    ind: ProgressIndicatorT,
}

impl<ProgressIndicatorT> JobStatusVisitor<ProgressIndicatorT> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tracker: Arc<JobStatusTracker>,
        test_listing: Arc<Mutex<Option<TestListing>>>,
        package: String,
        artifact: ArtifactKey,
        case: String,
        case_str: String,
        width: usize,
        ind: ProgressIndicatorT,
    ) -> Self {
        Self {
            tracker,
            test_listing,
            package,
            artifact,
            case,
            case_str,
            width,
            ind,
        }
    }
}

/// The Rust std test fixture prints out some output like "running 1 test" etc. This isn't very
/// useful, so we want to strip it out.
fn remove_fixture_output(case_str: &str, mut lines: Vec<String>) -> Vec<String> {
    if let Some(pos) = lines.iter().position(|s| s.as_str() == "running 1 test") {
        lines = lines[(pos + 1)..].to_vec();
    }
    if let Some(pos) = lines
        .iter()
        .rposition(|s| s.as_str().starts_with(&format!("test {case_str} ... ")))
    {
        lines = lines[..pos].to_vec();
    }
    lines
}

#[test]
fn remove_fixture_output_basic_case() {
    let example = "\n\
    running 1 test\n\
    this is some output from the test\n\
    this is too\n\
    test tests::i_be_failing ... FAILED\n\
    \n\
    failures:\n\
    \n\
    failures:\n\
        tests::i_be_failing\n\
        \n\
    test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 155 filtered out; \
    finished in 0.01s\n\
    \n\
    ";
    let cleansed = remove_fixture_output(
        "tests::i_be_failing",
        example.split('\n').map(ToOwned::to_owned).collect(),
    );
    assert_eq!(
        cleansed.join("\n") + "\n",
        "\
        this is some output from the test\n\
        this is too\n\
        "
    );
}

#[test]
fn remove_fixture_output_confusing_trailer() {
    let example = "\n\
    running 1 test\n\
    this is some output from the test\n\
    test tests::i_be_failing ... this is the test's own weird output\n\
    this is too\n\
    test tests::i_be_failing ... FAILED\n\
    \n\
    failures:\n\
    \n\
    failures:\n\
        tests::i_be_failing\n\
        \n\
    test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 155 filtered out; \
    finished in 0.01s\n\
    \n\
    ";
    let cleansed = remove_fixture_output(
        "tests::i_be_failing",
        example.split('\n').map(ToOwned::to_owned).collect(),
    );
    assert_eq!(
        cleansed.join("\n") + "\n",
        "\
        this is some output from the test\n\
        test tests::i_be_failing ... this is the test's own weird output\n\
        this is too\n\
        "
    );
}

fn format_test_output(
    res: &JobOutputResult,
    name: &str,
    cjid: ClientJobId,
    case_str: &str,
) -> Vec<String> {
    let (_, case_str) = case_str.rsplit_once(' ').unwrap_or(("", case_str));
    let mut test_output_lines = vec![];
    match res {
        JobOutputResult::None => {}
        JobOutputResult::Inline(bytes) => {
            test_output_lines.extend(
                String::from_utf8_lossy(bytes)
                    .split('\n')
                    .map(ToOwned::to_owned),
            );
            if name == "stdout" {
                test_output_lines = remove_fixture_output(case_str, test_output_lines);
            }
        }
        JobOutputResult::Truncated { first, truncated } => {
            test_output_lines.extend(
                String::from_utf8_lossy(first)
                    .split('\n')
                    .map(ToOwned::to_owned),
            );
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

impl<ProgressIndicatorT: ProgressIndicator> JobStatusVisitor<ProgressIndicatorT> {
    fn print_job_result(
        &self,
        result_str: ColoredString,
        duration_str: String,
        printer: &impl ProgressPrinter,
    ) {
        if self.width > 10 {
            let case_width = self.case_str.width();
            let trailer_str = format!("{result_str} {duration_str:>8}");
            let trailer_width = result_str.width() + 1 + std::cmp::max(duration_str.width(), 8);
            if case_width + trailer_width < self.width {
                let dots_width = self.width - trailer_width - case_width;
                let case = self.case_str.bold();

                printer.println(format!(
                    "{case}{empty:.<dots_width$}{trailer_str}",
                    empty = "",
                ));
            } else {
                let (case, case_width) = self
                    .case_str
                    .unicode_truncate_start(self.width - 2 - trailer_width);
                let case = case.bold();
                let dots_width = self.width - trailer_width - case_width - 1;
                printer.println(format!(
                    "<{case}{empty:.<dots_width$}{trailer_str}",
                    empty = ""
                ));
            }
        } else {
            printer.println(format!("{case} {result_str}", case = self.case_str));
        }
    }

    pub fn job_finished(&self, res: Result<(ClientJobId, JobOutcomeResult)>) {
        let result_str: ColoredString;
        let mut result_details: Option<String> = None;
        let mut test_output_stderr: Vec<String> = vec![];
        let mut test_output_stdout: Vec<String> = vec![];
        let mut duration_str = String::new();
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
                duration_str = format!("{:.3}s", duration.as_secs_f64());
                let mut job_failed = true;
                let exit_code = match status {
                    JobStatus::Exited(code) => {
                        result_str = if code == 0 {
                            job_failed = false;
                            "OK".green()
                        } else {
                            "FAIL".red()
                        };
                        ExitCode::from(code)
                    }
                    JobStatus::Signaled(signo) => {
                        result_str = "FAIL".red();
                        result_details = Some(format!("killed by signal {signo}"));
                        ExitCode::FAILURE
                    }
                };
                if job_failed {
                    test_output_stdout.extend(format_test_output(
                        &stdout,
                        "stdout",
                        cjid,
                        &self.case_str,
                    ));
                    test_output_stderr.extend(format_test_output(
                        &stderr,
                        "stderr",
                        cjid,
                        &self.case_str,
                    ));
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
                result_str = "TIMEOUT".red();
                result_details = Some("timed out".into());
                test_output_stdout.extend(format_test_output(
                    &stdout,
                    "stdout",
                    cjid,
                    &self.case_str,
                ));
                test_output_stderr.extend(format_test_output(
                    &stderr,
                    "stderr",
                    cjid,
                    &self.case_str,
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
                result_str = "ERR".yellow();
                result_details = Some(format!("execution error: {err}"));
                ExitCode::FAILURE
            }
            Ok((_, Err(JobError::System(err)))) => {
                result_str = "ERR".yellow();
                result_details = Some(format!("system error: {err}"));
                ExitCode::FAILURE
            }
            Err(err) => {
                result_str = "ERR".yellow();
                result_details = Some(format!("remote error: {err}"));
                ExitCode::FAILURE
            }
        };
        let printer = self.ind.lock_printing();
        self.print_job_result(result_str, duration_str, &printer);

        if let Some(details_str) = result_details {
            printer.println(details_str);
        }
        for line in test_output_stdout {
            printer.println(line);
        }
        for line in test_output_stderr {
            printer.eprintln(line);
        }
        drop(printer);

        self.ind.job_finished();

        // This call unblocks main thread, so it must go last
        self.tracker.job_exited(self.case_str.clone(), exit_code);
    }

    pub fn job_ignored(&self) {
        self.print_job_result("IGNORED".yellow(), "".into(), &self.ind.lock_printing());
        self.tracker.job_ignored(self.case_str.clone());
        self.ind.job_finished();
    }
}
