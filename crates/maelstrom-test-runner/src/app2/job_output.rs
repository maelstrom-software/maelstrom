use crate::ui::{UiJobId as JobId, UiJobResult, UiJobStatus};
use crate::CollectTests;
use anyhow::Result;
use maelstrom_base::{
    ClientJobId, JobCompleted, JobEffects, JobError, JobOutcome, JobOutcomeResult, JobOutputResult,
    JobTerminationStatus,
};
use maelstrom_util::process::ExitCode;

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

#[test]
fn split_into_lines() {
    assert_eq!(split_test_output_into_lines(b"a".as_slice()), vec!["a"]);
    assert_eq!(split_test_output_into_lines(b"a\n".as_slice()), vec!["a"]);
    assert_eq!(
        split_test_output_into_lines(b"a\nb".as_slice()),
        vec!["a", "b"]
    );
    assert_eq!(
        split_test_output_into_lines(b"a\nb\n".as_slice()),
        vec!["a", "b"]
    );
    assert_eq!(
        split_test_output_into_lines(b"a\nb\n\n".as_slice()),
        vec!["a", "b", ""]
    );
}

fn was_ignored<TestCollectorT: CollectTests>(res: &JobOutputResult, case_str: &str) -> bool {
    let (_, case_str) = case_str.rsplit_once(' ').unwrap_or(("", case_str));
    let lines = match res {
        JobOutputResult::None => vec![],
        JobOutputResult::Inline(bytes) => split_test_output_into_lines(bytes),
        JobOutputResult::Truncated { first, .. } => split_test_output_into_lines(first),
    };
    TestCollectorT::was_test_ignored(case_str, &lines)
}

fn format_test_output<TestCollectorT: CollectTests>(
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
            test_output_lines.extend(split_test_output_into_lines(bytes));
            if name == "stdout" {
                test_output_lines =
                    TestCollectorT::remove_fixture_output(case_str, test_output_lines);
            }
        }
        JobOutputResult::Truncated { first, truncated } => {
            test_output_lines.extend(split_test_output_into_lines(first));
            if name == "stdout" {
                test_output_lines =
                    TestCollectorT::remove_fixture_output(case_str, test_output_lines);
            }
            test_output_lines.push(format!(
                "job {cjid}: {name} truncated, {truncated} bytes lost"
            ));
        }
    }
    test_output_lines
}

pub fn build_ignored_ui_job_result(job_id: JobId, case_str: &str) -> UiJobResult {
    UiJobResult {
        name: case_str.into(),
        job_id,
        status: UiJobStatus::Ignored,
        duration: None,
        stdout: vec![],
        stderr: vec![],
    }
}

pub fn build_ui_job_result_and_exit_code<TestCollectorT: CollectTests>(
    job_id: JobId,
    case_str: &str,
    res: Result<(ClientJobId, JobOutcomeResult)>,
) -> (UiJobResult, ExitCode) {
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
                    test_status = UiJobStatus::Failure(Some(format!("killed by signal {signo}")));
                    ExitCode::FAILURE
                }
            };
            if job_failed {
                test_output_stdout.extend(format_test_output::<TestCollectorT>(
                    &stdout, "stdout", cjid, case_str,
                ));
                test_output_stderr.extend(format_test_output::<TestCollectorT>(
                    &stderr, "stderr", cjid, case_str,
                ));
            }

            if !job_failed && was_ignored::<TestCollectorT>(&stdout, case_str) {
                return (
                    build_ignored_ui_job_result(job_id, case_str),
                    ExitCode::SUCCESS,
                );
            }

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
            test_output_stdout.extend(format_test_output::<TestCollectorT>(
                &stdout, "stdout", cjid, case_str,
            ));
            test_output_stderr.extend(format_test_output::<TestCollectorT>(
                &stderr, "stderr", cjid, case_str,
            ));
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

    (
        UiJobResult {
            job_id,
            name: case_str.into(),
            status: test_status,
            duration: test_duration,
            stdout: test_output_stdout,
            stderr: test_output_stderr,
        },
        exit_code,
    )
}
