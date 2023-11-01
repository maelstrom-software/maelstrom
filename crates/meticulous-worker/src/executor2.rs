//! Easily start and stop processes.

#![allow(dead_code)]

use crate::config::InlineLimit;
use anyhow::{Error, Result};
use meticulous_base::{JobDetails, JobOutputResult, JobStatus};
use nix::unistd::Pid;
use std::{os::unix::process::ExitStatusExt as _, process::Stdio};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt as _},
    process::{Child, Command},
    task,
};

/*              _     _ _
 *  _ __  _   _| |__ | (_) ___
 * | '_ \| | | | '_ \| | |/ __|
 * | |_) | |_| | |_) | | | (__
 * | .__/ \__,_|_.__/|_|_|\___|
 * |_|
 *  FIGLET: public
 */

/// Return value from [`start`]. See [`JobResult`] for an explanation of the
/// [`StartResult::ExecutionError`] and [`StartResult::SystemError`] variants.
pub enum StartResult {
    Ok(Pid),
    ExecutionError(Error),
    SystemError(Error),
}

/// Start a process (i.e. job) and call the provided callback when it completes. The process
/// will be killed when the returned [Handle] is dropped, unless it has already completed. The
/// provided callback is always called on a separate task, even if an error occurs immediately.
#[must_use]
pub fn start(
    details: &JobDetails,
    inline_limit: InlineLimit,
    status_done: impl FnOnce(Result<JobStatus>) + Send + 'static,
    stdout_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
    stderr_done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
) -> StartResult {
    let result = Command::new(&details.program)
        .args(details.arguments.iter())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    match result {
        Err(error) => StartResult::ExecutionError(error.into()),
        Ok(mut child) => {
            let pid = Pid::from_raw(child.id().unwrap() as i32);
            task::spawn(output_reader_task_main(
                inline_limit,
                child.stdout.take().unwrap(),
                stdout_done,
            ));
            task::spawn(output_reader_task_main(
                inline_limit,
                child.stderr.take().unwrap(),
                stderr_done,
            ));
            task::spawn(waiter_task_main(child, status_done));
            StartResult::Ok(pid)
        }
    }
}

/*             _            _
 *  _ __  _ __(_)_   ____ _| |_ ___
 * | '_ \| '__| \ \ / / _` | __/ _ \
 * | |_) | |  | |\ V / (_| | ||  __/
 * | .__/|_|  |_| \_/ \__,_|\__\___|
 * |_|
 *  FIGLET: private
 */

async fn output_reader(
    inline_limit: InlineLimit,
    stream: impl AsyncRead + std::marker::Unpin,
) -> Result<JobOutputResult> {
    let mut buf = Vec::<u8>::new();
    let mut take = stream.take(inline_limit.into_inner());
    take.read_to_end(&mut buf).await?;
    let buf = buf.into_boxed_slice();
    let truncated = io::copy(&mut take.into_inner(), &mut io::sink()).await?;
    if truncated == 0 {
        if buf.is_empty() {
            Ok(JobOutputResult::None)
        } else {
            Ok(JobOutputResult::Inline(buf))
        }
    } else {
        Ok(JobOutputResult::Truncated {
            first: buf,
            truncated,
        })
    }
}

async fn output_reader_task_main(
    inline_limit: InlineLimit,
    stream: impl AsyncRead + std::marker::Unpin,
    done: impl FnOnce(Result<JobOutputResult>) + Send + 'static,
) {
    done(output_reader(inline_limit, stream).await);
}

async fn waiter(mut child: Child) -> Result<JobStatus> {
    let status = child.wait().await?;
    Ok(match status.code() {
        Some(code) => JobStatus::Exited(code as u8),
        None => JobStatus::Signalled(status.signal().unwrap() as u8),
    })
}

async fn waiter_task_main(child: Child, done: impl FnOnce(Result<JobStatus>) + Send + 'static) {
    done(waiter(child).await);
}

/*  _            _
 * | |_ ___  ___| |_ ___
 * | __/ _ \/ __| __/ __|
 * | ||  __/\__ \ |_\__ \
 *  \__\___||___/\__|___/
 *  FIGLET: tests
 */

#[cfg(test)]
mod tests {
    use super::*;
    use meticulous_test::boxed_u8;
    use tokio::sync::oneshot;

    macro_rules! bash {
        ($($tokens:expr),*) => {
            JobDetails {
                program: "bash".to_string(),
                arguments: vec![
                    "-c".to_string(),
                    format!($($tokens),*),
                ],
                layers: vec![],
            }
        };
    }

    async fn start_and_expect(
        details: JobDetails,
        inline_limit: u64,
        expected_status: JobStatus,
        expected_stdout: JobOutputResult,
        expected_stderr: JobOutputResult,
    ) {
        let (status_tx, status_rx) = oneshot::channel();
        let (stdout_tx, stdout_rx) = oneshot::channel();
        let (stderr_tx, stderr_rx) = oneshot::channel();
        assert!(matches!(
            start(
                &details,
                InlineLimit::from(inline_limit),
                |status| status_tx.send(status.unwrap()).unwrap(),
                |stdout| stdout_tx.send(stdout.unwrap()).unwrap(),
                |stderr| stderr_tx.send(stderr.unwrap()).unwrap(),
            ),
            StartResult::Ok(_)
        ));
        assert_eq!(status_rx.await.unwrap(), expected_status);
        assert_eq!(stdout_rx.await.unwrap(), expected_stdout);
        assert_eq!(stderr_rx.await.unwrap(), expected_stderr);
    }

    #[tokio::test]
    async fn exited_0() {
        start_and_expect(
            bash!("exit 0"),
            0,
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    async fn exited_1() {
        start_and_expect(
            bash!("exit 1"),
            0,
            JobStatus::Exited(1),
            JobOutputResult::None,
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    async fn signalled_15() {
        start_and_expect(
            bash!("echo a && echo b>&2 && kill $$"),
            2,
            JobStatus::Signalled(15),
            JobOutputResult::Inline(boxed_u8!(b"a\n")),
            JobOutputResult::Inline(boxed_u8!(b"b\n")),
        )
        .await;
    }

    #[tokio::test]
    async fn stdout() {
        start_and_expect(
            bash!("echo a"),
            0,
            JobStatus::Exited(0),
            JobOutputResult::Truncated {
                first: boxed_u8!(b""),
                truncated: 2,
            },
            JobOutputResult::None,
        )
        .await;
        start_and_expect(
            bash!("echo a"),
            1,
            JobStatus::Exited(0),
            JobOutputResult::Truncated {
                first: boxed_u8!(b"a"),
                truncated: 1,
            },
            JobOutputResult::None,
        )
        .await;
        start_and_expect(
            bash!("echo a"),
            2,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"a\n")),
            JobOutputResult::None,
        )
        .await;
        start_and_expect(
            bash!("echo a"),
            3,
            JobStatus::Exited(0),
            JobOutputResult::Inline(boxed_u8!(b"a\n")),
            JobOutputResult::None,
        )
        .await;
    }

    #[tokio::test]
    async fn stderr() {
        start_and_expect(
            bash!("echo a >&2"),
            0,
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::Truncated {
                first: boxed_u8!(b""),
                truncated: 2,
            },
        )
        .await;
        start_and_expect(
            bash!("echo a >&2"),
            1,
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::Truncated {
                first: boxed_u8!(b"a"),
                truncated: 1,
            },
        )
        .await;
        start_and_expect(
            bash!("echo a >&2"),
            2,
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::Inline(boxed_u8!(b"a\n")),
        )
        .await;
        start_and_expect(
            bash!("echo a >&2"),
            3,
            JobStatus::Exited(0),
            JobOutputResult::None,
            JobOutputResult::Inline(boxed_u8!(b"a\n")),
        )
        .await;
    }

    #[tokio::test]
    async fn unable_to_execute_result() {
        let details = JobDetails {
            program: "a_program_that_does_not_exist".to_string(),
            arguments: vec![],
            layers: vec![],
        };
        assert!(matches!(
            start(
                &details,
                0.into(),
                |_| unreachable!(),
                |_| unreachable!(),
                |_| unreachable!()
            ),
            StartResult::ExecutionError(_)
        ));
    }
}
