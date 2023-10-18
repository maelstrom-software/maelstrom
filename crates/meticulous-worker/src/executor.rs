//! Easily start and stop processes.

use crate::config::InlineLimit;
use anyhow::Result;
use meticulous_base::{JobDetails, JobOutputResult, JobResult, JobStatus};
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use std::{os::unix::process::ExitStatusExt as _, process::Stdio};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt as _},
    process::{Child, Command},
    sync::oneshot::{self, Receiver, Sender},
    task::{self, JoinHandle},
};

/*              _     _ _
 *  _ __  _   _| |__ | (_) ___
 * | '_ \| | | | '_ \| | |/ __|
 * | |_) | |_| | |_) | | | (__
 * | .__/ \__,_|_.__/|_|_|\___|
 * |_|
 *  FIGLET: public
 */

/// Start a process (i.e. job) and call the provided callback when it completes. The process
/// will be killed when the returned [Handle] is dropped, unless it has already completed. The
/// provided callback is always called on a separate task, even if an error occurs immediately.
pub fn start(
    details: &JobDetails,
    inline_limit: InlineLimit,
    done: impl FnOnce(JobResult) + Send + 'static,
) -> Handle {
    Handle(start_with_killer(
        details,
        inline_limit.into_inner(),
        done,
        (),
    ))
}

/// A handle that will kill the running process when dropped. If the process has already completed,
/// or if it failed to start, then dropping the Handle does nothing.
pub struct Handle(GenericHandle<()>);

/*             _            _
 *  _ __  _ __(_)_   ____ _| |_ ___
 * | '_ \| '__| \ \ / / _` | __/ _ \
 * | |_) | |  | |\ V / (_| | ||  __/
 * | .__/|_|  |_| \_/ \__,_|\__\___|
 * |_|
 *  FIGLET: private
 */

trait Killer: Send + 'static {
    fn kill(&mut self, pid: Pid, signal: Signal);
}

impl Killer for () {
    fn kill(&mut self, pid: Pid, signal: Signal) {
        signal::kill(pid, signal).ok();
    }
}

struct GenericHandle<K: Killer> {
    pid: Pid,
    done_receiver: Receiver<()>,
    killer: K,
}

impl<K: Killer> Drop for GenericHandle<K> {
    fn drop(&mut self) {
        match self.done_receiver.try_recv() {
            Ok(()) => {}
            Err(_) => {
                self.killer.kill(self.pid, Signal::SIGKILL);
            }
        }
    }
}

async fn output_reader(
    inline_limit: u64,
    stream: impl AsyncRead + std::marker::Unpin,
) -> Result<JobOutputResult> {
    let mut buf = Vec::<u8>::new();
    let mut take = stream.take(inline_limit);
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

async fn wait_for_child(
    mut child: Child,
    stdout_reader: JoinHandle<Result<JobOutputResult>>,
    stderr_reader: JoinHandle<Result<JobOutputResult>>,
) -> JobResult {
    let status = match child.wait().await {
        Ok(status) => status,
        Err(err) => {
            return JobResult::ExecutionError(err.to_string());
        }
    };
    let stdout = match stdout_reader.await.unwrap() {
        Ok(stdout) => stdout,
        Err(err) => {
            return JobResult::SystemError(format!("reading stdout: {err}"));
        }
    };
    let stderr = match stderr_reader.await.unwrap() {
        Ok(stderr) => stderr,
        Err(err) => {
            return JobResult::SystemError(format!("reading stderr: {err}"));
        }
    };
    match status.code() {
        Some(code) => JobResult::Ran {
            status: JobStatus::Exited(code as u8),
            stdout,
            stderr,
        },
        None => JobResult::Ran {
            status: JobStatus::Signalled(status.signal().unwrap() as u8),
            stdout,
            stderr,
        },
    }
}

async fn waiter_task_main(
    child: Child,
    stdout_reader: JoinHandle<Result<JobOutputResult>>,
    stderr_reader: JoinHandle<Result<JobOutputResult>>,
    done_sender: Sender<()>,
    done: impl FnOnce(JobResult) + Send + 'static,
) {
    done(wait_for_child(child, stdout_reader, stderr_reader).await);
    done_sender.send(()).ok();
}

fn start_with_killer<K: Killer>(
    details: &JobDetails,
    inline_limit: u64,
    done: impl FnOnce(JobResult) + Send + 'static,
    killer: K,
) -> GenericHandle<K> {
    let (done_sender, done_receiver) = oneshot::channel();
    let result = Command::new(&details.program)
        .args(details.arguments.iter())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    match result {
        Err(error) => {
            done_sender.send(()).ok();
            task::spawn(async move { done(JobResult::ExecutionError(error.to_string())) });
            GenericHandle {
                pid: Pid::from_raw(0),
                done_receiver,
                killer,
            }
        }
        Ok(mut child) => {
            let pid = Pid::from_raw(child.id().unwrap() as i32);
            let stdout_reader =
                task::spawn(output_reader(inline_limit, child.stdout.take().unwrap()));
            let stderr_reader =
                task::spawn(output_reader(inline_limit, child.stderr.take().unwrap()));
            task::spawn(async move {
                waiter_task_main(child, stdout_reader, stderr_reader, done_sender, done).await
            });
            GenericHandle {
                pid,
                done_receiver,
                killer,
            }
        }
    }
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
    use anyhow::anyhow;
    use std::sync::{Arc, Mutex};
    use tempfile;

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

    fn bad_program() -> JobDetails {
        JobDetails {
            program: "a_program_that_does_not_exist".to_string(),
            arguments: vec![],
            layers: vec![],
        }
    }

    async fn start_and_await(details: JobDetails, inline_limit: u64) -> JobResult {
        let (tx, rx) = oneshot::channel();
        let _handle = start(&details, inline_limit.into(), move |result| {
            tx.send(result).unwrap()
        });
        rx.await.unwrap()
    }

    impl Killer for Arc<Mutex<Option<Signal>>> {
        fn kill(&mut self, pid: Pid, signal: Signal) {
            assert!(self.lock().unwrap().replace(signal).is_none());
            signal::kill(pid, signal).ok();
        }
    }

    async fn start_and_await_with_logging_killer(
        details: JobDetails,
        inline_limit: u64,
    ) -> (JobResult, Option<Signal>) {
        let killer = Arc::new(Mutex::new(None));
        let (tx, rx) = oneshot::channel();
        let _handle = start_with_killer(
            &details,
            inline_limit,
            move |result| tx.send(result).unwrap(),
            killer.clone(),
        );
        let signal = *killer.lock().unwrap();
        (rx.await.unwrap(), signal)
    }

    #[tokio::test]
    async fn happy_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut tempfile = tempdir.path().to_path_buf();
        tempfile.push("foo");
        start_and_await(bash!("touch {}", tempfile.display()), 0).await;
        assert!(tempfile.exists());
    }

    #[tokio::test]
    async fn unhappy_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut tempfile = tempdir.path().to_path_buf();
        tempfile.push("foo");

        let (tx, rx) = oneshot::channel();
        let _ = start(
            &bash!("sleep infinity && touch {}", tempfile.display()),
            0.into(),
            move |result| tx.send(result).unwrap(),
        );
        let result = rx.await.unwrap();
        assert_eq!(
            result,
            JobResult::Ran {
                status: JobStatus::Signalled(9),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        );
        assert!(!tempfile.exists());
    }

    #[tokio::test]
    async fn exited_0_result() {
        assert_eq!(
            start_and_await(bash!("exit 0"), 0).await,
            JobResult::Ran {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        );
    }

    #[tokio::test]
    async fn stdout() {
        assert_eq!(
            start_and_await(bash!("echo a"), 0).await,
            JobResult::Ran {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::Truncated {
                    first: b"".to_vec().into_boxed_slice(),
                    truncated: 2
                },
                stderr: JobOutputResult::None,
            }
        );
        assert_eq!(
            start_and_await(bash!("echo a"), 1).await,
            JobResult::Ran {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::Truncated {
                    first: b"a".to_vec().into_boxed_slice(),
                    truncated: 1
                },
                stderr: JobOutputResult::None,
            }
        );
        assert_eq!(
            start_and_await(bash!("echo a"), 2).await,
            JobResult::Ran {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::Inline(b"a\n".to_vec().into_boxed_slice()),
                stderr: JobOutputResult::None,
            }
        );
        assert_eq!(
            start_and_await(bash!("echo a"), 3).await,
            JobResult::Ran {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::Inline(b"a\n".to_vec().into_boxed_slice()),
                stderr: JobOutputResult::None,
            }
        );
    }

    #[tokio::test]
    async fn stderr() {
        assert_eq!(
            start_and_await(bash!("echo a>&2"), 0).await,
            JobResult::Ran {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::Truncated {
                    first: b"".to_vec().into_boxed_slice(),
                    truncated: 2
                },
            }
        );
        assert_eq!(
            start_and_await(bash!("echo a>&2"), 1).await,
            JobResult::Ran {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::Truncated {
                    first: b"a".to_vec().into_boxed_slice(),
                    truncated: 1
                },
            }
        );
        assert_eq!(
            start_and_await(bash!("echo a>&2"), 2).await,
            JobResult::Ran {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::Inline(b"a\n".to_vec().into_boxed_slice()),
            }
        );
        assert_eq!(
            start_and_await(bash!("echo a>&2"), 3).await,
            JobResult::Ran {
                status: JobStatus::Exited(0),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::Inline(b"a\n".to_vec().into_boxed_slice()),
            }
        );
    }

    #[tokio::test]
    async fn exited_1_result() {
        assert_eq!(
            start_and_await(bash!("exit 1"), 0).await,
            JobResult::Ran {
                status: JobStatus::Exited(1),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        );
    }

    #[tokio::test]
    async fn signalled_15_result() {
        assert_eq!(
            start_and_await(bash!("echo a && echo b>&2 && kill $$"), 2).await,
            JobResult::Ran {
                status: JobStatus::Signalled(15),
                stdout: JobOutputResult::Inline(b"a\n".to_vec().into_boxed_slice()),
                stderr: JobOutputResult::Inline(b"b\n".to_vec().into_boxed_slice()),
            }
        );
    }

    #[tokio::test]
    async fn unable_to_execute_result() {
        if let JobResult::ExecutionError(_) = start_and_await(bad_program(), 0).await {
        } else {
            panic!("expected execution error");
        }
    }

    #[tokio::test]
    async fn unable_to_execute_callback_called_on_different_task() {
        let mutex: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
        let guard = mutex.lock().unwrap();
        let mutex_clone = mutex.clone();
        let (tx, rx) = oneshot::channel();
        let _handle = start(&bad_program(), 0.into(), move |result| {
            let _guard = mutex_clone.try_lock().unwrap();
            tx.send(result).unwrap()
        });
        drop(guard);
        if let JobResult::ExecutionError(_) = rx.await.unwrap() {
        } else {
            panic!("expected execution error");
        }
    }

    #[tokio::test]
    async fn handle_does_not_signal_if_process_exited() {
        let (result, killed) = start_and_await_with_logging_killer(bash!("exit 1"), 0).await;
        assert_eq!(
            result,
            JobResult::Ran {
                status: JobStatus::Exited(1),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        );
        assert!(killed.is_none());
    }

    #[tokio::test]
    async fn handle_does_not_signal_if_process_killed() {
        let (result, killed) = start_and_await_with_logging_killer(bash!("kill $$"), 0).await;
        assert_eq!(
            result,
            JobResult::Ran {
                status: JobStatus::Signalled(15),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        );
        assert!(killed.is_none());
    }

    #[tokio::test]
    async fn handle_does_not_signal_if_process_does_not_start() {
        let (result, killed) = start_and_await_with_logging_killer(bad_program(), 0).await;
        if let JobResult::ExecutionError(_) = result {
        } else {
            panic!("expected execution error");
        }
        assert!(killed.is_none());
    }

    #[tokio::test]
    async fn handle_sends_signal_on_drop_if_process_still_running() {
        let (tx, rx) = oneshot::channel();
        let killer = Arc::new(Mutex::new(None));
        let handle = start_with_killer(
            &bash!("sleep infinity"),
            0,
            move |result| tx.send(result).unwrap(),
            killer.clone(),
        );
        drop(handle);
        let result = rx.await.unwrap();
        assert_eq!(
            result,
            JobResult::Ran {
                status: JobStatus::Signalled(9),
                stdout: JobOutputResult::None,
                stderr: JobOutputResult::None,
            }
        );
        assert_eq!(*killer.lock().unwrap(), Some(Signal::SIGKILL));
    }

    #[tokio::test]
    async fn wait_for_child_stdout_error() {
        let child = Command::new("bash").stdin(Stdio::null()).spawn().unwrap();
        let stdout_reader = task::spawn(async { Err(anyhow!("an error")) });
        let stderr_reader = task::spawn(async { Ok(JobOutputResult::None) });
        assert_eq!(
            wait_for_child(child, stdout_reader, stderr_reader).await,
            JobResult::SystemError(format!("reading stdout: an error"))
        );
    }

    #[tokio::test]
    async fn wait_for_child_stderr_error() {
        let child = Command::new("bash").stdin(Stdio::null()).spawn().unwrap();
        let stdout_reader = task::spawn(async { Ok(JobOutputResult::None) });
        let stderr_reader = task::spawn(async { Err(anyhow!("an error")) });
        assert_eq!(
            wait_for_child(child, stdout_reader, stderr_reader).await,
            JobResult::SystemError(format!("reading stderr: an error"))
        );
    }
}
