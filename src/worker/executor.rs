use crate::{ExecutionDetails, ExecutionResult};
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};

/// A trait alias for a closure that will send a signal to a process.
trait Killer: Fn(Pid, Signal) -> nix::Result<()> + Send + 'static {}
impl<T> Killer for T where T: Fn(Pid, Signal) -> nix::Result<()> + Send + 'static {}

/// A handle for an execution that will kill the process when dropped.
pub struct Handle {
    pid: Pid,
    done_receiver: tokio::sync::oneshot::Receiver<()>,
    killer: Box<dyn Killer>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        match self.done_receiver.try_recv() {
            Ok(()) => {}
            Err(_) => {
                (self.killer)(self.pid, Signal::SIGKILL).ok();
            }
        }
    }
}

/// Start a process (i.e. execution). The process will be killed when the returned handle is
/// dropped, unless it has already completed. The provided done callback is always called on a
/// separate task, even if the error occurs immediately.
pub fn start<T>(details: ExecutionDetails, done: T) -> Handle
where
    T: FnOnce(ExecutionResult) + Send + 'static,
{
    start_with_killer(details, done, signal::kill)
}

fn start_with_killer<T, U>(details: ExecutionDetails, done: T, killer: U) -> Handle
where
    T: FnOnce(ExecutionResult) + Send + 'static,
    U: Killer,
{
    let killer = Box::new(killer);
    let (done_sender, done_receiver) = tokio::sync::oneshot::channel();
    let result = tokio::process::Command::new(details.program)
        .args(details.arguments)
        .stdin(std::process::Stdio::null())
        .spawn();
    match result {
        Err(error) => {
            done_sender.send(()).ok();
            tokio::task::spawn(async move { done(ExecutionResult::Error(error.to_string())) });
            Handle {
                pid: Pid::from_raw(0),
                done_receiver,
                killer,
            }
        }
        Ok(child) => {
            let pid = Pid::from_raw(child.id().unwrap() as i32);
            tokio::task::spawn(async move { waiter(child, done_sender, done).await });
            Handle {
                pid,
                done_receiver,
                killer,
            }
        }
    }
}

async fn waiter<T>(
    mut child: tokio::process::Child,
    done_sender: tokio::sync::oneshot::Sender<()>,
    done: T,
) where
    T: FnOnce(ExecutionResult) + Send + 'static,
{
    use std::os::unix::process::ExitStatusExt;
    done(match child.wait().await {
        Err(error) => ExecutionResult::Error(error.to_string()),
        Ok(status) => match status.code() {
            Some(code) => ExecutionResult::Exited(code as u8),
            None => ExecutionResult::Signalled(status.signal().unwrap() as u8),
        },
    });
    done_sender.send(()).ok();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tempfile;

    macro_rules! bash {
        ($($tokens:expr),*) => {
            ExecutionDetails {
                program: "bash".to_string(),
                arguments: vec![
                    "-c".to_string(),
                    format!($($tokens),*),
                ],
            }
        };
    }

    fn bad_program() -> ExecutionDetails {
        ExecutionDetails {
            program: "a_program_that_does_not_exist".to_string(),
            arguments: vec![],
        }
    }

    async fn start_and_await(details: ExecutionDetails) -> ExecutionResult {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _handle = start(details, move |result| tx.send(result).unwrap());
        rx.await.unwrap()
    }

    fn logging_killer() -> (impl Killer, impl FnOnce() -> Option<Signal>) {
        let killed: Arc<Mutex<Option<Signal>>> = Arc::new(Mutex::new(None));
        let killed_clone = killed.clone();
        (
            move |pid, sig| {
                assert!(killed_clone.lock().unwrap().replace(sig).is_none());
                signal::kill(pid, sig)
            },
            move || *killed.lock().unwrap(),
        )
    }

    async fn start_and_await_with_logging_killer(
        details: ExecutionDetails,
    ) -> (ExecutionResult, Option<Signal>) {
        let (killer, killed) = logging_killer();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _handle = start_with_killer(details, move |result| tx.send(result).unwrap(), killer);
        (rx.await.unwrap(), killed())
    }

    #[tokio::test]
    async fn happy_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut tempfile = tempdir.path().to_path_buf();
        tempfile.push("foo");
        start_and_await(bash!("touch {}", tempfile.display())).await;
        assert!(tempfile.exists());
    }

    #[tokio::test]
    async fn unhappy_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut tempfile = tempdir.path().to_path_buf();
        tempfile.push("foo");

        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = start(
            bash!("sleep infinity && touch {}", tempfile.display()),
            move |result| tx.send(result).unwrap(),
        );
        let result = rx.await.unwrap();
        assert_eq!(result, ExecutionResult::Signalled(9));
        assert!(!tempfile.exists());
    }

    #[tokio::test]
    async fn exited_0_result() {
        assert_eq!(
            start_and_await(bash!("exit 0")).await,
            ExecutionResult::Exited(0)
        );
    }

    #[tokio::test]
    async fn exited_1_result() {
        assert_eq!(
            start_and_await(bash!("exit 1")).await,
            ExecutionResult::Exited(1)
        );
    }

    #[tokio::test]
    async fn signalled_15_result() {
        assert_eq!(
            start_and_await(bash!("kill $$")).await,
            ExecutionResult::Signalled(15)
        );
    }

    #[tokio::test]
    async fn unable_to_execute_result() {
        if let ExecutionResult::Error(_) = start_and_await(bad_program()).await {
        } else {
            panic!("expected error");
        }
    }

    #[tokio::test]
    async fn unable_to_execute_callback_called_on_different_task() {
        let mutex: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
        let guard = mutex.lock().unwrap();
        let mutex_clone = mutex.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _handle = start(bad_program(), move |result| {
            let _guard = mutex_clone.try_lock().unwrap();
            tx.send(result).unwrap()
        });
        drop(guard);
        if let ExecutionResult::Error(_) = rx.await.unwrap() {
        } else {
            panic!("expected error");
        }
    }

    #[tokio::test]
    async fn handle_does_not_signal_if_process_exited() {
        let (result, killed) = start_and_await_with_logging_killer(bash!("exit 1")).await;
        assert_eq!(result, ExecutionResult::Exited(1));
        assert!(killed.is_none());
    }

    #[tokio::test]
    async fn handle_does_not_signal_if_process_killed() {
        let (result, killed) = start_and_await_with_logging_killer(bash!("kill $$")).await;
        assert_eq!(result, ExecutionResult::Signalled(15));
        assert!(killed.is_none());
    }

    #[tokio::test]
    async fn handle_does_not_signal_if_process_does_not_start() {
        let (result, killed) = start_and_await_with_logging_killer(bad_program()).await;
        if let ExecutionResult::Error(_) = result {
        } else {
            panic!("expected error");
        }
        assert!(killed.is_none());
    }

    #[tokio::test]
    async fn handle_sends_signal_on_drop_if_process_still_running() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (killer, killed) = logging_killer();
        let handle = start_with_killer(
            bash!("sleep infinity"),
            move |result| tx.send(result).unwrap(),
            killer,
        );
        drop(handle);
        let result = rx.await.unwrap();
        assert_eq!(result, ExecutionResult::Signalled(9));
        assert_eq!(killed(), Some(Signal::SIGKILL));
    }
}
