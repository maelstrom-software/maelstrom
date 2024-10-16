use futures::{stream::FuturesUnordered, StreamExt as _};
use maelstrom_linux::Signal;
use slog::{error, Logger};
use std::collections::{BTreeMap, BTreeSet};
use tokio::signal::unix::{self, Signal as TokioSignal, SignalKind as TokioSignalKind};

fn kind_from_sig(sig: Signal) -> TokioSignalKind {
    TokioSignalKind::from_raw(sig.as_c_int())
}

struct SignalWaiter {
    signals: BTreeMap<Signal, TokioSignal>,
    log: Logger,
}

impl SignalWaiter {
    fn new(log: Logger) -> Self {
        Self {
            log,
            signals: BTreeMap::new(),
        }
    }

    fn add_signal(&mut self, sig: Signal) {
        let tokio_sig = unix::signal(kind_from_sig(sig))
            .unwrap_or_else(|_| panic!("failed to register signal handler for {sig}"));
        self.signals.insert(sig, tokio_sig);
    }

    async fn wait_for_signal(&mut self) -> Signal {
        assert!(!self.signals.is_empty(), "SignalWaiter has no signals");

        let mut futs: FuturesUnordered<_> = self
            .signals
            .iter_mut()
            .map(|(k, s)| async move {
                s.recv().await;
                *k
            })
            .collect();
        let signal = futs.next().await.unwrap();
        error!(self.log, "received {signal}");
        signal
    }
}

const EXIT: [Signal; 12] = [
    Signal::ALRM,
    Signal::HUP,
    Signal::INT,
    Signal::IO,
    Signal::LOST,
    Signal::POLL,
    Signal::PROF,
    Signal::PWR,
    Signal::TERM,
    Signal::USR1,
    Signal::USR2,
    Signal::VTALRM,
];

const IGNORE: [Signal; 4] = [Signal::PIPE, Signal::TSTP, Signal::TTIN, Signal::TTOU];

fn default_signal_waiter(log: Logger) -> SignalWaiter {
    let mut w = SignalWaiter::new(log);
    for s in EXIT.iter().chain(IGNORE.iter()) {
        w.add_signal(*s);
    }
    w
}

async fn default_signal_handler(mut w: SignalWaiter) -> Signal {
    let exit: BTreeSet<_> = EXIT.into_iter().collect();
    let ignore: BTreeSet<_> = IGNORE.into_iter().collect();
    loop {
        let signal = w.wait_for_signal().await;
        if ignore.contains(&signal) {
            continue;
        } else if exit.contains(&signal) {
            break signal;
        } else {
            unreachable!()
        }
    }
}

pub async fn wait_for_signal(log: Logger) -> Signal {
    let w = default_signal_waiter(log);
    default_signal_handler(w).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::main(flavor = "current_thread")]
    async fn test_wait_for_signal(sock: std::os::unix::net::UnixStream) {
        use tokio::io::AsyncWriteExt as _;
        let mut sock = tokio::net::UnixStream::from_std(sock).unwrap();

        let log = crate::log::test_logger();
        let w = default_signal_waiter(log);
        sock.write_all(&[12]).await.unwrap();

        default_signal_handler(w).await;
    }

    enum ForkResult {
        Child(std::os::unix::net::UnixStream),
        Parent(maelstrom_linux::Pid),
    }

    /// Simulate a fork by calling ourselves via /proc/self/exe
    fn fake_fork(test: &str) -> ForkResult {
        use maelstrom_linux as linux;
        use std::io::Read as _;
        use std::os::linux::net::SocketAddrExt as _;
        use std::os::unix::net::{SocketAddr, UnixListener, UnixStream};

        if let Ok(value) = std::env::var("CHILD") {
            let address = SocketAddr::from_abstract_name(value).unwrap();
            let sock = UnixStream::connect_addr(&address).unwrap();
            ForkResult::Child(sock)
        } else {
            let sock = linux::socket(
                linux::SocketDomain::UNIX,
                linux::SocketType::STREAM,
                Default::default(),
            )
            .unwrap();
            linux::bind(&sock, &linux::SockaddrUnStorage::new_autobind()).unwrap();
            linux::listen(&sock, 1).unwrap();
            let listener = UnixListener::from(sock);
            let local_addr = listener.local_addr().unwrap();
            let address_bytes = local_addr.as_abstract_name().unwrap();
            let address_str = std::str::from_utf8(address_bytes).unwrap();

            let child = std::process::Command::new("/proc/self/exe")
                .args(["--exact", test])
                .env("CHILD", address_str)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap();
            let pid: linux::Pid = child.into();

            let (mut sock, _) = listener.accept().unwrap();

            let mut b = [0];
            sock.read_exact(&mut b).unwrap();

            ForkResult::Parent(pid)
        }
    }

    #[test]
    fn wait_for_signal_exit() {
        use maelstrom_linux as linux;

        for s in EXIT {
            match fake_fork("signal::tests::wait_for_signal_exit") {
                ForkResult::Parent(pid) => {
                    println!("trying signal {s}");
                    linux::kill(pid, s).unwrap();
                    let status = linux::waitpid(pid).unwrap();
                    assert_eq!(
                        status,
                        linux::WaitStatus::Exited(linux::ExitCode::from_u8(0))
                    );
                }
                ForkResult::Child(s) => {
                    test_wait_for_signal(s);
                    break;
                }
            }
        }
    }

    #[test]
    fn wait_for_signal_ignore() {
        use maelstrom_linux as linux;

        match fake_fork("signal::tests::wait_for_signal_ignore") {
            ForkResult::Parent(pid) => {
                let wait_handle = std::thread::spawn(move || {
                    let status = linux::waitpid(pid).unwrap();
                    assert_eq!(
                        status,
                        linux::WaitStatus::Exited(linux::ExitCode::from_u8(0))
                    );
                });

                for s in IGNORE {
                    println!("trying signal {s}");
                    linux::kill(pid, s).unwrap();

                    // Wait a little to see if the process died.
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    assert!(!wait_handle.is_finished());
                }

                // Make it exit now.
                println!("killing with signal SIGINT");
                linux::kill(pid, Signal::INT).unwrap();
                wait_handle.join().unwrap();
            }
            ForkResult::Child(s) => {
                test_wait_for_signal(s);
            }
        }
    }
}
