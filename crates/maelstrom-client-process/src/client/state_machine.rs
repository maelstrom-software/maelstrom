use anyhow::{anyhow, bail, Error, Result};
use std::{cell::UnsafeCell, future::Future};
use tokio::sync::watch::{Receiver, Sender};

#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Latent,
    Activating,
    Active,
    Failed,
}

/// This struct drives the state machine for the client object.
///
/// When the process starts, the client hasn't yet started. It doesn't start until a client RPC
/// tells it to. That RPC provides critical information necessary for starting the client. It also
/// gives us a way to communicate startup failures back over the client RPC.
///
/// RPCs arrive independently from each other, so we need to allow exactly one startup RPC to get
/// the green light to actually attempt to start the client. That startup process make take a
/// while, so we need to track the starting state explicitly. Startup will either succeed or fail.
/// Only if it succeeds can we actually give out references to the guarded client object.
///
/// At some point, we may receive an error asynchronously (not associated with a client RPC
/// request) from the client that tells us that the client has shut down. An example of this is
/// losing connection with the broker. When this happens, we want to provide a way to interrupt
/// long-running client RPCs and disallow new RPCs. In this implementation, we still allow old
/// references to the protected client to live as long as they like: we just disallow new ones.
/// It's up to the client RPCs themselves to detect the error with the client, or to use the
/// notification mechanism we provide here.
pub struct StateMachine<ActiveT> {
    active: UnsafeCell<Option<ActiveT>>,
    failed: UnsafeCell<Option<String>>,
    sender: Sender<State>,
}

unsafe impl<ActiveT: Send> Sync for StateMachine<ActiveT> {}

impl<ActiveT> Default for StateMachine<ActiveT> {
    fn default() -> Self {
        Self {
            active: UnsafeCell::new(None),
            failed: UnsafeCell::new(None),
            sender: Sender::new(State::Latent),
        }
    }
}

impl<ActiveT> StateMachine<ActiveT> {
    /// Attempt to become the starter. This will return an error unless the state machine is in the
    /// `Latent` state and this is the first attempt to start it. Upon success an
    /// [`ActivationHandle`] will be returned to be used to indicate the outcome of the startup
    /// attempt.
    pub fn try_to_begin_activation(&self) -> Result<ActivationHandle<'_, ActiveT>> {
        if !self.sender.send_if_modified(|state| {
            if *state == State::Latent {
                *state = State::Activating;
                true
            } else {
                false
            }
        }) {
            bail!("client already started");
        }
        Ok(ActivationHandle(self))
    }

    fn active_value(&self, state: State) -> Result<&ActiveT> {
        match state {
            State::Latent | State::Activating => Err(anyhow!("client not yet started")),
            State::Active => {
                let started_ptr = self.active.get();
                let state = unsafe { &*started_ptr }.as_ref().unwrap();
                Ok(state)
            }
            State::Failed => Err(self.failed_error()),
        }
    }

    /// Get a reference to the "guarded" started value. This is the actual client object. This
    /// reference is valid as long as the state machine is. Even if the state machine transitions
    /// to `Failed`, this reference will continue to be valid. We don't destroy the started value
    /// until the state machine is destroyed.
    pub fn active(&self) -> Result<&ActiveT> {
        self.active_value(*self.sender.borrow())
    }

    /// Like [`Self::active`], but also return a [`ActiveWatcher`] that can be used to interrupt
    /// long-running requests when the state machine transitions to failed. The issue here is that
    /// some sorts of out-of-band failures of the client may result in the client just "stopping"
    /// and not resolving requests one way or the other. A watcher can be used to detect this
    /// situation and abort an outstanding request.
    #[allow(dead_code)]
    pub fn active_with_watcher(&self) -> Result<(&ActiveT, ActiveWatcher<'_, ActiveT>)> {
        // We need to be careful to use the copy of our state from the receiver when we call
        // `active_value` so that we don't miss a message when we then go and wait on that
        // receiver.
        let receiver = self.sender.subscribe();
        let idx = *receiver.borrow();
        self.active_value(idx).map(|started| {
            (
                started,
                ActiveWatcher {
                    state_machine: self,
                    receiver,
                },
            )
        })
    }

    /// Can only be called when in [`State::Failed`]. Return the failure string in an
    /// [`anyhow::Error`].
    fn failed_error(&self) -> Error {
        let failed_ptr = self.failed.get();
        let err = unsafe { &*failed_ptr }.as_ref().unwrap();
        anyhow!("client failed with error: {err}")
    }

    /// Attempt to fail the state machine. This function will only succeed if the state machine is
    /// in the `Active` state. On success, the given `err` will be returned in an [`anyhow::Error`]
    /// anytime futurer callers try to activate or get a reference to the active value.
    ///
    /// To fail during activation, use [`ActivationHandle::fail`] instead.
    pub fn fail(&self, err: String) -> bool {
        self.sender.send_if_modified(move |state| {
            if *state == State::Active {
                let failed_ptr = self.failed.get();
                unsafe { *failed_ptr = Some(err) };
                *state = State::Failed;
                true
            } else {
                false
            }
        })
    }
}

/// Returned by [`StateMachine::try_to_begin_activation`]. See that method for details.
pub struct ActivationHandle<'a, ActiveT>(&'a StateMachine<ActiveT>);

impl<'a, ActiveT> ActivationHandle<'a, ActiveT> {
    /// Tell the state machine that activation succeeded. The given `active` will then be stored in
    /// the state machine, and references to it will be given out when future callers ask for the
    /// active value.
    pub fn activate(self, active: ActiveT) {
        self.0.sender.send_modify(|state| {
            assert_eq!(*state, State::Activating);
            let active_ptr = self.0.active.get();
            unsafe { *active_ptr = Some(active) };
            *state = State::Active;
        });
    }

    /// Tell the state machine that activation failed. The given `err` will be returned in an
    /// [`anyhow::Error`] anytime futurer callers try to activate or get a reference to the
    /// active value.
    pub fn fail(self, err: String) {
        self.0.sender.send_modify(|state| {
            assert_eq!(*state, State::Activating);
            let failed_ptr = self.0.failed.get();
            unsafe { *failed_ptr = Some(err) };
            *state = State::Failed;
        });
    }
}

#[allow(dead_code)]
pub struct ActiveWatcher<'a, ActiveT> {
    state_machine: &'a StateMachine<ActiveT>,
    receiver: Receiver<State>,
}

impl<'a, ActiveT> ActiveWatcher<'a, ActiveT> {
    /// Wait for `future`, interrupting if the state machine fails.
    ///
    /// If the future successfully yields a value, that value will be yielded in turn. However, if
    /// the future yields an error, that error will be ignored. The assumption is that the error is
    /// a result of the underlying client shutting down (like closing a channel sender). Instead,
    /// we wait for the state machine to transition to the failed state, then yield that error.
    ///
    /// If the future never yields a value, but the state machine transitions to failed, then the
    /// state machine error will be yielded.
    #[allow(dead_code)]
    pub async fn wait<F, T, E>(mut self, future: F) -> Result<T>
    where
        F: Future<Output = std::result::Result<T, E>>,
    {
        tokio::select! {
            Ok(result) = future => {
                Ok(result)
            }
            _ = self.receiver.changed() => {
                assert_eq!(*self.receiver.borrow(), State::Failed);
                Err(self.state_machine.failed_error())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, time::Duration};
    use tokio::{sync::oneshot::channel, task, time::sleep};

    #[test]
    fn active_while_latent() {
        let sm = StateMachine::<()>::default();
        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client not yet started"
        );
        let Err(err) = sm.active_with_watcher() else {
            panic!("error expected");
        };
        assert_eq!(err.to_string(), "client not yet started");
    }

    #[test]
    fn fail_while_latent() {
        let sm = StateMachine::<()>::default();
        assert!(!sm.fail("foo".to_string()));
        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client not yet started"
        );
        let Err(err) = sm.active_with_watcher() else {
            panic!("error expected");
        };
        assert_eq!(err.to_string(), "client not yet started");
    }

    #[test]
    fn active_while_activating() {
        let sm = StateMachine::<String>::default();
        let ah = sm.try_to_begin_activation().unwrap();

        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client not yet started"
        );
        let Err(err) = sm.active_with_watcher() else {
            panic!("error expected");
        };
        assert_eq!(err.to_string(), "client not yet started");

        ah.activate("active!".to_string());
        assert_eq!(sm.active().unwrap(), "active!");
    }

    #[test]
    fn fail_while_activating() {
        let sm = StateMachine::<String>::default();
        let ah = sm.try_to_begin_activation().unwrap();

        assert!(!sm.fail("foo".to_string()));
        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client not yet started"
        );
        let Err(err) = sm.active_with_watcher() else {
            panic!("error expected");
        };
        assert_eq!(err.to_string(), "client not yet started");

        ah.activate("active!".to_string());
        assert_eq!(sm.active().unwrap(), "active!");
    }

    #[test]
    fn try_to_begin_activation_while_activating() {
        let sm = StateMachine::<String>::default();
        let ah = sm.try_to_begin_activation().unwrap();

        let Err(err) = sm.try_to_begin_activation() else {
            panic!("error expected");
        };
        assert_eq!(err.to_string(), "client already started");

        ah.activate("active!".to_string());
        assert_eq!(sm.active().unwrap(), "active!");
    }

    #[test]
    fn active_while_active() {
        let sm = StateMachine::<String>::default();
        let ah = sm.try_to_begin_activation().unwrap();
        ah.activate("active!".to_string());
        assert_eq!(sm.active().unwrap(), "active!");
        assert_eq!(sm.active_with_watcher().unwrap().0, "active!");
    }

    #[test]
    fn try_to_begin_activation_while_active() {
        let sm = StateMachine::<String>::default();
        let ah = sm.try_to_begin_activation().unwrap();
        ah.activate("active!".to_string());

        let Err(err) = sm.try_to_begin_activation() else {
            panic!("error expected");
        };
        assert_eq!(err.to_string(), "client already started");

        assert_eq!(sm.active().unwrap(), "active!");
    }

    #[test]
    fn fail_while_active() {
        let sm = StateMachine::<()>::default();
        let ah = sm.try_to_begin_activation().unwrap();
        ah.activate(());

        assert!(sm.fail("failure!".to_string()));

        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client failed with error: failure!"
        );
    }

    #[test]
    fn active_while_failed() {
        let sm = StateMachine::<()>::default();
        let ah = sm.try_to_begin_activation().unwrap();
        ah.activate(());
        assert!(sm.fail("failure!".to_string()));

        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client failed with error: failure!"
        );
        let Err(err) = sm.active_with_watcher() else {
            panic!("error expected");
        };
        assert_eq!(err.to_string(), "client failed with error: failure!");
    }

    #[test]
    fn active_while_failed_activating() {
        let sm = StateMachine::<()>::default();
        let ah = sm.try_to_begin_activation().unwrap();
        ah.fail("failure!".to_string());

        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client failed with error: failure!"
        );
        let Err(err) = sm.active_with_watcher() else {
            panic!("error expected");
        };
        assert_eq!(err.to_string(), "client failed with error: failure!");
    }

    #[test]
    fn try_to_begin_activation_while_failed() {
        let sm = StateMachine::<()>::default();
        let ah = sm.try_to_begin_activation().unwrap();
        ah.activate(());
        assert!(sm.fail("failure!".to_string()));

        let Err(err) = sm.try_to_begin_activation() else {
            panic!("error expected");
        };
        assert_eq!(err.to_string(), "client already started");

        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client failed with error: failure!"
        );
    }

    #[test]
    fn try_to_begin_activation_while_failed_activating() {
        let sm = StateMachine::<()>::default();
        let ah = sm.try_to_begin_activation().unwrap();
        ah.fail("failure!".to_string());

        let Err(err) = sm.try_to_begin_activation() else {
            panic!("error expected");
        };
        assert_eq!(err.to_string(), "client already started");

        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client failed with error: failure!"
        );
    }

    #[test]
    fn fail_while_failed() {
        let sm = StateMachine::<()>::default();
        let ah = sm.try_to_begin_activation().unwrap();
        ah.activate(());
        assert!(sm.fail("failure!".to_string()));

        assert!(!sm.fail("foo".to_string()));

        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client failed with error: failure!"
        );
    }

    #[test]
    fn fail_while_failed_activating() {
        let sm = StateMachine::<()>::default();
        let ah = sm.try_to_begin_activation().unwrap();
        ah.fail("failure!".to_string());

        assert!(!sm.fail("foo".to_string()));

        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client failed with error: failure!"
        );
    }

    #[tokio::test]
    async fn active_watcher_wait_fail_before_wait() {
        let sm = StateMachine::<()>::default();
        let ah = sm.try_to_begin_activation().unwrap();
        ah.activate(());

        let aw = sm.active_with_watcher().unwrap().1;
        sm.fail("failure!".to_string());
        let (_tx, rx) = channel::<()>();
        assert_eq!(
            aw.wait(rx).await.unwrap_err().to_string(),
            "client failed with error: failure!"
        );
    }

    #[tokio::test]
    async fn active_watcher_wait_fail_during_wait() {
        let sm = Arc::new(StateMachine::<()>::default());
        let ah = sm.try_to_begin_activation().unwrap();
        ah.activate(());

        let aw = sm.active_with_watcher().unwrap().1;
        let (_tx, rx) = channel::<()>();
        let sm_clone = sm.clone();
        task::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            sm_clone.fail("failure!".to_string());
        });
        assert_eq!(
            aw.wait(rx).await.unwrap_err().to_string(),
            "client failed with error: failure!"
        );
    }

    #[tokio::test]
    async fn active_watcher_wait_future_error_ignored() {
        let sm = Arc::new(StateMachine::<()>::default());
        let ah = sm.try_to_begin_activation().unwrap();
        ah.activate(());

        let aw = sm.active_with_watcher().unwrap().1;
        let (tx, rx) = channel::<()>();
        let sm_clone = sm.clone();
        task::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            sm_clone.fail("failure!".to_string());
        });
        drop(tx);
        assert_eq!(
            aw.wait(rx).await.unwrap_err().to_string(),
            "client failed with error: failure!"
        );
    }

    #[tokio::test]
    async fn active_watcher_wait_success() {
        let sm = StateMachine::<()>::default();
        let ah = sm.try_to_begin_activation().unwrap();
        ah.activate(());

        let aw = sm.active_with_watcher().unwrap().1;
        let (tx, rx) = channel();
        tx.send(42).unwrap();
        assert_eq!(aw.wait(rx).await.unwrap(), 42);
    }
}
