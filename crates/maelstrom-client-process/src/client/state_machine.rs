use anyhow::{anyhow, bail, Error, Result};
use std::cell::UnsafeCell;
use tokio::sync::watch::Sender;

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
/// losing connection with the broker. When this happens, we want to disallow new RPCs. In this
/// implementation, we still allow old references to the protected client to live as long as they
/// like: we just disallow new ones. It's up to the client RPCs themselves to detect the error with
/// the client.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_while_latent() {
        let sm = StateMachine::<()>::default();
        assert_eq!(
            sm.active().unwrap_err().to_string(),
            "client not yet started"
        );
        let Err(err) = sm.active() else {
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
        let Err(err) = sm.active() else {
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
        let Err(err) = sm.active() else {
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
        let Err(err) = sm.active() else {
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
        assert_eq!(sm.active().unwrap(), "active!");
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
        let Err(err) = sm.active() else {
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
        let Err(err) = sm.active() else {
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
}
