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
/// When the process starts, the client hasn't yet started. It isn't started until a client RPC
/// tells it to. That RPC provides critical information necessary for starting the client. It also
/// gives us a way to communicate startup failures back to the client RPC.
///
/// RPCs arrive independently from each other, so we need to allow exactly one startup RPC to get
/// the green light to actually attempt to start the client. That startup process make take a
/// while, so we need to track the starting state explicitly. Startup will either succeed or fail.
/// Only if it succeeds can we actually give out references to the guarded client object.
///
/// At some point, we may receive an error asynchronously (not associated with a client RPC
/// request) from the client that tells us that the client has shut down. An example of this is
/// losing connection with the broker. When this happens, we want to provide a way to interrupt
/// long-running client RPCs and alow disallow new RPCs. In this implementation, we still allow old
/// references to the protected client to live as long as they like: we just disallow new ones.
/// It's up to the client RPCs themselves to detect the error with the client, or to use the
/// notification mechanism we provide here.
pub struct StateMachine<LatentT, ActiveT> {
    latent: UnsafeCell<Option<LatentT>>,
    active: UnsafeCell<Option<ActiveT>>,
    failed: UnsafeCell<Option<String>>,
    sender: Sender<State>,
}

unsafe impl<LatentT: Send, ActiveT: Send> Sync for StateMachine<LatentT, ActiveT> {}

impl<LatentT, ActiveT> StateMachine<LatentT, ActiveT> {
    /// Initialize the state machine, providing some extra information that will be passed to the
    /// starter.
    pub fn new(latent: LatentT) -> Self {
        Self {
            latent: UnsafeCell::new(Some(latent)),
            active: UnsafeCell::new(None),
            failed: UnsafeCell::new(None),
            sender: Sender::new(State::Latent),
        }
    }

    /// Attempt to become the starter. This will return an error unless the state machine is in the
    /// `Latent` state and this is the first attempt to start it. Upon success, two things
    /// will be returned: the extra information provided to [`Self::new`] as well as a
    /// [`StartingHandle`] to be used to indicate the outcome of the startup attempt.
    pub fn try_to_begin_activation(
        &self,
    ) -> Result<(LatentT, ActivationHandle<'_, LatentT, ActiveT>)> {
        let mut latent: Option<LatentT> = None;
        if !self.sender.send_if_modified(|state| {
            if *state == State::Latent {
                let latent_ptr = self.latent.get();
                latent = unsafe { &mut *latent_ptr }.take();
                *state = State::Activating;
                true
            } else {
                false
            }
        }) {
            bail!("client already started");
        }
        Ok((latent.unwrap(), ActivationHandle(self)))
    }

    fn active_value(&self, state: State) -> Result<&ActiveT> {
        match state {
            State::Latent | State::Activating => Err(anyhow!("client not yet started")),
            State::Active => {
                let started_ptr = self.active.get();
                let state = unsafe { &*started_ptr }.as_ref().unwrap();
                Ok(state)
            }
            State::Failed => Err(self.failed_value()),
        }
    }

    /// Get a reference to the "guarded" started value. This is the actual client object. This
    /// reference is valid as long as the state machine is. Even if the state machine transitions
    /// to `Failed`, this reference will continue to be valid. We don't destroy the started value
    /// until the state machine is destroyed.
    pub fn active(&self) -> Result<&ActiveT> {
        self.active_value(*self.sender.borrow())
    }

    /// Like [`started`], but also return a [`StateWatcher`] that can be used to interrupt
    /// long-running requests when the state machine transitions to failed. The issue here is that
    /// some sorts of out-of-band failures of the client may result in the client just "stopping"
    /// and not resolving requests one way or the other. A watcher can be used to detect this
    /// situation and abort an outstanding request.
    pub fn active_with_watcher(&self) -> Result<(&ActiveT, ActiveWatcher<'_, LatentT, ActiveT>)> {
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

    fn failed_value(&self) -> Error {
        let failed_ptr = self.failed.get();
        let err = unsafe { &*failed_ptr }.as_ref().unwrap();
        anyhow!("client failed with error: {err}")
    }

    /// Attempt to fail the state machine. This function will only succeed if the state machine is
    /// in the `Active` state.
    pub fn fail(&self, err: String) -> bool {
        self.sender.send_if_modified(move |state| {
            if *state == State::Active {
                let failed_ptr = self.failed.get();
                unsafe { *failed_ptr = Some(err) };
                true
            } else {
                false
            }
        })
    }
}

pub struct ActivationHandle<'a, LatentT, ActiveT>(&'a StateMachine<LatentT, ActiveT>);

impl<'a, LatentT, ActiveT> ActivationHandle<'a, LatentT, ActiveT> {
    pub fn activate(self, active: ActiveT) {
        self.0.sender.send_modify(|state| {
            assert_eq!(*state, State::Activating);
            let active_ptr = self.0.active.get();
            unsafe { *active_ptr = Some(active) };
            *state = State::Active;
        });
    }

    pub fn fail(self, err: String) {
        self.0.sender.send_modify(|state| {
            assert_eq!(*state, State::Activating);
            let failed_ptr = self.0.failed.get();
            unsafe { *failed_ptr = Some(err) };
            *state = State::Failed;
        });
    }
}

pub struct ActiveWatcher<'a, LatentT, ActiveT> {
    state_machine: &'a StateMachine<LatentT, ActiveT>,
    receiver: Receiver<State>,
}

impl<'a, LatentT, ActiveT> ActiveWatcher<'a, LatentT, ActiveT> {
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
                Err(self.state_machine.failed_value())
            }
        }
    }
}
