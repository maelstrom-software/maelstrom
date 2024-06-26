use super::ProgressIndicator;
use crate::ClientTrait;
use anyhow::Result;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

pub trait ProgressDriver<'scope> {
    fn drive<'client>(&mut self, deps: &'client impl ClientTrait, ind: impl ProgressIndicator)
    where
        'client: 'scope;

    fn stop(&mut self) -> Result<()>;
}

pub struct DefaultProgressDriver<'scope, 'env> {
    scope: &'scope thread::Scope<'scope, 'env>,
    handle: Option<thread::ScopedJoinHandle<'scope, Result<()>>>,
    canceled: Arc<AtomicBool>,
}

impl<'scope, 'env> DefaultProgressDriver<'scope, 'env> {
    pub fn new(scope: &'scope thread::Scope<'scope, 'env>) -> Self {
        Self {
            scope,
            handle: None,
            canceled: Default::default(),
        }
    }
}

impl<'scope, 'env> Drop for DefaultProgressDriver<'scope, 'env> {
    fn drop(&mut self) {
        self.canceled.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join().unwrap();
        }
    }
}

impl<'scope, 'env> ProgressDriver<'scope> for DefaultProgressDriver<'scope, 'env> {
    fn drive<'client>(&mut self, client: &'client impl ClientTrait, ind: impl ProgressIndicator)
    where
        'client: 'scope,
    {
        let canceled = self.canceled.clone();
        self.handle = Some(self.scope.spawn(move || {
            thread::scope(|scope| {
                scope.spawn(|| {
                    while ind.tick() && !canceled.load(Ordering::Acquire) {
                        thread::sleep(Duration::from_millis(500))
                    }
                });
                while !canceled.load(Ordering::Acquire) {
                    let introspect_resp = client.introspect()?;
                    if !ind.update_introspect_state(introspect_resp) {
                        return Ok(());
                    }

                    // Don't hammer server with requests
                    thread::sleep(Duration::from_millis(500));
                }
                Ok(())
            })
        }));
    }

    fn stop(&mut self) -> Result<()> {
        self.canceled.store(true, Ordering::Release);
        self.handle.take().unwrap().join().unwrap()
    }
}
