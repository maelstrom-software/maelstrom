use super::ProgressIndicator;
use anyhow::Result;
use meticulous_client::Client;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

pub trait ProgressDriver<'scope> {
    fn drive<'dep, ProgressIndicatorT>(
        &mut self,
        client: &'dep Mutex<Client>,
        ind: ProgressIndicatorT,
    ) where
        ProgressIndicatorT: ProgressIndicator,
        'dep: 'scope;

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
    }
}

impl<'scope, 'env> ProgressDriver<'scope> for DefaultProgressDriver<'scope, 'env> {
    fn drive<'dep, ProgressIndicatorT>(
        &mut self,
        client: &'dep Mutex<Client>,
        ind: ProgressIndicatorT,
    ) where
        ProgressIndicatorT: ProgressIndicator,
        'dep: 'scope,
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
                    let counts = client.lock().unwrap().get_job_state_counts_async()?;
                    if !ind.update_job_states(counts.recv()?)? {
                        break;
                    }

                    // Don't hammer server with requests
                    thread::sleep(Duration::from_millis(500));
                }
                Ok(())
            })
        }));
    }

    fn stop(&mut self) -> Result<()> {
        self.handle.take().unwrap().join().unwrap()
    }
}
