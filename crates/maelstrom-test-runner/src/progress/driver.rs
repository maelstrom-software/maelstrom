use super::ProgressIndicator;
use crate::ClientTrait;
use anyhow::Result;
use indicatif::ProgressBar;
use maelstrom_client::RemoteProgress;
use std::collections::{HashMap, HashSet};
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

#[derive(Default)]
struct RemoteProgressBarTracker {
    bars: HashMap<String, ProgressBar>,
}

impl RemoteProgressBarTracker {
    fn update(&mut self, ind: &impl ProgressIndicator, states: Vec<RemoteProgress>) {
        let mut existing = HashSet::new();
        for state in states {
            existing.insert(state.name.clone());

            let prog = match self.bars.get(&state.name) {
                Some(prog) => prog.clone(),
                None => {
                    let Some(prog) = ind.new_side_progress(&state.name) else {
                        continue;
                    };
                    self.bars.insert(state.name, prog.clone());
                    prog
                }
            };
            prog.set_length(state.size);
            prog.set_position(state.progress);
        }

        self.bars.retain(|name, bar| {
            if !existing.contains(name) {
                bar.finish_and_clear();
                false
            } else {
                true
            }
        });
    }
}

impl Drop for RemoteProgressBarTracker {
    fn drop(&mut self) {
        for bar in self.bars.values() {
            bar.finish_and_clear();
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
                let mut remote_bar_tracker = RemoteProgressBarTracker::default();
                while !canceled.load(Ordering::Acquire) {
                    let introspect_resp = client.introspect()?;
                    let mut states = introspect_resp.artifact_uploads;
                    states.extend(introspect_resp.image_downloads);
                    remote_bar_tracker.update(&ind, states);

                    if !ind.update_job_states(introspect_resp.job_state_counts)? {
                        return Ok(());
                    }

                    // Don't hammer server with requests
                    thread::sleep(Duration::from_millis(500));
                }
                ind.finished()?;
                Ok(())
            })
        }));
    }

    fn stop(&mut self) -> Result<()> {
        self.handle.take().unwrap().join().unwrap()
    }
}
