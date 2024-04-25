use super::ProgressIndicator;
use crate::MainAppDeps;
use anyhow::Result;
use indicatif::ProgressBar;
use maelstrom_client::ArtifactUploadProgress;
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
    fn drive<'dep>(&mut self, deps: &'dep impl MainAppDeps, ind: impl ProgressIndicator)
    where
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

#[derive(Default)]
struct UploadProgressBarTracker {
    uploads: HashMap<String, ProgressBar>,
}

impl UploadProgressBarTracker {
    fn update(&mut self, ind: &impl ProgressIndicator, states: Vec<ArtifactUploadProgress>) {
        let mut existing = HashSet::new();
        for state in states {
            existing.insert(state.name.clone());

            let prog = match self.uploads.get(&state.name) {
                Some(prog) => prog.clone(),
                None => {
                    let Some(prog) = ind.new_side_progress(&state.name) else {
                        continue;
                    };
                    self.uploads.insert(state.name, prog.clone());
                    prog
                }
            };
            prog.set_length(state.size);
            prog.set_position(state.progress);
        }

        self.uploads.retain(|name, bar| {
            if !existing.contains(name) {
                bar.finish_and_clear();
                false
            } else {
                true
            }
        });
    }
}

impl Drop for UploadProgressBarTracker {
    fn drop(&mut self) {
        for bar in self.uploads.values() {
            bar.finish_and_clear();
        }
    }
}

impl<'scope, 'env> ProgressDriver<'scope> for DefaultProgressDriver<'scope, 'env> {
    fn drive<'dep>(&mut self, deps: &'dep impl MainAppDeps, ind: impl ProgressIndicator)
    where
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
                let mut upload_bar_tracker = UploadProgressBarTracker::default();
                while !canceled.load(Ordering::Acquire) {
                    let upload_state = deps.get_artifact_upload_progress()?;
                    upload_bar_tracker.update(&ind, upload_state);

                    let counts = deps.get_job_state_counts()?;
                    if !ind.update_job_states(counts)? {
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
