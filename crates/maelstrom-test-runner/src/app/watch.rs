use anyhow::Result;
use maelstrom_client::ProjectDir;
use maelstrom_util::{root::Root, sync::Event as SyncEvent};
use notify::{
    event::{Event as NotifyEvent, EventKind},
    RecommendedWatcher, RecursiveMode, Watcher as _,
};
use slog::{debug, Logger};
use std::{collections::BTreeSet, path::PathBuf, sync::mpsc, thread::Scope, time::Duration};
use std_semaphore::Semaphore;

fn process_watch_events(
    events: impl IntoIterator<Item = NotifyEvent>,
    exclude_prefixes: &[PathBuf],
) -> BTreeSet<PathBuf> {
    events
        .into_iter()
        .filter_map(|event| {
            if matches!(event.kind, EventKind::Access(_)) {
                None
            } else {
                Some(event.paths)
            }
        })
        .flatten()
        .filter(|path| {
            !exclude_prefixes
                .iter()
                .any(|exclude| path.starts_with(exclude))
        })
        .collect()
}

pub struct Watcher<'deps, 'scope> {
    scope: &'scope Scope<'scope, 'deps>,
    log: Logger,
    project_dir: &'deps Root<ProjectDir>,
    watch_exclude_paths: &'deps Vec<PathBuf>,
    semaphore: &'deps Semaphore,
    done: &'deps SyncEvent,
    files_changed: &'deps SyncEvent,
}

impl<'deps, 'scope> Watcher<'deps, 'scope> {
    pub fn new(
        scope: &'scope Scope<'scope, 'deps>,
        log: Logger,
        project_dir: &'deps Root<ProjectDir>,
        watch_exclude_paths: &'deps Vec<PathBuf>,
        semaphore: &'deps Semaphore,
        done: &'deps SyncEvent,
        files_changed: &'deps SyncEvent,
    ) -> Self {
        Self {
            scope,
            log,
            project_dir,
            watch_exclude_paths,
            semaphore,
            done,
            files_changed,
        }
    }

    pub fn watch_for_changes(&self) -> Result<()> {
        let sem = self.semaphore;
        let project_dir = self.project_dir;
        let done = self.done;
        let files_changed = self.files_changed;
        let watch_exclude_paths = self.watch_exclude_paths;
        let log = self.log.clone();

        let (event_tx, event_rx) = mpsc::channel();
        let mut watcher = RecommendedWatcher::new(event_tx, Default::default())?;
        watcher.watch(project_dir.as_ref(), RecursiveMode::Recursive)?;

        self.scope.spawn(move || {
            let _guard = sem.access();

            // This loop attempts to batch up the events which happen around the same time. This
            // is acting as a kind of debounce so we don't kick off two back-to-back test
            // invocations every time we get a flurry of changes.
            while done.wait_timeout(Duration::from_millis(100)).timed_out() {
                let mut events = vec![];
                while let Ok(event_res) = event_rx.try_recv() {
                    if let Ok(event) = event_res {
                        events.push(event);
                    }
                }
                let changed_paths = process_watch_events(events, watch_exclude_paths);
                if !changed_paths.is_empty() {
                    debug!(log, "reacting to file changes"; "paths" => ?changed_paths);
                    files_changed.set();
                }
            }

            drop(watcher);
        });
        Ok(())
    }

    pub fn wait_for_changes(&self) {
        self.files_changed.wait_and_unset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{AccessKind, CreateKind, EventKind, ModifyKind, RemoveKind};

    #[track_caller]
    fn process_watch_events_test<'a>(
        events: impl IntoIterator<Item = NotifyEvent>,
        watch_exclude_paths: impl IntoIterator<Item = &'a str>,
        expected: impl IntoIterator<Item = &'a str>,
    ) {
        let watch_exclude_paths = watch_exclude_paths
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        let changed_paths = process_watch_events(events, &watch_exclude_paths);
        let expected_paths = expected.into_iter().map(Into::into).collect();
        assert_eq!(changed_paths, expected_paths);
    }

    #[test]
    fn files_any_processed() {
        process_watch_events_test(
            [
                NotifyEvent::new(EventKind::Any)
                    .add_path("foo.rs".into())
                    .add_path("bar.rs".into()),
                NotifyEvent::new(EventKind::Any).add_path("foo.rs".into()),
                NotifyEvent::new(EventKind::Any).add_path("baz.rs".into()),
                NotifyEvent::new(EventKind::Any).add_path("frob.rs".into()),
                NotifyEvent::new(EventKind::Any),
            ],
            [],
            ["foo.rs", "bar.rs", "baz.rs", "frob.rs"],
        )
    }

    #[test]
    fn files_access_ignored() {
        process_watch_events_test(
            [
                NotifyEvent::new(EventKind::Access(AccessKind::Any))
                    .add_path("foo.rs".into())
                    .add_path("bar.rs".into()),
                NotifyEvent::new(EventKind::Access(AccessKind::Any)).add_path("foo.rs".into()),
                NotifyEvent::new(EventKind::Access(AccessKind::Any)).add_path("baz.rs".into()),
                NotifyEvent::new(EventKind::Access(AccessKind::Any)).add_path("frob.rs".into()),
                NotifyEvent::new(EventKind::Access(AccessKind::Any)),
            ],
            [],
            [],
        )
    }

    #[test]
    fn files_created_processed() {
        process_watch_events_test(
            [
                NotifyEvent::new(EventKind::Create(CreateKind::Any))
                    .add_path("foo.rs".into())
                    .add_path("bar.rs".into()),
                NotifyEvent::new(EventKind::Create(CreateKind::Any)).add_path("foo.rs".into()),
                NotifyEvent::new(EventKind::Create(CreateKind::Any)).add_path("baz.rs".into()),
                NotifyEvent::new(EventKind::Create(CreateKind::Any)).add_path("frob.rs".into()),
                NotifyEvent::new(EventKind::Create(CreateKind::Any)),
            ],
            [],
            ["foo.rs", "bar.rs", "baz.rs", "frob.rs"],
        )
    }

    #[test]
    fn files_modified_processed() {
        process_watch_events_test(
            [
                NotifyEvent::new(EventKind::Modify(ModifyKind::Any))
                    .add_path("foo.rs".into())
                    .add_path("bar.rs".into()),
                NotifyEvent::new(EventKind::Modify(ModifyKind::Any)).add_path("foo.rs".into()),
                NotifyEvent::new(EventKind::Modify(ModifyKind::Any)).add_path("baz.rs".into()),
                NotifyEvent::new(EventKind::Modify(ModifyKind::Any)).add_path("frob.rs".into()),
                NotifyEvent::new(EventKind::Modify(ModifyKind::Any)),
            ],
            [],
            ["foo.rs", "bar.rs", "baz.rs", "frob.rs"],
        )
    }

    #[test]
    fn files_removed_processed() {
        process_watch_events_test(
            [
                NotifyEvent::new(EventKind::Remove(RemoveKind::Any))
                    .add_path("foo.rs".into())
                    .add_path("bar.rs".into()),
                NotifyEvent::new(EventKind::Remove(RemoveKind::Any)).add_path("foo.rs".into()),
                NotifyEvent::new(EventKind::Remove(RemoveKind::Any)).add_path("baz.rs".into()),
                NotifyEvent::new(EventKind::Remove(RemoveKind::Any)).add_path("frob.rs".into()),
                NotifyEvent::new(EventKind::Remove(RemoveKind::Any)),
            ],
            [],
            ["foo.rs", "bar.rs", "baz.rs", "frob.rs"],
        )
    }

    #[test]
    fn files_other_processed() {
        process_watch_events_test(
            [
                NotifyEvent::new(EventKind::Other)
                    .add_path("foo.rs".into())
                    .add_path("bar.rs".into()),
                NotifyEvent::new(EventKind::Other).add_path("foo.rs".into()),
                NotifyEvent::new(EventKind::Other).add_path("baz.rs".into()),
                NotifyEvent::new(EventKind::Other).add_path("frob.rs".into()),
                NotifyEvent::new(EventKind::Other),
            ],
            [],
            ["foo.rs", "bar.rs", "baz.rs", "frob.rs"],
        )
    }

    #[test]
    fn files_filtered_by_exclude_directories() {
        process_watch_events_test(
            [
                NotifyEvent::new(EventKind::Any)
                    .add_path("target/foo.rs".into())
                    .add_path("target/bar.rs".into()),
                NotifyEvent::new(EventKind::Any).add_path("target/foo.rs".into()),
                NotifyEvent::new(EventKind::Any).add_path("target/baz.rs".into()),
                NotifyEvent::new(EventKind::Any).add_path("target/frob.rs".into()),
                NotifyEvent::new(EventKind::Any),
            ],
            ["target"],
            [],
        )
    }

    #[test]
    fn kitchen_sink() {
        process_watch_events_test(
            [
                NotifyEvent::new(EventKind::Any)
                    .add_path("dir/subdir1/foo.rs".into())
                    .add_path("dir/subdir2/bar.rs".into())
                    .add_path("dir/subdir3/baz.rs".into()),
                NotifyEvent::new(EventKind::Access(AccessKind::Any))
                    .add_path("dir/subdir1/frob.rs".into())
                    .add_path("dir/subdir2/fitz.rs".into())
                    .add_path("dir/subdir3/quux.rs".into()),
                NotifyEvent::new(EventKind::Modify(ModifyKind::Any))
                    .add_path("dir/subdir3/quid.rs".into()),
                NotifyEvent::new(EventKind::Modify(ModifyKind::Any))
                    .add_path("dir/subdir2/bar.rs".into()),
            ],
            ["dir/subdir1", "dir/subdir2"],
            ["dir/subdir3/baz.rs", "dir/subdir3/quid.rs"],
        )
    }
}
