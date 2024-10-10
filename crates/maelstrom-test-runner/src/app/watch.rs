use anyhow::Result;
use maelstrom_client::ProjectDir;
use maelstrom_util::{root::Root, sync::Event};
use notify::Watcher as _;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;
use std_semaphore::Semaphore;

fn process_watch_events(
    log: &slog::Logger,
    events: Vec<notify::event::Event>,
    watch_exclude_paths: &[PathBuf],
    files_changed: &Event,
) {
    let path_excluded = |p: &PathBuf| watch_exclude_paths.iter().any(|pre| p.starts_with(pre));
    let event_paths = events.into_iter().flat_map(|e| e.paths.into_iter());
    let included_event_paths: BTreeSet<PathBuf> =
        event_paths.filter(|p| !path_excluded(p)).collect();

    if !included_event_paths.is_empty() {
        slog::debug!(log, "reacting to file changes"; "paths" => ?included_event_paths);
        files_changed.set();
    }
}

pub struct Watcher<'deps, 'scope> {
    scope: &'scope std::thread::Scope<'scope, 'deps>,
    log: slog::Logger,
    project_dir: &'deps Root<ProjectDir>,
    watch_exclude_paths: &'deps Vec<PathBuf>,
    semaphore: &'deps Semaphore,
    done: &'deps Event,
    files_changed: &'deps Event,
}

impl<'deps, 'scope> Watcher<'deps, 'scope> {
    pub fn new(
        scope: &'scope std::thread::Scope<'scope, 'deps>,
        log: slog::Logger,
        project_dir: &'deps Root<ProjectDir>,
        watch_exclude_paths: &'deps Vec<PathBuf>,
        semaphore: &'deps Semaphore,
        done: &'deps Event,
        files_changed: &'deps Event,
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

        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let mut watcher = notify::RecommendedWatcher::new(event_tx, notify::Config::default())?;
        watcher.watch(project_dir.as_ref(), notify::RecursiveMode::Recursive)?;

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
                if !events.is_empty() {
                    process_watch_events(&log, events, watch_exclude_paths, files_changed);
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
    use notify::event::{Event, EventAttributes, EventKind, ModifyKind};
    use std::path::PathBuf;

    fn process_watch_events_test(
        counts_as_change: bool,
        watch_exclude_paths: Vec<PathBuf>,
        events: Vec<Event>,
    ) {
        let log = maelstrom_util::log::test_logger();
        let changed = maelstrom_util::sync::Event::new();
        super::process_watch_events(&log, events, &watch_exclude_paths, &changed);
        assert_eq!(changed.is_set(), counts_as_change);
    }

    #[test]
    fn process_watch_events_file_modify() {
        process_watch_events_test(
            true, /* counts_as_change */
            vec![],
            vec![Event {
                kind: EventKind::Modify(ModifyKind::Any),
                paths: vec!["src/foo.rs".into()],
                attrs: EventAttributes::new(),
            }],
        )
    }

    #[test]
    fn process_watch_events_multiple_files_modified() {
        process_watch_events_test(
            true, /* counts_as_change */
            vec![],
            vec![
                Event {
                    kind: EventKind::Modify(ModifyKind::Any),
                    paths: vec!["src/foo.rs".into()],
                    attrs: EventAttributes::new(),
                },
                Event {
                    kind: EventKind::Modify(ModifyKind::Any),
                    paths: vec!["src/bar.rs".into()],
                    attrs: EventAttributes::new(),
                },
            ],
        )
    }

    #[test]
    fn process_watch_events_single_file_modify_ignored() {
        process_watch_events_test(
            false, /* counts_as_change */
            vec!["target".into()],
            vec![Event {
                kind: EventKind::Modify(ModifyKind::Any),
                paths: vec!["target/foo_bin".into()],
                attrs: EventAttributes::new(),
            }],
        )
    }

    #[test]
    fn process_watch_events_multiple_files_modified_ignored() {
        process_watch_events_test(
            false, /* counts_as_change */
            vec!["target".into()],
            vec![
                Event {
                    kind: EventKind::Modify(ModifyKind::Any),
                    paths: vec!["target/foo_bin".into()],
                    attrs: EventAttributes::new(),
                },
                Event {
                    kind: EventKind::Modify(ModifyKind::Any),
                    paths: vec!["target/bar_bin".into()],
                    attrs: EventAttributes::new(),
                },
            ],
        )
    }

    #[test]
    fn process_watch_events_single_modify_one_path_ignored() {
        process_watch_events_test(
            true, /* counts_as_change */
            vec!["target".into()],
            vec![Event {
                kind: EventKind::Modify(ModifyKind::Any),
                paths: vec!["src/foo.rs".into(), "target/foo_bin".into()],
                attrs: EventAttributes::new(),
            }],
        )
    }

    #[test]
    fn watch_multiple_file_modify_one_path_ignored() {
        process_watch_events_test(
            true, /* counts_as_change */
            vec!["target".into()],
            vec![
                Event {
                    kind: EventKind::Modify(ModifyKind::Any),
                    paths: vec!["src/foo.rs".into()],
                    attrs: EventAttributes::new(),
                },
                Event {
                    kind: EventKind::Modify(ModifyKind::Any),
                    paths: vec!["target/foo_bin".into()],
                    attrs: EventAttributes::new(),
                },
            ],
        )
    }
}
