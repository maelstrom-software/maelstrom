use anyhow::Result;
use maelstrom_util::sync::Event as SyncEvent;
use notify::{
    event::{Event as NotifyEvent, EventKind},
    RecommendedWatcher, RecursiveMode, Watcher as _,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct Watcher {
    _watcher: RecommendedWatcher,
    done: Arc<SyncEvent>,
}

fn event_matches(event: &NotifyEvent, exclude_prefixes: &[PathBuf]) -> bool {
    !matches!(event.kind, EventKind::Access(_))
        && event.paths.iter().any(|path| {
            !exclude_prefixes
                .iter()
                .any(|exclude| path.starts_with(exclude))
        })
}

impl Watcher {
    pub fn new<I, T>(base: impl AsRef<Path>, exclude_prefixes: I) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<Path>,
    {
        let done = Arc::new(SyncEvent::new());
        let done_clone = done.clone();
        let exclude_prefixes = Vec::from_iter(
            exclude_prefixes
                .into_iter()
                .map(|path| path.as_ref().to_owned()),
        );
        let handler = move |event| match event {
            Err(_) => {
                done_clone.set();
            }
            Ok(event) if event_matches(&event, &exclude_prefixes) => {
                done_clone.set();
            }
            Ok(_) => {}
        };
        let mut watcher = notify::recommended_watcher(handler)?;
        watcher.watch(base.as_ref(), RecursiveMode::Recursive)?;
        Ok(Self {
            _watcher: watcher,
            done,
        })
    }

    pub fn wait(self) {
        self.done.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{AccessKind, CreateKind, EventKind, ModifyKind, RemoveKind};

    #[track_caller]
    fn event_matches_test<'a>(
        event: NotifyEvent,
        exclude_paths: impl IntoIterator<Item = &'a str>,
        expected: bool,
    ) {
        let exclude_paths = exclude_paths
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        let actual = event_matches(&event, &exclude_paths);
        assert_eq!(actual, expected);
    }

    #[test]
    fn files_any_processed() {
        event_matches_test(
            NotifyEvent::new(EventKind::Any)
                .add_path("foo.rs".into())
                .add_path("bar.rs".into()),
            [],
            true,
        );
        event_matches_test(NotifyEvent::new(EventKind::Any), [], false);
    }

    #[test]
    fn files_access_ignored() {
        event_matches_test(
            NotifyEvent::new(EventKind::Access(AccessKind::Any))
                .add_path("foo.rs".into())
                .add_path("bar.rs".into()),
            [],
            false,
        );
        event_matches_test(
            NotifyEvent::new(EventKind::Access(AccessKind::Any)),
            [],
            false,
        );
    }

    #[test]
    fn files_created_processed() {
        event_matches_test(
            NotifyEvent::new(EventKind::Create(CreateKind::Any))
                .add_path("foo.rs".into())
                .add_path("bar.rs".into()),
            [],
            true,
        );
        event_matches_test(
            NotifyEvent::new(EventKind::Create(CreateKind::Any)),
            [],
            false,
        );
    }

    #[test]
    fn files_modified_processed() {
        event_matches_test(
            NotifyEvent::new(EventKind::Modify(ModifyKind::Any))
                .add_path("foo.rs".into())
                .add_path("bar.rs".into()),
            [],
            true,
        );
        event_matches_test(
            NotifyEvent::new(EventKind::Modify(ModifyKind::Any)),
            [],
            false,
        );
    }

    #[test]
    fn files_removed_processed() {
        event_matches_test(
            NotifyEvent::new(EventKind::Remove(RemoveKind::Any))
                .add_path("foo.rs".into())
                .add_path("bar.rs".into()),
            [],
            true,
        );
        event_matches_test(
            NotifyEvent::new(EventKind::Remove(RemoveKind::Any)),
            [],
            false,
        );
    }

    #[test]
    fn files_other_processed() {
        event_matches_test(
            NotifyEvent::new(EventKind::Other)
                .add_path("foo.rs".into())
                .add_path("bar.rs".into()),
            [],
            true,
        );
        event_matches_test(NotifyEvent::new(EventKind::Other), [], false);
    }

    #[test]
    fn files_filtered_by_exclude_directories() {
        event_matches_test(
            NotifyEvent::new(EventKind::Any)
                .add_path("exclude1/foo.rs".into())
                .add_path("exclude2/bar.rs".into()),
            ["exclude1", "exclude2"],
            false,
        );
        event_matches_test(
            NotifyEvent::new(EventKind::Any)
                .add_path("exclude1/foo.rs".into())
                .add_path("exclude2/bar.rs".into())
                .add_path("/bar.rs".into()),
            ["exclude1", "exclude2"],
            true,
        );
    }
}
