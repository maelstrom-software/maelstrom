use crate::ui::{UiSender, UiSlogDrain};
use maelstrom_util::config::common::LogLevel;
use slog::Drain as _;
use std::sync::{Arc, Mutex};

type TermDrain = slog::Fuse<slog_async::Async>;

enum LogDestinationInner {
    Ui(UiSlogDrain),
    Term(TermDrain),
}

impl Default for LogDestinationInner {
    fn default() -> Self {
        let decorator = slog_term::TermDecorator::new().stdout().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        Self::Term(drain)
    }
}

/// This object exists to allow us to switch where log messages go at run-time.
///
/// When the UI is running, it owns the terminal, so we need to send log messages to the UI during
/// this time. Before or after the UI is running though, we just display log messages more normally
/// directly to the terminal.
///
/// When this object is created via [`Default::default`], it starts out sending messages directly
/// to the terminal.
#[derive(Clone, Default)]
pub struct LogDestination {
    inner: Arc<Mutex<LogDestinationInner>>,
}

impl LogDestination {
    /// Send any future log messages to the given UI sender. It's Probably best to call this when
    /// the process is single-threaded.
    pub fn log_to_ui(&self, ui: UiSender) {
        *self.inner.lock().unwrap() = LogDestinationInner::Ui(UiSlogDrain::new(ui));
    }

    /// Send any future log messages directly to the terminal on `stdout`. It's probably best to
    /// call this when the process is single-threaded.
    pub fn log_to_terminal(&self) {
        *self.inner.lock().unwrap() = LogDestinationInner::default();
    }
}

impl slog::Drain for LogDestination {
    type Ok = ();
    type Err = <TermDrain as slog::Drain>::Err;

    fn log(
        &self,
        record: &slog::Record<'_>,
        values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        match &mut *self.inner.lock().unwrap() {
            LogDestinationInner::Ui(d) => d.log(record, values),
            LogDestinationInner::Term(d) => d.log(record, values),
        }
    }
}

/// A way to represent an already instantiated `[slog::Logger]` or the arguments to create a
/// `[slog::Logger]`.
///
/// This is used when invoking the main entry-point for test runners. The caller either wants the
/// test runner to create it own logger, or (in the case of the tests) use the given one.
pub enum LoggerBuilder {
    DefaultLogger(LogLevel),
    GivenLogger(slog::Logger),
}

impl LoggerBuilder {
    pub fn build(&self, destination: LogDestination) -> slog::Logger {
        match self {
            Self::DefaultLogger(level) => {
                let drain = slog::LevelFilter::new(destination, level.as_slog_level()).fuse();
                slog::Logger::root(drain, slog::o!())
            }
            Self::GivenLogger(logger) => logger.clone(),
        }
    }
}
