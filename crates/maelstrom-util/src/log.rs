use crate::config::common::LogLevel;
use slog::{o, Drain as _, LevelFilter, Logger};
use slog_async::Async;
use slog_term::{FullFormat, PlainDecorator, PlainSyncDecorator, TermDecorator, TestStdoutWriter};
use std::io::Write;

pub fn run_with_logger<T>(log_level: LogLevel, f: impl FnOnce(Logger) -> T) -> T {
    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let drain = LevelFilter::new(drain, log_level.as_slog_level()).fuse();
    let log = Logger::root(drain, o!());
    f(log)
}

pub fn file_logger(log_level: LogLevel, file: impl Write + Send + 'static) -> Logger {
    let decorator = PlainDecorator::new(file);
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let drain = LevelFilter::new(drain, log_level.as_slog_level()).fuse();
    Logger::root(drain, slog::o!())
}

pub fn test_logger() -> Logger {
    let decorator = PlainSyncDecorator::new(TestStdoutWriter);
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}
