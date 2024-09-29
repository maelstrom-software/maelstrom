mod progress;

use super::{JobStatuses, Terminal, Ui, UiJobResult, UiJobStatus, UiJobSummary, UiMessage};
use anyhow::Result;
use colored::Colorize as _;
use derive_more::From;
use progress::{
    MultipleProgressBars, NoBar, ProgressIndicator, TestListingProgress,
    TestListingProgressNoSpinner,
};
use slog::Drain as _;
use std::cell::RefCell;
use std::io::{self, Write as _};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};
use unicode_truncate::UnicodeTruncateStr as _;
use unicode_width::UnicodeWidthStr as _;

#[derive(From)]
enum ProgressImpl<TermT> {
    TestListingProgress(TestListingProgress<TermT>),
    TestListingProgressNoSpinner(TestListingProgressNoSpinner<TermT>),
    MultipleProgressBars(MultipleProgressBars<TermT>),
    NoBar(NoBar<TermT>),
}

pub struct SimpleUi<TermT> {
    prog_impl: ProgressImpl<TermT>,
    stdout_is_tty: bool,
}

impl<TermT> SimpleUi<TermT>
where
    TermT: Terminal,
{
    pub fn new(list: bool, stdout_is_tty: bool, term: TermT) -> Self
    where
        TermT: Terminal,
    {
        let prog_impl = if list {
            if stdout_is_tty {
                TestListingProgress::new(term, "starting...").into()
            } else {
                TestListingProgressNoSpinner::new(term).into()
            }
        } else if stdout_is_tty {
            MultipleProgressBars::new(term, "starting...").into()
        } else {
            NoBar::new(term).into()
        };
        Self {
            prog_impl,
            stdout_is_tty,
        }
    }
}

impl<TermT> Ui for SimpleUi<TermT>
where
    TermT: Terminal,
{
    fn run(&mut self, recv: Receiver<UiMessage>) -> Result<()> {
        match &mut self.prog_impl {
            ProgressImpl::TestListingProgress(p) => run_simple_ui(p, recv, self.stdout_is_tty),
            ProgressImpl::TestListingProgressNoSpinner(p) => {
                run_simple_ui(p, recv, self.stdout_is_tty)
            }
            ProgressImpl::MultipleProgressBars(p) => run_simple_ui(p, recv, self.stdout_is_tty),
            ProgressImpl::NoBar(p) => run_simple_ui(p, recv, self.stdout_is_tty),
        }
    }
}

fn job_finished<ProgressIndicatorT>(prog: &mut ProgressIndicatorT, res: &UiJobResult)
where
    ProgressIndicatorT: ProgressIndicator,
{
    let result_str = match &res.status {
        UiJobStatus::Ok => "OK".green(),
        UiJobStatus::Failure(_) => "FAIL".red(),
        UiJobStatus::TimedOut => "TIMEOUT".red(),
        UiJobStatus::Error(_) => "ERR".red(),
        UiJobStatus::Ignored => "IGNORED".yellow(),
    };

    prog.println_width(move |width| {
        let duration_str = res
            .duration
            .map(|d| format!("{:.3}s", d.as_secs_f64()))
            .unwrap_or_default();
        if width > 10 {
            let case_width = res.name.width();
            let trailer_str = format!("{result_str} {duration_str:>8}");
            let trailer_width = result_str.width() + 1 + std::cmp::max(duration_str.width(), 8);
            if case_width + trailer_width < width {
                let dots_width = width - trailer_width - case_width;
                let case = res.name.bold();
                format!("{case}{empty:.<dots_width$}{trailer_str}", empty = "",)
            } else {
                let (case, case_width) = res.name.unicode_truncate_start(width - 2 - trailer_width);
                let case = case.bold();
                let dots_width = width - trailer_width - case_width - 1;
                format!("<{case}{empty:.<dots_width$}{trailer_str}", empty = "")
            }
        } else {
            format!("{case} {result_str}", case = res.name)
        }
    });

    if let Some(details) = res.status.details() {
        prog.println(details.to_owned());
    }
    for line in &res.stdout {
        prog.println(line.to_owned());
    }
    for line in &res.stderr {
        prog.eprintln(line);
    }

    prog.job_finished();
}

fn all_jobs_finished<ProgressIndicatorT>(
    prog: &mut ProgressIndicatorT,
    summary: UiJobSummary,
) -> Result<()>
where
    ProgressIndicatorT: ProgressIndicator,
{
    prog.finished(move |width| summary.to_lines(width))
}

pub struct ProgressSlogRecordDecorator<'a, ProgressIndicatorT> {
    level: slog::Level,
    prog: &'a mut ProgressIndicatorT,
    line: String,
    use_color: bool,
}

impl<'a, ProgressIndicatorT> ProgressSlogRecordDecorator<'a, ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
{
    pub fn new(level: slog::Level, prog: &'a mut ProgressIndicatorT, use_color: bool) -> Self {
        Self {
            level,
            prog,
            line: String::new(),
            use_color,
        }
    }
}

impl<'a, ProgressIndicatorT> io::Write for ProgressSlogRecordDecorator<'a, ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.line += &String::from_utf8_lossy(buf);
        if let Some(p) = self.line.bytes().position(|b| b == b'\n') {
            let remaining = self.line.split_off(p);
            let line = std::mem::replace(&mut self.line, remaining[1..].into());
            self.prog.println(line);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a, ProgressIndicatorT> slog_term::RecordDecorator
    for ProgressSlogRecordDecorator<'a, ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
{
    fn reset(&mut self) -> io::Result<()> {
        if !self.use_color {
            return Ok(());
        }

        self.write_all(b"\x1B[0m")
    }

    fn start_level(&mut self) -> io::Result<()> {
        if !self.use_color {
            return Ok(());
        }

        self.write_all(b"\x1B[")?;
        self.write_all(match self.level {
            slog::Level::Critical => b"35", // Magenta
            slog::Level::Error => b"31",    // Red
            slog::Level::Warning => b"33",  // Yellow
            slog::Level::Info => b"32",     // Green
            slog::Level::Debug => b"36",    // Cyan
            slog::Level::Trace => b"34",    // Blue
        })?;
        self.write_all(b"m")?;
        Ok(())
    }

    fn start_key(&mut self) -> io::Result<()> {
        if !self.use_color {
            return Ok(());
        }

        self.write_all(b"\x1B[1m")
    }

    fn start_msg(&mut self) -> io::Result<()> {
        if !self.use_color {
            return Ok(());
        }

        self.write_all(b"\x1B[1m")
    }
}

struct ProgressSlogDecorator<'a, ProgressIndicatorT> {
    prog: RefCell<&'a mut ProgressIndicatorT>,
    use_color: bool,
}

impl<'a, ProgressIndicatorT> ProgressSlogDecorator<'a, ProgressIndicatorT> {
    fn new(prog: &'a mut ProgressIndicatorT, use_color: bool) -> Self {
        Self {
            prog: RefCell::new(prog),
            use_color,
        }
    }
}

impl<'a, ProgressIndicatorT> slog_term::Decorator for ProgressSlogDecorator<'a, ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
{
    fn with_record<F>(
        &self,
        record: &slog::Record,
        _logger_values: &slog::OwnedKVList,
        f: F,
    ) -> io::Result<()>
    where
        F: FnOnce(&mut dyn slog_term::RecordDecorator) -> io::Result<()>,
    {
        let mut prog = self.prog.borrow_mut();
        let mut d = ProgressSlogRecordDecorator::new(record.level(), *prog, self.use_color);
        f(&mut d)
    }
}

fn run_simple_ui<ProgressIndicatorT>(
    prog: &mut ProgressIndicatorT,
    recv: Receiver<UiMessage>,
    stdout_is_tty: bool,
) -> Result<()>
where
    ProgressIndicatorT: ProgressIndicator,
{
    let mut collection_output = String::new();
    let mut jobs = JobStatuses::default();
    let mut last_tick = Instant::now();
    loop {
        if last_tick.elapsed() > Duration::from_millis(500) {
            prog.tick();
            last_tick = Instant::now();
        }

        match recv.recv_timeout(Duration::from_millis(500)) {
            Ok(msg) => match msg {
                UiMessage::List(line) => prog.println(line),
                UiMessage::BuildOutputLine(_) => {}
                UiMessage::BuildOutputChunk(_) => {}
                UiMessage::SlogRecord(r) => {
                    let slog_dec = ProgressSlogDecorator::new(prog, stdout_is_tty);
                    let slog_drain = slog_term::FullFormat::new(slog_dec).build().fuse();
                    let _ = r.0.log_to(&slog_drain);
                }
                UiMessage::JobUpdated(msg) => {
                    jobs.job_updated(msg.job_id, msg.status);
                    prog.update_job_states(jobs.counts());
                }
                UiMessage::JobFinished(res) => {
                    job_finished(prog, &res);
                    jobs.job_finished(res);
                    prog.update_job_states(jobs.counts());

                    let num_failed = jobs.failed();
                    if num_failed > 0 {
                        prog.update_failed(num_failed);
                    }
                }
                UiMessage::UpdatePendingJobsCount(count) => prog.update_length(count),
                UiMessage::JobEnqueued(msg) => {
                    jobs.job_enqueued(msg.job_id, msg.name);
                    prog.update_job_states(jobs.counts());
                }
                UiMessage::UpdateIntrospectState(resp) => {
                    prog.update_introspect_state(resp);
                }
                UiMessage::UpdateEnqueueStatus(msg) => prog.update_enqueue_status(msg),
                UiMessage::DoneQueuingJobs => prog.done_queuing_jobs(),
                UiMessage::DoneBuilding => {}
                UiMessage::AllJobsFinished(summary) => all_jobs_finished(prog, summary)?,
                UiMessage::CollectionOutput(output) => {
                    collection_output += &output;
                }
            },
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    for line in collection_output.split('\n') {
        prog.println(line.into());
    }

    Ok(())
}
