mod progress;

use super::{Ui, UiJobResult, UiJobStatus, UiJobSummary, UiMessage};
use crate::config::Quiet;
use anyhow::Result;
use colored::Colorize as _;
use derive_more::From;
use indicatif::TermLike;
use progress::{
    MultipleProgressBars, NoBar, ProgressIndicator, ProgressPrinter as _, QuietNoBar,
    QuietProgressBar, TestListingProgress, TestListingProgressNoSpinner,
};
use slog::Drain as _;
use std::io::{self, Write as _};
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};
use unicode_truncate::UnicodeTruncateStr as _;
use unicode_width::UnicodeWidthStr as _;

pub trait Terminal: TermLike + Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static {}

impl<TermT> Terminal for TermT where
    TermT: TermLike + Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static
{
}

#[derive(From)]
enum ProgressImpl<TermT> {
    TestListingProgress(TestListingProgress<TermT>),
    TestListingProgressNoSpinner(TestListingProgressNoSpinner<TermT>),
    QuietProgressBar(QuietProgressBar<TermT>),
    MultipleProgressBars(MultipleProgressBars<TermT>),
    QuietNoBar(QuietNoBar<TermT>),
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
    pub fn new(list: bool, stdout_is_tty: bool, quiet: Quiet, term: TermT) -> Self
    where
        TermT: Terminal,
    {
        let prog_impl = if list {
            if stdout_is_tty {
                TestListingProgress::new(term, "starting...").into()
            } else {
                TestListingProgressNoSpinner::new(term).into()
            }
        } else {
            match (stdout_is_tty, quiet.into_inner()) {
                (true, true) => QuietProgressBar::new(term).into(),
                (true, false) => MultipleProgressBars::new(term, "starting...").into(),
                (false, true) => QuietNoBar::new(term).into(),
                (false, false) => NoBar::new(term).into(),
            }
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
            ProgressImpl::QuietProgressBar(p) => run_simple_ui(p, recv, self.stdout_is_tty),
            ProgressImpl::MultipleProgressBars(p) => run_simple_ui(p, recv, self.stdout_is_tty),
            ProgressImpl::QuietNoBar(p) => run_simple_ui(p, recv, self.stdout_is_tty),
            ProgressImpl::NoBar(p) => run_simple_ui(p, recv, self.stdout_is_tty),
        }
    }
}

fn job_finished<ProgressIndicatorT>(prog: &ProgressIndicatorT, res: UiJobResult)
where
    ProgressIndicatorT: ProgressIndicator,
{
    let printer = prog.lock_printing();
    let result_str = match &res.status {
        UiJobStatus::Ok => "OK".green(),
        UiJobStatus::Failure(_) => "FAIL".red(),
        UiJobStatus::TimedOut => "TIMEOUT".red(),
        UiJobStatus::Error(_) => "ERR".red(),
        UiJobStatus::Ignored => "IGNORED".yellow(),
    };

    printer.println_width(move |width| {
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
        printer.println(details);
    }
    for line in res.stdout {
        printer.println(line);
    }
    for line in res.stderr {
        printer.eprintln(line);
    }

    drop(printer);
    prog.job_finished();
}

fn all_jobs_finished<ProgressIndicatorT>(
    prog: &ProgressIndicatorT,
    summary: UiJobSummary,
) -> Result<()>
where
    ProgressIndicatorT: ProgressIndicator,
{
    prog.finished(move |width| {
        let mut summary_lines = vec![];
        summary_lines.push("".into());

        let heading = " Test Summary ";
        let equal_width = (width - heading.width()) / 2;
        summary_lines.push(format!(
            "{empty:=<equal_width$}{heading}{empty:=<equal_width$}",
            empty = ""
        ));
        let success = "Successful Tests";
        let failure = "Failed Tests";
        let ignore = "Ignored Tests";
        let mut column1_width = std::cmp::max(success.width(), failure.width());
        let max_digits = 9;
        let num_failed = summary.failed.len();
        let num_ignored = summary.ignored.len();
        let num_succeeded = summary.succeeded;
        if num_ignored > 0 {
            column1_width = std::cmp::max(column1_width, ignore.width());
        }
        summary_lines.push(format!(
            "{:<column1_width$}: {num_succeeded:>max_digits$}",
            success.green(),
        ));
        summary_lines.push(format!(
            "{:<column1_width$}: {num_failed:>max_digits$}",
            failure.red(),
        ));
        let failed_width = summary.failed.iter().map(|n| n.width()).max().unwrap_or(0);
        for failed in &summary.failed {
            summary_lines.push(format!("    {failed:<failed_width$}: {}", "failure".red()));
        }
        if num_ignored > 0 {
            summary_lines.push(format!(
                "{:<column1_width$}: {num_ignored:>max_digits$}",
                ignore.yellow(),
            ));
            let failed_width = summary.ignored.iter().map(|n| n.width()).max().unwrap_or(0);
            for ignored in &summary.ignored {
                summary_lines.push(format!(
                    "    {ignored:<failed_width$}: {}",
                    "ignored".yellow()
                ));
            }
        }
        summary_lines
    })
}

pub struct ProgressSlogRecordDecorator<ProgressIndicatorT> {
    level: slog::Level,
    prog: ProgressIndicatorT,
    line: String,
    use_color: bool,
}

impl<ProgressIndicatorT> ProgressSlogRecordDecorator<ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
{
    pub fn new(level: slog::Level, prog: ProgressIndicatorT, use_color: bool) -> Self {
        Self {
            level,
            prog,
            line: String::new(),
            use_color,
        }
    }
}

impl<ProgressIndicatorT> io::Write for ProgressSlogRecordDecorator<ProgressIndicatorT>
where
    ProgressIndicatorT: ProgressIndicator,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.line += &String::from_utf8_lossy(buf);
        if let Some(p) = self.line.bytes().position(|b| b == b'\n') {
            let remaining = self.line.split_off(p);
            let line = std::mem::replace(&mut self.line, remaining[1..].into());
            self.prog.lock_printing().println(line);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<ProgressIndicatorT> slog_term::RecordDecorator
    for ProgressSlogRecordDecorator<ProgressIndicatorT>
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

struct ProgressSlogDecorator<ProgressIndicatorT> {
    prog: ProgressIndicatorT,
    use_color: bool,
}

impl<ProgressIndicatorT> ProgressSlogDecorator<ProgressIndicatorT> {
    fn new(prog: ProgressIndicatorT, use_color: bool) -> Self {
        Self { prog, use_color }
    }
}

impl<ProgressIndicatorT> slog_term::Decorator for ProgressSlogDecorator<ProgressIndicatorT>
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
        let mut d =
            ProgressSlogRecordDecorator::new(record.level(), self.prog.clone(), self.use_color);
        f(&mut d)
    }
}

fn run_simple_ui<ProgressIndicatorT>(
    prog: &ProgressIndicatorT,
    recv: Receiver<UiMessage>,
    stdout_is_tty: bool,
) -> Result<()>
where
    ProgressIndicatorT: ProgressIndicator,
{
    let slog_dec = ProgressSlogDecorator::new(prog.clone(), stdout_is_tty);
    let slog_drain = slog_term::FullFormat::new(slog_dec).build().fuse();

    let mut last_tick = Instant::now();
    loop {
        if last_tick.elapsed() > Duration::from_millis(500) {
            prog.tick();
            last_tick = Instant::now();
        }

        match recv.recv_timeout(Duration::from_millis(500)) {
            Ok(msg) => match msg {
                UiMessage::List(line) => prog.lock_printing().println(line),
                UiMessage::BuildOutputLine(_) => {}
                UiMessage::BuildOutputChunk(_) => {}
                UiMessage::SlogRecord(r) => {
                    let _ = r.log_to(&slog_drain);
                }
                UiMessage::JobFinished(res) => job_finished(prog, res),
                UiMessage::UpdatePendingJobsCount(count) => prog.update_length(count),
                UiMessage::JobEnqueued(_) => {}
                UiMessage::UpdateIntrospectState(resp) => {
                    prog.update_introspect_state(resp);
                }
                UiMessage::UpdateEnqueueStatus(msg) => prog.update_enqueue_status(msg),
                UiMessage::DoneQueuingJobs => prog.done_queuing_jobs(),
                UiMessage::DoneBuilding => {}
                UiMessage::AllJobsFinished(summary) => all_jobs_finished(prog, summary)?,
            },
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}
