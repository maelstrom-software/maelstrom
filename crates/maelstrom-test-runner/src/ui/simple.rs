use super::UiMessage;
use crate::config::Quiet;
use crate::progress::{
    MultipleProgressBars, NoBar, ProgressIndicator, ProgressPrinter as _, QuietNoBar,
    QuietProgressBar, Terminal, TestListingProgress, TestListingProgressNoSpinner,
};
use anyhow::Result;
use derive_more::From;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, SystemTime};

#[derive(From)]
pub enum SimpleUi<TermT> {
    TestListingProgress(TestListingProgress<TermT>),
    TestListingProgressNoSpinner(TestListingProgressNoSpinner<TermT>),
    QuietProgressBar(QuietProgressBar<TermT>),
    MultipleProgressBars(MultipleProgressBars<TermT>),
    QuietNoBar(QuietNoBar<TermT>),
    NoBar(NoBar<TermT>),
}

impl<TermT> SimpleUi<TermT>
where
    TermT: Terminal,
{
    pub fn new(
        spinner_message: &'static str,
        list: bool,
        stdout_is_tty: bool,
        quiet: Quiet,
        term: TermT,
    ) -> Self
    where
        TermT: Terminal,
    {
        if list {
            if stdout_is_tty {
                TestListingProgress::new(term, spinner_message).into()
            } else {
                TestListingProgressNoSpinner::new(term).into()
            }
        } else {
            match (stdout_is_tty, quiet.into_inner()) {
                (true, true) => QuietProgressBar::new(term).into(),
                (true, false) => MultipleProgressBars::new(term, spinner_message).into(),
                (false, true) => QuietNoBar::new(term).into(),
                (false, false) => NoBar::new(term).into(),
            }
        }
    }

    pub fn run(&self, recv: Receiver<UiMessage>) -> Result<()> {
        match self {
            Self::TestListingProgress(p) => run_simple_ui(p, recv),
            Self::TestListingProgressNoSpinner(p) => run_simple_ui(p, recv),
            Self::QuietProgressBar(p) => run_simple_ui(p, recv),
            Self::MultipleProgressBars(p) => run_simple_ui(p, recv),
            Self::QuietNoBar(p) => run_simple_ui(p, recv),
            Self::NoBar(p) => run_simple_ui(p, recv),
        }
    }
}

fn run_simple_ui<ProgressIndicatorT>(
    prog: &ProgressIndicatorT,
    recv: Receiver<UiMessage>,
) -> Result<()>
where
    ProgressIndicatorT: ProgressIndicator,
{
    let mut last_tick = SystemTime::now();
    loop {
        if last_tick
            .elapsed()
            .is_ok_and(|v| v > Duration::from_millis(500))
        {
            prog.tick();
            last_tick = SystemTime::now();
        }

        match recv.recv_timeout(Duration::from_millis(500)) {
            Ok(msg) => match msg {
                UiMessage::PrintLine(line) => prog.lock_printing().println(line),
                UiMessage::PrintLineWidth(cb) => prog.lock_printing().println_width(cb),
                UiMessage::JobFinished => prog.job_finished(),
                UiMessage::UpdatePendingJobsCount(count) => prog.update_length(count),
                UiMessage::UpdateIntrospectState(resp) => {
                    prog.update_introspect_state(resp);
                }
                UiMessage::UpdateEnqueueStatus(msg) => prog.update_enqueue_status(msg),
                UiMessage::DoneQueuingJobs => prog.done_queuing_jobs(),
                UiMessage::AllJobsFinished(summary) => prog.finished(summary)?,
            },
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}
