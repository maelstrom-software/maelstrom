mod multiple_progress_bars;
mod no_bar;
mod quiet_no_bar;
mod quiet_progress_bar;
mod test_listing;

pub use multiple_progress_bars::MultipleProgressBars;
pub use no_bar::NoBar;
pub use quiet_no_bar::QuietNoBar;
pub use quiet_progress_bar::QuietProgressBar;
pub use test_listing::{TestListingProgress, TestListingProgressNoSpinner};

use crate::ui::{PrintWidthCb, Terminal};
use anyhow::Result;
use colored::Colorize as _;
use indicatif::{ProgressBar, ProgressStyle};
use maelstrom_client::IntrospectResponse;
use std::{
    panic::{RefUnwindSafe, UnwindSafe},
    sync::MutexGuard,
};

pub trait ProgressPrinter {
    /// Prints a line to stdout while not interfering with any progress bars
    fn println(&self, msg: String);

    /// Prints a line to stdout while not interfering with any progress bars
    fn println_width(&self, cb: impl PrintWidthCb<String>);

    /// Prints a line to stdout while not interfering with any progress bars and indicating it was
    /// stderr
    fn eprintln(&self, msg: impl AsRef<str>) {
        for line in msg.as_ref().lines() {
            self.println(format!("{} {line}", "stderr:".red()))
        }
    }
}

pub trait ProgressIndicator: Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static {
    type Printer<'a>: ProgressPrinter;

    /// Begin outputting some messages to the terminal. While the given object exists, it holds a
    /// lock on outputting messages like this.
    fn lock_printing(&self) -> Self::Printer<'_>;

    /// Meant to be called with the job is complete, it updates the complete bar with this status
    fn job_finished(&self) {}

    /// Update the number of pending jobs indicated
    fn update_length(&self, _new_length: u64) {}

    /// Update progress with new introspect data.
    fn update_introspect_state(&self, _resp: IntrospectResponse) {}

    /// Tick any spinners.
    fn tick(&self) {}

    /// Update the message for the spinner which indicates jobs are being enqueued
    fn update_enqueue_status(&self, _msg: impl Into<String>) {}

    /// Called when all jobs are running
    fn done_queuing_jobs(&self) {}

    /// Called when all jobs are done
    fn finished(&self, _summary: impl PrintWidthCb<Vec<String>>) -> Result<()> {
        Ok(())
    }
}

//                      waiting for artifacts, pending, running, complete
const COLORS: [&str; 4] = ["red", "yellow", "blue", "green"];

fn make_main_progress_bar(color: &str, message: impl Into<String>, msg_len: usize) -> ProgressBar {
    ProgressBar::new(0).with_message(message.into()).with_style(
        ProgressStyle::with_template(&format!(
            "{{wide_bar:.{color}}} {{pos}}/{{len}} {{msg:{msg_len}}}"
        ))
        .unwrap()
        .progress_chars("##-"),
    )
}

fn make_side_progress_bar(color: &str, message: impl Into<String>, msg_len: usize) -> ProgressBar {
    ProgressBar::new(0).with_message(message.into()).with_style(
        ProgressStyle::with_template(&format!(
            "{{wide_bar:.{color}}} {{msg:{msg_len}}} {{bytes}}/{{total_bytes}}"
        ))
        .unwrap()
        .progress_chars("##-"),
    )
}

pub struct ProgressBarPrinter<'a> {
    out: ProgressBar,
    width: usize,
    _guard: MutexGuard<'a, ()>,
}

impl<'a> ProgressPrinter for ProgressBarPrinter<'a> {
    fn println(&self, msg: String) {
        self.out.println(msg);
    }

    fn println_width(&self, cb: impl PrintWidthCb<String>) {
        self.println(cb(self.width));
    }
}

pub struct NullPrinter;

impl ProgressPrinter for NullPrinter {
    fn println(&self, _msg: String) {}
    fn println_width(&self, _cb: impl PrintWidthCb<String>) {}
}

pub struct TermPrinter<'a, TermT> {
    out: MutexGuard<'a, TermT>,
}

impl<'a, TermT: Terminal> ProgressPrinter for TermPrinter<'a, TermT> {
    fn println(&self, msg: String) {
        let _ = self.out.write_line(&msg);
        let _ = self.out.flush();
    }

    fn println_width(&self, cb: impl FnOnce(usize) -> String + Send + Sync + 'static) {
        self.println(cb(self.out.width() as usize))
    }
}
