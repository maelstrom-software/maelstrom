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

use super::Terminal;
use crate::ui::PrintWidthCb;
use anyhow::Result;
use colored::Colorize as _;
use indicatif::{ProgressBar, ProgressStyle};
use maelstrom_client::IntrospectResponse;

pub trait ProgressIndicator {
    /// Prints a line to stdout while not interfering with any progress bars
    fn println(&mut self, msg: String);

    /// Prints a line to stdout while not interfering with any progress bars
    fn println_width(&mut self, cb: impl PrintWidthCb<String>);

    /// Prints a line to stdout while not interfering with any progress bars. Prefixes line with
    /// "stderr"
    fn eprintln(&mut self, msg: impl AsRef<str>) {
        for line in msg.as_ref().lines() {
            self.println(format!("{} {line}", "stderr:".red()))
        }
    }

    /// Meant to be called with the job is complete, it updates the complete bar with this status
    fn job_finished(&mut self) {}

    /// Update the number of pending jobs indicated
    fn update_length(&mut self, _new_length: u64) {}

    /// Update progress with new introspect data.
    fn update_introspect_state(&mut self, _resp: IntrospectResponse) {}

    /// Tick any spinners.
    fn tick(&mut self) {}

    /// Update the message for the spinner which indicates jobs are being enqueued
    fn update_enqueue_status(&mut self, _msg: impl Into<String>) {}

    /// Called when all jobs are running
    fn done_queuing_jobs(&mut self) {}

    /// Called when all jobs are done
    fn finished(&mut self, _summary: impl PrintWidthCb<Vec<String>>) -> Result<()> {
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
