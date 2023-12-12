mod driver;
mod multiple_progress_bars;
mod no_bar;
mod quiet_no_bar;
mod quiet_progress_bar;

use anyhow::Result;
use colored::Colorize as _;
use indicatif::{ProgressBar, ProgressStyle};
use meticulous_base::stats::JobStateCounts;

pub use driver::{DefaultProgressDriver, ProgressDriver};
pub use multiple_progress_bars::MultipleProgressBars;
pub use no_bar::NoBar;
pub use quiet_no_bar::QuietNoBar;
pub use quiet_progress_bar::QuietProgressBar;

pub trait ProgressIndicator: Clone + Send + Sync + 'static {
    /// Prints a line to stdout while not interfering with any progress bars
    fn println(&self, msg: String);

    /// Prints a line to stdout while not interfering with any progress bars and indicating it was
    /// stderr
    fn eprintln(&self, msg: impl AsRef<str>) {
        for line in msg.as_ref().lines() {
            self.println(format!("{} {line}", "stderr:".red()))
        }
    }

    /// Meant to be called with the job is complete, it updates the complete bar with this status
    fn job_finished(&self) {}

    /// Update the number of pending jobs indicated
    fn update_length(&self, _new_length: u64) {}

    /// Add another progress bar which is meant to show progress of some sub-task, like downloading
    /// an image or uploading an artifact
    fn new_side_progress(&self, _msg: impl Into<String>) -> Option<ProgressBar> {
        None
    }

    /// Update any information pertaining to the states of jobs. Should be called repeatedly until
    /// it returns false
    fn update_job_states(&self, _counts: JobStateCounts) -> Result<bool> {
        Ok(false)
    }

    /// Tick and spinners
    fn tick(&self) -> bool {
        false
    }

    /// Update the message for the spinner which indicates jobs are being enqueued
    fn update_enqueue_status(&self, _msg: impl Into<String>) {}

    /// Called when all jobs are running
    fn done_queuing_jobs(&self) {}

    /// Called when all jobs are done
    fn finished(&self) -> Result<()> {
        Ok(())
    }
}

//                      waiting for artifacts, pending, running, complete
const COLORS: [&str; 4] = ["red", "yellow", "blue", "green"];

fn make_progress_bar(
    color: &str,
    message: impl Into<String>,
    msg_len: usize,
    bytes: bool,
) -> ProgressBar {
    let prog_line = if bytes {
        "{bytes}/{total_bytes}"
    } else {
        "{pos}/{len}"
    };
    ProgressBar::new(0).with_message(message.into()).with_style(
        ProgressStyle::with_template(&format!(
            "{{wide_bar:.{color}}} {prog_line} {{msg:{msg_len}}}"
        ))
        .unwrap()
        .progress_chars("##-"),
    )
}
