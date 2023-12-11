use anyhow::Result;
use colored::Colorize as _;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle, TermLike};
use meticulous_base::stats::{JobState, JobStateCounts};
use std::{
    cmp::max,
    collections::HashMap,
    str,
    sync::{Arc, Mutex},
};

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

#[derive(Default)]
struct MultiState {
    done_queuing_jobs: bool,
    length: u64,
    finished: u64,
}

#[derive(Clone)]
pub struct MultipleProgressBars {
    multi_bar: MultiProgress,
    bars: HashMap<JobState, ProgressBar>,
    enqueue_spinner: ProgressBar,
    state: Arc<Mutex<MultiState>>,
}

impl MultipleProgressBars {
    pub fn new(term: impl TermLike + 'static) -> Self {
        let multi_bar = MultiProgress::new();
        multi_bar.set_draw_target(ProgressDrawTarget::term_like_with_hz(Box::new(term), 20));
        let enqueue_spinner =
            multi_bar.add(ProgressBar::new_spinner().with_message("building artifacts..."));

        let mut bars = HashMap::new();
        for (state, color) in JobState::iter().zip(COLORS) {
            let bar = multi_bar.add(make_progress_bar(color, state.to_string(), 21, false));
            bars.insert(state, bar);
        }
        Self {
            multi_bar,
            bars,
            enqueue_spinner,
            state: Default::default(),
        }
    }
}

impl ProgressIndicator for MultipleProgressBars {
    fn println(&self, msg: String) {
        let com = self.bars.get(&JobState::Complete).unwrap();
        com.println(msg);
    }

    fn job_finished(&self) {
        let mut state = self.state.lock().unwrap();
        state.finished += 1;

        for bar in self.bars.values() {
            let pos = max(bar.position(), state.finished);
            bar.set_position(pos);
        }
    }

    fn update_length(&self, new_length: u64) {
        let mut state = self.state.lock().unwrap();
        state.length = new_length;

        for bar in self.bars.values() {
            bar.set_length(new_length);
        }
    }

    fn new_side_progress(&self, msg: impl Into<String>) -> Option<ProgressBar> {
        Some(
            self.multi_bar
                .insert(1, make_progress_bar("white", msg, 21, true)),
        )
    }

    fn update_enqueue_status(&self, msg: impl Into<String>) {
        self.enqueue_spinner.set_message(msg.into());
    }

    fn update_job_states(&self, counts: JobStateCounts) -> Result<bool> {
        let state = self.state.lock().unwrap();

        for job_state in JobState::iter().filter(|s| s != &JobState::Complete) {
            let jobs = JobState::iter()
                .filter(|s| s >= &job_state)
                .map(|s| counts[s])
                .sum();
            let bar = self.bars.get(&job_state).unwrap();
            let pos = max(jobs, state.finished);
            bar.set_position(pos);
        }

        let finished = state.done_queuing_jobs && state.finished >= state.length;
        Ok(!finished)
    }

    fn tick(&self) -> bool {
        let state = self.state.lock().unwrap();

        if state.done_queuing_jobs {
            return false;
        }

        self.enqueue_spinner.tick();
        true
    }

    fn done_queuing_jobs(&self) {
        let mut state = self.state.lock().unwrap();
        state.done_queuing_jobs = true;

        self.enqueue_spinner.finish_and_clear();
    }

    fn finished(&self) -> Result<()> {
        for bar in self.bars.values() {
            bar.finish_and_clear();
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct QuietProgressBar {
    bar: ProgressBar,
}

impl QuietProgressBar {
    pub fn new(term: impl TermLike + 'static) -> Self {
        let bar = make_progress_bar("white", "jobs", 4, false);
        bar.set_draw_target(ProgressDrawTarget::term_like_with_hz(Box::new(term), 20));
        Self { bar }
    }
}

impl ProgressIndicator for QuietProgressBar {
    fn println(&self, _msg: String) {
        // quiet mode doesn't print anything
    }

    fn job_finished(&self) {
        self.bar.inc(1);
    }

    fn update_length(&self, new_length: u64) {
        self.bar.set_length(new_length);
    }

    fn finished(&self) -> Result<()> {
        self.bar.finish_and_clear();
        Ok(())
    }
}

#[derive(Clone)]
pub struct QuietNoBar<Term> {
    term: Term,
}

impl<Term> QuietNoBar<Term> {
    pub fn new(term: Term) -> Self {
        Self { term }
    }
}

impl<Term> ProgressIndicator for QuietNoBar<Term>
where
    Term: TermLike + Clone + Send + Sync + 'static,
{
    fn println(&self, _msg: String) {
        // quiet mode doesn't print anything
    }

    fn finished(&self) -> Result<()> {
        self.term.write_line("all jobs completed")?;
        self.term.flush()?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct NoBar<Term> {
    term: Term,
}

impl<Term> NoBar<Term> {
    pub fn new(term: Term) -> Self {
        Self { term }
    }
}

impl<Term> ProgressIndicator for NoBar<Term>
where
    Term: TermLike + Clone + Send + Sync + 'static,
{
    fn println(&self, msg: String) {
        self.term.write_line(&msg).ok();
    }

    fn finished(&self) -> Result<()> {
        self.term.write_line("all jobs completed")?;
        self.term.flush()?;
        Ok(())
    }
}
