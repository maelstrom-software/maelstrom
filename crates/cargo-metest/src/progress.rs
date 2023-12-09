use anyhow::Result;
use colored::Colorize as _;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle, TermLike};
use meticulous_base::stats::{JobState, JobStateCounts};
use std::{
    collections::HashMap,
    str,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
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
    fn job_finished(&self);

    /// Update the number of pending jobs indicated
    fn update_length(&self, new_length: u64);

    /// Add a new progress bar which is meant to represent a container being downloaded
    fn new_container_progress(&self) -> Option<ProgressBar> {
        None
    }

    fn update_job_states(&self, counts: JobStateCounts) -> Result<bool>;

    /// Called when all jobs are running
    fn done_queuing_jobs(&self);

    /// Called when all jobs are done
    fn finished(&self) -> Result<()>;
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

#[derive(Clone)]
pub struct MultipleProgressBars {
    multi_bar: MultiProgress,
    bars: HashMap<JobState, ProgressBar>,
    done_queuing_jobs: Arc<AtomicBool>,
    build_spinner: ProgressBar,
}

impl MultipleProgressBars {
    pub fn new(term: impl TermLike + 'static) -> Self {
        let multi_bar = MultiProgress::new();
        multi_bar.set_draw_target(ProgressDrawTarget::term_like_with_hz(Box::new(term), 20));
        let build_spinner =
            multi_bar.add(ProgressBar::new_spinner().with_message("building artifacts..."));

        let mut bars = HashMap::new();
        for (state, color) in JobState::iter().zip(COLORS) {
            let bar = multi_bar.add(make_progress_bar(color, state.to_string(), 21, false));
            bars.insert(state, bar);
        }
        Self {
            multi_bar,
            bars,
            build_spinner,
            done_queuing_jobs: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_finished(&self) -> bool {
        let com = self.bars.get(&JobState::Complete).unwrap();
        self.done_queuing_jobs.load(Ordering::Relaxed) && com.position() >= com.length().unwrap()
    }
}

impl ProgressIndicator for MultipleProgressBars {
    fn println(&self, msg: String) {
        let com = self.bars.get(&JobState::Complete).unwrap();
        com.println(msg);
    }

    fn job_finished(&self) {
        let com = self.bars.get(&JobState::Complete).unwrap();
        com.inc(1);

        for bar in self.bars.values() {
            bar.set_position(std::cmp::max(com.position(), bar.position()));
        }
    }

    fn update_length(&self, new_length: u64) {
        for bar in self.bars.values() {
            bar.set_length(new_length);
        }
    }

    fn new_container_progress(&self) -> Option<ProgressBar> {
        Some(
            self.multi_bar
                .insert(1, make_progress_bar("white", "downloading image", 21, true)),
        )
    }

    fn update_job_states(&self, counts: JobStateCounts) -> Result<bool> {
        let com = self.bars.get(&JobState::Complete).unwrap();
        for state in JobState::iter().filter(|s| s != &JobState::Complete) {
            let jobs = JobState::iter()
                .filter(|s| s >= &state)
                .map(|s| counts[s])
                .sum();
            self.bars
                .get(&state)
                .unwrap()
                .set_position(std::cmp::max(jobs, com.position()));
        }

        if !self.done_queuing_jobs.load(Ordering::Relaxed) {
            self.build_spinner.tick();
        }
        Ok(!self.is_finished())
    }

    fn done_queuing_jobs(&self) {
        self.done_queuing_jobs.store(true, Ordering::Relaxed);

        self.build_spinner.finish_and_clear();
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

    fn update_job_states(&self, _counts: JobStateCounts) -> Result<bool> {
        // do nothing
        Ok(false)
    }

    fn done_queuing_jobs(&self) {
        // do nothing
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

    fn job_finished(&self) {
        // do nothing
    }

    fn update_length(&self, _new_length: u64) {
        // do nothing
    }

    fn update_job_states(&self, _counts: JobStateCounts) -> Result<bool> {
        // do nothing
        Ok(false)
    }

    fn done_queuing_jobs(&self) {
        // do nothing
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

    fn job_finished(&self) {
        // do nothing
    }

    fn update_length(&self, _new_length: u64) {
        // do nothing
    }

    fn update_job_states(&self, _counts: JobStateCounts) -> Result<bool> {
        // do nothing
        Ok(false)
    }

    fn done_queuing_jobs(&self) {
        // do nothing
    }

    fn finished(&self) -> Result<()> {
        self.term.write_line("all jobs completed")?;
        self.term.flush()?;
        Ok(())
    }
}
