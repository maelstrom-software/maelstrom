use anyhow::Result;
use colored::Colorize as _;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle, TermLike};
use meticulous_base::stats::JobState;
use meticulous_client::Client;
use std::{
    collections::HashMap,
    str,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    time::Duration,
};

pub trait ProgressIndicatorScope: Clone + Send + Sync + 'static {
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
}

pub trait ProgressIndicator {
    type Scope: ProgressIndicatorScope;

    /// Potentially runs background thread while body is running allowing implementations to update
    /// progress in the background.
    fn run(
        self,
        client: Mutex<Client>,
        body: impl FnOnce(&Mutex<Client>, &Self::Scope) -> Result<()>,
    ) -> Result<()>;
}

//                      waiting for artifacts, pending, running, complete
const COLORS: [&str; 4] = ["red", "yellow", "blue", "green"];

fn make_progress_bar(color: &str, message: impl Into<String>, msg_len: usize) -> ProgressBar {
    ProgressBar::new(0).with_message(message.into()).with_style(
        ProgressStyle::with_template(&format!(
            "{{wide_bar:.{color}}} {{pos}}/{{len}} {{msg:{msg_len}}}"
        ))
        .unwrap()
        .progress_chars("##-"),
    )
}

pub struct MultipleProgressBars {
    scope: ProgressBarsScope,
    done_queuing_jobs: AtomicBool,
    build_spinner: ProgressBar,
}

impl MultipleProgressBars {
    pub fn new(term: impl TermLike + 'static) -> Self {
        let multi_bar = MultiProgress::new();
        multi_bar.set_draw_target(ProgressDrawTarget::term_like_with_hz(Box::new(term), 20));
        let build_spinner =
            multi_bar.add(ProgressBar::new_spinner().with_message("building artifacts..."));
        build_spinner.enable_steady_tick(Duration::from_millis(500));

        let mut bars = HashMap::new();
        for (state, color) in JobState::iter().zip(COLORS) {
            let bar = multi_bar.add(make_progress_bar(color, state.to_string(), 21));
            bars.insert(state, bar);
        }
        Self {
            scope: ProgressBarsScope { bars },
            build_spinner,
            done_queuing_jobs: AtomicBool::new(false),
        }
    }

    fn update_progress(&self, client: &Mutex<Client>) -> Result<()> {
        while !self.finished() {
            let counts = client.lock().unwrap().get_job_state_counts()?;
            for state in JobState::iter().filter(|s| s != &JobState::Complete) {
                let jobs = JobState::iter()
                    .filter(|s| s >= &state)
                    .map(|s| counts[s])
                    .sum();
                self.scope.bars.get(&state).unwrap().set_position(jobs);
            }
        }
        Ok(())
    }

    fn done_queuing_jobs(&self) {
        self.done_queuing_jobs.store(true, Ordering::Relaxed);

        self.build_spinner.disable_steady_tick();
        self.build_spinner.finish_and_clear();
    }

    fn finished(&self) -> bool {
        let com = self.scope.bars.get(&JobState::Complete).unwrap();
        self.done_queuing_jobs.load(Ordering::Relaxed) && com.position() >= com.length().unwrap()
    }
}

#[derive(Clone)]
pub struct ProgressBarsScope {
    bars: HashMap<JobState, ProgressBar>,
}

impl ProgressIndicatorScope for ProgressBarsScope {
    fn println(&self, msg: String) {
        let com = self.bars.get(&JobState::Complete).unwrap();
        com.println(msg);
    }

    fn job_finished(&self) {
        let com = self.bars.get(&JobState::Complete).unwrap();
        com.inc(1);
    }

    fn update_length(&self, new_length: u64) {
        for bar in self.bars.values() {
            bar.set_length(new_length);
        }
    }
}

impl ProgressIndicator for MultipleProgressBars {
    type Scope = ProgressBarsScope;

    fn run(
        self,
        client: Mutex<Client>,
        body: impl FnOnce(&Mutex<Client>, &ProgressBarsScope) -> Result<()>,
    ) -> Result<()> {
        std::thread::scope(|scope| -> Result<()> {
            let update_thread = scope.spawn(|| self.update_progress(&client));
            let res = body(&client, &self.scope);
            self.done_queuing_jobs();

            res?;
            update_thread.join().unwrap()?;
            Ok(())
        })?;

        // not necessary, but might as well
        client.into_inner().unwrap().wait_for_outstanding_jobs()?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct QuietProgressBar {
    bar: ProgressBar,
}

impl QuietProgressBar {
    pub fn new(term: impl TermLike + 'static) -> Self {
        let bar = make_progress_bar("white", "jobs", 4);
        bar.set_draw_target(ProgressDrawTarget::term_like_with_hz(Box::new(term), 20));
        Self { bar }
    }
}

impl ProgressIndicator for QuietProgressBar {
    type Scope = Self;

    fn run(
        self,
        client: Mutex<Client>,
        body: impl FnOnce(&Mutex<Client>, &Self) -> Result<()>,
    ) -> Result<()> {
        let res = body(&client, &self);
        client.into_inner().unwrap().wait_for_outstanding_jobs()?;
        res
    }
}

impl ProgressIndicatorScope for QuietProgressBar {
    fn println(&self, _msg: String) {
        // quiet mode doesn't print anything
    }

    fn job_finished(&self) {
        self.bar.inc(1);
    }

    fn update_length(&self, new_length: u64) {
        self.bar.set_length(new_length);
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
    type Scope = Self;

    fn run(
        self,
        client: Mutex<Client>,
        body: impl FnOnce(&Mutex<Client>, &Self) -> Result<()>,
    ) -> Result<()> {
        let res = body(&client, &self);
        client.into_inner().unwrap().wait_for_outstanding_jobs()?;
        self.term.write_line("all jobs completed")?;
        self.term.flush()?;
        res
    }
}

impl<Term> ProgressIndicatorScope for QuietNoBar<Term>
where
    Term: Clone + Send + Sync + 'static,
{
    fn println(&self, _msg: String) {
        // quiet mode doesn't print anything
    }

    fn job_finished(&self) {
        // nothing to do
    }

    fn update_length(&self, _new_length: u64) {
        // nothing to do
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
    type Scope = Self;

    fn run(
        self,
        client: Mutex<Client>,
        body: impl FnOnce(&Mutex<Client>, &Self) -> Result<()>,
    ) -> Result<()> {
        let res = body(&client, &self);
        client.into_inner().unwrap().wait_for_outstanding_jobs()?;
        self.term.write_line("all jobs completed")?;
        self.term.flush()?;
        res
    }
}

impl<Term> ProgressIndicatorScope for NoBar<Term>
where
    Term: TermLike + Clone + Send + Sync + 'static,
{
    fn println(&self, msg: String) {
        self.term.write_line(&msg).ok();
    }

    fn job_finished(&self) {
        // nothing to do
    }

    fn update_length(&self, _new_length: u64) {
        // nothing to do
    }
}
