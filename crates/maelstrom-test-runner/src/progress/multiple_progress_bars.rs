use super::{
    PrintWidthCb, ProgressBarPrinter, ProgressIndicator, ProgressPrinter as _, Terminal, COLORS,
};
use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget};
use maelstrom_base::stats::{JobState, JobStateCounts};
use maelstrom_client::{IntrospectResponse, RemoteProgress};
use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct RemoteProgressBarTracker {
    bars: HashMap<String, ProgressBar>,
}

impl RemoteProgressBarTracker {
    fn update<TermT>(&mut self, ind: &MultipleProgressBars<TermT>, states: Vec<RemoteProgress>)
    where
        TermT: Terminal,
    {
        let mut existing = HashSet::new();
        for state in states {
            existing.insert(state.name.clone());

            let prog = match self.bars.get(&state.name) {
                Some(prog) => prog.clone(),
                None => {
                    let Some(prog) = ind.new_side_progress(&state.name) else {
                        continue;
                    };
                    self.bars.insert(state.name, prog.clone());
                    prog
                }
            };
            prog.set_length(state.size);
            prog.set_position(state.progress);
        }

        self.bars.retain(|name, bar| {
            if !existing.contains(name) {
                bar.finish_and_clear();
                false
            } else {
                true
            }
        });
    }
}

impl Drop for RemoteProgressBarTracker {
    fn drop(&mut self) {
        for bar in self.bars.values() {
            bar.finish_and_clear();
        }
    }
}

#[derive(Default)]
struct State {
    done_queuing_jobs: bool,
    length: u64,
    finished: u64,
}

#[derive(Clone)]
pub struct MultipleProgressBars<TermT> {
    multi_bar: MultiProgress,
    bars: HashMap<JobState, ProgressBar>,
    enqueue_spinner: ProgressBar,
    state: Arc<Mutex<State>>,
    print_lock: Arc<Mutex<()>>,
    remote_bar_tracker: Arc<Mutex<RemoteProgressBarTracker>>,
    term: TermT,
}

impl<TermT> MultipleProgressBars<TermT>
where
    TermT: Terminal,
{
    pub fn new(term: TermT, spinner_message: &'static str) -> Self {
        let multi_bar = MultiProgress::new();
        multi_bar.set_draw_target(ProgressDrawTarget::term_like_with_hz(
            Box::new(term.clone()),
            20,
        ));
        let enqueue_spinner =
            multi_bar.add(ProgressBar::new_spinner().with_message(spinner_message));

        let mut bars = HashMap::new();
        for (state, color) in JobState::iter().zip(COLORS) {
            let bar = multi_bar.add(super::make_main_progress_bar(color, state.to_string(), 21));
            bars.insert(state, bar);
        }
        Self {
            multi_bar,
            bars,
            enqueue_spinner,
            state: Default::default(),
            print_lock: Default::default(),
            remote_bar_tracker: Default::default(),
            term,
        }
    }

    fn new_side_progress(&self, msg: impl Into<String>) -> Option<ProgressBar> {
        Some(
            self.multi_bar
                .insert(1, super::make_side_progress_bar("white", msg, 21)),
        )
    }

    fn update_job_states(&self, counts: JobStateCounts) -> bool {
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
        !finished
    }
}

impl<TermT> ProgressIndicator for MultipleProgressBars<TermT>
where
    TermT: Terminal,
{
    type Printer<'a> = ProgressBarPrinter<'a>;

    fn lock_printing(&self) -> Self::Printer<'_> {
        ProgressBarPrinter {
            out: self.bars.get(&JobState::Complete).unwrap().clone(),
            _guard: self.print_lock.lock().unwrap(),
            width: self.term.width() as usize,
        }
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

    fn tick(&self) -> bool {
        let state = self.state.lock().unwrap();

        if state.done_queuing_jobs {
            return false;
        }

        self.enqueue_spinner.tick();
        true
    }

    fn update_enqueue_status(&self, msg: impl Into<String>) {
        self.enqueue_spinner.set_message(msg.into());
    }

    fn update_introspect_state(&self, resp: IntrospectResponse) -> bool {
        let mut states = resp.artifact_uploads;
        states.extend(resp.image_downloads);
        self.remote_bar_tracker.lock().unwrap().update(self, states);
        self.update_job_states(resp.job_state_counts)
    }

    fn done_queuing_jobs(&self) {
        let mut state = self.state.lock().unwrap();
        state.done_queuing_jobs = true;

        self.enqueue_spinner.finish_and_clear();
    }

    fn finished(&self, summary: impl PrintWidthCb<Vec<String>>) -> Result<()> {
        for bar in self.bars.values() {
            bar.finish_and_clear();
        }
        self.enqueue_spinner.finish_and_clear();

        let printer = self.lock_printing();
        for line in summary(self.term.width() as usize) {
            printer.println(line);
        }
        Ok(())
    }
}
