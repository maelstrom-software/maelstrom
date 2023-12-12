use super::{ProgressIndicator, COLORS};
use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, TermLike};
use meticulous_base::stats::{JobState, JobStateCounts};
use std::{
    cmp::max,
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct State {
    done_queuing_jobs: bool,
    length: u64,
    finished: u64,
}

#[derive(Clone)]
pub struct MultipleProgressBars {
    multi_bar: MultiProgress,
    bars: HashMap<JobState, ProgressBar>,
    enqueue_spinner: ProgressBar,
    state: Arc<Mutex<State>>,
}

impl MultipleProgressBars {
    pub fn new(term: impl TermLike + 'static) -> Self {
        let multi_bar = MultiProgress::new();
        multi_bar.set_draw_target(ProgressDrawTarget::term_like_with_hz(Box::new(term), 20));
        let enqueue_spinner =
            multi_bar.add(ProgressBar::new_spinner().with_message("building artifacts..."));

        let mut bars = HashMap::new();
        for (state, color) in JobState::iter().zip(COLORS) {
            let bar = multi_bar.add(super::make_progress_bar(
                color,
                state.to_string(),
                21,
                false,
            ));
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
                .insert(1, super::make_progress_bar("white", msg, 21, true)),
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
