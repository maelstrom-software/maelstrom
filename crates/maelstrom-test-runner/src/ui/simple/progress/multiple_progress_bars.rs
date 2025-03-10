use super::{PrintWidthCb, ProgressIndicator, Terminal, COLORS};
use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget};
use maelstrom_base::stats::{JobState, JobStateCounts};
use maelstrom_client::{IntrospectResponse, RemoteProgress};
use std::{
    cmp::max,
    collections::{HashMap, HashSet},
};
use unicode_width::UnicodeWidthStr as _;

#[derive(Default)]
struct RemoteProgressBarTracker {
    bars: HashMap<String, ProgressBar>,
}

impl RemoteProgressBarTracker {
    fn update(
        &mut self,
        mut new_side_progress: impl FnMut(String) -> ProgressBar,
        states: Vec<RemoteProgress>,
    ) {
        let mut existing = HashSet::new();
        for state in states {
            existing.insert(state.name.clone());

            let prog = match self.bars.get(&state.name) {
                Some(prog) => prog.clone(),
                None => {
                    let prog = new_side_progress(state.name.clone());
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

pub struct MultipleProgressBars<TermT> {
    multi_bar: MultiProgress,
    bars: HashMap<JobState, ProgressBar>,
    enqueue_spinner: ProgressBar,
    state: State,
    remote_bar_tracker: RemoteProgressBarTracker,
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
            let max_len = "complete (xxx failed)".width();
            let bar = multi_bar.add(super::make_main_progress_bar(
                color,
                state.to_string(),
                max_len,
            ));
            bars.insert(state, bar);
        }
        Self {
            multi_bar,
            bars,
            enqueue_spinner,
            state: Default::default(),
            remote_bar_tracker: Default::default(),
            term,
        }
    }
}

impl<TermT> ProgressIndicator for MultipleProgressBars<TermT>
where
    TermT: Terminal,
{
    fn println(&mut self, msg: String) {
        let com = self.bars.get(&JobState::Complete).unwrap();
        com.println(msg);
    }

    fn println_width<'a>(&mut self, cb: impl PrintWidthCb<'a, String>) {
        self.println(cb(self.term.width() as usize));
    }

    fn job_finished(&mut self) {
        self.state.finished += 1;

        for bar in self.bars.values() {
            let pos = max(bar.position(), self.state.finished);
            bar.set_position(pos);
        }
    }

    fn update_length(&mut self, new_length: u64) {
        self.state.length = new_length;

        for bar in self.bars.values() {
            bar.set_length(new_length);
        }
    }

    fn tick(&mut self) {
        if self.state.done_queuing_jobs {
            return;
        }

        self.enqueue_spinner.tick();
    }

    fn update_enqueue_status(&mut self, msg: impl Into<String>) {
        self.enqueue_spinner.set_message(msg.into());
    }

    fn update_introspect_state(&mut self, resp: IntrospectResponse) {
        let mut states = resp.artifact_uploads;
        states.extend(resp.image_downloads);
        let new_side_progress = |msg| {
            self.multi_bar
                .insert(1, super::make_side_progress_bar("white", msg, 21))
        };
        self.remote_bar_tracker.update(new_side_progress, states);
    }

    fn update_job_states(&self, counts: JobStateCounts) {
        for job_state in JobState::iter().filter(|s| s != &JobState::Complete) {
            let jobs = JobState::iter()
                .filter(|s| s >= &job_state)
                .map(|s| counts[s])
                .sum();
            let bar = self.bars.get(&job_state).unwrap();
            let pos = max(jobs, self.state.finished);
            bar.set_position(pos);
        }
    }

    fn update_failed(&self, failed: u64) {
        let bar = self.bars.get(&JobState::Complete).unwrap();
        bar.set_message(format!("completed ({failed} failed)"));
    }

    fn done_queuing_jobs(&mut self) {
        self.state.done_queuing_jobs = true;

        self.enqueue_spinner.finish_and_clear();
    }

    fn finished<'a>(&mut self, summary: impl PrintWidthCb<'a, Vec<String>>) -> Result<()> {
        for bar in self.bars.values() {
            bar.finish_and_clear();
        }
        self.enqueue_spinner.finish_and_clear();

        for line in summary(self.term.width() as usize) {
            self.println(line);
        }
        Ok(())
    }
}
