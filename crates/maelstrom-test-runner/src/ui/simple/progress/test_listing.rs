use super::{ProgressIndicator, Terminal};
use crate::ui::PrintWidthCb;
use indicatif::ProgressBar;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    done_queuing_jobs: bool,
}

#[derive(Clone)]
pub struct TestListingProgress<TermT> {
    enqueue_spinner: ProgressBar,
    state: Arc<Mutex<State>>,
    term: TermT,
}

impl<TermT> TestListingProgress<TermT>
where
    TermT: Terminal,
{
    pub fn new(term: TermT, spinner_message: &'static str) -> Self {
        let enqueue_spinner = ProgressBar::new_spinner().with_message(spinner_message);
        Self {
            enqueue_spinner,
            state: Default::default(),
            term,
        }
    }
}

impl<TermT> ProgressIndicator for TestListingProgress<TermT>
where
    TermT: Terminal,
{
    fn println(&self, msg: String) {
        self.enqueue_spinner.println(msg);
    }

    fn println_width(&self, cb: impl PrintWidthCb<String>) {
        self.println(cb(self.term.width() as usize));
    }

    fn update_enqueue_status(&self, msg: impl Into<String>) {
        self.enqueue_spinner.set_message(msg.into());
    }

    fn tick(&self) {
        let state = self.state.lock().unwrap();

        if state.done_queuing_jobs {
            return;
        }

        self.enqueue_spinner.tick();
    }

    fn done_queuing_jobs(&self) {
        let mut state = self.state.lock().unwrap();
        state.done_queuing_jobs = true;

        self.enqueue_spinner.finish_and_clear();
    }
}

#[derive(Clone)]
pub struct TestListingProgressNoSpinner<TermT> {
    term: TermT,
}

impl<TermT> TestListingProgressNoSpinner<TermT> {
    pub fn new(term: TermT) -> Self {
        Self { term }
    }
}

impl<TermT> ProgressIndicator for TestListingProgressNoSpinner<TermT>
where
    TermT: Terminal,
{
    fn println(&self, msg: String) {
        let _ = self.term.write_line(&msg);
        let _ = self.term.flush();
    }

    fn println_width(&self, cb: impl PrintWidthCb<String>) {
        self.println(cb(self.term.width() as usize));
    }
}
