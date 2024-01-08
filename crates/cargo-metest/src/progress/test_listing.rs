use super::ProgressIndicator;
use anyhow::Result;
use indicatif::{ProgressBar, TermLike};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    done_queuing_jobs: bool,
}

#[derive(Clone)]
pub struct TestListingProgress {
    enqueue_spinner: ProgressBar,
    state: Arc<Mutex<State>>,
}

impl TestListingProgress {
    pub fn new(_term: impl TermLike + 'static) -> Self {
        let enqueue_spinner = ProgressBar::new_spinner().with_message("building artifacts...");
        Self {
            enqueue_spinner,
            state: Default::default(),
        }
    }
}

impl ProgressIndicator for TestListingProgress {
    fn println(&self, msg: String) {
        self.enqueue_spinner.println(msg);
    }

    fn update_enqueue_status(&self, msg: impl Into<String>) {
        self.enqueue_spinner.set_message(msg.into());
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
    TermT: TermLike + Clone + Send + Sync + 'static,
{
    fn println(&self, msg: String) {
        self.term.write_line(&msg).ok();
    }

    fn finished(&self) -> Result<()> {
        self.term.flush()?;
        Ok(())
    }
}
