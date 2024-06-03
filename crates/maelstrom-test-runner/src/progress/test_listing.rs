use super::{ProgressBarPrinter, ProgressIndicator, TermPrinter};
use anyhow::Result;
use indicatif::{ProgressBar, TermLike};
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    done_queuing_jobs: bool,
}

#[derive(Clone)]
pub struct TestListingProgress {
    enqueue_spinner: ProgressBar,
    state: Arc<Mutex<State>>,
    print_lock: Arc<Mutex<()>>,
}

impl TestListingProgress {
    pub fn new(_term: impl TermLike + 'static, spinner_message: &'static str) -> Self {
        let enqueue_spinner = ProgressBar::new_spinner().with_message(spinner_message);
        Self {
            enqueue_spinner,
            state: Default::default(),
            print_lock: Default::default(),
        }
    }
}

impl ProgressIndicator for TestListingProgress {
    type Printer<'a> = ProgressBarPrinter<'a>;

    fn lock_printing(&self) -> Self::Printer<'_> {
        ProgressBarPrinter {
            out: self.enqueue_spinner.clone(),
            _guard: self.print_lock.lock().unwrap(),
        }
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
    term: Arc<Mutex<TermT>>,
}

impl<TermT> TestListingProgressNoSpinner<TermT> {
    pub fn new(term: TermT) -> Self {
        Self {
            term: Arc::new(Mutex::new(term)),
        }
    }
}

impl<TermT> ProgressIndicator for TestListingProgressNoSpinner<TermT>
where
    TermT: TermLike + Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static,
{
    type Printer<'a> = TermPrinter<'a, TermT>;

    fn lock_printing(&self) -> Self::Printer<'_> {
        TermPrinter {
            out: self.term.lock().unwrap(),
        }
    }

    fn finished(&self) -> Result<()> {
        self.term.lock().unwrap().flush()?;
        Ok(())
    }
}
