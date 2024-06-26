use super::{ProgressBarPrinter, ProgressIndicator, TermPrinter, Terminal};
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
    print_lock: Arc<Mutex<()>>,
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
            print_lock: Default::default(),
            term,
        }
    }
}

impl<TermT> ProgressIndicator for TestListingProgress<TermT>
where
    TermT: Terminal,
{
    type Printer<'a> = ProgressBarPrinter<'a>;

    fn lock_printing(&self) -> Self::Printer<'_> {
        ProgressBarPrinter {
            out: self.enqueue_spinner.clone(),
            _guard: self.print_lock.lock().unwrap(),
            width: self.term.width() as usize,
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
    TermT: Terminal,
{
    type Printer<'a> = TermPrinter<'a, TermT>;

    fn lock_printing(&self) -> Self::Printer<'_> {
        TermPrinter {
            out: self.term.lock().unwrap(),
        }
    }
}
