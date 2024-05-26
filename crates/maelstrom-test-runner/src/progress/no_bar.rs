use super::{ProgressIndicator, TermPrinter};
use anyhow::Result;
use indicatif::TermLike;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct NoBar<TermT> {
    term: Arc<Mutex<TermT>>,
}

impl<TermT> NoBar<TermT> {
    pub fn new(term: TermT) -> Self {
        Self {
            term: Arc::new(Mutex::new(term)),
        }
    }
}

impl<TermT> ProgressIndicator for NoBar<TermT>
where
    TermT: TermLike + Clone + Send + Sync + RefUnwindSafe + UnwindSafe + 'static,
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
