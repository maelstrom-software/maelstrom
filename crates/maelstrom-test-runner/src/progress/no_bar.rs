use super::{PrintWidthCb, ProgressIndicator, ProgressPrinter as _, TermPrinter, Terminal};
use anyhow::Result;
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
    TermT: Terminal,
{
    type Printer<'a> = TermPrinter<'a, TermT>;

    fn lock_printing(&self) -> Self::Printer<'_> {
        TermPrinter {
            out: self.term.lock().unwrap(),
        }
    }

    fn finished(&self, summary: impl PrintWidthCb<Vec<String>>) -> Result<()> {
        let printer = self.lock_printing();
        for line in summary(printer.out.width() as usize) {
            printer.println(line)
        }
        Ok(())
    }
}
