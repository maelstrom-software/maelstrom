use super::ProgressIndicator;
use anyhow::Result;
use indicatif::TermLike;
use std::panic::{RefUnwindSafe, UnwindSafe};

#[derive(Clone)]
pub struct NoBar<TermT> {
    term: TermT,
}

impl<TermT> NoBar<TermT> {
    pub fn new(term: TermT) -> Self {
        Self { term }
    }
}

impl<TermT> ProgressIndicator for NoBar<TermT>
where
    TermT: TermLike + Clone + Send + Sync + RefUnwindSafe + UnwindSafe + 'static,
{
    fn println(&self, msg: String) {
        let _ = self.term.write_line(&msg);
        let _ = self.term.flush();
    }

    fn finished(&self) -> Result<()> {
        self.term.flush()?;
        Ok(())
    }
}
