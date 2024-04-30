use super::ProgressIndicator;
use anyhow::Result;
use indicatif::TermLike;
use std::panic::{RefUnwindSafe, UnwindSafe};

#[derive(Clone)]
pub struct QuietNoBar<TermT> {
    term: TermT,
}

impl<TermT> QuietNoBar<TermT> {
    pub fn new(term: TermT) -> Self {
        Self { term }
    }
}

impl<TermT> ProgressIndicator for QuietNoBar<TermT>
where
    TermT: TermLike + Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static,
{
    fn println(&self, _msg: String) {
        // quiet mode doesn't print anything
    }

    fn finished(&self) -> Result<()> {
        self.term.flush()?;
        Ok(())
    }
}
