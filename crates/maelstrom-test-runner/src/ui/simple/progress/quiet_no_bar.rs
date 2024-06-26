use super::{NullPrinter, PrintWidthCb, ProgressIndicator, Terminal};
use anyhow::Result;

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
    TermT: Terminal,
{
    type Printer<'a> = NullPrinter;

    fn lock_printing(&self) -> Self::Printer<'_> {
        // quiet mode doesn't print anything
        NullPrinter
    }

    fn finished(&self, summary: impl PrintWidthCb<Vec<String>>) -> Result<()> {
        for line in summary(self.term.width() as usize) {
            self.term.write_line(&line)?;
        }
        self.term.flush()?;
        Ok(())
    }
}
