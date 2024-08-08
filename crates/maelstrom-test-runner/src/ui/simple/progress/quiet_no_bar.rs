use super::{PrintWidthCb, ProgressIndicator, Terminal};
use anyhow::Result;

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
    fn println(&mut self, _msg: String) {}
    fn println_width(&mut self, _cb: impl PrintWidthCb<String>) {}

    fn finished(&mut self, summary: impl PrintWidthCb<Vec<String>>) -> Result<()> {
        for line in summary(self.term.width() as usize) {
            self.term.write_line(&line)?;
        }
        self.term.flush()?;
        Ok(())
    }
}
